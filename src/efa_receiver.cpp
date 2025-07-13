#include "getopt.h"
#include "rdma_defs.h"
#include "rdma_types.h"

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include <moodycamel/blockingconcurrentqueue.h>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/signal_set.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <memory>
#include <mutex>
#include <stdlib.h>
#include <string>
#include <system_error>
#include <thread>
#include <utility>
#include <vector>

struct app_options
{
    std::string        device_address{""};
    std::string        address{""};
    std::uint16_t      port{16002};
    std::string        provider_name{""};
    std::uint32_t      frame_size{1920 * 1080 * 8 / 3};
    std::string        flow_id{};
    efa::transmit_mode transmit_mode{efa::transmit_mode::SEND_RECV};
    int                num_messages{100};
    int                num_receivers{1};
    int                num_domains{1};
    bool               verbose{false};

    bool is_rdma() const { return transmit_mode == efa::transmit_mode::RDMA_WRITE; }
};

template <int NameVal>
class base_timeout_option
{
public:
    explicit base_timeout_option(std::chrono::milliseconds timeout)
    {
#if defined(_WIN32)
        timeout_ = (uint32_t)timeout.count();
#else
        timeout_.tv_sec = (long)(std::chrono::duration_cast<std::chrono::seconds>(timeout).count());
        timeout_.tv_usec = (long)(timeout.count() % 1000);
#endif
    }

    template <typename Protocol>
    int level(const Protocol&) const
    {
        return SOL_SOCKET;
    }

    template <typename Protocol>
    int name(const Protocol&) const
    {
        return NameVal;
    }

    template <typename Protocol>
    const void* data(const Protocol&) const
    {
        return &timeout_;
    }

    template <typename Protocol>
    std::size_t size(const Protocol&) const
    {
        return sizeof(timeout_);
    }

private:
#if defined(_WIN32)
    uint32_t timeout_;
#else
    struct timeval timeout_;
#endif
};

using send_timeout_option = base_timeout_option<SO_SNDTIMEO>;
using recv_timeout_option = base_timeout_option<SO_RCVTIMEO>;

class app : public std::enable_shared_from_this<app>
{
public:
    app(app_options options, asio::io_context& ctx, std::shared_ptr<efa_domain> domain) :
        options_{std::move(options)},
        ctx_{ctx},
        domain_{std::move(domain)}
    {
    }

    void start()
    {
        endpoint_ = std::make_shared<efa_endpoint>(domain_);
        progressable_ =
            std::make_shared<progressable>(*endpoint_->completion_queue_, completion_queue_);

        for (int i = 0; i < 2; ++i)
        {
            frames_[i].payload = std::make_unique<char[]>(options_.frame_size);

            int res = fi_mr_reg(domain_->domain_.get(),
                            frames_[i].payload.get(),
                            options_.frame_size,
                            FI_SEND,
                            0,
                            domain_->next_mr_key++,
                            0,
                            make_out_pointer(frames_[i].memory_region),
                            nullptr);
            if (res != 0)
            {
                throw rdma_error{"fi_mr_reg", res};
            }

            if (!options_.is_rdma())
            {
                ssize_t result = fi_recv(endpoint_->endpoint_.get(),
                                         frames_[i].payload.get(),
                                         options_.frame_size,
                                         fi_mr_desc(frames_[i].memory_region.get()),
                                         FI_ADDR_UNSPEC,
                                         nullptr);
                if (result != 0)
                {
                    throw rdma_error{"fi_recv", static_cast<int>(result)};
                }
            }
        }

        asio::ip::tcp::endpoint sender_ep{asio::ip::make_address(options_.address), options_.port};
        asio::ip::tcp::socket   socket{ctx_, asio::ip::tcp::v4()};
        socket.set_option(send_timeout_option{std::chrono::milliseconds(1000)});
        socket.set_option(recv_timeout_option{std::chrono::milliseconds(1000)});
        socket.set_option(asio::ip::tcp::no_delay(true));

        socket.connect(sender_ep);

        if (options_.is_rdma())
        {
            std::unique_ptr<char[]> message = std::make_unique<char[]>(sizeof(efa::client_connect_rdma_v1) +
                                                                        2 * sizeof(efa::mr_info));
            efa::client_connect_rdma_v1* connect_message = new (message.get()) efa::client_connect_rdma_v1;
            connect_message->num_mr      = 2;
            for (auto i = 0U; i < connect_message->num_mr; ++i)
            {
                *connect_message->get_mr(i) =
                    efa::mr_info{/*.address = */(std::uint64_t)frames_[i].payload.get(),
                                 /*.size    = */options_.frame_size,
                                 /*.rkey    = */frames_[i].memory_region->key};
            }
            initialize_connect_message(*connect_message);

            efa::control_message_header send_header;
            send_header.type   = efa::control_message_type::CLIENT_CONNECT_RDMA_V1;
            send_header.length = sizeof(*connect_message) + connect_message->num_mr * sizeof(efa::mr_info);

            std::array<asio::const_buffer, 2> send_buffs = {
                asio::buffer(&send_header, sizeof(send_header)),
                asio::buffer(&connect_message, send_header.length)};
            asio::write(socket, send_buffs);

            connect_message->~client_connect_rdma_v1(); // Call destructor to clean up
        }
        else
        {
            efa::client_connect_v1 connect_message;
            initialize_connect_message(connect_message);

            efa::control_message_header send_header;
            send_header.type   = efa::control_message_type::CLIENT_CONNECT_V1;
            send_header.length = sizeof(connect_message);

            std::array<asio::const_buffer, 2> send_buffs = {
                asio::buffer(&send_header, sizeof(send_header)),
                asio::buffer(&connect_message, send_header.length)};
            asio::write(socket, send_buffs);
        }

        efa::control_message_header receive_header;
        size_t n = asio::read(socket, asio::buffer(&receive_header, sizeof(receive_header)));
        if (n != sizeof(efa::control_message_header))
        {
            throw std::runtime_error{"Failed to read response header"};
        }

        if (receive_header.protocol_version != PROTOCOL_VERSION)
        {
            throw std::runtime_error{"Invalid protocol version"};
        }
        if (receive_header.type != efa::control_message_type::SERVER_REJECT_V1 &&
            receive_header.type != efa::control_message_type::SERVER_ACCEPT_V1)
        {
            throw std::runtime_error{
                "Invalid control message type, expected SERVER_REJECT or SERVER_ACCEPT"};
        }

        if (receive_header.type == efa::control_message_type::SERVER_REJECT_V1)
        {
            assert(receive_header.length == sizeof(efa::server_reject_v1));
            efa::server_reject_v1 reject_message;
            n = asio::read(socket, asio::buffer(&reject_message, sizeof(efa::server_reject_v1)));
            if (n != sizeof(efa::server_reject_v1))
            {
                throw std::runtime_error{"Failed to read server_reject_v1 payload"};
            }
            throw std::runtime_error{"Connection rejected: " +
                                     std::string(reject_message.error_message)};
        }
        else if (receive_header.type == efa::control_message_type::SERVER_ACCEPT_V1)
        {
            assert(receive_header.length == sizeof(efa::server_accept_v1));
            efa::server_accept_v1 accept_message;
            n = asio::read(socket, asio::buffer(&accept_message, sizeof(efa::server_accept_v1)));
            if (n != sizeof(efa::server_accept_v1))
            {
                throw std::runtime_error{"Failed to read accept_message payload"};
            }

            LOG_DEBUG("Connection accepted:\n  accept time: %lu\n  frame size: %u\n  metadata "
                      "size: %u\n  has active producers: %u",
                      accept_message.accept_connection_time,
                      accept_message.frame_size,
                      accept_message.frame_metadata_size,
                      accept_message.has_active_producers);

            if (accept_message.frame_size != options_.frame_size)
            {
                efa::client_shutdown_v1 shutdown_message;
                efa::control_message_header send_header;
                send_header.length = sizeof(shutdown_message);
                send_header.type   = efa::control_message_type::CLIENT_SHUTDOWN_V1;
                std::array<asio::const_buffer, 2> send_buffs = {
                    asio::buffer(&send_header, sizeof(send_header)),
                    asio::buffer(&shutdown_message, send_header.length)};
                asio::write(socket, send_buffs);
                socket.close();
                throw std::runtime_error{"Invalid frame size"};
            }
        }

        domain_->execution_ctx_->add_progressable(progressable_);
        domain_->execution_ctx_->post_work(progressable_, 2);

        try
        {
            receive_loop();
        }
        catch (...)
        {
            domain_->execution_ctx_->remove_progressable(progressable_);
            throw;
        }

        LOG_DEBUG("Sending shutdown");

        efa::client_shutdown_v1 shutdown_message;
        efa::control_message_header send_header;
        send_header.length = sizeof(shutdown_message);
        send_header.type   = efa::control_message_type::CLIENT_SHUTDOWN_V1;
        std::array<asio::const_buffer, 2> send_buffs = {
            asio::buffer(&send_header, sizeof(send_header)),
            asio::buffer(&shutdown_message, send_header.length)};
        asio::write(socket, send_buffs);
        socket.close();

        domain_->execution_ctx_->remove_progressable(progressable_);

        endpoint_.reset();

        for (auto& frame : frames_)
        {
            frame.memory_region.reset();
            frame.payload.reset();
        }
    }

    void stop()
    {
        stopped_ = true;
        completion_queue_.enqueue(completion_entry{0xDEAD, 0, 0});
    }

private:
    template <typename ConnectMessage>
    void initialize_connect_message(ConnectMessage& connect_message)
    {
        char   addr_bytes[128] = {};
        size_t addr_len        = sizeof(addr_bytes);
        int    res             = fi_getname(to_fid(endpoint_->endpoint_), addr_bytes, &addr_len);
        if (res != 0)
        {
            throw rdma_error{"fi_getname", res};
        }
        char source_addr_bytes[128];

        if (options_.device_address.empty())
        {
            std::memcpy(source_addr_bytes, addr_bytes, addr_len);
        }
        else
        {
            if (!parse_fabric_address(options_.device_address,
                                      domain_->fabric_->fabric_info_->addr_format,
                                      source_addr_bytes,
                                      sizeof(source_addr_bytes)))
            {
                throw rdma_error{"parse_fabric_address", -FI_EINVAL};
            }
        }

        connect_message.address_format = (uint16_t)domain_->fabric_->fabric_info_->addr_format;
        connect_message.address_length = (uint16_t)addr_len;
        if (addr_len > sizeof(connect_message.dest_address_bytes))
        {
            throw std::runtime_error{"Fabric address length is too big for V1 protocol"};
        }
        memcpy(&connect_message.dest_address_bytes, addr_bytes, addr_len);
        memcpy(&connect_message.source_address_bytes, source_addr_bytes, addr_len);
        connect_message.wants_frame_metadata = false;

        if (!options_.flow_id.empty())
        {
            auto& fi = connect_message.flow_id;
            (void)std::sscanf(options_.flow_id.c_str(),
                              "%02hhx%02hhx%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%"
                              "02hhx%02hhx%02hhx%02hhx%02hhx%02hhx",
                              &fi[0],
                              &fi[1],
                              &fi[2],
                              &fi[3],
                              &fi[4],
                              &fi[5],
                              &fi[6],
                              &fi[7],
                              &fi[8],
                              &fi[9],
                              &fi[10],
                              &fi[11],
                              &fi[12],
                              &fi[13],
                              &fi[14],
                              &fi[15]);
        }
    }

    void receive_loop()
    {
        unsigned frame_to_repost      = 1;
        unsigned num_message_received = 0;

        while (!stopped_)
        {
            completion_entry entry;
            const bool       got_entry = completion_queue_.wait_dequeue_timed(entry, 1'000 * 2000);
            if (!got_entry)
            {
                LOG_DEBUG("Timeout waiting for completion");
                break;
            }
            if (entry.error_code == 0xDEAD)
            {
                break;
            }

            ssize_t result = fi_recv(endpoint_->endpoint_.get(),
                                     frames_[frame_to_repost].payload.get(),
                                     options_.frame_size,
                                     fi_mr_desc(frames_[frame_to_repost].memory_region.get()),
                                     FI_ADDR_UNSPEC,
                                     nullptr);
            if (result != 0)
            {
                throw rdma_error{"fi_recv", static_cast<int>(result)};
            }
            domain_->execution_ctx_->post_work(progressable_, 1);
            frame_to_repost = 1 - frame_to_repost;

            ++num_message_received;
            if (options_.verbose)
            {
                LOG_DEBUG("Received: %u (%zu bytes)", num_message_received, entry.len);
            }
            if (options_.num_messages > 0 &&
                num_message_received >= (unsigned)options_.num_messages)
            {
                LOG_DEBUG("Received %u messages, stopping", num_message_received);
                break;
            }
        }
    }

private:
    const app_options                 options_;
    asio::io_context&                 ctx_;
    const std::shared_ptr<efa_domain> domain_;

    struct completion_entry
    {
        int      error_code;
        uint64_t flags;
        size_t   len;
    };

    moodycamel::BlockingConcurrentQueue<completion_entry> completion_queue_;

    struct progressable : efa_progressable
    {
        progressable(fid_cq& cq, moodycamel::BlockingConcurrentQueue<completion_entry>& queue) :
            completion_queue_(cq),
            completion_entry_queue_(queue)
        {
        }

        fid_cq&                                                completion_queue_;
        moodycamel::BlockingConcurrentQueue<completion_entry>& completion_entry_queue_;

        void on_completion(uint64_t flags, size_t length) noexcept override
        {
            completion_entry_queue_.enqueue(completion_entry{0, flags, length});
        }

        void on_error(int error_code) noexcept override
        {
            completion_entry_queue_.enqueue(completion_entry{error_code, 0, 0});
        }

        fid_cq& completion_queue() const override { return completion_queue_; }
    };

    struct frame
    {
        std::unique_ptr<fid_mr> memory_region;
        std::unique_ptr<char[]> payload;
    };

    frame frames_[2];

    std::shared_ptr<efa_endpoint> endpoint_;
    std::shared_ptr<progressable> progressable_;

    std::atomic<bool> stopped_ = false;
};

int main(int argc, char* argv[])
{
    setenv("FI_UNIVERSE_SIZE", "4", 1);
    setenv("FI_EFA_ENABLE_SHM_TRANSFER", "0", 1);

    app_options options;

    int opt;
    while ((opt = getopt(argc, argv, "d:a:B:p:n:r:f:s:N:t:v")) != -1)
    {
        switch (opt)
        {
        case 'd': options.device_address = optarg; break;
        case 'a': options.address = optarg; break;
        case 'B': options.port = (uint16_t)std::atoi(optarg); break;
        case 'p': options.provider_name = optarg; break;
        case 'n': options.num_messages = std::atoi(optarg); break;
        case 'r': options.num_receivers = std::atoi(optarg); break;
        case 'f': options.flow_id = optarg; break;
        case 's': options.frame_size = std::atoi(optarg); break;
        case 'N': options.num_domains = std::atoi(optarg); break;
        case 't': options.transmit_mode = efa::transmit_mode_from_string(optarg); break;
        case 'v': options.verbose = true; break;
        case '?': std::fprintf(stderr, "Unknown option: %c\n", opt); return 1;
        default:  return 1;
        }
    }

    if (options.address.empty())
    {
        std::fprintf(stderr, "Address (-a) is required\n");
        return 1;
    }

    try
    {
        asio::io_context ctx;

        struct bundle
        {
            std::unique_ptr<app> app_ptr;
            std::thread          thread;
        };

        std::vector<std::unique_ptr<bundle>> bundles;

        asio::signal_set signals{ctx};
        signals.add(SIGINT);
#if defined(SIGBREAK)
        signals.add(SIBREAK);
#endif
        signals.async_wait([&](const std::error_code&, int) {
            for (auto& b : bundles)
            {
                b->app_ptr->stop();
            }
        });

        std::vector<int> finished(options.num_receivers, 0);
        std::mutex       finished_mutex;

        std::shared_ptr<fi_info> hints = create_fabric_info_hints_rdm(options.provider_name);
        std::shared_ptr<fi_info> fabric_info;
        int                      res = fi_getinfo(
            FABRIC_VERSION, nullptr, nullptr, 0U, hints.get(), make_out_pointer(fabric_info));
        if (res != 0 || !fabric_info)
        {
            throw rdma_error{"fi_getinfo", res};
        }
        LOG_DEBUG("Provider: %s", fabric_info->fabric_attr->prov_name);
        LOG_DEBUG("Fabric address: %s", get_fabric_local_address_as_string(*fabric_info).c_str());

        std::shared_ptr<efa_fabric> fabric = std::make_shared<efa_fabric>(std::move(fabric_info));
        std::vector<std::shared_ptr<efa_domain>> domains;
        for (int i = 0; i < options.num_domains; ++i)
        {
            domains.push_back(std::make_shared<efa_domain>(fabric));
        }

        for (int i = 0, d = 0; i < options.num_receivers; ++i)
        {
            auto bundle_ptr = std::make_unique<bundle>();
            LOG_DEBUG("CREATING APP [%d] with DOMAIN [%d]", i, d);
            bundle_ptr->app_ptr = std::make_unique<app>(options, ctx, domains[d]);
            d                   = (d + 1) % options.num_domains;
            bundle_ptr->thread  = std::thread{
                [self = bundle_ptr->app_ptr.get(), i, &finished_mutex, &finished, &ctx]() mutable {
                    try
                    {
                        self->start();
                    }
                    catch (const std::exception& ex)
                    {
                        LOG_DEBUG("EXCEPTION on receiver[%d]: %s", i, ex.what());
                    }

                    {
                        std::lock_guard<std::mutex> lock{finished_mutex};
                        finished[i] = 1;
                        if (std::all_of(
                                finished.begin(), finished.end(), [](int f) { return f == 1; }))
                        {
                            LOG_DEBUG("All receivers finished, stopping context");
                            ctx.stop();
                        }
                    }
                }};
            bundles.push_back(std::move(bundle_ptr));
        }

        ctx.run();

        for (auto& b : bundles)
        {
            b->thread.join();
        }
    }
    catch (const std::exception& ex)
    {
        std::fprintf(stderr, "EXCEPTION on main thread: %s\n", ex.what());
    }
}
