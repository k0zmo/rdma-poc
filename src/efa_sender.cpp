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
#include <asio/completion_condition.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/read.hpp>
#include <asio/signal_set.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <exception>
#include <memory>
#include <stdlib.h>
#include <string>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

struct app_options
{
    std::uint16_t port{16002};
    std::string   provider_name{""};
    std::uint32_t frame_size{1920 * 1080 * 8 / 3};
    int           num_domains{1};
    int           interval_ms{20};
    bool          verbose{false};
};

static std::string get_address_as_string(fid_av& av, const void* addr)
{
    char        buf[128]{};
    size_t      len = sizeof(buf);
    const char* res = fi_av_straddr(&av, addr, buf, &len);
    return res ? std::string{buf, len} : std::string{};
}

template <typename T>
size_t copy_from_buffer(asio::const_buffer& buf, T& value)
{
    static_assert(std::is_trivially_copy_assignable_v<T>);
    std::memcpy(&value, buf.data(), sizeof(T));
    buf += sizeof(T);
    return sizeof(T);
}

template <typename T>
std::shared_ptr<std::vector<uint8_t>>
serialize_control_message(const efa::control_message_header& header, const T& message)
{
    static_assert(std::is_trivially_copy_assignable_v<T>);
    const auto size = sizeof(header) + sizeof(message);
    auto       buf  = std::make_shared<std::vector<uint8_t>>(size);
    memcpy(buf->data(), &header, sizeof(header));
    memcpy(buf->data() + sizeof(header), &message, sizeof(message));
    return buf;
}

asio::const_buffers_1 get_as_buffer(const std::vector<uint8_t>& buf)
{
    return asio::buffer(buf.data(), buf.size());
}

class peer final : public std::enable_shared_from_this<peer>
{
    static constexpr size_t max_message_buffer_size = 64 * 1024;

public:
    peer(asio::io_context&           ctx,
         asio::ip::tcp::socket       socket,
         std::shared_ptr<efa_domain> domain,
         app_options                 options) :
        ctx_{ctx},
        socket_{std::move(socket)},
        options_{std::move(options)},
        domain_{std::move(domain)}
    {
    }

    ~peer()
    {
        stopped_ = true;
        if (sending_thread_.joinable())
        {
            sending_thread_.join();
        }
    }

    void start()
    {
        asio::async_read(socket_,
                         dynamic_buf_.prepare(1024),
                         asio::transfer_at_least(sizeof(efa::control_message_header) +
                                                 sizeof(efa::client_connect_v1)),
                         [self = shared_from_this()](const auto& error_code, size_t bytes_xfer) {
                             self->on_client_request_received(error_code, bytes_xfer);
                         });
    }

    void stop()
    {
        stopped_ = true;
        socket_.cancel();
        completion_queue_.enqueue({0xDEAD, 0, 0});
        if (sending_thread_.joinable())
        {
            sending_thread_.join();
        }
    }

private:
    void reject_connection(const std::string& message)
    {
        efa::server_reject_v1       reject_message;
        efa::control_message_header header;
        header.type   = efa::control_message_type::SERVER_REJECT_V1;
        header.length = sizeof(reject_message);

        if (message.size() > sizeof(reject_message.error_message) - 1)
        {
            std::string truncated_message{message};
            truncated_message.resize(sizeof(reject_message.error_message) - 1);
            strncpy(
                reject_message.error_message, truncated_message.c_str(), truncated_message.size());
        }
        else
        {
            strncpy(reject_message.error_message, message.c_str(), message.size());
        }

        auto       send_buffer = serialize_control_message(header, reject_message);
        const auto asio_buf    = get_as_buffer(*send_buffer);

        asio::async_write(socket_,
                          asio_buf,
                          [self = shared_from_this(), send_buffer = std::move(send_buffer)](
                              const std::error_code&, size_t) { return; });
    }

    void accept_connection()
    {
        efa::server_accept_v1       accept_message;
        efa::control_message_header header;
        header.type                           = efa::control_message_type::SERVER_ACCEPT_V1;
        header.length                         = sizeof(accept_message);
        accept_message.accept_connection_time = 1111111;
        accept_message.frame_size             = options_.frame_size;
        accept_message.frame_metadata_size    = 0;
        accept_message.has_active_producers   = true;

        auto       send_buffer = serialize_control_message(header, accept_message);
        const auto asio_buf    = get_as_buffer(*send_buffer);

        asio::async_write(socket_,
                          asio_buf,
                          [self = shared_from_this(),
                           buf  = std::move(send_buffer)](const std::error_code&, size_t) {});
    }

    bool handle_client_connect(size_t message_size, asio::const_buffer payload_buf)
    {
        if (accepted_)
        {
            reject_connection("client_connect_v1 message already handled");
            stop();
            return false;
        }

        if (message_size != sizeof(efa::client_connect_v1))
        {
            reject_connection("client_connect_v1 message size differs");
            return false;
        }

        efa::client_connect_v1 client_connect;
        const auto             consumed = copy_from_buffer(payload_buf, client_connect);
        dynamic_buf_.consume(consumed);

        if (client_connect.address_format != domain_->fabric_->fabric_info_->addr_format)
        {
            std::stringstream ss;
            ss << "Invalid address format, must be "
               << (uint32_t)domain_->fabric_->fabric_info_->addr_format;
            reject_connection(ss.str());
            return false;
        }
        if (client_connect.address_length > sizeof(client_connect.dest_address_bytes))
        {
            std::stringstream ss;
            ss << "Invalid address length, can't be greater than "
               << sizeof(client_connect.dest_address_bytes);
            reject_connection(ss.str());
            return false;
        }

        accept_connection();

        sending_thread_ = std::thread{[self = shared_from_this(), client_connect]() mutable {
            try
            {
                self->create_efa_endpoint(client_connect);
                self->create_data();
                self->send_loop();

                auto& ctx = self->ctx_;
                asio::post(ctx, [self = std::move(self)]() { self->stop(); });
            }
            catch (const std::exception& ex)
            {
                LOG_DEBUG("EXCEPTION: %s", ex.what());
                auto& ctx = self->ctx_;
                asio::post(ctx, [self = std::move(self)]() { self->stop(); });
                return;
            }
        }};

        return message_size;
    }

    bool handle_client_shutdown(size_t message_size, asio::const_buffer payload_buf)
    {
        LOG_DEBUG("Received shutdown request");
        (void)message_size;
        (void)payload_buf;
        stop();
        return false;
    }

    void on_client_request_received(const std::error_code& error_code, size_t bytes_xfer)
    {
        if (stopped_)
        {
            return;
        }

        if (error_code)
        {
            LOG_DEBUG("Error reading client requiest: %s", error_code.message().c_str());
            stop();
            return;
        }

        assert(bytes_xfer >= sizeof(efa::control_message_header));

        dynamic_buf_.commit(bytes_xfer);
        asio::const_buffer buf              = dynamic_buf_.data();
        const auto         num_bytes_in_buf = buf.size();

        efa::control_message_header header;
        copy_from_buffer(buf, header);

        if (num_bytes_in_buf < header.length + sizeof(efa::control_message_header))
        {
            const auto bytes_to_read = std::min<size_t>(
                max_message_buffer_size,
                header.length + sizeof(efa::control_message_header) - num_bytes_in_buf);
            asio::async_read(
                socket_,
                dynamic_buf_.prepare(max_message_buffer_size),
                asio::transfer_at_least(bytes_to_read),
                [self = shared_from_this()](const auto& error_code, size_t bytes_xfer) {
                    self->on_client_request_received(error_code, bytes_xfer);
                });
            return;
        }

        dynamic_buf_.consume(sizeof(efa::control_message_header));

        switch (header.type)
        {
        case efa::control_message_type::CLIENT_CONNECT_V1:
            if (!handle_client_connect(header.length, buf))
            {
                return;
            }
            break;
        case efa::control_message_type::CLIENT_SHUTDOWN_V1:
            handle_client_shutdown(header.length, buf);
            break;
        case efa::control_message_type::SERVER_ACCEPT_V1:
        case efa::control_message_type::SERVER_REJECT_V1:
            LOG_DEBUG("Invalid control message: %hu, closing connection", (uint16_t)header.type);
            return;
        default:
            LOG_DEBUG("Unsupported control message: %hu, skipping", (uint16_t)header.type);
            break;
        }

        asio::async_read(socket_,
                         dynamic_buf_.prepare(max_message_buffer_size),
                         asio::transfer_at_least(sizeof(efa::control_message_header)),
                         [self = shared_from_this()](const auto& error_code, size_t bytes_xfer) {
                             self->on_client_request_received(error_code, bytes_xfer);
                         });
    }

    void create_efa_endpoint(const efa::client_connect_v1& client_connect)
    {
        endpoint_ = std::make_shared<efa_endpoint>(domain_);
        progressable_ =
            std::make_shared<progressable>(*endpoint_->completion_queue_, completion_queue_);

        auto res = fi_av_insert(endpoint_->address_vector_.get(),
                                client_connect.dest_address_bytes,
                                1,
                                &addr_vector_,
                                0U,
                                nullptr);
        if (res != 1)
        {
            LOG_DEBUG("fi_av_insert: %d", res);
        }

        const auto peer_address =
            get_address_as_string(*endpoint_->address_vector_, client_connect.dest_address_bytes);
        LOG_DEBUG("Peer address: %s", peer_address.c_str());

        char   addr_buf[128]{};
        size_t addr_len = sizeof(addr_buf);
        res             = fi_getname(to_fid(endpoint_->endpoint_), addr_buf, &addr_len);
        if (res == 0)
        {
            const auto local_address = get_address_as_string(*endpoint_->address_vector_, addr_buf);
            LOG_DEBUG("Local address: %s", local_address.c_str());
        }
    }

    void create_data()
    {
        message_       = std::make_unique<char[]>(options_.frame_size);
        const auto res = fi_mr_reg(domain_->domain_.get(),
                                   message_.get(),
                                   options_.frame_size,
                                   FI_SEND,
                                   0,
                                   domain_->next_mr_key++,
                                   0,
                                   make_out_pointer(memory_region_),
                                   nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_mr_reg", res};
        }
    }

    void send_loop()
    {
        using namespace std::chrono;
        auto     next_time_point  = steady_clock::now() + milliseconds{options_.interval_ms};
        unsigned num_message_sent = 0;

        unsigned attempts = 0;
        domain_->execution_ctx_->add_progressable(progressable_);

        while (!stopped_)
        {
            ssize_t result = fi_send(endpoint_->endpoint_.get(),
                                     message_.get(),
                                     options_.frame_size,
                                     fi_mr_desc(memory_region_.get()),
                                     addr_vector_,
                                     nullptr);
            if (result == 0)
            {
                domain_->execution_ctx_->post_work(progressable_, 1);
                attempts = 0;
            }
            else
            {
                LOG_DEBUG("fi_send result: %zd", result);
            }

            if (result == -FI_EAGAIN)
            {
                attempts++;
                if (attempts > 100)
                {
                    LOG_DEBUG("fi_send: EAGAIN");
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
                continue;
            }
            else if (result < 0)
            {
                break;
            }

            completion_entry cqe;
            const bool       got_entry = completion_queue_.wait_dequeue_timed(cqe, 1'000 * 2000);
            if (!got_entry)
            {
                LOG_DEBUG("Timeout waiting for completion");
                break;
            }
            if (cqe.error_code == 0xDEAD)
            {
                LOG_DEBUG("Received EOS entry");
                break;
            }
            else if (cqe.error_code != 0)
            {
                LOG_DEBUG("Error on send: %s (%d)", fi_strerror(cqe.error_code), cqe.error_code);
                break;
            }
            else if (options_.verbose)
            {
                ++num_message_sent;
                LOG_DEBUG(
                    "Sent completed: %u [f=%lu l=%zu]", num_message_sent, cqe.flags, cqe.length);
            }

            if (options_.interval_ms > 0)
            {
                std::this_thread::sleep_until(next_time_point);
                next_time_point = next_time_point + milliseconds{options_.interval_ms};
            }
        }

        domain_->execution_ctx_->remove_progressable(progressable_);
    }

private:
    asio::io_context&     ctx_;
    asio::ip::tcp::socket socket_;
    const app_options     options_;

    asio::streambuf dynamic_buf_;

    std::shared_ptr<efa_domain> domain_;

    fi_addr_t addr_vector_;

    struct completion_entry
    {
        int      error_code;
        uint64_t flags;
        size_t   length;
    };

    moodycamel::BlockingConcurrentQueue<completion_entry> completion_queue_;

    struct progressable : efa_progressable
    {
        progressable(fid_cq& cq, moodycamel::BlockingConcurrentQueue<completion_entry>& queue) :
            cq_(cq),
            completion_queue_(queue)
        {
        }

        fid_cq&                                                cq_;
        moodycamel::BlockingConcurrentQueue<completion_entry>& completion_queue_;

        void on_completion(uint64_t flags, size_t length) noexcept override
        {
            completion_queue_.enqueue(completion_entry{0, flags, length});
        }

        void on_error(int error_code) noexcept override
        {
            completion_queue_.enqueue(completion_entry{error_code, 0, 0});
        }

        fid_cq& completion_queue() const override { return cq_; }
    };

    std::unique_ptr<char[]>       message_;
    std::unique_ptr<fid_mr>       memory_region_;
    std::shared_ptr<efa_endpoint> endpoint_;
    std::shared_ptr<progressable> progressable_;
    std::thread                   sending_thread_;

    bool accepted_ = false;

    static std::atomic<uint64_t> key_;
    std::atomic<bool>            stopped_ = false;
};

std::atomic<uint64_t> peer::key_{1};

class app : public std::enable_shared_from_this<app>
{
public:
    app(app_options options, asio::io_context& ctx) :
        options_{std::move(options)},
        ctx_{ctx},
        acceptor_{ctx_}
    {
    }

    void start()
    {
        std::shared_ptr<fi_info> hints = create_fabric_info_hints_rdm(options_.provider_name);
        std::shared_ptr<fi_info> fabric_info;
        int                      res = fi_getinfo(
            FABRIC_VERSION, nullptr, nullptr, 0U, hints.get(), make_out_pointer(fabric_info));
        if (res != 0 || !fabric_info)
        {
            throw rdma_error{"fi_getinfo", res};
        }
        LOG_DEBUG("Provider: %s", fabric_info->fabric_attr->prov_name);
        LOG_DEBUG("Fabric address: %s", get_fabric_local_address_as_string(*fabric_info).c_str());

        fabric_ = std::make_shared<efa_fabric>(std::move(fabric_info));
        domains_.reserve(options_.num_domains);
        for (int i = 0; i < options_.num_domains; ++i)
        {
            domains_.push_back(std::make_shared<efa_domain>(fabric_));
        }

        acceptor_.open(asio::ip::tcp::v4());
        acceptor_.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        acceptor_.bind({asio::ip::tcp::v4(), options_.port});
        acceptor_.listen();
        accept_next();
    }

    void stop()
    {
        stopped_ = true;
        acceptor_.cancel();
        for (auto& peer_ptr : peers_)
        {
            if (auto p = peer_ptr.lock())
            {
                p->stop();
            }
        }
    }

private:
    void accept_next()
    {
        acceptor_.async_accept([this](std::error_code ec, asio::ip::tcp::socket socket) {
            if (stopped_)
            {
                return;
            }

            if (!ec)
            {
                auto peer_ptr = std::make_shared<peer>(
                    ctx_, std::move(socket), domains_[current_domain_], options_);
                current_domain_ = (current_domain_ + 1) % options_.num_domains;
                peers_.push_back(peer_ptr);
                peer_ptr->start();
            }

            accept_next();
        });
    }

private:
    const app_options                        options_;
    asio::io_context&                        ctx_;
    asio::ip::tcp::acceptor                  acceptor_;
    std::vector<std::weak_ptr<peer>>         peers_;
    std::shared_ptr<efa_fabric>              fabric_;
    std::vector<std::shared_ptr<efa_domain>> domains_;
    int                                      current_domain_ = 0;
    bool                                     stopped_        = false;
};

int main(int argc, char* argv[])
{
    setenv("FI_UNIVERSE_SIZE", "4", 1);
    setenv("FI_EFA_ENABLE_SHM_TRANSFER", "0", 1);

    app_options options;

    int opt;
    while ((opt = getopt(argc, argv, "B:p:vs:N:t:")) != -1)
    {
        switch (opt)
        {
        case 'B': options.port = (uint16_t)std::atoi(optarg); break;
        case 'p': options.provider_name = optarg; break;
        case 'v': options.verbose = true; break;
        case 's': options.frame_size = (unsigned)std::atoi(optarg); break;
        case 't': options.interval_ms = std::atoi(optarg); break;
        case 'N': options.num_domains = std::atoi(optarg); break;
        case '?': std::fprintf(stderr, "Unknown option: %c\n", opt); return 1;
        default:  return 1;
        }
    }

    try
    {
        asio::io_context ctx;
        app              app_instance{options, ctx};

        asio::signal_set signals{ctx};
        signals.add(SIGINT);
#if defined(SIGBREAK)
        signals.add(SIBREAK);
#endif
        signals.async_wait([&](const std::error_code&, int) { app_instance.stop(); });

        app_instance.start();
        ctx.run();
    }
    catch (const std::exception& ex)
    {
        std::fprintf(stderr, "EXCEPTION on main thread: %s\n", ex.what());
    }
}
