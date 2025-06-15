#include "getopt.h"
#include "rdma_defs.h"
#include "rdma_types.h"

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#ifdef _WIN32
#  include <winerror.h>
#else
#  include <sys/types.h>
#endif

#include <signal.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

struct app_options
{
    std::string address{};
    std::string port{"8001"};
    std::string provider_name{"verbs"};
    std::string local_address{};
    std::string flow_id{};
    int         num_messages{100};
    int         sleep_time{0};
    bool        verbose{false};
};

bool              quit    = false;
std::atomic<bool> stopped = false;

void sighandler(int sig)
{
    (void)sig;
    quit    = true;
    stopped = true;
}

enum class wait_result
{
    GOT_MESSAGE,
    GOT_ERROR,
    SHUTDOWN,
    TIMEOUT
};

#ifdef _WIN32
#  define GET_LOCALTIME(tm, time) ::localtime_s(&tm, &time);
#else
#  define GET_LOCALTIME(tm, time) ::localtime_r(&time, &tm);
#endif

void handle_connected(rdma_endpoint& endpoint, std::uint32_t frame_size, const app_options& cfg)
{
    //  0-10
    // 11-20
    // 21-30 ...
    unsigned hist[17] = {};

    static constexpr std::chrono::milliseconds SEND_TIMEOUT    = std::chrono::seconds{2};
    static constexpr std::chrono::milliseconds RECEIVE_TIMEOUT = std::chrono::milliseconds{1500};
    static constexpr std::uint64_t             SEND_COMPLETION_FLAGS = FI_SEND | FI_MSG;
    static constexpr std::uint64_t             RECV_COMPLETION_FLAGS = FI_RECV | FI_MSG;

    const size_t message_size = sizeof(frame_information) +
                                frame_size + // contains FrameBufferHeader at the head
                                sizeof(frame_buffer_header);

    std::unique_ptr<char[]> buf = std::make_unique<char[]>(message_size);
    std::unique_ptr<fid_mr> memory_region;
    int res = fi_mr_reg(endpoint.domain_.get(),
                        buf.get(),
                        message_size,
                        FI_RECV,
                        0,
                        0,
                        0,
                        make_out_pointer(memory_region),
                        nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_mr_reg", res};
    }

    int num_message_received = 0;

    uint32_t   event         = 0;
    const auto cm_entry_size = endpoint.max_connection_data_size() + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> cm_entry_buffer = std::make_unique<uint8_t[]>(cm_entry_size);
    fi_eq_cm_entry*            cm_entry = reinterpret_cast<fi_eq_cm_entry*>(cm_entry_buffer.get());

    using namespace std::chrono;
    steady_clock::time_point before            = steady_clock::now();
    steady_clock::time_point acc_data_tp       = before;
    std::uint64_t            acc_data_received = 0;

    while (!stopped)
    {
        ssize_t ret = fi_recv(endpoint.endpoint_.get(),
                              buf.get(),
                              message_size,
                              fi_mr_desc(memory_region.get()),
                              FI_ADDR_UNSPEC,
                              nullptr);
        if (ret != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(ret)};
        }
        endpoint.send_empty_message();

        wait_result  wait_result    = wait_result::TIMEOUT;
        bool         recv_completed = false, send_completed = false;
        milliseconds completion_timeout      = SEND_TIMEOUT;
        uint32_t     send_completion_time_ms = 0, recv_completion_time_ms = 0;
        std::size_t  bytes_transferred = 0;

        while (true)
        {
            // Wait for both completions (send+recv) but no more than 2 seconds in total
            auto            waiting_start = steady_clock::now();
            fi_cq_msg_entry entry;
            ret = fi_cq_sread(endpoint.completion_queue_.get(),
                              &entry,
                              1,
                              nullptr,
                              static_cast<int>(completion_timeout.count()));
            if (ret == 1)
            {
                if ((entry.flags & SEND_COMPLETION_FLAGS) == SEND_COMPLETION_FLAGS)
                {
                    send_completed     = true;
                    completion_timeout = RECEIVE_TIMEOUT;
                    send_completion_time_ms =
                        duration_cast<milliseconds>(steady_clock::now() - waiting_start).count();
                    waiting_start = steady_clock::now();
                }
                else if ((entry.flags & RECV_COMPLETION_FLAGS) == RECV_COMPLETION_FLAGS)
                {
                    recv_completed    = true;
                    bytes_transferred = entry.len;
                    acc_data_received += bytes_transferred;
                    recv_completion_time_ms =
                        duration_cast<milliseconds>(steady_clock::now() - waiting_start).count();
                    waiting_start = steady_clock::now();
                }

                // Both send and receive were completed, we got the message
                if (send_completed && recv_completed)
                {
                    wait_result = wait_result::GOT_MESSAGE;
                    break;
                }
                else if (recv_completed) // receive completed first
                {
                    std::cout << "Receive completed before send?!\n";
                }
            }
            else if (ret == -FI_EAGAIN)
            {
                ret = fi_eq_read(endpoint.event_queue_.get(), &event, cm_entry, cm_entry_size, 10U);
                if (ret > 0 && event == FI_SHUTDOWN)
                {
                    std::cout << "Received SHUTDOWN from the peer\n";
                    wait_result = wait_result::SHUTDOWN;
                }
                else
                {
                    wait_result = wait_result::TIMEOUT;
                }
                break;
            }
            else
            {
                std::string error_message;
                int         error_code = (int)ret;
                if (ret == -FI_EAVAIL)
                {
                    fi_cq_err_entry err{};
                    fi_cq_readerr(endpoint.completion_queue_.get(), &err, 0);
                    error_code = err.err;
                    if (err.err_data_size > 0)
                    {
                        error_message.assign((const char*)err.err_data, err.err_data_size);
                    }
                }
                std::cout << "Error on CQ: " << fi_strerror(error_code) << " (code: " << error_code
                          << ')';
                if (!error_message.empty())
                {
                    std::cout << ". Message: " << error_message;
                }
                std::cout << std::endl;

                wait_result = wait_result::GOT_ERROR;
                break;
            }
        }

        if (wait_result != wait_result::GOT_MESSAGE)
        {
            if (wait_result == wait_result::TIMEOUT)
            {
                std::cout << "Timeout receiving a message from sender (recv_completed ="
                          << recv_completed << " (" << recv_completion_time_ms
                          << "), send_completed = " << send_completed << " ("
                          << send_completion_time_ms << ")"
                          << "\n";
            }
            quit = true;
            break;
        }

        num_message_received += 1;

        frame_information*   fi = reinterpret_cast<frame_information*>(buf.get());
        frame_buffer_header* fbh =
            reinterpret_cast<frame_buffer_header*>(buf.get() + sizeof(frame_information));
        frame_buffer_header* fbh_tail = reinterpret_cast<frame_buffer_header*>(
            buf.get() + sizeof(frame_information) + frame_size);

        const auto  tp      = system_clock::now();
        const auto  millis  = duration_cast<milliseconds>(tp.time_since_epoch()).count() % 1000LL;
        std::time_t time_tt = system_clock::to_time_t(tp);
        std::tm     t{};
        GET_LOCALTIME(t, time_tt)
        char buffer[256];
        auto len = strftime(buffer, sizeof(buffer), "%H:%M:%S", &t);
        len += std::sprintf(buffer + len, ".%03u", static_cast<unsigned>(millis));

        const auto now     = steady_clock::now();
        const auto diff    = now - before;
        before             = now;
        const auto diff_ms = duration_cast<milliseconds>(diff).count();

        if (diff_ms <= 10)
            ++hist[0];
        else if (diff_ms <= 20)
            ++hist[1];
        else if (diff_ms <= 20)
            ++hist[2];
        else if (diff_ms <= 30)
            ++hist[3];
        else if (diff_ms <= 40)
            ++hist[4];
        else if (diff_ms <= 50)
            ++hist[5];
        else if (diff_ms <= 60)
            ++hist[6];
        else if (diff_ms <= 70)
            ++hist[7];
        else if (diff_ms <= 80)
            ++hist[8];
        else if (diff_ms <= 90)
            ++hist[9];
        else if (diff_ms <= 100)
            ++hist[10];
        else if (diff_ms <= 110)
            ++hist[11];
        else if (diff_ms <= 120)
            ++hist[12];
        else if (diff_ms <= 130)
            ++hist[13];
        else if (diff_ms <= 140)
            ++hist[14];
        else if (diff_ms <= 150)
            ++hist[15];
        else
            ++hist[16];

        const bool invalid_message = fbh->usage_counter != fi->buffer_usage_count ||
                                     fbh_tail->usage_counter != fi->buffer_usage_count;

        if (cfg.verbose || invalid_message)
        {
            std::cout << buffer << "  Got " << num_message_received
                      << "th message: " << bytes_transferred;
            std::cout << ", frameIndex: " << fi->frame_index;
            std::cout << ", flags: " << fi->flags;
            std::cout << ", sendWait: " << send_completion_time_ms;
            std::cout << ", recvWait: " << recv_completion_time_ms;
            std::cout << ", diff: " << diff_ms;
            if (invalid_message)
            {
                std::cout << " (received message became invalid!)";
            }
            std::cout << std::endl;
        }

        if (now - acc_data_tp >= seconds{4})
        {
            const auto acc_diff = duration_cast<milliseconds>(now - acc_data_tp).count();
            const auto mbps     = (acc_data_received * 8ull) / acc_diff / 1000;
            std::cout << "Bandwidth: " << mbps * 0.001 << " Gbps\n";
            acc_data_received = 0;
            acc_data_tp       = now;
        }

        if (cfg.sleep_time > 0)
        {
            std::this_thread::sleep_for(milliseconds{cfg.sleep_time});
        }

        if (cfg.num_messages > 0 && num_message_received >= cfg.num_messages)
        {
            std::cout << "Sent " << cfg.num_messages << ". Quitting\n";
            quit = true;
            break;
        }
    }

    const auto now      = steady_clock::now();
    const auto acc_diff = duration_cast<milliseconds>(now - acc_data_tp).count();
    const auto mbps     = (acc_data_received * 8ull) / acc_diff / 1000;
    std::cout << "Bandwidth: " << mbps * 0.001 << " Gbps\n";
    std::cout << "Histogram:\n";
    for (unsigned i = 0u; i < std::size(hist); ++i)
    {
        std::cout << "[" << i * 10 + (i > 0 ? 1 : 0) << "-" << (i + 1) * 10 << "ms]: " << hist[i]
                  << "\n";
    }

    fi_shutdown(endpoint.endpoint_.get(), 0);
}

static bool is_connection_refused(int error_code)
{
    if (error_code == FI_ECONNREFUSED)
    {
        return true;
    }
#ifdef _WIN32
    if (error_code == WSAECONNREFUSED)
    {
        return true;
    }
#endif
    return false;
}

void run(const app_options& cfg)
{
    auto fabric_info = get_fabric_info(cfg.provider_name, cfg.address, cfg.port, cfg.local_address);
    rdma_adapter  adapter{std::move(fabric_info)};
    rdma_endpoint ep{adapter};

    client_connection_flow_v1b client_data;
    client_data.wants_frame_metadata = false;
    client_data.identifier           = PROTOCOL_IDENTIFIER;

    if (!cfg.flow_id.empty())
    {
        auto& fi = client_data.flow_id;
        std::memset(&client_data.flow_id, 0, sizeof(client_data.flow_id));
        (void)std::sscanf( // Dont bother validating it
            cfg.flow_id.c_str(),
            "%02hhx%02hhx%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx%02hhx%"
            "02hhx%02hhx%02hhx",
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
    else
    {
        // "e569f502-8891-4c9f-92d4-51702b158bd5";
        static constexpr uint8_t DEFAULT_FLOAT_ID[] = {0xe5,
                                                       0x69,
                                                       0xf5,
                                                       0x02,
                                                       0x88,
                                                       0x91,
                                                       0x4c,
                                                       0x9f,
                                                       0x92,
                                                       0xd4,
                                                       0x51,
                                                       0x70,
                                                       0x2b,
                                                       0x15,
                                                       0x8b,
                                                       0xd5};
        std::memcpy(&client_data.flow_id, DEFAULT_FLOAT_ID, sizeof(client_data.flow_id));
    }

    int connect_res = fi_connect(
        ep.endpoint_.get(), adapter.fabric_info_->dest_addr, &client_data, sizeof(client_data));
    if (connect_res != 0)
    {
        throw rdma_error{"fi_connect", connect_res};
    }

    const auto max_entry_size = ep.max_connection_data_size() + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> connect_buffer = std::make_unique<uint8_t[]>(max_entry_size);
    fi_eq_cm_entry*            entry = reinterpret_cast<fi_eq_cm_entry*>(connect_buffer.get());
    uint32_t                   event = 0;

    while (!quit)
    {
        const ssize_t res =
            fi_eq_sread(ep.event_queue_.get(), &event, entry, max_entry_size, 2000, 0);
        if (res < 0)
        {
            if (res == -FI_EAVAIL)
            {
                fi_eq_err_entry err{};
                fi_eq_readerr(ep.event_queue_.get(), &err, 0);
                if (is_connection_refused(err.err))
                {
                    if (err.err_data_size > 0)
                    {
                        std::string_view error_message{(const char*)err.err_data,
                                                       err.err_data_size};
                        std::cout << "Connection refused, reason: " << error_message << std::endl;
                    }
                    else
                    {
                        std::cout << "Connection refused, reason unknown." << std::endl;
                    }
                }
                else
                {
                    std::cout << "Error while trying to establish a connection: "
                              << fi_strerror(err.err) << " (code: " << err.err << ")" << std::endl;
                }
            }
            else
            {
                std::cout << "Error calling fi_eq_sread(): " << fi_strerror((int)res)
                          << " (code: " << res << ")" << std::endl;
            }
            break;
        }
        if (event == FI_SHUTDOWN)
        {
            std::cout << "Shutdown received - quitting.\n";
            break;
        }
        if (event != FI_CONNECTED || entry->fid != &ep.endpoint_->fid)
        {
            std::cout << "Unexpected CM event: " << event << std::endl;
            continue;
        }
        std::unique_ptr<fi_info> entry_raii{entry->info};
        if (static_cast<size_t>(res) < sizeof(*entry))
        {
            std::cout << "Unexpected size of connection data: " << res << std::endl;
            continue;
        }

        const auto connection_data_size = res - sizeof(*entry);
        std::cout << "Received extra bytes: " << connection_data_size << std::endl;

        server_connection_flow_v1b server_data;
        memcpy(&server_data, entry->data, connection_data_size);
        std::cout << "Connection data:"
                  << "\n  frameMetadataSize: " << server_data.frame_metadata_size
                  << "\n  frameSize: " << server_data.frame_size
                  << "\n  acceptConnectionTime: " << server_data.accept_connection_time
                  << "\n  hasActiveProducers: " << server_data.has_active_producers << std::endl;
        handle_connected(ep, server_data.frame_size, cfg);
    }
}

int main(int argc, char* argv[])
{
    signal(SIGINT, &sighandler);
#ifdef SIGBREAK
    signal(SIGBREAK, &sighandler);
#endif

    app_options options;

    int opt;
    while ((opt = getopt(argc, argv, "a:B:p:n:I:f:s:v")) != -1)
    {
        switch (opt)
        {
        case 'a': options.address = optarg; break;
        case 'B': options.port = optarg; break;
        case 'p': options.provider_name = optarg; break;
        case 'n': options.num_messages = std::atoi(optarg); break;
        case 'I': options.local_address = optarg; break;
        case 'f': options.flow_id = optarg; break;
        case 's': options.sleep_time = std::atoi(optarg); break;
        case 'v': options.verbose = true; break;
        case '?': std::cerr << "Unknown option: " << char(optopt) << std::endl; return 1;
        default:  return 1;
        }
    }

    if (options.address.empty())
    {
        std::cerr << "Address (-a) must be provided" << std::endl;
        return 1;
    }

    try
    {
        run(options);
    }
    catch (const std::exception& ex)
    {
        std::cerr << "EXCEPTION on main thread: " << ex.what() << std::endl;
    }
}
