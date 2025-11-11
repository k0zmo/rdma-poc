#include "getopt.h"
#include "rdma_defs.h"
#include "rdma_types.h"

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include <rdma/fi_ext.h>
#include <rdma/providers/fi_log.h>
#include <rdma/providers/fi_prov.h>

#ifndef _WIN32
#  include <arpa/inet.h>
#  include <netdb.h>
#  include <netinet/in.h>
#  include <netinet/ip.h>
#  include <sys/socket.h>
#  include <sys/types.h>
#else
#  include <Ws2ipdef.h>
#  include <ws2tcpip.h>
#endif

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iostream>
#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

struct app_options
{
    std::string   address{};
    std::string   port{"8001"};
    std::string   provider_name{"verbs"};
    std::uint32_t frame_size{5 * 1024 * 1024}; // 5MB
    std::uint32_t max_chunk_size{std::numeric_limits<std::uint32_t>::max()};
    int           interval_ms{20};
    bool          verbose{false};
};

enum class wait_result
{
    SENT_MESSAGE,
    GOT_ERROR,
    SHUTDOWN,
    TIMEOUT
};

void handle_connection(rdma_endpoint& endpoint, const app_options& cfg, const client_connection_flow_v1c& client_data)
{
    static constexpr std::chrono::milliseconds ACCEPT_TIMEOUT        = std::chrono::seconds{2};
    static constexpr std::chrono::milliseconds INITIAL_RECV_TIMEOUT  = std::chrono::seconds{2};
    static constexpr std::chrono::milliseconds SEND_TIMEOUT          = std::chrono::seconds{2};
    static constexpr std::uint64_t             SEND_COMPLETION_FLAGS = FI_SEND | FI_MSG;
    static constexpr std::uint64_t             RECV_COMPLETION_FLAGS = FI_RECV | FI_MSG;

    const auto message_size = sizeof(frame_information) + cfg.frame_size + sizeof(frame_buffer_header);

    std::unique_ptr<char[]> buf = std::make_unique<char[]>(message_size);
    std::memset(buf.get(), 0, message_size);
    std::unique_ptr<fid_mr> memory_region;
    int res = fi_mr_reg(endpoint.domain_.get(),
                        buf.get(),
                        message_size,
                        FI_SEND,
                        0,
                        0,
                        0,
                        make_out_pointer(memory_region),
                        nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_mr_reg", res};
    }

    endpoint.receive_sync_message();

    const auto max_chunk_size = std::min(cfg.max_chunk_size, client_data.max_chunk_size);

    server_connection_flow_v1c server_data{};
    server_data.frame_size             = cfg.frame_size;
    server_data.accept_connection_time = FabricClock::now().time_since_epoch().count();
    server_data.has_active_producers   = true;
    server_data.frame_metadata_size    = 0;
    server_data.max_chunk_size         = max_chunk_size;
    res = fi_accept(endpoint.endpoint_.get(), &server_data, sizeof(server_data));
    if (res != 0)
    {
        throw rdma_error{"fi_accept", res};
    }

    std::cout << "Waiting on connected event";

    uint32_t   event;
    const auto cm_entry_size = endpoint.max_connection_data_size() + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> cm_entry_buffer = std::make_unique<uint8_t[]>(cm_entry_size);
    fi_eq_cm_entry*            cm_entry = reinterpret_cast<fi_eq_cm_entry*>(cm_entry_buffer.get());

    ssize_t ret = fi_eq_sread(endpoint.event_queue_.get(),
                              &event,
                              cm_entry,
                              cm_entry_size,
                              static_cast<int>(ACCEPT_TIMEOUT.count()),
                              0U);
    if (ret <= 0 || event != FI_CONNECTED)
    {
        std::cout << " - Failed to connect!\n";
        return;
    }
    std::cout << " - CONNECTED\n";

    // Wait for first signal-ready message
    fi_cq_data_entry entry;
    ret = fi_cq_sread(endpoint.completion_queue_.get(),
                      &entry,
                      1,
                      nullptr,
                      static_cast<int>(INITIAL_RECV_TIMEOUT.count()));
    if (ret <= 0 || (entry.flags & RECV_COMPLETION_FLAGS) != RECV_COMPLETION_FLAGS)
    {
        std::cout << "Didn't receive first signal-ready message\n";
        return;
    }

    int       num_message_sent = 0;
    uintptr_t client_id        = reinterpret_cast<uintptr_t>(&endpoint);

    frame_information* fi  = reinterpret_cast<frame_information*>(buf.get());
    fi->frame_index        = 10000 + num_message_sent;
    fi->buffer_usage_count = num_message_sent + 1;
    fi->flags              = FRAME_INFORMATION_FLAG_FLOW_HAS_ACTIVE_PRODUCERS;

    frame_buffer_header* fbh =
        reinterpret_cast<frame_buffer_header*>(buf.get() + sizeof(frame_information));
    fbh->usage_counter            = fi->buffer_usage_count;
    frame_buffer_header* fbh_tail = reinterpret_cast<frame_buffer_header*>(
        buf.get() + sizeof(frame_information) + cfg.frame_size);
    fbh_tail->usage_counter = fi->buffer_usage_count;

    using namespace std::chrono;
    auto next_time_point = steady_clock::now() + milliseconds{cfg.interval_ms};
    const unsigned num_chunks     = max_chunk_size == std::numeric_limits<uint32_t>::max()
                                        ? 1
                                        : (message_size + max_chunk_size - 1) / max_chunk_size;
    std::cout << "Number of chunks per message: " << num_chunks << std::endl;

    while (true)
    {
        endpoint.receive_sync_message();

        uint64_t       remaining_size = message_size;
        uint64_t       offset         = 0;
        while (remaining_size > 0)
        {
            const auto chunk_size = std::min<uint64_t>(remaining_size, max_chunk_size);
            ret = fi_send(endpoint.endpoint_.get(),
                          buf.get() + offset,
                          chunk_size,
                          fi_mr_desc(memory_region.get()),
                          FI_ADDR_UNSPEC,
                          nullptr);
            if (ret != 0)
            {
                throw rdma_error{"fi_send", static_cast<int>(ret)};
            }

            remaining_size -= chunk_size;
            offset += chunk_size;
        }

        wait_result  wr                  = wait_result::TIMEOUT;
        bool         next_recv_completed = false;
        unsigned     num_send_completed  = 0;
        milliseconds send_timeout = SEND_TIMEOUT;

        while (send_timeout.count() > 0)
        {
            const auto waiting_start = steady_clock::now();
            ret = fi_cq_sread(endpoint.completion_queue_.get(),
                              &entry,
                              1,
                              nullptr,
                              static_cast<int>(send_timeout.count()));
            if (ret == 1)
            {
                if ((entry.flags & SEND_COMPLETION_FLAGS) == SEND_COMPLETION_FLAGS)
                {
                    num_send_completed += 1;
                }
                else if ((entry.flags & RECV_COMPLETION_FLAGS) == RECV_COMPLETION_FLAGS)
                {
                    next_recv_completed = true;
                }

                // Both send and receive were completed, we sent the message and the client is ready
                // for the next message
                if (num_send_completed == num_chunks && next_recv_completed)
                {
                    wr = wait_result::SENT_MESSAGE;
                    break;
                }
                else
                {
                    // We receive first completion notification, adjust completion timeout for 2nd
                    // message
                    send_timeout -=
                        duration_cast<milliseconds>(steady_clock::now() - waiting_start);
                }
            }
            else if (ret == -FI_EAGAIN)
            {
                ret = fi_eq_read(endpoint.event_queue_.get(), &event, cm_entry, cm_entry_size, 10U);
                if (ret > 0 && event == FI_SHUTDOWN)
                {
                    std::cout << "Received SHUTDOWN from the peer\n";
                    wr = wait_result::SHUTDOWN;
                }
                else
                {
                    wr = wait_result::TIMEOUT;
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

                wr = wait_result::GOT_ERROR;
                break;
            }
        }

        if (wr != wait_result::SENT_MESSAGE)
        {
            if (wr == wait_result::TIMEOUT)
            {
                std::cout << client_id << ": Timeout sending a payload message\n";
            }
            break;
        }

        num_message_sent += 1;
        if (cfg.verbose)
        {
            std::cout << client_id << ": Message (" << num_message_sent << ", " << message_size
                      << " bytes) sent to client.\n";
        }

        fi->frame_index         = 10000 + num_message_sent;
        fi->buffer_usage_count  = num_message_sent + 1;
        fi->flags               = FRAME_INFORMATION_FLAG_FLOW_HAS_ACTIVE_PRODUCERS;
        fbh->usage_counter      = fi->buffer_usage_count;
        fbh_tail->usage_counter = fi->buffer_usage_count;

        std::this_thread::sleep_until(next_time_point);
        next_time_point = next_time_point + milliseconds{cfg.interval_ms};
    }

    fi_shutdown(endpoint.endpoint_.get(), 0U);
}

void run(const app_options& cfg)
{
    auto                    fabric_info = get_fabric_info(cfg.provider_name, cfg.address, cfg.port, false);
    rdma_adapter            adapter{std::move(fabric_info)};
    rdma_listening_endpoint listening_endpoint{adapter};

    const auto max_connection_data_size       = listening_endpoint.max_connection_data_size();
    const auto entry_max_size                 = max_connection_data_size + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> connect_buffer = std::make_unique<uint8_t[]>(entry_max_size);
    fi_eq_cm_entry*            entry = reinterpret_cast<fi_eq_cm_entry*>(connect_buffer.get());
    uint32_t                   event = 0;

    while (true)
    {
        const auto    eq  = listening_endpoint.event_queue_.get();
        const ssize_t res = fi_eq_sread(eq, &event, entry, entry_max_size, -1, 0);
        if (res == -FI_EAGAIN || res == -FI_EINTR)
        {
            continue;
        }

        if (res < 0)
        {
            std::string error_message;
            int         error_code = (int)res;
            if (res == -FI_EAVAIL)
            {
                fi_eq_err_entry err{};
                fi_eq_readerr(eq, &err, 0);
                error_code = err.err;
                if (err.err_data_size > 0)
                {
                    error_message.assign((const char*)err.err_data, err.err_data_size);
                }
            }

            std::cout << "Error calling fi_eq_sread(): " << fi_strerror(error_code)
                      << " (code: " << error_code << ")";
            if (!error_message.empty())
            {
                std::cout << ". Message: " << error_message;
            }
            std::cout << std::endl;
            continue;
        }
        if (event != FI_CONNREQ)
        {
            std::cout << "Unexpected event - " << event << std::endl;
            continue;
        }

        std::unique_ptr<fi_info> entry_raii{entry->info};
        if (static_cast<size_t>(res) < sizeof(*entry))
        {
            std::cout << "Unexpected size of connection data: " << res << std::endl;
            continue;
        }

        const auto  connection_data_size = res - sizeof(*entry);
        std::string inbound_addr;
        if (entry->info->dest_addrlen == INET_ADDRSTRLEN)
        {
            inbound_addr.resize(INET_ADDRSTRLEN);
            auto* sock_addr = reinterpret_cast<sockaddr_in*>(entry->info->dest_addr);
            inet_ntop(AF_INET, &sock_addr->sin_addr, inbound_addr.data(), inbound_addr.size());
        }
        else if (entry->info->dest_addrlen == INET6_ADDRSTRLEN)
        {
            inbound_addr.resize(INET6_ADDRSTRLEN);
            auto* sock_addr = reinterpret_cast<sockaddr_in6*>(entry->info->dest_addr);
            inet_ntop(AF_INET6, &sock_addr->sin6_addr, inbound_addr.data(), inbound_addr.size());
        }
        std::cout << "Connection inbound (" << inbound_addr
                  << "). Received extra bytes: " << connection_data_size << std::endl;

        std::stringstream error_message_stream;

        if (connection_data_size >= sizeof(client_connection))
        {
            client_connection client_connection_data;
            std::memcpy(&client_connection_data, entry->data, sizeof(client_connection));
            if (client_connection_data.identifier == PROTOCOL_IDENTIFIER)
            {
                if (connection_data_size >= sizeof(client_connection_flow_v1))
                {
                    client_connection_flow_v1c client_connection_v1{};
                    client_connection_v1.wants_frame_metadata = false;

                    std::memcpy(&client_connection_v1,
                                entry->data,
                                std::min(sizeof(client_connection_flow_v1c), connection_data_size));

                    char flow_id[32 + 4 + 1];
                    std::sprintf(
                        flow_id,
                        "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                        client_connection_v1.flow_id[0],
                        client_connection_v1.flow_id[1],
                        client_connection_v1.flow_id[2],
                        client_connection_v1.flow_id[3],
                        client_connection_v1.flow_id[4],
                        client_connection_v1.flow_id[5],
                        client_connection_v1.flow_id[6],
                        client_connection_v1.flow_id[7],
                        client_connection_v1.flow_id[8],
                        client_connection_v1.flow_id[9],
                        client_connection_v1.flow_id[10],
                        client_connection_v1.flow_id[11],
                        client_connection_v1.flow_id[12],
                        client_connection_v1.flow_id[13],
                        client_connection_v1.flow_id[14],
                        client_connection_v1.flow_id[15]);

                    std::cout << "Connection data:"
                              << "\n  Flow identifier: " << flow_id
                              << "\n  Wants metadata: " << std::boolalpha << client_connection_v1.wants_frame_metadata
                              << "\n  Chunk size: " << client_connection_v1.max_chunk_size
                              << std::endl;

                    const bool older_receiver = connection_data_size < sizeof(client_connection_flow_v1c);
                    if (older_receiver && cfg.max_chunk_size != std::numeric_limits<uint32_t>::max())
                    {
                        error_message_stream << "No max_chunk_size support, rejected";
                    }
                    else if (!std::strcmp(flow_id, "e569f502-8891-4c9f-92d4-51702b158bd5"))
                    {
                        try
                        {
                            std::thread th{
                                [ep = rdma_endpoint{adapter, *entry->info}, cfg, client_connection_v1]() mutable -> void {
                                    handle_connection(ep, cfg, client_connection_v1);
                                }};
                            th.detach();
                        }
                        catch (const std::exception& ex)
                        {
                            std::cout << "EXCEPTION when creating rdma_endpoint: " << ex.what()
                                      << std::endl;
                            error_message_stream << "EXCEPTION when creating rdma_endpoint";
                        }
                    }
                    else
                    {
                        error_message_stream << "Flow does not exist";
                    }
                }
                else
                {
                    error_message_stream
                        << "Received wrong private data size for FlowV1 connection (expected "
                        << sizeof(client_connection_flow_v1) << " but received "
                        << connection_data_size << ")";
                }
            }
            else
            {
                error_message_stream << "Unknown protocol identifier "
                                     << client_connection_data.identifier;
            }

            auto error_message = error_message_stream.str();
            if (!error_message.empty())
            {
                std::cout << "Connection rejected: " << error_message << std::endl;
                if (error_message.size() > max_connection_data_size - 1)
                {
                    error_message.resize(max_connection_data_size - 1);
                }
                fi_reject(listening_endpoint.passive_endpoint_.get(),
                          entry->info->handle,
                          error_message.c_str(),
                          error_message.size() + 1);
            }
        }
        else
        {
            std::cout << "GetConnectionData failed, connection data size = " << connection_data_size
                      << ". Connection will be rejected." << std::endl;
            fi_reject(listening_endpoint.passive_endpoint_.get(), entry->info->handle, nullptr, 0);
        }
    }
}

int main(int argc, char* argv[])
{
    app_options options;

    int opt;
    while ((opt = getopt(argc, argv, "a:B:p:s:t:c:v")) != -1)
    {
        switch (opt)
        {
        case 'a': options.address = optarg; break;
        case 'B': options.port = optarg; break;
        case 'p': options.provider_name = optarg; break;
        case 's': options.frame_size = (unsigned)std::atoi(optarg); break;
        case 't': options.interval_ms = std::atoi(optarg); break;
        case 'c': options.max_chunk_size = (unsigned)std::atoi(optarg); break;
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
        // Print current options
        std::cout << "Current options:" << std::endl;
        std::cout << "  Address: " << options.address << std::endl;
        std::cout << "  Port: " << options.port << std::endl;
        std::cout << "  Provider: " << options.provider_name << std::endl;
        std::cout << "  Frame size: " << options.frame_size << std::endl;
        std::cout << "  Max chunk size: " << options.max_chunk_size << std::endl;
        std::cout << "  Interval (ms): " << options.interval_ms << std::endl;
        std::cout << "  Verbose: " << std::boolalpha << options.verbose << std::endl;

        run(options);
    }
    catch (const std::exception& ex)
    {
        std::cerr << "EXCEPTION from main thread: " << ex.what() << std::endl;
    }
}
