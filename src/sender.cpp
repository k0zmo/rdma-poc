#include "rdma_defs.h"
#include "rdma_types.h"
#include "getopt.h"

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#ifndef _WIN32
#  include <arpa/inet.h>
#  include <netdb.h>
#  include <netinet/ip.h>
#  include <sys/socket.h>
#  include <sys/types.h>
#endif

#include <chrono>
#include <cstdint>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <utility>

struct AppOptions
{
    std::string address{"192.168.110.8"};
    std::string port{"8001"};
    std::string providerName{"verbs"};
};

enum class WaitResult
{
    SENT_MESSAGE,
    GOT_ERROR,
    SHUTDOWN,
    TIMEOUT
};

void handleConnection(RdmaEndpoint& in_endpoint)
{
    static constexpr size_t BUFFER_SIZE = 5 * 1024 * 1024; // 5 MB
    static constexpr std::chrono::milliseconds ACCEPT_TIMEOUT = std::chrono::seconds{2};
    static constexpr std::chrono::milliseconds INITIAL_RECV_TIMEOUT = std::chrono::seconds{2};
    static constexpr std::chrono::milliseconds SEND_TIMEOUT = std::chrono::seconds{2};
    static constexpr std::uint64_t SEND_COMPLETION_FLAGS = FI_SEND | FI_MSG;
    static constexpr std::uint64_t RECV_COMPLETION_FLAGS = FI_RECV | FI_MSG;

    std::unique_ptr<char[]> buf = std::make_unique<char[]>(BUFFER_SIZE);
    std::memset(buf.get(), 0, BUFFER_SIZE);
    std::unique_ptr<fid_mr> memoryRegion;
    int res = fi_mr_reg(in_endpoint._domain.get(), buf.get(), BUFFER_SIZE, FI_SEND, 0, 0, 0,
                        makeOutPointer(memoryRegion), nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_mr_reg", res};
    }

    in_endpoint.receiveEmptyMessage();

    ServerConnectionFlowV1B serverData{};
    serverData._frameSize = 1024;
    serverData._acceptConnectionTime = 1111111;
    serverData._hasActiveProducers = true;
    serverData._frameMetadataSize = 16;
    res = fi_accept(in_endpoint._endpoint.get(), &serverData, sizeof(serverData));
    if (res != 0)
    {
        throw rdma_error{"fi_accept", res};
    }

    std::cout << "Waiting on connected event";

    uint32_t event;
    const auto cmEntrySize = in_endpoint.getMaxConnectionDataSize() + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> cmEntryBuffer = std::make_unique<uint8_t[]>(cmEntrySize);
    fi_eq_cm_entry* cmEntry = reinterpret_cast<fi_eq_cm_entry*>(cmEntryBuffer.get());

    ssize_t ret = fi_eq_sread(in_endpoint._eventQueue.get(), &event, cmEntry, cmEntrySize,
                              static_cast<int>(ACCEPT_TIMEOUT.count()), 0U);
    if (ret <= 0 || event != FI_CONNECTED)
    {
        std::cout << " - Failed to connect!\n";
        return;
    }
    std::cout << " - CONNECTED\n";

    // Wait for first signal-ready message
    fi_cq_msg_entry entry;
    ret = fi_cq_sread(in_endpoint._completionQueue.get(), &entry, 1, 
                      nullptr, static_cast<int>(INITIAL_RECV_TIMEOUT.count()));
    if (ret <= 0 || (entry.flags & RECV_COMPLETION_FLAGS) != RECV_COMPLETION_FLAGS)
    {
        std::cout << "Didn't receive first signal-ready message\n";
        return;
    }

    int numMessageSent = 0;
    uintptr_t clientId = reinterpret_cast<uintptr_t>(&in_endpoint);

    while (true)
    {
        // Simulate some working being done
        std::this_thread::sleep_for(std::chrono::milliseconds{20});

        in_endpoint.receiveEmptyMessage();

        ret = fi_send(in_endpoint._endpoint.get(), buf.get(), BUFFER_SIZE, fi_mr_desc(memoryRegion.get()),
                      FI_ADDR_UNSPEC, nullptr);
        if (ret != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(ret)};
        }

        WaitResult waitResult = WaitResult::TIMEOUT;
        bool nextRecvCompleted = false, sendCompleted = false;
        std::chrono::milliseconds sendTimeout = SEND_TIMEOUT;
        
        while (sendTimeout.count() > 0)
        {
            // Check event queue first to check if the peer didn't disconnected on us
            ret = fi_eq_read(in_endpoint._eventQueue.get(), &event, cmEntry, cmEntrySize, 0U);
            if (ret > 0)
            {
                if (event == FI_SHUTDOWN)
                {
                    std::cout << "Received SHUTDOWN from the peer\n";
                    waitResult = WaitResult::SHUTDOWN;
                    break;
                }
            }
            else if (ret != -FI_EAGAIN && ret != -FI_EINTR)
            {
                std::cout << "Error on EQ: " << fi_strerror(ret) << "\n";
                waitResult = WaitResult::GOT_ERROR;
                break;
            }

            const auto waitingStart = std::chrono::steady_clock::now();
            fi_cq_msg_entry entry;
            ret = fi_cq_sread(in_endpoint._completionQueue.get(), &entry, 1, nullptr, static_cast<int>(sendTimeout.count()));
            if (ret == 1)
            {
                if ((entry.flags & SEND_COMPLETION_FLAGS) == SEND_COMPLETION_FLAGS)
                {
                    sendCompleted = true;
                }
                else if ((entry.flags & RECV_COMPLETION_FLAGS) == RECV_COMPLETION_FLAGS)
                {
                    nextRecvCompleted = true;
                }

                // Both send and receive were completed, we send the message and the client is ready for the next message
                if (sendCompleted && nextRecvCompleted)
                {
                    waitResult = WaitResult::SENT_MESSAGE;
                    break;
                }
                else
                {
                    // We receive first completion notification, adjust completion timeout for 2nd message
                    sendTimeout -= std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - waitingStart);
                }
            }
            else if (ret == -FI_EAGAIN)
            {
            ret = fi_eq_read(in_endpoint._eventQueue.get(), &event, cmEntry, cmEntrySize, 0U);
                if (ret > 0 && event == FI_SHUTDOWN)
                {
                    std::cout << "Received SHUTDOWN from the peer\n";
                    waitResult = WaitResult::SHUTDOWN;
                }
                else
                {
                    waitResult = WaitResult::TIMEOUT;
                }
                break;
            }
            else
            {
                if (ret == -FI_EAVAIL)
            {
                fi_cq_err_entry err{};
                fi_cq_readerr(in_endpoint._completionQueue.get(), &err, 0);
                if (err.err_data_size > 0)
                {
                    std::string errorMessage{(const char*)err.err_data, err.err_data_size};
                    std::cout << "Error on CQ: " << errorMessage << std::endl;
                }
                else
                {
                    std::cout << "Error on CQ ?!" << std::endl;
                }
                }
                else
                {
                    std::cout << "Error on CQ: " << fi_strerror(static_cast<int>(ret)) <<  " (" << ret << ')' << std::endl;
                }
                waitResult = WaitResult::GOT_ERROR;
                break;
            }
        }

        if (waitResult != WaitResult::SENT_MESSAGE)
        {
            if (waitResult == WaitResult::TIMEOUT)
            {
                std::cout << clientId << ": Timeout sending a payload message\n";
            }
            break;
        }

        numMessageSent += 1;
        std::cout << clientId << ": Message (" << numMessageSent << ", " << BUFFER_SIZE << " bytes) sent to client.\n";
        std::memset(buf.get(), numMessageSent % 256, BUFFER_SIZE);
    }

    fi_shutdown(in_endpoint._endpoint.get(), 0U);
}

void run(const AppOptions& in_cfg)
{
    auto fabricInfo = getFabricInfo(in_cfg.providerName, in_cfg.address, in_cfg.port, true);
    RdmaAdapter adapter{std::move(fabricInfo)};
    RdmaListeningEndpoint listeningEndpoint{adapter};

    const auto entryMaxSize = listeningEndpoint.getMaxConnectionDataSize() + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> connectBuffer = std::make_unique<uint8_t[]>(entryMaxSize);
    fi_eq_cm_entry* entry = reinterpret_cast<fi_eq_cm_entry*>(connectBuffer.get());
    uint32_t event = 0;

    while (true)
    {
        const auto eq = listeningEndpoint._eventQueue.get();
        std::cout << "  Calling fi_eq_sread()\n";
        const ssize_t bytesRead = fi_eq_sread(eq, &event, entry, entryMaxSize, -1, 0);
        std::cout << "  fi_eq_sread: " << bytesRead << ", event: " << event << std::endl;

        if (bytesRead == -FI_EAGAIN || bytesRead == -FI_EINTR)
        {
            continue;
        }
    
        if (bytesRead < 0)
        {
            if (bytesRead == -FI_EAVAIL)
            {
                fi_eq_err_entry err{};
                fi_eq_readerr(eq, &err, 0);
                if (err.err_data_size > 0)
                {
                    std::string errorMessage{(const char*)err.err_data, err.err_data_size};
                    std::cout << "Error calling fi_eq_sread(): " << errorMessage << std::endl;
                }
                else
                {
                    std::cout << "Error calling fi_eq_sread(), unknown reason" << bytesRead << std::endl;
                }
            }
            else
            {
                std::cout << "Error calling fi_eq_sread(): " << bytesRead << std::endl;
            }
            continue;
        }
        if (event != FI_CONNREQ)
        {
            std::cout << "EQ: Unexpected event - " << event << std::endl;
            continue;
        }

        std::unique_ptr<fi_info> entryRaii{entry->info};
        if (static_cast<size_t>(bytesRead) < sizeof(*entry))
        {
            std::cout << "EQ: Unexpected size of connection data: " << bytesRead << std::endl;
            continue;
        }

        const auto connectionDataSize = bytesRead - sizeof(*entry);
        std::cout << "EQ: Received extra bytes: " << connectionDataSize << std::endl;

        std::stringstream errorMessageStream;

        if (connectionDataSize >= sizeof(ClientConnection))
        {
            ClientConnection clientConnectionData;
            std::memcpy(&clientConnectionData, entry->data, sizeof(ClientConnection));
            if (clientConnectionData._identifier == PROTOCOL_IDENTIFIER)
            {
                if (connectionDataSize >= sizeof(ClientConnectionFlowV1))
                {
                    ClientConnectionFlowV1B clientConnectionV1{};
                    if (connectionDataSize >= sizeof(ClientConnectionFlowV1B))
                    {
                        std::memcpy(&clientConnectionV1, entry->data, sizeof(ClientConnectionFlowV1B));
                    }
                    else
                    {
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
                        std::memcpy(&clientConnectionV1, entry->data, sizeof(ClientConnectionFlowV1));
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
                    }

                    std::stringstream ss;
                    ss << std::hex << +clientConnectionV1._flowIdentifier[0];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[1];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[2];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[3];
                    ss << '-';
                    ss << std::hex << +clientConnectionV1._flowIdentifier[4];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[5];
                    ss << '-';
                    ss << std::hex << +clientConnectionV1._flowIdentifier[6];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[7];
                    ss << '-';
                    ss << std::hex << +clientConnectionV1._flowIdentifier[8];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[9];
                    ss << '-';
                    ss << std::hex << +clientConnectionV1._flowIdentifier[10];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[11];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[12];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[13];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[14];
                    ss << std::hex << +clientConnectionV1._flowIdentifier[15];

                    std::cout << "Connection data:"
                              << "\nFlow identifier: " << ss.str()
                              << "\nWants metadata: " << std::boolalpha << clientConnectionV1._wantsFrameMetadata << std::endl;

                    std::thread th{[ep = RdmaEndpoint{adapter, *entry->info}]() mutable -> void {
                        handleConnection(ep);
                    }};
                    th.detach();
                }
                else
                {
                    errorMessageStream << "Received wrong private data size for FlowV1 connection (expected "
                                        << sizeof(ClientConnectionFlowV1) << " but received " << connectionDataSize
                                        << ")";
                }
            }
            else
            {
                errorMessageStream << "Unknown protocol identifier " << clientConnectionData._identifier;
            }

            auto errorMessage = errorMessageStream.str();
            if (!errorMessage.empty())
            {
                std::cout << "Connection rejected: " << errorMessage << std::endl;
                if (errorMessage.size() > entryMaxSize - 1)
                {
                    errorMessage.resize(entryMaxSize - 1);
                }
                fi_reject(listeningEndpoint._passiveEndpoint.get(), entry->info->handle,
                            errorMessage.c_str(), errorMessage.size() + 1);               
            }
        }
        else
        {
            std::cout << "GetConnectionData failed, connectionDataSize = " << connectionDataSize
                      << ". Connection will be rejected." << std::endl;
            fi_reject(listeningEndpoint._passiveEndpoint.get(), entry->info->handle, nullptr, 0);
        }
    }
}

int main(int argc, char* argv[])
{
    AppOptions options;

    int opt;
    while ((opt = getopt(argc, argv, "a:B:p:")) != -1)
    {
        switch (opt)
        {
        case 'a':
            options.address = optarg;
            break;
        case 'B':
            options.port = optarg;
            break;
        case 'p':
            options.providerName = optarg;
            break;
        case '?':
            std::cerr << "Unknown option: " << char(optopt) << std::endl;
            return 1;
        default:
            return 1;
        }
    }

    try
    {
        std::stringstream ss;
        std::unique_ptr<fi_info> fabricInfo;
        int res = fi_getinfo(FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION), nullptr, nullptr, FI_PROV_ATTR_ONLY,
                             nullptr, makeOutPointer(fabricInfo));
        if (res != 0)
        {
            throw rdma_error{"fi_getinfo", res};
        }
        for (auto fi = fabricInfo.get(); fi; fi = fi->next)
        {
            if (!std::string_view{fi->fabric_attr->prov_name}.rfind("ofi_hook_", std::string::npos))
                continue;
            if (ss.tellp() > 0)
                ss << " ";
            ss << fi->fabric_attr->prov_name;
        }
        std::cout << "Compiled providers: " << ss.rdbuf() << std::endl;
        
        run(options);
    }
    catch (const std::exception& ex)
    {
        std::cerr << "Unhandled exception: " << ex.what() << std::endl;
    }
}
