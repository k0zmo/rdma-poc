#include "rdma_defs.h"
#include "rdma_types.h"
#include "getopt.h"

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
#  include <netinet/ip.h>
#  include <netinet/in.h>
#  include <sys/socket.h>
#  include <sys/types.h>
#else
#  include <Ws2ipdef.h>
#  include <ws2tcpip.h>
#endif

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <exception>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

struct AppOptions
{
    std::string _address{};
    std::string _port{"8001"};
    std::string _providerName{"verbs"};
    std::uint32_t _frameSize{5 * 1024 * 1024}; // 5MB
    int _intervalMs{20};
    bool _verbose{false};
};

enum class WaitResult
{
    SENT_MESSAGE,
    GOT_ERROR,
    SHUTDOWN,
    TIMEOUT
};

void handleConnection(RdmaEndpoint& in_endpoint, const AppOptions& in_cfg)
{
    static constexpr std::chrono::milliseconds ACCEPT_TIMEOUT = std::chrono::seconds{2};
    static constexpr std::chrono::milliseconds INITIAL_RECV_TIMEOUT = std::chrono::seconds{2};
    static constexpr std::chrono::milliseconds SEND_TIMEOUT = std::chrono::seconds{2};
    static constexpr std::uint64_t SEND_COMPLETION_FLAGS = FI_SEND | FI_MSG;
    static constexpr std::uint64_t RECV_COMPLETION_FLAGS = FI_RECV | FI_MSG;

    const auto messageSize = sizeof(FrameInformation) + in_cfg._frameSize + sizeof(FrameBufferHeader);

    std::unique_ptr<char[]> buf = std::make_unique<char[]>(messageSize);
    std::memset(buf.get(), 0, messageSize);
    std::unique_ptr<fid_mr> memoryRegion;
    int res = fi_mr_reg(in_endpoint._domain.get(), buf.get(), messageSize, FI_SEND, 0, 0, 0,
                        makeOutPointer(memoryRegion), nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_mr_reg", res};
    }

    in_endpoint.receiveEmptyMessage();

    ServerConnectionFlowV1B serverData{};
    serverData._frameSize = in_cfg._frameSize;
    serverData._acceptConnectionTime = 1111111;
    serverData._hasActiveProducers = true;
    serverData._frameMetadataSize = 0;
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

    FrameInformation* fi = reinterpret_cast<FrameInformation*>(buf.get());
    fi->_frameIndex = 10000 + numMessageSent;
    fi->_bufferUsageCount = numMessageSent + 1;
    fi->_flags = FRAME_INFORMATION_FLAG_FLOW_HAS_ACTIVE_PRODUCERS;

    FrameBufferHeader* fbh = reinterpret_cast<FrameBufferHeader*>(buf.get() + sizeof(FrameInformation));
    fbh->_usageCounter = fi->_bufferUsageCount;
    FrameBufferHeader* fbhTail = reinterpret_cast<FrameBufferHeader*>(buf.get() + sizeof(FrameInformation) + in_cfg._frameSize);
    fbhTail->_usageCounter = fi->_bufferUsageCount;

    using namespace std::chrono;
    auto nextTimePoint = steady_clock::now() + milliseconds{in_cfg._intervalMs};

    while (true)
    {
        in_endpoint.receiveEmptyMessage();

        ret = fi_send(in_endpoint._endpoint.get(), buf.get(), messageSize, fi_mr_desc(memoryRegion.get()),
                      FI_ADDR_UNSPEC, nullptr);
        if (ret != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(ret)};
        }

        WaitResult waitResult = WaitResult::TIMEOUT;
        bool nextRecvCompleted = false, sendCompleted = false;
        milliseconds sendTimeout = SEND_TIMEOUT;

        while (sendTimeout.count() > 0)
        {
            const auto waitingStart = steady_clock::now();
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

                // Both send and receive were completed, we sent the message and the client is ready for the next message
                if (sendCompleted && nextRecvCompleted)
                {
                    waitResult = WaitResult::SENT_MESSAGE;
                    break;
                }
                else
                {
                    // We receive first completion notification, adjust completion timeout for 2nd message
                    sendTimeout -= duration_cast<milliseconds>(steady_clock::now() - waitingStart);
                }
            }
            else if (ret == -FI_EAGAIN)
            {
                ret = fi_eq_read(in_endpoint._eventQueue.get(), &event, cmEntry, cmEntrySize, 10U);
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
                std::string errorMessage;
                int errorCode = (int)ret;
                if (ret == -FI_EAVAIL)
                {
                    fi_cq_err_entry err{};
                    fi_cq_readerr(in_endpoint._completionQueue.get(), &err, 0);
                    errorCode = err.err;
                    if (err.err_data_size > 0)
                    {
                        errorMessage.assign((const char*)err.err_data, err.err_data_size);
                    }
                }
                std::cout << "Error on CQ: " << fi_strerror(errorCode) <<  " (code: " << errorCode << ')';
                if (!errorMessage.empty())
                {
                    std::cout << ". Message: " << errorMessage;
                }
                std::cout << std::endl;

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
        if (in_cfg._verbose)
        {
            std::cout << clientId << ": Message (" << numMessageSent << ", " << messageSize << " bytes) sent to client.\n";
        }

        fi->_frameIndex = 10000 + numMessageSent;
        fi->_bufferUsageCount = numMessageSent + 1;
        fi->_flags = FRAME_INFORMATION_FLAG_FLOW_HAS_ACTIVE_PRODUCERS;
        fbh->_usageCounter = fi->_bufferUsageCount;
        fbhTail->_usageCounter = fi->_bufferUsageCount;

        std::this_thread::sleep_until(nextTimePoint);
        nextTimePoint = nextTimePoint + milliseconds{in_cfg._intervalMs};
    }

    fi_shutdown(in_endpoint._endpoint.get(), 0U);
}

void run(const AppOptions& in_cfg)
{
    auto fabricInfo = getFabricInfo(in_cfg._providerName, in_cfg._address, in_cfg._port);
    RdmaAdapter adapter{std::move(fabricInfo)};
    RdmaListeningEndpoint listeningEndpoint{adapter};

    const auto entryMaxSize = listeningEndpoint.getMaxConnectionDataSize() + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> connectBuffer = std::make_unique<uint8_t[]>(entryMaxSize);
    fi_eq_cm_entry* entry = reinterpret_cast<fi_eq_cm_entry*>(connectBuffer.get());
    uint32_t event = 0;

    while (true)
    {
        const auto eq = listeningEndpoint._eventQueue.get();
        const ssize_t res = fi_eq_sread(eq, &event, entry, entryMaxSize, -1, 0);
        if (res == -FI_EAGAIN || res == -FI_EINTR)
        {
            continue;
        }

        if (res < 0)
        {
            std::string errorMessage;
            int errorCode = (int)res;
            if (res == -FI_EAVAIL)
            {
                fi_eq_err_entry err{};
                fi_eq_readerr(eq, &err, 0);
                errorCode = err.err;
                if (err.err_data_size > 0)
                {
                    errorMessage.assign((const char*)err.err_data, err.err_data_size);
                }
            }

            std::cout << "Error calling fi_eq_sread(): " << fi_strerror(errorCode) << " (code: " << errorCode << ")";
            if (!errorMessage.empty())
            {
                std::cout << ". Message: " << errorMessage;
            }
            std::cout << std::endl;
            continue;
        }
        if (event != FI_CONNREQ)
        {
            std::cout << "Unexpected event - " << event << std::endl;
            continue;
        }

        std::unique_ptr<fi_info> entryRaii{entry->info};
        if (static_cast<size_t>(res) < sizeof(*entry))
        {
            std::cout << "Unexpected size of connection data: " << res << std::endl;
            continue;
        }

        const auto connectionDataSize = res - sizeof(*entry);
        std::string inboundAddr;
        if (entry->info->dest_addrlen == INET_ADDRSTRLEN)
        {
            inboundAddr.resize(INET_ADDRSTRLEN);
            auto* sockAddr = reinterpret_cast<sockaddr_in*>(entry->info->dest_addr);
            inet_ntop(AF_INET, &sockAddr->sin_addr, inboundAddr.data(), inboundAddr.size());
        }
        else if (entry->info->dest_addrlen == INET6_ADDRSTRLEN)
        {
            inboundAddr.resize(INET6_ADDRSTRLEN);
            auto* sockAddr = reinterpret_cast<sockaddr_in6*>(entry->info->dest_addr);
            inet_ntop(AF_INET6, &sockAddr->sin6_addr, inboundAddr.data(), inboundAddr.size());
        }
        std::cout << "Connection inbound (" << inboundAddr << "). Received extra bytes: " << connectionDataSize << std::endl;

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

                    char flowId[32 + 4 + 1];
                    std::sprintf(flowId, "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                        clientConnectionV1._flowIdentifier[0],
                        clientConnectionV1._flowIdentifier[1],
                        clientConnectionV1._flowIdentifier[2],
                        clientConnectionV1._flowIdentifier[3],
                        clientConnectionV1._flowIdentifier[4],
                        clientConnectionV1._flowIdentifier[5],
                        clientConnectionV1._flowIdentifier[6],
                        clientConnectionV1._flowIdentifier[7],
                        clientConnectionV1._flowIdentifier[8],
                        clientConnectionV1._flowIdentifier[9],
                        clientConnectionV1._flowIdentifier[10],
                        clientConnectionV1._flowIdentifier[11],
                        clientConnectionV1._flowIdentifier[12],
                        clientConnectionV1._flowIdentifier[13],
                        clientConnectionV1._flowIdentifier[14],
                        clientConnectionV1._flowIdentifier[15]);

                    std::cout << "Connection data:"
                              << "\n  Flow identifier: " << flowId
                              << "\n  Wants metadata: " << std::boolalpha << clientConnectionV1._wantsFrameMetadata << std::endl;

                    if (!std::strcmp(flowId, "e569f502-8891-4c9f-92d4-51702b158bd5"))
                    {
                        try
                        {
                            std::thread th{[ep = RdmaEndpoint{adapter, *entry->info}, in_cfg]() mutable -> void {
                                handleConnection(ep, in_cfg);
                            }};
                            th.detach();
                        }
                        catch (const std::exception& ex)
                        {
                            std::cout << "EXCEPTION when creating RdmaEndpoint: " << ex.what() << std::endl;
                            errorMessageStream << "EXCEPTION when creating RdmaEndpoint";
                        }
                    }
                    else
                    {
                        errorMessageStream << "Flow does not exist";
                    }
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
    while ((opt = getopt(argc, argv, "a:B:p:s:t:v")) != -1)
    {
        switch (opt)
        {
        case 'a':
            options._address = optarg;
            break;
        case 'B':
            options._port = optarg;
            break;
        case 'p':
            options._providerName = optarg;
            break;
        case 's':
            options._frameSize = (unsigned)std::atoi(optarg);
            break;
        case 't':
            options._intervalMs = std::atoi(optarg);
            break;
        case 'v':
            options._verbose = true;
            break;
        case '?':
            std::cerr << "Unknown option: " << char(optopt) << std::endl;
            return 1;
        default:
            return 1;
        }
    }

    if (options._address.empty())
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
        std::cerr << "EXCEPTION from main thread: " << ex.what() << std::endl;
    }
}
