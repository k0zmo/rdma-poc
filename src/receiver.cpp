#include "rdma_defs.h"
#include "rdma_types.h"
#include "getopt.h"

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

#include <chrono>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

struct AppOptions
{
    std::string _address{"172.19.41.49"};
    std::string _port{"8001"};
    std::string _providerName{"verbs"};
    std::string _localAddress{};
    int _numMessages{100};
};

bool quit = false;

enum class WaitResult
{
    GOT_MESSAGE,
    GOT_ERROR,
    SHUTDOWN,
    TIMEOUT
};

void handleConnected(RdmaEndpoint& in_endpoint, int maxMessages)
{
    static constexpr size_t BUFFER_SIZE = 5 * 1024 * 1024; // 5 MB
    static constexpr std::chrono::milliseconds RECEIVE_TIMEOUT = std::chrono::seconds{2};
    static constexpr std::uint64_t SEND_COMPLETION_FLAGS = FI_SEND | FI_MSG;
    static constexpr std::uint64_t RECV_COMPLETION_FLAGS = FI_RECV | FI_MSG;

    std::unique_ptr<char[]> buf = std::make_unique<char[]>(BUFFER_SIZE);
    std::unique_ptr<fid_mr> memoryRegion;
    int res = fi_mr_reg(in_endpoint._domain.get(), buf.get(), BUFFER_SIZE, FI_RECV, 0, 0, 0,
                        makeOutPointer(memoryRegion), nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_mr_reg", res};
    }

    int numMessagesReceived = 0;

    uint32_t event = 0;
    const auto cmEntrySize = in_endpoint.getMaxConnectionDataSize() + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> cmEntryBuffer = std::make_unique<uint8_t[]>(cmEntrySize);
    fi_eq_cm_entry* cmEntry = reinterpret_cast<fi_eq_cm_entry*>(cmEntryBuffer.get());

    while (true)
    {
        ssize_t ret = fi_recv(in_endpoint._endpoint.get(), buf.get(), BUFFER_SIZE, fi_mr_desc(memoryRegion.get()),
                              FI_ADDR_UNSPEC, nullptr);
        if (ret != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(ret)};
        }
        in_endpoint.sendEmptyMessage();

        WaitResult waitResult = WaitResult::TIMEOUT;
        bool recvCompleted = false, sendCompleted = false;
        std::chrono::milliseconds receiveTimeout = RECEIVE_TIMEOUT;
        std::size_t bytesTransferred = 0;
        
        while (receiveTimeout.count() > 0)
        {
            // Wait for both completions (send+recv) but no more than 2 seconds in total
            const auto waitingStart = std::chrono::steady_clock::now();
            fi_cq_msg_entry entry;
            ret = fi_cq_sread(in_endpoint._completionQueue.get(), &entry, 1, nullptr, static_cast<int>(receiveTimeout.count()));
            if (ret == 1)
            {
                if ((entry.flags & SEND_COMPLETION_FLAGS) == SEND_COMPLETION_FLAGS)
                {
                    sendCompleted = true;
                }
                else if ((entry.flags & RECV_COMPLETION_FLAGS) == RECV_COMPLETION_FLAGS)
                {
                    recvCompleted = true;
                    bytesTransferred = entry.len;
                }

                // Both send and receive were completed, we got the message
                if (sendCompleted && recvCompleted)
                {
                    waitResult = WaitResult::GOT_MESSAGE;
                    break;
                }
                else 
                {
                    // We receive first completion notification, adjust completion timeout for 2nd message
                    receiveTimeout -= std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - waitingStart);
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

        if (waitResult != WaitResult::GOT_MESSAGE)
        {
            if (waitResult == WaitResult::TIMEOUT)
            {
                std::cout << "Timeout receiving a message from sender\n";
            }
            quit = true;
            break;
        }

        numMessagesReceived += 1;
        std::cout << "  Got " << numMessagesReceived << " message from the sender: " << bytesTransferred << std::endl;

        if (maxMessages > 0 && numMessagesReceived >= maxMessages)
        {
            std::cout << "Sent " << maxMessages << ". Quitting\n";
            quit = true;
            break;
        }
    }

    fi_shutdown(in_endpoint._endpoint.get(), 0);
}

static bool isConnectionRefused(int in_fiErrorCode)
{
    if (in_fiErrorCode == FI_ECONNREFUSED)
    {
        return true;
    }
#ifdef _WIN32
    if (in_fiErrorCode == WSAECONNREFUSED)
    {
        return true;
    }
#endif 
    return false;
}

void run(const AppOptions& in_cfg)
{
    auto fabricInfo = getFabricInfo(in_cfg._providerName, in_cfg._address, in_cfg._port, in_cfg._localAddress);
    RdmaAdapter adapter{std::move(fabricInfo)};
    RdmaEndpoint ep{adapter};

    ClientConnectionFlowV1B clientData;
    clientData._wantsFrameMetadata = false;
    clientData._identifier = PROTOCOL_IDENTIFIER;
    // "e569f502-8891-4c9f-92d4-51702b158bd5";
    clientData._flowIdentifier[0] = 0xe5;
    clientData._flowIdentifier[1] = 0x69;
    clientData._flowIdentifier[2] = 0xf5;
    clientData._flowIdentifier[3] = 0x02;
    clientData._flowIdentifier[4] = 0x88;
    clientData._flowIdentifier[5] = 0x91;
    clientData._flowIdentifier[6] = 0x4c;
    clientData._flowIdentifier[7] = 0x9f;
    clientData._flowIdentifier[8] = 0x92;
    clientData._flowIdentifier[9] = 0xd4;
    clientData._flowIdentifier[10] = 0x51;
    clientData._flowIdentifier[11] = 0x70;
    clientData._flowIdentifier[12] = 0x2b;
    clientData._flowIdentifier[13] = 0x15;
    clientData._flowIdentifier[14] = 0x8b;
    clientData._flowIdentifier[15] = 0xd5;
    int connectRes = fi_connect(ep._endpoint.get(), adapter._fabricInfo->dest_addr, &clientData, sizeof(clientData));
    if (connectRes != 0)
    {
        throw rdma_error{"fi_connect", connectRes};
    }

    const auto maxEntrySize = ep.getMaxConnectionDataSize() + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> connectBuffer = std::make_unique<uint8_t[]>(maxEntrySize);
    fi_eq_cm_entry* entry = reinterpret_cast<fi_eq_cm_entry*>(connectBuffer.get());
    uint32_t event = 0;

    while (!quit)
    {
        const ssize_t res = fi_eq_sread(ep._eventQueue.get(), &event, entry, maxEntrySize, -1, 0);
        if (res < 0)
        {
            if (res == -FI_EAVAIL)
            {
                fi_eq_err_entry err{};
                fi_eq_readerr(ep._eventQueue.get(), &err, 0);
                if (isConnectionRefused(err.err))
                {
                    if (err.err_data_size > 0)
                    {
                        std::string_view errorMessage{(const char*)err.err_data, err.err_data_size};
                        std::cout << "Connection refused, reason: " << errorMessage << std::endl;
                    }
                    else
                    {
                        std::cout << "Connection refused, reason unknown." << std::endl;
                    }
                }
                else
                {
                    std::cout << "Error while trying to establish a connection: " << fi_strerror(err.err) << " (code: " << err.err << ")" << std::endl;
                }
            }
            else
            {
                std::cout << "Error calling fi_eq_sread(): " << fi_strerror((int)res) << " (code: " << res << ")" << std::endl;
            }
            break;
        }
        if (event == FI_SHUTDOWN)
        {
            std::cout << "Shutdown received - quitting.\n";
            break;
        }
        if (event != FI_CONNECTED || entry->fid != &ep._endpoint->fid)
        {
            std::cout << "Unexpected CM event: " << event << std::endl;
            continue;
        }
        std::unique_ptr<fi_info> entryRaii{entry->info};
        if (static_cast<size_t>(res) < sizeof(*entry))
        {
            std::cout << "Unexpected size of connection data: " << res << std::endl;
            continue;
        }

        const auto connectionDataSize = res - sizeof(*entry);
        std::cout << "Received extra bytes: " << connectionDataSize << std::endl;

        ServerConnectionFlowV1B serverData;
        memcpy(&serverData, entry->data, connectionDataSize);
        std::cout << "Connection data:"
                  << "\n  frameMetadataSize: " << serverData._frameMetadataSize
                  << "\n  frameSize: " << serverData._frameSize
                  << "\n  acceptConnectionTime: " << serverData._acceptConnectionTime
                  << "\n  hasActiveProducers: " << serverData._hasActiveProducers << std::endl;
        handleConnected(ep, in_cfg._numMessages);
    }
}

int main(int argc, char* argv[])
{
    AppOptions options;

    int opt;
    while ((opt = getopt(argc, argv, "a:B:p:n:I:")) != -1)
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
        case 'n':
            options._numMessages = std::atoi(optarg);
            break;
        case 'I':
            options._localAddress = optarg;
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
        run(options);
    }
    catch (const std::exception& ex)
    {
        std::cerr << "EXCEPTION on main thread: " << ex.what() << std::endl;
    }
}
