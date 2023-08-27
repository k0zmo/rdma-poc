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
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

struct AppOptions
{
    std::string address{"192.168.110.8"};
    std::string port{"8001"};
    std::string providerName{"verbs"};
};

bool quit = false;

enum class WaitResult
{
    GOT_MESSAGE,
    GOT_ERROR,
    SHUTDOWN,
    TIMEOUT
};

void handleConnected(RdmaEndpoint& in_endpoint)
{
    constexpr auto bufSize = 5 * 1024 * 1024; // 5 MB
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(bufSize);
    std::unique_ptr<fid_mr> memoryRegion;
    int res = fi_mr_reg(in_endpoint._domain.get(), buf.get(), bufSize, FI_RECV, 0, 0, 0,
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
        //std::cin.get();

        std::cout << "Signaling to sender we're ready to receive new message\n";
        ssize_t ret = fi_recv(in_endpoint._endpoint.get(), buf.get(), bufSize, fi_mr_desc(memoryRegion.get()),
                              FI_ADDR_UNSPEC, nullptr);
        if (ret != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(ret)};
        }
        in_endpoint.sendEmptyMessage();

        const auto waitingStart = std::chrono::steady_clock::now();
        WaitResult waitResult = WaitResult::TIMEOUT;
        
        fi_cq_msg_entry entry;
        while(std::chrono::steady_clock::now() - waitingStart < std::chrono::seconds{5})
        {
            // Or after fi_cq_read?
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

            ret = fi_cq_read(in_endpoint._completionQueue.get(), &entry, 1);
            if (ret != -FI_EAGAIN)
            {
                //std::cout << "CQ: got " << entry.flags << std::endl;
                if ((entry.flags & (FI_RECV | FI_MSG)) == (FI_RECV | FI_MSG))
                {
                    waitResult = WaitResult::GOT_MESSAGE;
                    break;
                }
            }
            else if (ret == -FI_EAVAIL)
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

        std::cout << "  Got " << numMessagesReceived << " message from the sender: " << entry.len << std::endl;

        if (++numMessagesReceived > 100)
        {
            fi_shutdown(in_endpoint._endpoint.get(), 0);
            quit = true;
            break;
        }
    }

    //crashes on vrb_ep_close?!
    //fi_shutdown(in_endpoint._endpoint.get(), 0);
}

void run(const AppOptions& in_cfg)
{
    auto fabricInfo = getFabricInfo(in_cfg.providerName, in_cfg.address, in_cfg.port, false);
    RdmaAdapter adapter{std::move(fabricInfo)};

    // Start of receiver specific
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
    clientData._flowIdentifier[15] = 0xd4;
    int res = fi_connect(ep._endpoint.get(), adapter._fabricInfo->dest_addr, &clientData, sizeof(clientData));
    if (res != 0)
    {
        throw rdma_error{"fi_connect", res};
    }

    const auto maxEntrySize = ep.getMaxConnectionDataSize() + sizeof(fi_eq_cm_entry);
    std::unique_ptr<uint8_t[]> connectBuffer = std::make_unique<uint8_t[]>(maxEntrySize);
    fi_eq_cm_entry* entry = reinterpret_cast<fi_eq_cm_entry*>(connectBuffer.get());
    uint32_t event = 0;

    while (!quit)
    {
        const ssize_t rd = fi_eq_sread(ep._eventQueue.get(), &event, entry, maxEntrySize, -1, 0);
        if (rd < 0)
        {
            if (rd == -FI_EAVAIL)
            {
                fi_eq_err_entry err{};
                fi_eq_readerr(ep._eventQueue.get(), &err, 0);
                if (err.err == FI_ECONNREFUSED
#ifdef _WIN32
                 || err.err == WSAECONNREFUSED
#endif
                )
                {
                    if (err.err_data_size > 0)
                    {
                        std::string errorMessage{(const char*)err.err_data, err.err_data_size};
                        std::cout << "Connection refused, reason: " << errorMessage << std::endl;
                    }
                    else
                    {
                        std::cout << "Connection refused, unknown reason." << std::endl;
                    }
                }
                else
                {
                    std::cout << "Error while trying to establish a connection: " << fi_strerror(err.err) << std::endl;
                }
            }
            std::cout << "Error calling fi_eq_sread(): " << rd << std::endl;
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
        if (static_cast<size_t>(rd) < sizeof(*entry))
        {
            std::cout << "Unexpected size of connection data: " << rd << std::endl;
            continue;
        }

        const auto connectionDataSize = rd - sizeof(*entry);
        std::cout << "Received extra bytes: " << connectionDataSize << std::endl;

        ServerConnectionFlowV1B serverData;
        memcpy(&serverData, entry->data, connectionDataSize);
        std::cout << "Connection data:"
                  << "\n  frameMetadataSize: " << serverData._frameMetadataSize
                  << "\n  frameSize: " << serverData._frameSize
                  << "\n  acceptConnectionTime: " << serverData._acceptConnectionTime
                  << "\n  hasActiveProducers: " << serverData._hasActiveProducers << std::endl;

            handleConnected(ep);
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
        run(options);
    }
    catch (const std::exception& ex)
    {
        std::cerr << "Unhandled exception: " << ex.what() << std::endl;
    }
}
