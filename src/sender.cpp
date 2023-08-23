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
#  include <netinet/ip.h>
#endif

#include <cassert>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

// For TCP proviver:
//  - trzeba zawolac fi_eq_sread z krotkim timeoutem na samym poczatku (najlepiej bez zadnych zrodel)
//    tak zeby dostac nfds=3 a nie nfds=1 na ktorym poll() nie bedzie dzialac
//  - przy poll'u trzeba zrobic +1, pomijajac pierwszy fd (ktory jest signalled dopoki nie zawolasz fi_eq_sread)
//  - poll() potrafi zwrocic, po czym fi_eq_read zwraca EAGAIN i nastepny poll() jest juz OK
// For verbs/ndirect
//  - Przez to ze epoll'a nie ma na windowsie nie mozemy w ogole dostac FI_GETOBJ

struct AppOptions
{
    std::string address{"192.168.110.8"};
    std::string port{"8001"};
    std::string providerName{"verbs"};
};

enum class WaitResult
{
    GOT_MESSAGE,
    ERROR,
    SHUTDOWN,
    TIMEOUT
};

void handleConnection(RdmaEndpoint& in_endpoint)
{
    static uint64_t key = 1;
    int value = 0;
    constexpr auto bufSize = 5 * 1024 * 1024; // 1 MB
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(bufSize);
    std::memset(buf.get(), value++, bufSize);
    std::unique_ptr<fid_mr> memoryRegion;
    int res = fi_mr_reg(in_endpoint._domain.get(), buf.get(), bufSize, FI_SEND, 0, key++, 0,
                        makeOutPointer(memoryRegion), nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_mr_reg", res};
    }

    // Normally, fi_accept calls fi_enable but we want to post receive before doing so.
    fi_enable(in_endpoint._endpoint.get());

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
    const auto cmEntrySize = in_endpoint.getMaxConnectionDataSize();
    std::unique_ptr<uint8_t[]> cmEntryBuffer = std::make_unique<uint8_t[]>(cmEntrySize);
    fi_eq_cm_entry* cmEntry = reinterpret_cast<fi_eq_cm_entry*>(cmEntryBuffer.get());

    ssize_t ret = fi_eq_sread(in_endpoint._eventQueue.get(), &event, cmEntry, cmEntrySize, 2000, 0U);
    if (ret <= 0 || event != FI_CONNECTED)
    {
        std::cout << " - Failed to connect!\n";
        return;
    }
    std::cout << " - CONNECTED\n";

    while (true)
    {
        // Simulate some working being done
        std::this_thread::sleep_for(std::chrono::milliseconds{500});

        // wait until client is ready to receive data (by sending us an empty message)
        std::cout << "Waiting on client send-sync.\n";

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
                waitResult = WaitResult::ERROR;
                break;
            }

            ret = fi_cq_read(in_endpoint._completionQueue.get(), &entry, 1);
            if (ret != -FI_EAGAIN)
            {
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
                waitResult = WaitResult::ERROR;
                break;
            }
        }

        if (waitResult != WaitResult::GOT_MESSAGE)
        {
            if (waitResult == WaitResult::TIMEOUT)
            {
                std::cout << "Timeout receiving a sync-message\n";
            }
            break;
        }

        std::cout << "Got send-sync from client, sending a message.\n";
        in_endpoint.receiveEmptyMessage();

        ret = fi_send(in_endpoint._endpoint.get(), buf.get(), bufSize, fi_mr_desc(memoryRegion.get()),
                      FI_ADDR_UNSPEC, nullptr);
        if (ret != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(ret)};
        }

        const auto sendingStart = std::chrono::steady_clock::now();
        waitResult = WaitResult::TIMEOUT;
        while(std::chrono::steady_clock::now() - sendingStart < std::chrono::seconds{5})
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
                waitResult = WaitResult::ERROR;
                break;
            }

            ret = fi_cq_read(in_endpoint._completionQueue.get(), &entry, 1);
            if (ret != -FI_EAGAIN)
            {
                //std::cout << "CQ2: got " << entry.flags << std::endl;
                if ((entry.flags & (FI_SEND | FI_MSG)) == (FI_SEND | FI_MSG))
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
                waitResult = WaitResult::ERROR;
                    break;
                }
            }

        if (waitResult != WaitResult::GOT_MESSAGE)
        {
            if (waitResult == WaitResult::TIMEOUT)
            {
                std::cout << "Timeout sending a payload message\n";
            }
            break;
        }

        std::cout << "Message (" << bufSize << " bytes) sent to client.\n";
        std::memset(buf.get(), value, bufSize);
        value = (value + 1) % 256;

        if (value == 5)
        {
            //fi_shutdown(in_endpoint._endpoint.get(), 0U);
            //break;
        }
    }

    //crashes on vrb_ep_close?! seems like it can only be called on still valid QP
    //fi_shutdown(in_endpoint._endpoint.get(), 0U);
}

void run(const AppOptions& in_cfg)
{
    auto fabricInfo = getFabricInfo(in_cfg.providerName, in_cfg.address, in_cfg.port, true);
    RdmaAdapter adapter{std::move(fabricInfo)};
    RdmaListeningEndpoint listeningEndpoint{adapter};

    const auto entryMaxSize = listeningEndpoint.getMaxConnectionDataSize();
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
            if (!std::string_view{fi->fabric_attr->prov_name}.rfind("ofi_hook_", -1))
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
