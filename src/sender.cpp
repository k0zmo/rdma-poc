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

void handleConnection(RdmaEndpoint& in_endpoint)
{
    static uint64_t key = 1;
    int value = 0;
    constexpr auto bufSize = 1024 * 1024; // 1 MB
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(bufSize);
    std::memset(buf.get(), value++, bufSize);
    std::unique_ptr<fid_mr> memoryRegion;
    int res = fi_mr_reg(in_endpoint._domain.get(), buf.get(), bufSize, FI_SEND, 0, key++, 0, makeOutPointer(memoryRegion), nullptr);
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
    char eventBuf[128];
    fi_eq_sread(in_endpoint._eventQueue.get(), &event, eventBuf, sizeof(eventBuf), -1, 0U);
    assert(event == FI_CONNECTED);
    std::cout << " CONNECTED\n";

    // recv przed loop
    // loop:
    //   wait for recv
    //   post another recv
    //   send message
    //   wait for send

    // std::thread th{[&]() {
    //     std::this_thread::sleep_for(std::chrono::seconds{10});
    //     fi_cq_signal(in_endpoint._inboundQueue.get());
    // }};
    // th.detach();

    while (true)
    {
        std::cout << "Waiting on client send-sync.\n";
        // wait until client is ready to receive data (by sending us an empty message)
        {
            fi_cq_msg_entry entry;
            ssize_t ret = fi_cq_sread(in_endpoint._inboundQueue.get(), &entry, 1, nullptr, -1);
            if (ret == -FI_EAGAIN)
            {

            }
            if (ret == -FI_EAVAIL)
            {
                fi_cq_err_entry err;
                fi_cq_readerr(in_endpoint._inboundQueue.get(), &err, 0);
                if (err.err_data_size > 0)
                {
                    std::string errorMessage{(const char*)err.err_data, err.err_data_size};
                    std::cout << "Error on CQ: " << errorMessage << std::endl;
                }
                else
                {
                    std::cout << "Error on CQ ?!" << std::endl;
                }
                break;
            }
            // while(1)
            // {
            //     ssize_t ret = fi_cq_read(in_endpoint._inboundQueue.get(), &entry, 1);
            //     if (ret == -FI_EAVAIL)
            //     {
            //         fi_cq_err_entry err;
            //         fi_cq_readerr(in_endpoint._inboundQueue.get(), &err, 0);
            //         if (err.err_data_size > 0)
            //         {
            //             std::string errorMessage{(const char*)err.err_data, err.err_data_size};
            //             std::cout << "Error on CQ: " << errorMessage << std::endl;
            //         }
            //         else
            //         {
            //             std::cout << "Error on CQ ?!" << std::endl;
            //         }
            //     }
            //     else if (ret != -FI_EAGAIN)
            //     {
            //         //std::cout << "cq_read from inbound queue: " << ret << '\n';
            //         break;
            //     }
            // }
        }
        std::cout << "Got send-sync from client, sending a message.\n";
        in_endpoint.receiveEmptyMessage();

        fi_send(in_endpoint._endpoint.get(), buf.get(), bufSize, fi_mr_desc(memoryRegion.get()), FI_ADDR_UNSPEC, nullptr);
        {
            fi_cq_msg_entry entry;
            while(1)
            {
                ssize_t ret = fi_cq_read(in_endpoint._outboundQueue.get(), &entry, 1);
                if (ret == -FI_EAVAIL)
                {
                    fi_cq_err_entry err;
                    ret = fi_cq_readerr(in_endpoint._outboundQueue.get(), &err, 0);
                    if (err.err_data_size > 0)
                    {
                        std::string errorMessage{(const char*)err.err_data, err.err_data_size};
                        std::cout << "Error on CQ: " << errorMessage << std::endl;
                    }
                    else
                    {
                        std::cout << "Error on CQ ?!" << std::endl;
                    }
                    return;
                }
                else if (ret != -FI_EAGAIN)
                {
                    //std::cout << "cq_read from outbound queue: " << ret << '\n';
                    break;
                }
            }
        }
        std::cout << "Message sent to client.\n";

        std::memset(buf.get(), value, bufSize);
        value = (value + 1) % 256;

        //if (value == 5)
        //    break;
    }
}

void run(const AppOptions& in_cfg)
{
    RdmaAdapter adapter{in_cfg.providerName, in_cfg.address, in_cfg.port, true};
    RdmaListeningEndpoint listeningEndpoint{adapter};

    const auto entryMaxSize = listeningEndpoint.getMaxConnectionDataSize();
    std::unique_ptr<uint8_t[]> connectBuffer = std::make_unique<uint8_t[]>(entryMaxSize);
    fi_eq_cm_entry* entry = reinterpret_cast<fi_eq_cm_entry*>(connectBuffer.get());
    uint32_t event = 0;

    //std::thread th{[&]() {
        //std::this_thread::sleep_for(std::chrono::seconds{10});
        //int32_t message = 1; // Zero-sized message causes poll-stuck for verbs provider
        //std::cout << "POSTing custom message to break out of the loop\n";
        //fi_eq_write(eventQueue.get(), 123, &message, sizeof(message), 0);
    //}};
    //th.detach();

    while (true)
    {
        const auto eq = listeningEndpoint._eventQueue.get();
        std::cout << "    calling fi_eq_sread()\n";
        const ssize_t rd = fi_eq_sread(eq, &event, entry, entryMaxSize, -1, 0);
        std::cout << "    eq_sread: " << rd << ", event: " << event << std::endl;

        if (rd == -FI_EAGAIN)
        {
            continue;
        }
        if (rd < 0)
        {
            if (rd == -FI_EAVAIL)
            {
                fi_eq_err_entry err;
                fi_eq_readerr(eq, &err, 0);
                if (err.err_data_size > 0)
                {
                    std::string errorMessage{(const char*)err.err_data, err.err_data_size};
                    std::cout << "Error calling fi_eq_read(): " << errorMessage << std::endl;
                }
                else
                {
                    std::cout << "Error calling fi_eq_read(), unknown reason" << rd << std::endl;
                }
            }
            else
            {
                std::cout << "Error calling fi_eq_read(): " << rd << std::endl;
            }
            continue;
        }
        // if (event == FI_CONNECTED)
        // {
        //     std::cout << "EQ: Endpoint connected\n";
        //     continue;
        // }
        // if (event == FI_SHUTDOWN)
        // {
        //     std::cout << "EQ: Endpoint shutdown\n";
        //     continue;
        // }
        if (event != FI_CONNREQ)
        {
            std::cout << "EQ: Unexpected event - " << event << std::endl;
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

                    RdmaEndpoint ep{adapter, *entry->info};

                    std::thread th{[ep = std::move(ep)]() mutable -> void {
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

            const auto errorMessage = errorMessageStream.str();
            if (!errorMessage.empty())
            {
                std::cout << "Connection rejected: " << errorMessage << std::endl;
                fi_reject(listeningEndpoint._passiveEndpoint.get(), entry->info->handle, errorMessage.c_str(), errorMessage.size() + 1);
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
        int res = fi_getinfo(FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION), nullptr, nullptr, FI_PROV_ATTR_ONLY, nullptr, makeOutPointer(fabricInfo));
        if (res != 0)
        {
            throw rdma_error{"fi_getinfo", res};
        }
        for (auto fi = fabricInfo.get(); fi; fi = fi->next)
        {
            if (!std::string_view{fi->fabric_attr->prov_name}.rfind("ofi_hook_",-1))
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
