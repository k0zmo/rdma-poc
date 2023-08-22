#include "rdma_defs.h"
#include "rdma_types.h"
#include "getopt.h"

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>

#ifdef _WIN32
#  include <winerror.h>
#endif

#include <cstring>
#include <cstdint>
#include <string>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>

struct AppOptions
{
    std::string address{" 172.19.41.49"};
    std::string port{"8001"};
    std::string providerName{"verbs"};
};

bool quit = false;

void handleConnected(RdmaEndpoint& in_endpoint)
{
    constexpr auto bufSize = 1024 * 1024; // 1 MB
    static uint64_t key = 1;
    std::unique_ptr<char[]> buf = std::make_unique<char[]>(bufSize);
    std::unique_ptr<fid_mr> memoryRegion;
    int res = fi_mr_reg(in_endpoint._domain.get(), buf.get(), bufSize, FI_RECV, 0, key++, 0, makeOutPointer(memoryRegion), nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_mr_reg", res};
    }

    while (true)
    {
        std::cin.get();
        std::cout << "Signaling to sender we're ready to receive new message\n";
        ssize_t ret = fi_recv(in_endpoint._endpoint.get(), buf.get(), bufSize, fi_mr_desc(memoryRegion.get()), FI_ADDR_UNSPEC, nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(res)};
        }
        in_endpoint.sendEmptyMessage();

        {
            fi_cq_entry entry;
            while(1)
            {
                ret = fi_cq_read(in_endpoint._inboundQueue.get(), &entry, 1);
                if (ret != -FI_EAGAIN)
                {
                    //std::cout << "cq_read from inbound queue: " << ret << '\n';
                    break;
                }
            }
        }

        std::cout << "Got message from the sender: " << +buf[1] << std::endl;

        if (buf[0] == 2)
        {
            //quit = true;
            break;
        }

        {
            fi_cq_entry entry;
            while(1)
            {
                ret = fi_cq_read(in_endpoint._outboundQueue.get(), &entry, 1);
                if (ret != -FI_EAGAIN)
                {
                    //std::cout << "cq_read from outbound queue: " << ret << '\n';
                    break;
                }
            }
        }
    }

    fi_shutdown(in_endpoint._endpoint.get(), 0);
}

void run(const AppOptions& in_cfg)
{
    std::unique_ptr<fi_info> hints{fi_allocinfo()};
    if (!hints)
    {
        throw std::runtime_error{"hints is null"};
    }

    hints->fabric_attr->prov_name = strdup(in_cfg.providerName.c_str());
    hints->ep_attr->type = FI_EP_MSG;
    hints->caps = FI_MSG;
    hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;

    const char* node = in_cfg.address.c_str();
    const char* service = in_cfg.port.c_str();
    const uint64_t flags = 0;

    std::unique_ptr<fi_info> fabricInfo;
    int res = fi_getinfo(FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION), node, service, flags, hints.get(),
                         makeOutPointer(fabricInfo));
    if (res != 0)
    {
        throw rdma_error{"fi_getinfo", res};
    }

    // Fabric + Domain + event queue = "shared resources"

    std::shared_ptr<fid_fabric> fabric;
    res = fi_fabric(fabricInfo->fabric_attr, makeOutPointerForFI(fabric), nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_fabric", res};
    }

    std::shared_ptr<fid_domain> domain;
    res = fi_domain(fabric.get(), fabricInfo.get(), makeOutPointerForFI(domain), nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_domain", res};
    }

    std::shared_ptr<fid_eq> eventQueue; // For connection management (slow-path)
    fi_eq_attr eq_attr = {};
    eq_attr.wait_obj = FI_WAIT_UNSPEC;
    res = fi_eq_open(fabric.get(), &eq_attr, makeOutPointerForFI(eventQueue), nullptr);
    if (res != 0)
    {
        throw rdma_error{"fi_eq_open", res};
    }

    // Start of receiver specific
    RdmaEndpoint ep{domain, eventQueue, *fabricInfo};

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
    res = fi_connect(ep._endpoint.get(), fabricInfo->dest_addr, &clientData, sizeof(clientData));
    if (res != 0)
    {
        throw rdma_error{"fi_connect", res};
    }

    size_t maxConnectionDataSize = 0, maxConnectionDataSizeLength = sizeof(maxConnectionDataSize);
    res = fi_getopt(reinterpret_cast<fid_t>(ep._endpoint.get()), FI_OPT_ENDPOINT,
                    FI_OPT_CM_DATA_SIZE, &maxConnectionDataSize, &maxConnectionDataSizeLength);
    if (res != 0)
    {
        throw rdma_error{"fi_getopt", res};
    }

    std::unique_ptr<uint8_t[]> connectBuffer = std::make_unique<uint8_t[]>(maxConnectionDataSize);
    fi_eq_cm_entry* entry = reinterpret_cast<fi_eq_cm_entry*>(connectBuffer.get());
    uint32_t event = 0;

    std::thread th;

    while (!quit)
    {
        // timeout trzeba dac jakis sensowny
        // dla verbsow nie dostajemy zadnego sygnalu
        const ssize_t rd = fi_eq_sread(eventQueue.get(), &event, entry, maxConnectionDataSize, -1, 0);
        if (rd == -FI_EAVAIL)
        {
            fi_eq_err_entry err;
            fi_eq_readerr(eventQueue.get(), &err, 0);
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
            break;
        }
        if (event == FI_SHUTDOWN)
        {
            std::cout << "Shutdown processed.\n";
            break;
        }
        if (event != FI_CONNECTED || entry->fid != &ep._endpoint->fid)
        {
            std::cout << "Unexpected CM event: " << event << std::endl;
            continue;
        }
        std::unique_ptr<fi_info> entryRaii{entry->info};
        if (rd < (ssize_t)sizeof(*entry))
        {
            std::cout << "Unexpected size of connection data: " << rd << std::endl;
            continue;
        }

        const auto connectionDataSize = rd - sizeof(*entry);
        std::cout << "Received extra bytes: " << connectionDataSize << std::endl;

        ServerConnectionFlowV1B serverData;
        memcpy(&serverData, entry->data, connectionDataSize);
        std::cout << "Connection data:"
                  << "\nframeMetadataSize: " << serverData._frameMetadataSize
                  << "\nframeSize: " << serverData._frameSize
                  << "\nacceptConnectionTime: " << serverData._acceptConnectionTime
                  << "\nhasActiveProducers: " << serverData._hasActiveProducers << std::endl;

        //std::thread th1{[&] {
            handleConnected(ep);
        //}};
        //th.swap(th1);
    }

    if (th.joinable())
        th.join();
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
