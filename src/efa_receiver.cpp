#include "rdma_types.h"
#include "rdma_defs.h"
#include "getopt.h"
#include "efa_progress_engine.h"

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include <moodycamel/blockingconcurrentqueue.h>

#include <asio/buffer.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/signal_set.hpp>
#include <asio/read.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include <atomic>
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
#include <utility>
#include <vector>

struct AppOptions
{
    std::string _address{""};
    std::uint16_t _port{8002};
    std::string _providerName{""};
    std::uint32_t _frameSize{1920 * 1080 * 8 / 3}; // Full HD v210
//    std::string _localAddress{};
//    std::string _flowId{};
    int _numMessages{100};
    int _numReceivers{1};
    bool _verbose{false};
};

template <int Name>
class BaseTimeoutOption
{
public:
    explicit BaseTimeoutOption(std::chrono::milliseconds timeout)
        : _timeout{}
    {
#if defined(_WIN32)
        _timeout = (uint32_t)timeout.count();
#else
        _timeout.tv_sec = (long)(std::chrono::duration_cast<std::chrono::seconds>(timeout).count());
        _timeout.tv_usec = (long)(timeout.count() % 1000);
#endif
    }

    template <typename Protocol>
    int level(const Protocol&) const
    {
        return SOL_SOCKET;
    }

    template <typename Protocol>
    int name(const Protocol&) const
    {
        return Name;
    }

    template <typename Protocol>
    const void* data(const Protocol&) const
    {
        return &_timeout;
    }

    template <typename Protocol>
    std::size_t size(const Protocol&) const
    {
        return sizeof(_timeout);
    }

private:
#if defined(_WIN32)
    uint32_t _timeout;
#else
    struct timeval _timeout;
#endif
};

using SendTimeoutOption = BaseTimeoutOption<SO_SNDTIMEO>;
using RecvTimeoutOption = BaseTimeoutOption<SO_RCVTIMEO>;

static unsigned PeerId = 0;

class App : public std::enable_shared_from_this<App>, public EfaProgressCallback
{
    unsigned _peerId;
public:
    App(AppOptions options, asio::io_context& ctx, std::shared_ptr<EfaProgressEngine> progressEngine)
        : _options{std::move(options)},
          _ctx{ctx},
          _progressEngine{std::move(progressEngine)}
    {
        _peerId = ++PeerId;
    }

    void start()
    {
        // TODO: go through all fi_info (->next) and create EfaAdapter from all of them
        std::shared_ptr<fi_info> hints = createFabricInfoHintsRdm("");
        std::shared_ptr<fi_info> fabricInfo;
        int res = fi_getinfo(FABRIC_VERSION, nullptr, nullptr, 0U, hints.get(), makeOutPointer(fabricInfo));
        if (res != 0 || !fabricInfo)
        {
            throw rdma_error{"fi_getinfo", res};
        }
        DEBUG_LOG("Provider: %s", fabricInfo->fabric_attr->prov_name);
        DEBUG_LOG("Fabric address: %s", getFabricLocalAddressAsString(*fabricInfo).c_str());

        _adapter = std::make_shared<RdmaAdapter>(std::move(fabricInfo));
        _endpoint = std::make_shared<RdmEndpoint>(*_adapter);

        char addrBytes[128] = {};
        size_t addrLen = sizeof(addrBytes);
        res = fi_getname(toFid(_endpoint->_endpoint), addrBytes, &addrLen);
        if (res != 0)
        {
            throw rdma_error{"fi_getname", res};
        }

        uint64_t mrKey = 1;
        for (int i = 0; i < 2; ++i)
        {
            _frames[i]._payload = std::make_unique<char[]>(_options._frameSize);

            res = fi_mr_reg(_endpoint->_domain.get(), _frames[i]._payload.get(), _options._frameSize, FI_SEND, 0,
                            mrKey++, 0, makeOutPointer(_frames[i]._memoryRegion), nullptr);
            if (res != 0)
            {
                throw rdma_error{"fi_mr_reg", res};
            }

            ssize_t result = fi_recv(_endpoint->_endpoint.get(), _frames[i]._payload.get(), _options._frameSize,
                                     fi_mr_desc(_frames[i]._memoryRegion.get()), FI_ADDR_UNSPEC, (EfaProgressCallback*)this);
            if (result != 0)
            {
                throw rdma_error{"fi_recv", static_cast<int>(result)};
            }
        }

        // FIXME: resolver?
        asio::ip::tcp::endpoint senderEp{asio::ip::make_address(_options._address), _options._port};
        asio::ip::tcp::socket socket{_ctx, asio::ip::tcp::v4()};
        socket.set_option(SendTimeoutOption{std::chrono::milliseconds(1000)});
        socket.set_option(RecvTimeoutOption{std::chrono::milliseconds(1000)});
        socket.set_option(asio::ip::tcp::no_delay(true));

        socket.connect(senderEp);

        EfaControlMessage<EfaClientConnectV1> connectMessage;
        connectMessage._length = sizeof(connectMessage);
        connectMessage._type = EfaControlMessageType::CLIENT_CONNECT_V1;
        connectMessage._payload._addressFormat = (uint16_t)_adapter->_fabricInfo->addr_format;
        connectMessage._payload._addressLength = (uint16_t)addrLen;
        if (addrLen > sizeof(connectMessage._payload._addressBytes))
        {
            throw std::runtime_error{"Fabric address length is too big for V1 protocol"};
        }
        memcpy(&connectMessage._payload._addressBytes, addrBytes, addrLen);
        // connectMessage._payload._flowIdentifier = ...
        connectMessage._payload._expectedFrameSize = _options._frameSize;

        asio::write(socket, asio::buffer(&connectMessage, sizeof(connectMessage)));
        //asio::read(socket, )

        // TODO: read response asio::read();
        // TODO: send expected cadence

        _progressEngine->addEndpoint(_endpoint, this);
        // defer( _progressEngine->removeEndpoint(_endpoint) );

        _progressEngine->postWork(_endpoint, 2);

        try
        {
            receiveLoop();
        }
        catch ( ... )
        {
            
            _progressEngine->removeEndpoint(_endpoint);
            throw;
        }

        DEBUG_LOG("Sending shutdown");

        EfaControlMessage<EfaClientShutdownV1> shutdownMessage;
        shutdownMessage._length = sizeof(shutdownMessage);
        shutdownMessage._type = EfaControlMessageType::CLIENT_SHUTDOWN_V1;
        asio::write(socket, asio::buffer(&shutdownMessage, sizeof(shutdownMessage)));
        socket.close();

        _progressEngine->removeEndpoint(_endpoint);
        
        // This flushes all unsend/received data, we need buffers/mrs to be alive
        _endpoint.reset();

        for (auto& frame : _frames)
        {
            frame._memoryRegion.reset();
            frame._payload.reset();
        }

        _adapter.reset();
    }

    void stop()
    {
        _stopped = true;
        _completionQueue.enqueue(CompletionEntry{0xDEAD, 0, 0});
    }

private:
    void onCompletion(uint64_t flags, size_t length) noexcept override
    {
        _completionQueue.enqueue(CompletionEntry{0, flags, length});
    }

    void onError(int errorCode) noexcept override
    {
        _completionQueue.enqueue(CompletionEntry{errorCode, 0, 0});
    }

private:
    void receiveLoop()
    {
        unsigned frameToRepost = 1;
        unsigned numMessageReceived = 0;

        while (!_stopped)
        {
            // Wait on completion
            CompletionEntry entry;
            const bool gotEntry = _completionQueue.wait_dequeue_timed(entry, 1'000 * 2000); // 200 ms
            if (!gotEntry)
            {
                DEBUG_LOG("Timeout waiting for completion");
                break;
            }
            if (entry.errorCode == 0xDEAD) // EOS entry
            {
                break;
            }

            ssize_t result = fi_recv(_endpoint->_endpoint.get(), _frames[frameToRepost]._payload.get(), _options._frameSize,
                                     fi_mr_desc(_frames[frameToRepost]._memoryRegion.get()), FI_ADDR_UNSPEC, (EfaProgressCallback*)this);
            if (result != 0)
            {
                throw rdma_error{"fi_recv", static_cast<int>(result)};
            }
            _progressEngine->postWork(_endpoint, 1);
            frameToRepost = 1 - frameToRepost;

            ++numMessageReceived;
            if (_options._verbose)
                DEBUG_LOG("Received: %u (%zu bytes id=%u)", numMessageReceived, entry.len, _peerId);
        }
    }

private:
    const AppOptions _options;
    asio::io_context& _ctx;
    const std::shared_ptr<EfaProgressEngine> _progressEngine;

    struct CompletionEntry
    {
        int errorCode;
        uint64_t flags;
        size_t len;
    };
    moodycamel::BlockingConcurrentQueue<CompletionEntry> _completionQueue;

    std::shared_ptr<RdmaAdapter> _adapter;

    struct Frame
    {
        std::unique_ptr<fid_mr> _memoryRegion;
        std::unique_ptr<char[]> _payload;
    };
    Frame _frames[2];

    std::shared_ptr<RdmEndpoint> _endpoint;

    std::atomic<bool> _stopped = false;
};

int main(int argc, char* argv[])
{
    setenv("FI_UNIVERSE_SIZE", "4", 1);
    setenv("FI_EFA_ENABLE_SHM_TRANSFER", "0", 1);

    AppOptions options;

    int opt;
    while ((opt = getopt(argc, argv, "a:B:p:n:r:s:v")) != -1)
    {
        switch (opt)
        {
        case 'a':
            options._address = optarg;
            break;
        case 'B':
           options._port = (uint16_t)std::atoi(optarg);
           break;
        case 'p':
            options._providerName = optarg;
            break;
        case 'n':
            options._numMessages = std::atoi(optarg);
            break;
        case 'r':
            options._numReceivers = std::atoi(optarg);
            break;
        //case 'f':
        //    options._flowId = optarg;
        //    break;
        case 's':
           options._frameSize = std::atoi(optarg);
           break;
        case 'v':
           options._verbose = true;
           break;
        case '?':
            std::fprintf(stderr, "Unknown option: %c\n", opt);
            return 1;
        default:
            return 1;
        }
    }

    if (options._address.empty())
    {
        std::fprintf(stderr, "Address (-a) is required\n");
        return 1;
    }

    try
    {
        asio::io_context ctx;
        auto progress = std::make_shared<EfaProgressEngine>();

        struct Bundle
        {
            std::unique_ptr<App> app;
            std::thread thread;
        };

        std::vector<std::unique_ptr<Bundle>> bundles;

        asio::signal_set signals{ctx};
        signals.add(SIGINT);
#if defined(SIGBREAK)
        signals.add(SIBREAK);
#endif
        signals.async_wait([&](const std::error_code&, int) {
            for (auto& b : bundles)
                b->app->stop();
        });

        for (int i = 0; i < options._numReceivers; ++i)
        {
            auto bundle = std::make_unique<Bundle>();
            bundle->app = std::make_unique<App>(options, ctx, progress);
            bundle->thread = std::thread{[self = bundle->app.get(), i]() mutable {
                try
                {
                    self->start();
                }
                catch (const std::exception& ex)
                {
                    DEBUG_LOG("EXCEPTION on receiver[%d]: %s", i, ex.what());
                }
            }};
            bundles.push_back(std::move(bundle));
        }

        ctx.run();

        for (auto& b : bundles)
            b->thread.join();
    }
    catch (const std::exception& ex)
    {
        std::fprintf(stderr, "EXCEPTION on main thread: %s\n", ex.what());
    }
}
