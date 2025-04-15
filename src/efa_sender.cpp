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
#include <asio/completion_condition.hpp>
#include <asio/io_context.hpp>
#include <asio/ip/address.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/read.hpp>
#include <asio/signal_set.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sstream>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include <atomic>
#include <cassert>
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
#include <type_traits>
#include <utility>
#include <vector>

struct AppOptions
{
//    std::string _address{""};
    std::uint16_t _port{8002};
    std::string _providerName{""};
//    std::string _flowId{};
    std::uint32_t _frameSize{1920 * 1080 * 8 / 3}; // Full HD v210
    int _intervalMs{20}; // 50p
    bool _verbose{false};
};

static std::string getAddressAsString(fid_av& av, const void* addr)
{
    char buf[128]{};
    size_t len = sizeof(buf);
    const char* res = fi_av_straddr(&av, addr, buf, &len);
    return res ? std::string{buf, len} : std::string{};
}

template <typename T>
size_t copyFromBuffer(asio::const_buffer& inout_buf, T& in_value)
{
    static_assert(std::is_trivially_copy_assignable_v<T>, "");
    std::memcpy(&in_value, inout_buf.data(), sizeof(T));
    inout_buf += sizeof(T);
    return sizeof(T);
}

static unsigned PeerId = 0;

class Peer : public std::enable_shared_from_this<Peer>, public EfaProgressCallback
{
    unsigned _peerId;
public:
    Peer(asio::io_context&                  ctx,
         asio::ip::tcp::socket              socket,
         std::shared_ptr<fi_info>           fabricInfo,
         std::shared_ptr<EfaProgressEngine> progress,
         AppOptions                         options) :
        _ctx{ctx},
        _socket{std::move(socket)},
        _options{std::move(options)},
        _adapter{std::make_shared<RdmaAdapter>(std::move(fabricInfo))},
        _progress{std::move(progress)}
    {
        _peerId = ++PeerId;

        DEBUG_LOG("Peer::Peer this=%lu", (uintptr_t)this);
    }

    ~Peer()
    {
        DEBUG_LOG("Peer::~Peer");
        _stopped = true;
        if (_sendingThread.joinable())
            _sendingThread.join();
    }

    void start()
    {
        asio::async_read(_socket,
                         _dynamicBuf.prepare(1024),
                         asio::transfer_at_least(sizeof(EfaControlMessage<EfaClientConnectV1>)),
                         [self = shared_from_this()](const auto& errorCode, size_t bytesXfer) {
                             self->onClientRequestReceived(errorCode, bytesXfer);
                         });
    }

    void stop()
    {
        _stopped = true;
        _socket.cancel();
        _completionQueue.enqueue({0xDEAD, 0, 0});
        if (_sendingThread.joinable())
            _sendingThread.join();
    }

private:
    void rejectConnection(const std::string& message)
    {
        if (message.size() > sizeof(_rejectMessage._payload._errorMessage) - 1)
        {
            std::string truncatedMessage{message};
            truncatedMessage.resize(sizeof(_rejectMessage._payload._errorMessage) - 1);
            strncpy(_rejectMessage._payload._errorMessage,
                    truncatedMessage.c_str(),
                    truncatedMessage.size());
        }
        else
        {
            strncpy(_rejectMessage._payload._errorMessage, message.c_str(), message.size());
        }
        _rejectMessage._type   = EfaControlMessageType::SERVER_REJECT_V1;
        _rejectMessage._length = sizeof(_rejectMessage);

        asio::async_write(_socket,
                          asio::buffer(&_rejectMessage, sizeof(_rejectMessage)),
                          [self = shared_from_this()](const std::error_code&, size_t) {
                              return; // Let the connection die on its own
                          });
    }

    void acceptConnection()
    {
        _acceptMessage._type                          = EfaControlMessageType::SERVER_ACCEPT_V1;
        _acceptMessage._length                        = sizeof(_acceptMessage);
        _acceptMessage._payload._acceptConnectionTime = 1111111;
        _acceptMessage._payload._frameSize            = _options._frameSize;
        _acceptMessage._payload._hasActiveProducers   = true;

        asio::async_write(_socket,
                          asio::buffer(&_acceptMessage, sizeof(_acceptMessage)),
                          [self = shared_from_this()](const std::error_code&, size_t) {});
    }

    bool handleClientConnect(size_t messageSize, asio::const_buffer payloadBuf)
    {
        // TODO: STATE
        if (_clientConnect._addressFormat != 0) // We've already process connect request
        {
            rejectConnection("EfaClientConnectV1 message already handled");
            stop();
            return false;
        }

        if (messageSize != sizeof(EfaControlMessage<EfaClientConnectV1>))
        {
            rejectConnection("EfaClientConnectV1 message size differs");
            return false;
        }

        const auto consumed = copyFromBuffer(payloadBuf, /*inout*/_clientConnect);
        _dynamicBuf.consume(consumed);

        if (_clientConnect._addressFormat != _adapter->_fabricInfo->addr_format)
        {
            std::stringstream ss;
            ss << "Invalid address format, must be " << (uint32_t)_adapter->_fabricInfo->addr_format;
            rejectConnection(ss.str());
            return false;
        }
        if (_clientConnect._addressLength > sizeof(_clientConnect._addressBytes))
        {
            std::stringstream ss;
            ss << "Invalid address length, can't be greater than " << sizeof(_clientConnect._addressBytes);
            rejectConnection(ss.str());
            return false;
        }
        if (_clientConnect._expectedFrameSize != 0 &&
            _clientConnect._expectedFrameSize != _options._frameSize)
        {
            std::stringstream ss;
            ss << "Expected frame size is different than the actual one: " << _options._frameSize;
            rejectConnection(ss.str());
            return false;
        }

        acceptConnection();

        _sendingThread = std::thread{[self = shared_from_this()]() mutable {
            try
            {
                self->createRdmEndpoint();
                self->createData();
                self->sendLoop();

                auto& ctx = self->_ctx; // self is moved before we can pass self->_ctx
                asio::post(ctx, [self = std::move(self)]() { self->stop(); });
            }
            catch (const std::exception& ex)
            {
                DEBUG_LOG("EXCEPTION: %s", ex.what());
                auto& ctx = self->_ctx;
                asio::post(ctx, [self = std::move(self)]() { self->stop(); });
                return;
            }
        }};

        return messageSize;
    }

    bool handleClientShutdown(size_t messageSize, asio::const_buffer payloadBuf)
    {
        DEBUG_LOG("Received shutdown request");
        (void)messageSize;
        (void)payloadBuf;
        stop();
        return false;
    }

    void onClientRequestReceived(const std::error_code& errorCode, size_t bytesXfer)
    {
        if (_stopped)
            return;

        if (errorCode)
        {
            DEBUG_LOG("Error reading client requiest: %s", errorCode.message().c_str());
            stop();
            return;
        }

        assert(bytesXfer >= sizeof(EfaControlMessageHeader));
        
        _dynamicBuf.commit(bytesXfer);
        asio::const_buffer buf = _dynamicBuf.data();
        const auto numBytesInBuf = buf.size();

        // Read size and tag of the control message
        EfaControlMessageHeader header;
        copyFromBuffer(buf, /*inout*/header);

        if (header._length > numBytesInBuf)
        {
            // Read more data from the socket. We already have `numBytesInBuf` bytes in the
            // streambuf from the total of `header._length`. Read the remaining bytes. Because
            // header's length is u16, we dont really need to protect ourselves from too huge values
            // and reading 4GB of data from socket in the worst case scenario.
            asio::async_read(_socket,
                             _dynamicBuf.prepare(header._length),
                             asio::transfer_at_least(header._length - numBytesInBuf),
                             [self = shared_from_this()](const auto& errorCode, size_t bytesXfer) {
                                 self->onClientRequestReceived(errorCode, bytesXfer);
                             });
            return;
        }

        _dynamicBuf.consume(sizeof(EfaControlMessageHeader));

        // At this point the message should be complete
        switch (header._type)
        {
        case EfaControlMessageType::CLIENT_CONNECT_V1:
            if (!handleClientConnect(header._length, buf))
            {
                return; // Don't read more requests
            }
            break;
        case EfaControlMessageType::CLIENT_SHUTDOWN_V1:
            if (!handleClientShutdown(header._length, buf))
            {
                return; // Don't read more requests
            }
            break;
        case EfaControlMessageType::SERVER_ACCEPT_V1:
        case EfaControlMessageType::SERVER_REJECT_V1:
            DEBUG_LOG("Invalid control message: %hu, closing connection", (uint16_t)header._type);
            return; // Protocol fatal error
        default:
            DEBUG_LOG("Unsupported control message: %hu, skipping", (uint16_t)header._type);
            break;
        }
        
        asio::async_read(_socket,
                         _dynamicBuf.prepare(1024),
                         asio::transfer_at_least(sizeof(EfaControlMessageHeader)),
                         [self = shared_from_this()](const auto& errorCode, size_t bytesXfer) {
                             self->onClientRequestReceived(errorCode, bytesXfer);
                         });
    }

    void createRdmEndpoint()
    {
        _endpoint = std::make_shared<RdmEndpoint>(*_adapter);

        auto res = fi_av_insert(_endpoint->_addressVector.get(), _clientConnect._addressBytes, 1, &_addrVector, 0U, nullptr);
        if (res != 1) // Returns number of addresses inserted
        {
            DEBUG_LOG("fi_av_insert: %d", res);
        }

        const auto peerAddress = getAddressAsString(*_endpoint->_addressVector, _clientConnect._addressBytes);
        DEBUG_LOG("Peer address: %s", peerAddress.c_str());

        char addrBuf[128]{};
        size_t addrLen = sizeof(addrBuf);
        res = fi_getname(toFid(_endpoint->_endpoint), addrBuf, &addrLen);
        if (res == 0)
        {
            const auto localAddress = getAddressAsString(*_endpoint->_addressVector, addrBuf);
            DEBUG_LOG("Local address: %s", localAddress.c_str());            
        }
    }

    void createData()
    {
        _message = std::make_unique<char[]>(_options._frameSize);
        const auto res = fi_mr_reg(_adapter->_domain.get(), _message.get(), _options._frameSize, FI_SEND, 0, _key++, 0,
                                   makeOutPointer(_memoryRegion), (EfaProgressCallback*)this);
        if (res != 0)
        {
            throw rdma_error{"fi_mr_reg", res};
        }
    }

    void onCompletion(uint64_t flags, size_t length) noexcept override
    {
        _completionQueue.enqueue({0, flags, length});
    }

    void onError(int errorCode) noexcept override
    {
        _completionQueue.enqueue({errorCode, 0, 0});
    }

    void sendLoop()
    {
        using namespace std::chrono;
        auto nextTimePoint = steady_clock::now() + milliseconds{_options._intervalMs};
        unsigned numMessageSent = 0;

        _progress->addEndpoint(_endpoint, this);
        unsigned attempts = 0;

        while (!_stopped)
        {
            ssize_t result = fi_send(_endpoint->_endpoint.get(), _message.get(), _options._frameSize,
                                     fi_mr_desc(_memoryRegion.get()), _addrVector, (EfaProgressCallback*)this);
            if (result == 0)
            {
                _progress->postWork(_endpoint, 1);
                attempts = 0;
            }
            else
            {
                DEBUG_LOG("fi_send result: %zd", result);
            }

            if (result == -FI_EAGAIN)
            {
                attempts++;
                if (attempts > 100)
                {
                    DEBUG_LOG("fi_send: EAGAIN");
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds{1});
                continue;
            }
            else if (result < 0)
            {
                break;
            }

            CompletionEntry cqe;
            const bool gotEntry = _completionQueue.wait_dequeue_timed(cqe, 1'000 * 2000); // 200 ms
            if (!gotEntry)
            {
                DEBUG_LOG("Timeout waiting for completion");
                break;
            }
            if (cqe.errorCode == 0xDEAD)
            {
                DEBUG_LOG("Received EOS entry");
                break;
            }
            else if (cqe.errorCode != 0)
            {
                DEBUG_LOG("Error on send: %s (%d)", fi_strerror(cqe.errorCode), cqe.errorCode);
                break;
            }
            else if (_options._verbose)
            {
                ++numMessageSent;
                DEBUG_LOG("Sent completed: %u [f=%lu l=%zu id=%u]", numMessageSent, cqe.flags, cqe.length, _peerId);
            }
    
            std::this_thread::sleep_until(nextTimePoint);
            nextTimePoint = nextTimePoint + milliseconds{_options._intervalMs};
        }

        _progress->removeEndpoint(_endpoint);
    }

private:
    asio::io_context& _ctx;
    asio::ip::tcp::socket _socket;
    const AppOptions _options;

    asio::streambuf _dynamicBuf;

    EfaControlMessage<EfaServerRejectV1> _rejectMessage;
    EfaControlMessage<EfaServerAcceptV1> _acceptMessage;
    EfaClientConnectV1 _clientConnect;

    std::shared_ptr<RdmaAdapter> _adapter;

    fi_addr_t _addrVector;

    struct CompletionEntry
    {
        int errorCode;
        uint64_t flags;
        size_t length;
    };
    moodycamel::BlockingConcurrentQueue<CompletionEntry> _completionQueue;

    std::unique_ptr<char[]> _message;
    std::unique_ptr<fid_mr> _memoryRegion;
    std::shared_ptr<EfaProgressEngine> _progress;
    std::shared_ptr<RdmEndpoint> _endpoint;
    std::thread _sendingThread;

    static std::atomic<uint64_t> _key;
    std::atomic<bool> _stopped = false;
};

std::atomic<uint64_t> Peer::_key{1};

class App : public std::enable_shared_from_this<App>
{
public:
    App(AppOptions options, asio::io_context& ctx)
        : _options{std::move(options)},
          _ctx{ctx},
          _acceptor{_ctx}
    {}

    void start()
    {
        // TODO: go through all fi_info (->next) and create EfaAdapter from all of them
        std::shared_ptr<fi_info> hints = createFabricInfoHintsRdm("");
        int res = fi_getinfo(FABRIC_VERSION, nullptr, nullptr, 0U, hints.get(), makeOutPointer(_fabricInfo));
        if (res != 0 || !_fabricInfo)
        {
            throw rdma_error{"fi_getinfo", res};
        }
        DEBUG_LOG("Provider: %s", _fabricInfo->fabric_attr->prov_name);
        DEBUG_LOG("Fabric address: %s", getFabricLocalAddressAsString(*_fabricInfo).c_str());

        _progress = std::make_shared<EfaProgressEngine>();

        _acceptor.open(asio::ip::tcp::v4());
        _acceptor.set_option(asio::ip::tcp::acceptor::reuse_address(true));
        _acceptor.bind({asio::ip::tcp::v4(), _options._port});
        _acceptor.listen();
        acceptNext();
    }

    void stop()
    {
        _stopped = true;
        _acceptor.cancel();
        for (auto& peer : _peers)
        {
            if (auto p = peer.lock())
            {
                p->stop();
            }
        }
    }

private:
    void acceptNext()
    {
        _acceptor.async_accept([this](std::error_code ec, asio::ip::tcp::socket socket) {
            if (_stopped)
            {
                return;
            }

            if (!ec)
            {
                auto peer =
                    std::make_shared<Peer>(_ctx, std::move(socket), _fabricInfo, _progress, _options);
                _peers.push_back(peer);
                peer->start();
            }

            acceptNext();
        });
    }

private:
    const AppOptions _options;
    asio::io_context& _ctx;
    asio::ip::tcp::acceptor _acceptor;
    std::vector<std::weak_ptr<Peer>> _peers;
    std::shared_ptr<fi_info> _fabricInfo;
    std::shared_ptr<EfaProgressEngine> _progress;
    bool _stopped = false;
};

int main(int argc, char* argv[])
{
    setenv("FI_UNIVERSE_SIZE", "4", 1);
    setenv("FI_EFA_ENABLE_SHM_TRANSFER", "0", 1);
    
    AppOptions options;

    int opt;
    while ((opt = getopt(argc, argv, "a:B:p:f:vs:t:")) != -1)
    {
        switch (opt)
        {
        ///case 'a':
        ///    options._address = optarg;
        ///    break;
        case 'B':
            options._port = (uint16_t)std::atoi(optarg);
            break;
        case 'p':
            options._providerName = optarg;
            break;
        //case 'f':
        //    options._flowId = optarg;
        //    break;
        case 'v':
           options._verbose = true;
           break;
        case 's':
            options._frameSize = (unsigned)std::atoi(optarg);
            break;
        case 't':
            options._intervalMs = std::atoi(optarg);
            break;
        case '?':
            std::fprintf(stderr, "Unknown option: %c\n", opt);
            return 1;
        default:
            return 1;
        }
    }

    try
    {
        asio::io_context ctx;
        App app{options, ctx};

        asio::signal_set signals{ctx};
        signals.add(SIGINT);
#if defined(SIGBREAK)
        signals.add(SIBREAK);
#endif
        signals.async_wait([&](const std::error_code&, int) {
            app.stop();
        });

        app.start();
        ctx.run();
    }
    catch (const std::exception& ex)
    {
        std::fprintf(stderr, "EXCEPTION on main thread: %s\n", ex.what());
    }
}
