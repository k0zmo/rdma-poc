#include "rdma_defs.h"
#include "rdma_types.h"
#include "getopt.h"

#include <asio/buffer.hpp>
#include <asio/completion_condition.hpp>
#include <asio/ip/address.hpp>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include <moodycamel/blockingconcurrentqueue.h>

#include <asio/io_context.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/post.hpp>
#include <asio/read.hpp>
#include <asio/signal_set.hpp>
#include <asio/streambuf.hpp>
#include <asio/write.hpp>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <exception>
#include <system_error>
#include <thread>
#include <stdlib.h>
#include <string>
#include <memory>
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

/*
 * client:
 *   u32 addrFormat
 *   u32 addrLength // minimum 16 for sockaddr
 *   u8 addrBytes[addrLength]
 *   u8 flowIdentifier[16];
 *   u32 expectedFrameSize;
 *   u32 state; // paused/sending
 *
 *   minimum expected size: 4 + 4 + 16 + 16 + 4 + 4 = 48
 */

template <typename T>
void copyFromBuffer(asio::const_buffer& inout_buf, T& in_value)
{
    static_assert(std::is_trivially_copy_assignable_v<T>, "");
    std::memcpy(&in_value, inout_buf.data(), sizeof(T));
    inout_buf += sizeof(T);
}

void copyFromBuffer(asio::const_buffer& inout_buf, uint8_t* in_value, size_t in_length)
{
    std::memcpy(in_value, inout_buf.data(), in_length);
    inout_buf += in_length;
}

class Peer : public std::enable_shared_from_this<Peer>
{
public:
    Peer(asio::io_context& ctx,
         asio::ip::tcp::socket socket,
         std::shared_ptr<RdmaAdapter> adapter,
         AppOptions options)
        : _ctx{ctx},
          _socket{std::move(socket)},
          _options{std::move(options)},
          _adapter{std::move(adapter)}
    {
    }

    ~Peer()
    {
        _stopped = true;
        if (_sendingThread.joinable())
            _sendingThread.join();
    }

    void start()
    {
        asio::async_read(_socket, _buf.prepare(512), asio::transfer_at_least(48),
                         [self = shared_from_this()](const auto& errorCode, size_t bytesXfer) {
                             self->onAddrFormatAndLengthReceived(errorCode, bytesXfer);
                         });
    }

    void stop()
    {
        _stopped = true;
        _socket.cancel();
        if (_sendingThread.joinable())
            _sendingThread.join();
    }

private:
    void onAddrFormatAndLengthReceived(const std::error_code& errorCode, size_t bytesXfer)
    {
        if (_stopped)
            return;

        if (errorCode)
        {
            DEBUG_LOG("Error reading client requiest: %s", errorCode.message().c_str());
            return;
        }
        
        if (bytesXfer < 48)
        {
            DEBUG_LOG("Received too small message: %zu", bytesXfer);
            return;
        }

        _buf.commit(bytesXfer);
        asio::const_buffer buf = _buf.data();

        copyFromBuffer(buf, _addrFormat);
        copyFromBuffer(buf, _addrLen);

        if (_addrFormat != _adapter->_fabricInfo->addr_format)
        {
            DEBUG_LOG("Address format mismatch - %u vs %u\n", _addrFormat, _adapter->_fabricInfo->addr_format);
            return;
        }
        if (_addrLen > 256)
        {
            DEBUG_LOG("Address length is too big: %u", _addrLen);
            return;
        }

        if (buf.size() < _addrLen + 16 + 4 + 4)
        {
            // We need more data
            asio::async_read(_socket, _buf.prepare(512), asio::transfer_at_least(48 - 16 + _addrLen),
                             [self = shared_from_this()](const auto& errorCode, size_t bytesXfer) {
                                 self->onAddrFormatAndLengthReceived(errorCode, bytesXfer);
                             });
            return;
        }

        _addrBytes = std::make_unique<uint8_t[]>(_addrLen);
        copyFromBuffer(buf, _addrBytes.get(), _addrLen);

        assert(buf.size() >= 16 + 4 + 4);

        // std::memcpy(&_flowId, buf.data(), 16);
        // buf += 16;
        // std::memcpy(&_expectedFrameSize, buf.data(), 4);
        // buf += 4;
        // std::memcpy(&_state, buf.data(), 4);
        // buf += 4;

        const auto numConsumed = reinterpret_cast<const uint8_t*>(buf.data()) - 
                                 reinterpret_cast<const uint8_t*>(_buf.data().data());

        _buf.consume(numConsumed);

        try
        {
            createRdmEndpoint();
            createData();
            startSending();
        }
        catch (const std::exception& ex)
        {
            DEBUG_LOG("EXCEPTION: %s", ex.what());
            asio::post(_ctx, [self = shared_from_this()]() { self->stop(); });
            return;
        }
    }

    void createRdmEndpoint()
    {
        _endpoint = std::make_shared<RdmEndpoint>(*_adapter);

        auto res = fi_av_insert(_endpoint->_addressVector.get(), _addrBytes.get(), 1, &_addrVector, 0U, nullptr);
        DEBUG_LOG("fi_av_insert: %d", res);
        // 1 on correct address, 0 otherwise

        const auto peerAddress = getAddressAsString(*_endpoint->_addressVector, _addrBytes.get());
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
                                   makeOutPointer(_memoryRegion), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_mr_reg", res};
        }
    }

    void startSending()
    {
        _sendingThread = std::thread{[self = shared_from_this()]() mutable {
            self->sendLoop();
            self.reset();
        }};
    }

/**
 * When attempting to execute an OFI operation we need to handle
 * resource overrun cases. When a call to an OFI OP fails with -FI_EAGAIN
 * the OFI mtl/btl will attempt to progress any pending Completion Queue
 * events that may prevent additional operations to be enqueued.
 * If the call to ofi progress is successful, then the function call
 * will be retried.
 */
 #define OFI_RETRY_UNTIL_DONE(FUNC, RETURN)             \
 do {                                               \
     do {                                           \
         RETURN = FUNC;                             \
         if (OPAL_LIKELY(0 == RETURN)) {break;}     \
         if (OPAL_LIKELY(RETURN == -FI_EAGAIN)) {   \
             opal_progress();                       \
         }                                          \
     } while (OPAL_LIKELY(-FI_EAGAIN == RETURN));   \
 } while (0);

    void sendLoop()
    {
        using namespace std::chrono;
        auto nextTimePoint = steady_clock::now() + milliseconds{_options._intervalMs};
        unsigned numMessageSent = 0;

        while (!_stopped)
        {
            ssize_t result = fi_send(_endpoint->_endpoint.get(), _message.get(), _options._frameSize,
                                     fi_mr_desc(_memoryRegion.get()), _addrVector, nullptr);
            if (result != 0)
            {
                DEBUG_LOG("fi_send result: %zd", result);
            }
            if (result == -FI_EAGAIN)
            {
                std::this_thread::sleep_for(std::chrono::milliseconds{1});

                // jesli chcemy miec taki "progress", to trzeba miec callbacki bo nie wiadomo kto zostanie tutaj zakonczony

                //fi_cq_msg_entry entry;
                //fi_cq_read(_endpoint->_completionQueue.get(), &entry, 1);

                // if (providerName.find("ofi_rxm") != std::string::npos || 
                //     providerName.find("tcp") != std::string::npos)
                // {
                //     // Call fi_send again
                //     // Sprawdzic co robia inni ale chyba wolaja 'progress'
                continue;
                // }
            }
            else if (result < 0)
            {
                break;
            }
    
            fi_cq_msg_entry entry;
            while (true)
            {
                result = fi_cq_read(_endpoint->_completionQueue.get(), &entry, 1);
                if (result == 1)
                {
                    if (_options._verbose)
                        DEBUG_LOG("Sent completed: %u [f=%lu l=%zu]", numMessageSent, entry.flags, entry.len);
                    break;
                }
                else if (_stopped)
                {
                    break;
                }
            }
    
            std::this_thread::sleep_until(nextTimePoint);
            nextTimePoint = nextTimePoint + milliseconds{_options._intervalMs};
            ++numMessageSent;
        }
    }

private:
    asio::io_context& _ctx;
    asio::ip::tcp::socket _socket;
    const AppOptions _options;

    asio::streambuf _buf;

    uint32_t _addrFormat = 0;
    uint32_t _addrLen = 0;
    std::unique_ptr<uint8_t[]> _addrBytes;
    fi_addr_t _addrVector;

    std::shared_ptr<RdmaAdapter> _adapter;
    std::shared_ptr<RdmEndpoint> _endpoint;
    std::unique_ptr<char[]> _message;
    std::unique_ptr<fid_mr> _memoryRegion;
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
        std::shared_ptr<fi_info> fabricInfo;
        int res = fi_getinfo(FABRIC_VERSION, nullptr, nullptr, 0U, hints.get(), makeOutPointer(fabricInfo));
        if (res != 0 || !fabricInfo)
        {
            throw rdma_error{"fi_getinfo", res};
        }
        DEBUG_LOG("Provider: %s", fabricInfo->fabric_attr->prov_name);
        DEBUG_LOG("Fabric address: %s", getFabricLocalAddressAsString(*fabricInfo).c_str());

        _adapter = std::make_shared<RdmaAdapter>(std::move(fabricInfo));
    
        // RdmaAdapter adapter{app.fabricInfo};
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
        _acceptor.async_accept(
            [this](std::error_code ec, asio::ip::tcp::socket socket) {
                if (_stopped)
                    return;

                if (!ec)
                {
                    auto peer = std::make_shared<Peer>(_ctx, std::move(socket), _adapter, _options);
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
    std::shared_ptr<RdmaAdapter> _adapter;
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
