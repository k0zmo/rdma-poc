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

#include <signal.h>

#include <chrono>
#include <cstring>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <chrono>
#include <thread>

struct AppOptions
{
    std::string _address{"172.19.41.49"};
    std::string _port{"8001"};
    std::string _providerName{"verbs"};
    std::string _localAddress{};
    std::string _flowId{};
    int _numMessages{100};
    int _sleepTime{0};
    bool _verbose{false};
};

bool quit = false;
std::atomic<bool> stopped = false;

void sighandler(int sig)
{
    (void)sig;
    quit = true;
    stopped = true;
}

enum class WaitResult
{
    GOT_MESSAGE,
    GOT_ERROR,
    SHUTDOWN,
    TIMEOUT
};

#ifdef _WIN32
#define DEBUG_LOG_LOCALTIME(tm, time) ::localtime_s(&tm, &time);
#else
#define DEBUG_LOG_LOCALTIME(tm, time) ::localtime_r(&time, &tm);
#endif

void handleConnected(RdmaEndpoint& in_endpoint, std::uint32_t in_frameSize, const AppOptions& in_cfg)
{
    //  0-10
    // 11-20
    // 21-30 ...
    unsigned hist[17] = {};

    static constexpr std::chrono::milliseconds SEND_TIMEOUT = std::chrono::seconds{2};
    static constexpr std::chrono::milliseconds RECEIVE_TIMEOUT = std::chrono::milliseconds{1500};
    static constexpr std::uint64_t SEND_COMPLETION_FLAGS = FI_SEND | FI_MSG;
    static constexpr std::uint64_t RECV_COMPLETION_FLAGS = FI_RECV | FI_MSG;

    const size_t messageSize = sizeof(FrameInformation) +
                               in_frameSize + // contains FrameBufferHeader at the head
                               sizeof(FrameBufferHeader);

    std::unique_ptr<char[]> buf = std::make_unique<char[]>(messageSize);
    std::unique_ptr<fid_mr> memoryRegion;
    int res = fi_mr_reg(in_endpoint._domain.get(), buf.get(), messageSize, FI_RECV, 0, 0, 0,
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

    using namespace std::chrono;
    steady_clock::time_point before = steady_clock::now();
    steady_clock::time_point accDataTp = before;
    std::uint64_t accDataReceived = 0;

    while (!stopped)
    {
        ssize_t ret = fi_recv(in_endpoint._endpoint.get(), buf.get(), messageSize, fi_mr_desc(memoryRegion.get()),
                              FI_ADDR_UNSPEC, nullptr);
        if (ret != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(ret)};
        }
        in_endpoint.sendEmptyMessage();

        WaitResult waitResult = WaitResult::TIMEOUT;
        bool recvCompleted = false, sendCompleted = false;
        milliseconds completionTimeout = SEND_TIMEOUT;
        uint32_t sendCompletionTimeMs = 0, recvCompletionTimeMs = 0;
        std::size_t bytesTransferred = 0;

        while (true)
        {
            // Wait for both completions (send+recv) but no more than 2 seconds in total
            auto waitingStart = steady_clock::now();
            fi_cq_msg_entry entry;
            ret = fi_cq_sread(in_endpoint._completionQueue.get(), &entry, 1, nullptr, static_cast<int>(completionTimeout.count()));
            if (ret == 1)
            {
                if ((entry.flags & SEND_COMPLETION_FLAGS) == SEND_COMPLETION_FLAGS)
                {
                    sendCompleted = true;
                    completionTimeout = RECEIVE_TIMEOUT;
                    sendCompletionTimeMs = duration_cast<milliseconds>(steady_clock::now() - waitingStart).count();
                    waitingStart = steady_clock::now();
                }
                else if ((entry.flags & RECV_COMPLETION_FLAGS) == RECV_COMPLETION_FLAGS)
                {
                    recvCompleted = true;
                    bytesTransferred = entry.len;
                    accDataReceived += bytesTransferred;
                    recvCompletionTimeMs = duration_cast<milliseconds>(steady_clock::now() - waitingStart).count();
                    waitingStart = steady_clock::now();
                }

                // Both send and receive were completed, we got the message
                if (sendCompleted && recvCompleted)
                {
                    waitResult = WaitResult::GOT_MESSAGE;
                    break;
                }
                else if (recvCompleted) // receive completed first
                {
                    std::cout << "Receive completed before send?!\n";
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
                std::cout << "Timeout receiving a message from sender (recvCompleted =" << recvCompleted << " ("
                          << recvCompletionTimeMs << "), sendCompleted = " << sendCompleted << " ("
                          << sendCompletionTimeMs << ")"
                          << "\n";
            }
            quit = true;
            break;
        }

        numMessagesReceived += 1;

        FrameInformation* fi = reinterpret_cast<FrameInformation*>(buf.get());
        FrameBufferHeader* fbh = reinterpret_cast<FrameBufferHeader*>(buf.get() + sizeof(FrameInformation));
        FrameBufferHeader* fbhTail = reinterpret_cast<FrameBufferHeader*>(buf.get() + sizeof(FrameInformation) + in_frameSize);

        const auto tp = system_clock::now();
        const auto millis = duration_cast<milliseconds>(tp.time_since_epoch()).count() % 1000LL;
        std::time_t time_tt = system_clock::to_time_t(tp);
        std::tm t{};
        DEBUG_LOG_LOCALTIME(t, time_tt)
        char buffer[256];
        auto len = strftime(buffer, sizeof(buffer), "%H:%M:%S", &t);
        len += std::sprintf(buffer + len, ".%03u", static_cast<unsigned>(millis));

        const auto now = steady_clock::now();
        const auto diff = now - before;
        before = now;
        const auto diffMs = duration_cast<milliseconds>(diff).count();

        if (diffMs <= 10)
            ++hist[0];
        else if (diffMs <= 20)
            ++hist[1];
        else if (diffMs <= 20)
            ++hist[2];
        else if (diffMs <= 30)
            ++hist[3];
        else if (diffMs <= 40)
            ++hist[4];
        else if (diffMs <= 50)
            ++hist[5];
        else if (diffMs <= 60)
            ++hist[6];
        else if (diffMs <= 70)
            ++hist[7];
        else if (diffMs <= 80)
            ++hist[8];
        else if (diffMs <= 90)
            ++hist[9];
        else if (diffMs <= 100)
            ++hist[10];
        else if (diffMs <= 110)
            ++hist[11];
        else if (diffMs <= 120)
            ++hist[12];
        else if (diffMs <= 130)
            ++hist[13];
        else if (diffMs <= 140)
            ++hist[14];
        else if (diffMs <= 150)
            ++hist[15];
        else
            ++hist[16];

        const bool invalidMessage =
            fbh->_usageCounter != fi->_bufferUsageCount ||
            fbhTail->_usageCounter != fi->_bufferUsageCount;

        if (in_cfg._verbose || diffMs > 40 || invalidMessage)
        {
            std::cout << buffer << "  Got " << numMessagesReceived << "th message: " << bytesTransferred;
            std::cout << ", frameIndex: " << fi->_frameIndex;
            std::cout << ", flags: " << fi->_flags;
            std::cout << ", sendWait: " << sendCompletionTimeMs;
            std::cout << ", recvWait: " << recvCompletionTimeMs;
            std::cout << ", diff: " << diffMs;
            if (diffMs > 40)
                std::cout << " (!)";
            if (invalidMessage)
                std::cout << " (received message became invalid!)";
            std::cout << std::endl;
        }

        if (now - accDataTp >= seconds{4})
        {
            const auto accDiff = duration_cast<milliseconds>(now - accDataTp).count();
            const auto mbps = (accDataReceived * 8ull) / accDiff / 1000;
            std::cout << "Bandwidth: " << mbps * 0.001 << " Gbps\n";
            accDataReceived = 0;
            accDataTp = now;
        }

        if (in_cfg._sleepTime > 0)
        {
            std::this_thread::sleep_for(milliseconds{in_cfg._sleepTime});
        }

        if (in_cfg._numMessages > 0 && numMessagesReceived >= in_cfg._numMessages)
        {
            std::cout << "Sent " << in_cfg._numMessages << ". Quitting\n";
            quit = true;
            break;
        }
    }

    const auto now = steady_clock::now();
    const auto accDiff = duration_cast<milliseconds>(now - accDataTp).count();
    const auto mbps = (accDataReceived * 8ull) / accDiff / 1000;
    std::cout << "Bandwidth: " << mbps * 0.001 << " Gbps\n";
    std::cout << "Histogram:\n";
    for (unsigned i = 0u; i < std::size(hist); ++i)
    {
        std::cout << "[" << i*10 + (i > 0 ? 1 : 0) << "-" << (i+1) * 10 << "ms]: " << hist[i] << "\n";
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

    if (!in_cfg._flowId.empty())
    {
        auto& fi = clientData._flowIdentifier;
        std::memset(&clientData._flowIdentifier, 0, sizeof(clientData._flowIdentifier));
        (void)std::sscanf( // Dont bother validating it
            in_cfg._flowId.c_str(),
            "%02hhx%02hhx%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx-%02hhx%02hhx%02hhx%02hhx%02hhx%02hhx",
            &fi[0], &fi[1], &fi[2], &fi[3],
            &fi[4], &fi[5],
            &fi[6], &fi[7],
            &fi[8], &fi[9],
            &fi[10], &fi[11], &fi[12], &fi[13], &fi[14], &fi[15]);
    }
    else
    {
        // "e569f502-8891-4c9f-92d4-51702b158bd5";
        static constexpr uint8_t DEFAULT_FLOAT_ID[] = {
            0xe5, 0x69, 0xf5, 0x02,
            0x88, 0x91,
            0x4c, 0x9f,
            0x92, 0xd4,
            0x51, 0x70, 0x2b, 0x15, 0x8b, 0xd5
        };
        std::memcpy(&clientData._flowIdentifier, DEFAULT_FLOAT_ID, sizeof(clientData._flowIdentifier));
    }

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
        const ssize_t res = fi_eq_sread(ep._eventQueue.get(), &event, entry, maxEntrySize, 2000, 0);
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
        handleConnected(ep, serverData._frameSize, in_cfg);
    }
}

int main(int argc, char* argv[])
{
    signal(SIGINT, &sighandler);
#ifdef SIGBREAK
    signal(SIGBREAK, &sighandler);
#endif

    AppOptions options;

    int opt;
    while ((opt = getopt(argc, argv, "a:B:p:n:I:f:s:v")) != -1)
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
        case 'f':
            options._flowId = optarg;
            break;
        case 's':
            options._sleepTime = std::atoi(optarg);
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

    try
    {
        run(options);
    }
    catch (const std::exception& ex)
    {
        std::cerr << "EXCEPTION on main thread: " << ex.what() << std::endl;
    }
}
