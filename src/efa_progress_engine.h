#pragma once

#include "rdma_types.h"

#include <cassert>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#include <algorithm>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

class EfaProgressCallback
{
public:
    virtual void onCompletion(uint64_t flags, size_t length) noexcept = 0;
    virtual void onError(int errorCode) noexcept                   = 0;

protected:
    ~EfaProgressCallback() = default;
};

// alternative names: EfaCompletionPoller, a ProgressEngine moze byc czyms wyzej
// wtedy jaka nazwa na EfaProgress? EfaCompletionSource

// class EfaProgress
// {
// public:
//     virtual ~EfaProgress() = default;
//     virtual void                         onCompletion(uint64_t flags, size_t length) noexcept = 0;
//     virtual void                         onError(int errorCode) noexcept                      = 0;
//     virtual std::shared_ptr<RdmEndpoint> getEndpoint()                                        = 0;
//     virtual std::shared_ptr<fid_cq>      getCompletionQueue()                                 = 0;
// };

class EfaProgressEngine final
{
public:
    static constexpr uint32_t MAX_COMPLETION_ENTRY_PROGRESS = 4;

    EfaProgressEngine() :
        _stopped{false},
        _workThread{&EfaProgressEngine::threadFunc, this}
    {
    }

    EfaProgressEngine(const EfaProgressEngine&)            = delete;
    EfaProgressEngine& operator=(const EfaProgressEngine&) = delete;

    ~EfaProgressEngine()
    {
        {
            std::lock_guard lock{_mtx};
            _stopped = true;
        }
        _condVar.notify_one();

        if (_workThread.joinable())
        {
            _workThread.join();
        }
    }

    void postWork(const std::shared_ptr<RdmEndpoint>& endpoint, uint32_t count = 1)
    {
        const bool needNotification = [&] {
            std::lock_guard lock{_mtx};
            const auto      it = findEndpoint(endpoint);
            if (it == _endpoints.end())
            {
                return false;
            }
            const auto prevOutstandingWork = it->_outstandingWork;
            it->_outstandingWork += count;
            return prevOutstandingWork == 0;
        }();

        if (needNotification)
        {
            _condVar.notify_one();
        }
    }

    void addEndpoint(std::shared_ptr<RdmEndpoint> endpoint, EfaProgressCallback* callback)
    {
        std::lock_guard lock{_mtx};
        const auto it = findEndpoint(endpoint);
        if (it != _endpoints.end())
        {
            // Already added
            return;
        }
        _endpoints.emplace_back(std::move(endpoint), callback);
    }

    void removeEndpoint(const std::shared_ptr<RdmEndpoint>& endpoint)
    {
        std::lock_guard lock{_mtx};
        const auto it = findEndpoint(endpoint);
        if (it != _endpoints.end())
        {
            _endpoints.erase(it);
        }
    }

    size_t getNumEndpoints() const
    {
        std::lock_guard lock{_mtx};
        return _endpoints.size();
    }

private:
    uint32_t getNumOutstandingWorkNoLock() const
    {
        uint32_t total = 0;
        for (const auto& ep : _endpoints)
        {
            total += ep._outstandingWork;
        }
        return total;
    }

    void threadFunc()
    {
        fi_cq_msg_entry entry[MAX_COMPLETION_ENTRY_PROGRESS];
        while (true)
        {
            std::unique_lock lock{_mtx};
            _condVar.wait(lock, [this] { return getNumOutstandingWorkNoLock() > 0 || _stopped; });
            if (_stopped)
            {
                return;
            }

            for (auto& ep : _endpoints)
            {
                if (ep._outstandingWork == 0)
                {
                    continue;
                }
                const int n = fi_cq_read(
                    ep._endpoint->_completionQueue.get(), &entry, MAX_COMPLETION_ENTRY_PROGRESS);
                if (n > 0)
                {
                    ep._outstandingWork -= n;

                    for (int i = 0; i < n; ++i)
                    {
                        ep._callback->onCompletion(entry[i].flags, entry[i].len);                        
                    }
                }
                else if (n != -FI_EAGAIN && n != -FI_EINTR)
                {
                    fi_cq_err_entry errEntry;
                    const auto      ret =
                        fi_cq_readerr(ep._endpoint->_completionQueue.get(), &errEntry, 0);
                    if (ret < 0)
                    {
                        // Could happen if there's another progress engine, polling the same CQ
                        // We don't do it but let's ignore that warning for now.
                        // Going through the libfabric code, it's the only error that may be
                        // returned from fi_cq_readerr
                        assert(ret == -FI_EAGAIN);
                    }

                    ep._outstandingWork -= 1;
                    ep._callback->onError(errEntry.err);
                }
            }
        }
    }

private:
    struct Endpoint
    {
        Endpoint(std::shared_ptr<RdmEndpoint> endpoint, EfaProgressCallback* callback)
            : _endpoint{std::move(endpoint)}
            , _callback{callback}
            , _outstandingWork{0}
        {
            _ccc = uintptr_t(_callback);
        }

        std::shared_ptr<RdmEndpoint> _endpoint;
        EfaProgressCallback* _callback;
        uintptr_t _ccc;
        uint32_t _outstandingWork;
    };

    std::vector<Endpoint>::iterator findEndpoint(const std::shared_ptr<RdmEndpoint>& endpoint)
    {
        return std::find_if(_endpoints.begin(), _endpoints.end(), [&](const Endpoint& ep) {
            return ep._endpoint == endpoint;
        });
    }

    mutable std::mutex _mtx;
    std::condition_variable _condVar;
    std::vector<Endpoint> _endpoints;

    bool _stopped;
    std::thread _workThread;
};
