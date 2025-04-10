#pragma once

#include "rdma_types.h"

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
    virtual void onCompletion(uint64_t flags, size_t len) = 0;
    virtual void onError(int errorCode)                   = 0;

protected:
    ~EfaProgressCallback() = default;
};

class EfaProgressEngine
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

    void postWork(const std::shared_ptr<RdmEndpoint> endpoint, uint32_t count = 1)
    {
        bool needNotification = false;
        {
            std::lock_guard lock{_mtx};
            const auto it =
                std::find_if(_endpoints.begin(), _endpoints.end(), [&](const Endpoint& ep) {
                    return ep._endpoint == endpoint;
                });
            if (it == _endpoints.end())
            {
                return;
            }

            const auto prevOutstandingWork = it->_outstandingWork;
            it->_outstandingWork += count;
            if (prevOutstandingWork == 0)
            {
                needNotification = true;
            }
        }
        if (needNotification)
        {
            _condVar.notify_one();
        }
    }

    void addEndpoint(std::shared_ptr<RdmEndpoint> endpoint)
    {
        std::lock_guard lock{_mtx};
        _endpoints.emplace_back(std::move(endpoint));
    }

    void removeEndpoint(const std::shared_ptr<RdmEndpoint>& endpoint)
    {
        std::lock_guard lock{_mtx};
        auto it = std::find_if(_endpoints.begin(), _endpoints.end(), [&](const Endpoint& ep) {
            return ep._endpoint == endpoint;
        });
        if (it != _endpoints.end())
        {
            _endpoints.erase(it);
        }
    }

private:
    uint32_t numOutstandingWorkNoLock() const
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
            _condVar.wait(lock, [this] { return numOutstandingWorkNoLock() > 0 || _stopped; });
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
                        if (const auto ctx =
                                reinterpret_cast<EfaProgressCallback*>(entry[i].op_context))
                        {
                            // try-catch
                            ctx->onCompletion(entry[i].flags, entry[i].len);
                        }
                    }
                }
                else if (n != -FI_EAGAIN && n != FI_EINTR)
                {
                    fi_cq_err_entry errEntry;
                    const auto      ret =
                        fi_cq_readerr(ep._endpoint->_completionQueue.get(), &errEntry, 0);
                    if (ret < 0)
                    {
                        if (ret == -FI_EAGAIN)
                        {
                            // Could happen if there's another progress engine, polling the same CQ
                            // We don't do it but let's ignore that warning for now
                            continue;
                        }

                        // TODO handle CQ error
                        DEBUG_LOG(
                            "fi_cq_readerr error: %zd on error: %d (%s)", ret, n, fi_strerror(n));
                        continue;
                    }

                    ep._outstandingWork -= 1;

                    if (const auto ctx =
                            reinterpret_cast<EfaProgressCallback*>(errEntry.op_context))
                    {
                        // try-catch
                        ctx->onError(errEntry.err);
                    }
                    else
                    {
                        DEBUG_LOG(
                            "Error on CQ: %s (code: %d)", fi_strerror(errEntry.err), errEntry.err);
                    }
                }
            }
        }
    }

private:
    struct Endpoint
    {
        Endpoint(std::shared_ptr<RdmEndpoint> endpoint)
            : _endpoint{std::move(endpoint)}
            , _outstandingWork{0}
        {
        }

        std::shared_ptr<RdmEndpoint> _endpoint;
        uint32_t _outstandingWork;
    };

    std::mutex _mtx;
    std::condition_variable _condVar;
    std::vector<Endpoint> _endpoints;

    bool _stopped;
    std::thread _workThread;
};
