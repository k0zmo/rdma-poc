#pragma once

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#ifndef _WIN32
#  include <arpa/inet.h>
#  include <netinet/in.h>
#  include <sys/socket.h>
#  include <sys/types.h>
#else
#  include <ws2tcpip.h>
#endif

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string.h>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#define LOG_DEBUG(...)                                                                             \
    do                                                                                             \
    {                                                                                              \
        using namespace std::chrono;                                                               \
        const auto  tp      = system_clock::now();                                                 \
        const auto  millis  = duration_cast<milliseconds>(tp.time_since_epoch()).count() % 1000LL; \
        std::time_t time_tt = system_clock::to_time_t(tp);                                         \
        std::tm     t{};                                                                           \
        ::localtime_r(&time_tt, &t);                                                               \
        char buffer[512];                                                                          \
        auto len = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &t);                      \
        len += std::sprintf(buffer + len, ".%03u ", static_cast<unsigned>(millis));                \
        std::sprintf(buffer + len, __VA_ARGS__);                                                   \
        std::fprintf(stdout, "%s\n", buffer);                                                      \
        std::fflush(stdout);                                                                       \
    } while (0);

inline const auto FABRIC_VERSION = FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION);

// Deleter that works for any type from libfabric but fi_info
template <typename T>
struct fabric_interface_deleter
{
    void operator()(T* pointer)
    {
        if (pointer)
        {
            int res = fi_close(&pointer->fid);
            if (res != 0)
            {
                LOG_DEBUG("fi_close failed: %s (%d)", fi_strerror(res), res);
            }
        }
    }
};

template <>
struct fabric_interface_deleter<fi_info>
{
    void operator()(fi_info* pointer)
    {
        if (pointer)
        {
            fi_freeinfo(pointer);
        }
    }
};

namespace std {

template <> struct default_delete<fi_info>    : fabric_interface_deleter<fi_info>    {};
template <> struct default_delete<fid_fabric> : fabric_interface_deleter<fid_fabric> {};
template <> struct default_delete<fid_domain> : fabric_interface_deleter<fid_domain> {};
template <> struct default_delete<fid_eq>     : fabric_interface_deleter<fid_eq>     {};
template <> struct default_delete<fid_pep>    : fabric_interface_deleter<fid_pep>    {};
template <> struct default_delete<fid_ep>     : fabric_interface_deleter<fid_ep>     {};
template <> struct default_delete<fid_cq>     : fabric_interface_deleter<fid_cq>     {};
template <> struct default_delete<fid_mr>     : fabric_interface_deleter<fid_mr>     {};
template <> struct default_delete<fid_av>     : fabric_interface_deleter<fid_av>     {};
template <> struct default_delete<fid_cntr>   : fabric_interface_deleter<fid_cntr>   {};

} // namespace std

// Utility class for passing out pointer to constructor-like functions.
// Shouldn't be used directly but with `make_out_pointer` wrapper function
// that does the type deduction.
template <typename SmartPointer>
class out_pointer
{
public:
    using element_type = typename SmartPointer::element_type;
    using pointer_type = std::add_pointer_t<element_type>;

    explicit out_pointer(SmartPointer& pointer) :
        smart_pointer_{pointer},
        raw_pointer_{nullptr}
    {
        smart_pointer_.reset();
    }

    ~out_pointer()
    {
        if (raw_pointer_)
        {
            smart_pointer_.reset(raw_pointer_);
        }
    }

    out_pointer(const out_pointer&)            = delete;
    out_pointer& operator=(const out_pointer&) = delete;

    operator pointer_type*() { return &raw_pointer_; }

    operator void**() { return reinterpret_cast<void**>(&raw_pointer_); }

private:
    SmartPointer& smart_pointer_;
    pointer_type  raw_pointer_;
};

template <typename SmartPointer, typename Deleter>
class out_pointer_with_dynamic_deleter
{
public:
    using element_type = typename SmartPointer::element_type;
    using pointer_type = std::add_pointer_t<element_type>;

    explicit out_pointer_with_dynamic_deleter(SmartPointer& pointer, Deleter deleter) :
        smart_pointer_{pointer},
        raw_pointer_{nullptr},
        deleter_{std::move(deleter)}
    {
        smart_pointer_.reset();
    }

    ~out_pointer_with_dynamic_deleter()
    {
        if (raw_pointer_)
        {
            smart_pointer_.reset(raw_pointer_, std::move(deleter_));
        }
    }

    out_pointer_with_dynamic_deleter(const out_pointer_with_dynamic_deleter&)            = delete;
    out_pointer_with_dynamic_deleter& operator=(const out_pointer_with_dynamic_deleter&) = delete;

    operator pointer_type*() { return &raw_pointer_; }

    operator void**() { return reinterpret_cast<void**>(&raw_pointer_); }

private:
    SmartPointer& smart_pointer_;
    pointer_type  raw_pointer_;
    Deleter       deleter_;
};

template <typename SmartPointer>
out_pointer<SmartPointer> make_out_pointer(SmartPointer& pointer)
{
    return out_pointer<SmartPointer>(pointer);
}

template <typename SmartPointer, typename Deleter>
out_pointer_with_dynamic_deleter<SmartPointer, Deleter> make_out_pointer(SmartPointer& pointer,
                                                                         Deleter       deleter)
{
    return out_pointer_with_dynamic_deleter<SmartPointer, Deleter>(pointer, std::move(deleter));
}

template <typename T>
auto make_out_pointer(std::shared_ptr<T>& pointer)
{
    return make_out_pointer(pointer, fabric_interface_deleter<T>{});
}

struct rdma_error : std::runtime_error
{
    explicit rdma_error(const char* function_name, int error_code) :
        std::runtime_error{format_error_message(function_name, error_code)}
    {
    }

private:
    static std::string format_error_message(const char* function_name, int error_code)
    {
        std::stringstream ss;
        ss << function_name << " returned: '" << fi_strerror(error_code) << " (" << error_code
           << ")'";
        return ss.str();
    }
};

template <typename T>
fid_t to_fid(const T& fabric_interface)
{
    return &fabric_interface.get()->fid;
}

inline std::shared_ptr<fi_info> create_fabric_info_hints(const std::string& provider_name,
                                                         const std::string& src_address)
{
    fi_info* raw_hints = fi_allocinfo();
    if (!raw_hints)
    {
        throw rdma_error{"hints is null", -FI_ENOMEM};
    }
    std::shared_ptr<fi_info> hints{raw_hints, fi_freeinfo};

    if (!provider_name.empty())
    {
        hints->fabric_attr->prov_name = strdup(provider_name.c_str());
    }
    hints->ep_attr->type        = FI_EP_MSG;
    hints->caps                 = FI_MSG;
    hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    hints->rx_attr->iov_limit   = 4;
    hints->tx_attr->iov_limit   = 4;

    if (!src_address.empty())
    {
        in_addr src_addr{};
        if (inet_pton(AF_INET, src_address.c_str(), &src_addr) == 1)
        {
            sockaddr_in* addr = reinterpret_cast<sockaddr_in*>(malloc(sizeof(sockaddr_in)));
            memset(addr, 0, sizeof(sockaddr_in));
            addr->sin_addr     = src_addr;
            addr->sin_family   = AF_INET;
            hints->addr_format = FI_SOCKADDR_IN;
            hints->src_addr    = addr;
            hints->src_addrlen = sizeof(sockaddr_in);
        }
        else
        {
            throw rdma_error{"Invalid source address", -FI_ENODATA};
        }
    }

    return hints;
}

inline std::shared_ptr<fi_info> create_fabric_info_hints_rdm(const std::string& provider_name)
{
    fi_info* raw_hints = fi_allocinfo();
    if (!raw_hints)
    {
        throw rdma_error{"hints is null", -FI_ENOMEM};
    }
    std::shared_ptr<fi_info> hints{raw_hints, fi_freeinfo};

    if (!provider_name.empty())
    {
        hints->fabric_attr->prov_name = strdup(provider_name.c_str());
    }
    hints->ep_attr->type         = FI_EP_RDM;
    hints->caps                  = FI_MSG | FI_RECV | FI_SEND | FI_REMOTE_COMM;
    hints->domain_attr->mr_mode  = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    hints->domain_attr->progress = FI_PROGRESS_MANUAL;
    hints->domain_attr->threading = FI_THREAD_SAFE;
    hints->rx_attr->iov_limit     = 4;
    hints->tx_attr->iov_limit     = 4;

    return hints;
}

inline std::shared_ptr<fi_info> get_fabric_info(const std::string& provider_name,
                                                const std::string& dest_address,
                                                const std::string& dest_port,
                                                const std::string& src_address)
{
    std::shared_ptr<fi_info> hints = create_fabric_info_hints(provider_name, src_address);
    std::shared_ptr<fi_info> fabric_info;
    int res = fi_getinfo(FABRIC_VERSION,
                         dest_address.c_str(),
                         dest_port.c_str(),
                         0U,
                         hints.get(),
                         make_out_pointer(fabric_info));
    if (res != 0)
    {
        throw rdma_error{"fi_getinfo", res};
    }

    if (!strcmp(fabric_info->fabric_attr->prov_name, "tcp"))
    {
        fabric_info->domain_attr->mr_mode |= FI_MR_PROV_KEY;
    }

    return fabric_info;
}

inline std::shared_ptr<fi_info> get_fabric_info(const std::string& provider_name,
                                                const std::string& src_address,
                                                const std::string& src_port)
{
    std::shared_ptr<fi_info> hints = create_fabric_info_hints(provider_name, "");
    std::shared_ptr<fi_info> fabric_info;
    int res = fi_getinfo(FABRIC_VERSION,
                         src_address.c_str(),
                         src_port.c_str(),
                         FI_SOURCE,
                         hints.get(),
                         make_out_pointer(fabric_info));
    if (res != 0)
    {
        throw rdma_error{"fi_getinfo", res};
    }

    if (!strcmp(fabric_info->fabric_attr->prov_name, "tcp"))
    {
        fabric_info->domain_attr->mr_mode |= FI_MR_PROV_KEY;
    }

    return fabric_info;
}

struct rdma_adapter
{
    std::shared_ptr<fi_info>    fabric_info_;
    std::shared_ptr<fid_fabric> fabric_;
    std::shared_ptr<fid_domain> domain_;

    explicit rdma_adapter(std::shared_ptr<fi_info> fabric_info) :
        fabric_info_{std::move(fabric_info)}
    {
        int res = fi_fabric(fabric_info_->fabric_attr, make_out_pointer(fabric_), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_fabric", res};
        }

        std::shared_ptr<fid_domain> domain;
        res = fi_domain(fabric_.get(), fabric_info_.get(), make_out_pointer(domain_), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_domain", res};
        }
    }
};

struct rdma_endpoint
{
    std::shared_ptr<fid_domain> domain_;
    std::shared_ptr<fid_fabric> fabric_;

    std::unique_ptr<fid_eq> event_queue_;
    std::unique_ptr<fid_cq> completion_queue_;
    std::unique_ptr<fid_ep> endpoint_; // must be before EQ and CQ

    explicit rdma_endpoint(const rdma_adapter& adapter) :
        rdma_endpoint(adapter, *adapter.fabric_info_)
    {
    }

    rdma_endpoint(const rdma_adapter& adapter, const fi_info& fabric_info) :
        domain_{adapter.domain_},
        fabric_{adapter.fabric_}
    {
        assert(fabric_info.ep_attr->type == FI_EP_MSG);

        int res = fi_endpoint(domain_.get(),
                              const_cast<fi_info*>(&fabric_info),
                              make_out_pointer(endpoint_),
                              nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_endpoint", res};
        }

        fi_eq_attr eq_attrs = {};
        eq_attrs.wait_obj   = FI_WAIT_UNSPEC;
        res = fi_eq_open(fabric_.get(), &eq_attrs, make_out_pointer(event_queue_), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_eq_open", res};
        }

        fi_cq_attr cq_attrs = {};
        cq_attrs.size       = 4; // Derived from the protocol requirements
                                 // (we expect max two completions at given time) times two
        cq_attrs.wait_obj = FI_WAIT_UNSPEC;
        cq_attrs.format   = FI_CQ_FORMAT_MSG;
        res = fi_cq_open(domain_.get(), &cq_attrs, make_out_pointer(completion_queue_), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_cq_open", res};
        }

        res = fi_ep_bind(endpoint_.get(), to_fid(event_queue_), 0);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind to EQ", res};
        }
        res = fi_ep_bind(endpoint_.get(), to_fid(completion_queue_), FI_RECV | FI_SEND);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind to CQ", res};
        }
        // We want to post receive before accepting a connection.
        res = fi_enable(endpoint_.get());
        if (res != 0)
        {
            throw rdma_error{"fi_enable", res};
        }
    }

    void receive_empty_message()
    {
        ssize_t res = fi_recv(endpoint_.get(), nullptr, 0, nullptr, FI_ADDR_UNSPEC, nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(res)};
        }
    }

    void send_empty_message()
    {
        ssize_t res = fi_send(endpoint_.get(), nullptr, 0, nullptr, FI_ADDR_UNSPEC, nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_send", static_cast<int>(res)};
        }
    }

    size_t max_connection_data_size() const
    {
        size_t value        = 0;
        size_t value_length = sizeof(value);
        int    res          = fi_getopt(
            to_fid(endpoint_), FI_OPT_ENDPOINT, FI_OPT_CM_DATA_SIZE, &value, &value_length);
        if (res != 0)
        {
            throw rdma_error{"fi_getopt", res};
        }
        return value;
    }
};

struct rdma_listening_endpoint
{
    std::shared_ptr<fid_fabric> fabric_;

    std::unique_ptr<fid_eq>  event_queue_;
    std::unique_ptr<fid_pep> passive_endpoint_; // must be before EQ

    explicit rdma_listening_endpoint(const rdma_adapter& adapter) :
        fabric_{adapter.fabric_}
    {
        assert(adapter.fabric_info_->ep_attr->type == FI_EP_MSG);

        int res = fi_passive_ep(fabric_.get(),
                                adapter.fabric_info_.get(),
                                make_out_pointer(passive_endpoint_),
                                nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_passive_ep", res};
        }

        fi_eq_attr eq_attr = {};
        eq_attr.wait_obj   = FI_WAIT_UNSPEC;
        eq_attr.flags      = FI_WRITE; // We'll insert custom events into EQ
        res = fi_eq_open(fabric_.get(), &eq_attr, make_out_pointer(event_queue_), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_eq_open", res};
        }

        res = fi_pep_bind(passive_endpoint_.get(), to_fid(event_queue_), 0);
        if (res != 0)
        {
            throw rdma_error{"fi_pep_bind", res};
        }

        res = fi_listen(passive_endpoint_.get());
        if (res != 0)
        {
            throw rdma_error{"fi_listen", res};
        }
    }

    size_t max_connection_data_size() const
    {
        size_t value        = 0;
        size_t value_length = sizeof(value);
        int    res          = fi_getopt(
            to_fid(passive_endpoint_), FI_OPT_ENDPOINT, FI_OPT_CM_DATA_SIZE, &value, &value_length);
        if (res != 0)
        {
            throw rdma_error{"fi_getopt", res};
        }
        return value;
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////

struct efa_fabric
{
    std::shared_ptr<fi_info>    fabric_info_;
    std::shared_ptr<fid_fabric> fabric_;

    explicit efa_fabric(std::shared_ptr<fi_info> fabric_info) :
        fabric_info_{std::move(fabric_info)}
    {
        int res = fi_fabric(fabric_info_->fabric_attr, make_out_pointer(fabric_), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_fabric", res};
        }
    }
};

class efa_completion_callback
{
public:
    virtual void on_completion(uint64_t flags, size_t length) noexcept = 0;

protected:
    ~efa_completion_callback() {}
};

class efa_progressable : public efa_completion_callback
{
public:
    virtual ~efa_progressable() {}

    virtual void    on_error(int errorCode) noexcept = 0;
    virtual fid_cq& completion_queue() const         = 0;
};

class efa_execution_context final
{
public:
    static constexpr uint32_t max_completion_entry_progress = 4;

    efa_execution_context() :
        stopped_{false},
        work_thread_{&efa_execution_context::thread_func, this}
    {
    }

    efa_execution_context(const efa_execution_context&)            = delete;
    efa_execution_context& operator=(const efa_execution_context&) = delete;

    ~efa_execution_context()
    {
        {
            std::lock_guard lock{mtx_};
            stopped_ = true;
        }
        cond_var_.notify_one();

        if (work_thread_.joinable())
        {
            work_thread_.join();
        }
    }

    void post_work(const std::shared_ptr<efa_progressable>& progressable, uint32_t count = 1)
    {
        const bool need_notification = [&] {
            std::lock_guard lock{mtx_};
            const auto      it = find_progressable_context(progressable);
            if (it == progressables_.end())
            {
                return false;
            }
            const auto prev_outstanding_work = it->pending_ops_;
            it->pending_ops_ += count;
            return prev_outstanding_work == 0;
        }();

        if (need_notification)
        {
            cond_var_.notify_one();
        }
    }

    void add_progressable(std::shared_ptr<efa_progressable> progressable)
    {
        std::lock_guard lock{mtx_};
        const auto      it = find_progressable_context(progressable);
        if (it != progressables_.end())
        {
            // Already added
            return;
        }
        progressables_.emplace_back(std::move(progressable));
    }

    void remove_progressable(const std::shared_ptr<efa_progressable>& progressable)
    {
        std::lock_guard lock{mtx_};
        const auto      it = find_progressable_context(progressable);
        if (it != progressables_.end())
        {
            progressables_.erase(it);
        }
    }

    size_t num_progressables() const
    {
        std::lock_guard lock{mtx_};
        return progressables_.size();
    }

private:
    uint32_t getNumPendingOps() const
    {
        uint32_t total = 0;
        for (const auto& ep : progressables_)
        {
            total += ep.pending_ops_;
        }
        return total;
    }

    void thread_func()
    {
        fi_cq_msg_entry entry[max_completion_entry_progress] = {};
        while (true)
        {
            std::unique_lock lock{mtx_};
            cond_var_.wait(lock, [this] { return getNumPendingOps() > 0 || stopped_; });
            if (stopped_)
            {
                return;
            }

            for (auto& ctx : progressables_)
            {
                if (ctx.pending_ops_ == 0)
                {
                    continue;
                }
                const ssize_t n = fi_cq_read(
                    &ctx.progressable_->completion_queue(), &entry, max_completion_entry_progress);
                if (n > 0)
                {
                    ctx.pending_ops_ -= n;

                    for (int i = 0; i < n; ++i)
                    {
                        if (!entry[i].op_context)
                        {
                            // Use a default completion callback
                            ctx.progressable_->on_completion(entry[i].flags, entry[i].len);
                        }
                        else
                        {
                            // If the op_context is set, it means that the operation was posted
                            // with a custom context, so we can use it to identify the operation.
                            // Otherwise, we just call on_completion with flags and length.
                            auto* ctx = static_cast<efa_completion_callback*>(entry[i].op_context);
                            ctx->on_completion(entry[i].flags, entry[i].len);
                        }
                    }
                }
                else if (n != -FI_EAGAIN && n != -FI_EINTR)
                {
                    fi_cq_err_entry errEntry{};
                    const auto      ret =
                        fi_cq_readerr(&ctx.progressable_->completion_queue(), &errEntry, 0);
                    if (ret < 0)
                    {
                        // Could happen if there's another progress engine, polling the same CQ
                        // We don't do it but let's ignore that warning for now.
                        // Going through the libfabric code, it's the only error that may be
                        // returned from fi_cq_readerr
                        assert(ret == -FI_EAGAIN);
                    }

                    ctx.pending_ops_ -= 1;
                    ctx.progressable_->on_error(errEntry.err);
                }
            }
        }
    }

private:
    struct ProgressableContext
    {
        ProgressableContext(std::shared_ptr<efa_progressable> progressable) :
            progressable_{std::move(progressable)},
            pending_ops_{0}
        {
        }

        std::shared_ptr<efa_progressable> progressable_;
        uint32_t                          pending_ops_;
    };

    std::vector<ProgressableContext>::iterator
    find_progressable_context(const std::shared_ptr<efa_progressable>& progressable)
    {
        return std::find_if(
            progressables_.begin(), progressables_.end(), [&](const ProgressableContext& ep) {
                return ep.progressable_ == progressable;
            });
    }

    mutable std::mutex               mtx_;
    std::condition_variable          cond_var_;
    std::vector<ProgressableContext> progressables_;

    bool        stopped_;
    std::thread work_thread_;
};

struct efa_domain
{
    std::shared_ptr<efa_fabric>            fabric_;
    std::unique_ptr<fid_domain>            domain_;
    std::unique_ptr<efa_execution_context> execution_ctx_;
    std::atomic<std::uint32_t>             next_mr_key = 1;

    explicit efa_domain(std::shared_ptr<efa_fabric> in_fabric) :
        fabric_{std::move(in_fabric)}
    {
        const int res = fi_domain(fabric_->fabric_.get(),
                                  fabric_->fabric_info_.get(),
                                  make_out_pointer(domain_),
                                  nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_domain", res};
        }

        execution_ctx_ = std::make_unique<efa_execution_context>();
    }
};

struct efa_endpoint
{
    std::shared_ptr<efa_domain> domain_;

    std::unique_ptr<fid_cq> completion_queue_;
    std::unique_ptr<fid_av> address_vector_;
    std::unique_ptr<fid_ep> endpoint_;

    explicit efa_endpoint(std::shared_ptr<efa_domain> domain) :
        domain_{std::move(domain)}
    {
        assert(domain_->fabric_->fabric_info_->ep_attr->type == FI_EP_RDM);

        int res = fi_endpoint(domain_->domain_.get(),
                              domain_->fabric_->fabric_info_.get(),
                              make_out_pointer(endpoint_),
                              nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_endpoint", res};
        }

        fi_av_attr avAttrs = {};
        avAttrs.type       = domain_->fabric_->fabric_info_->domain_attr->av_type;
        avAttrs.count      = 1;
        res =
            fi_av_open(domain_->domain_.get(), &avAttrs, make_out_pointer(address_vector_), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_av_open", res};
        }

        fi_cq_attr cq_attrs = {};
        cq_attrs.size       = domain_->fabric_->fabric_info_->tx_attr->size;
        cq_attrs.wait_obj   = FI_WAIT_NONE;
        cq_attrs.format     = FI_CQ_FORMAT_MSG;
        res                 = fi_cq_open(
            domain_->domain_.get(), &cq_attrs, make_out_pointer(completion_queue_), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_cq_open", res};
        }

        res = fi_ep_bind(endpoint_.get(), to_fid(completion_queue_), FI_RECV | FI_SEND);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind to CQ", res};
        }
        res = fi_ep_bind(endpoint_.get(), to_fid(address_vector_), 0);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind to AV", res};
        }
        // We want to post receive before accepting a connection.
        res = fi_enable(endpoint_.get());
        if (res != 0)
        {
            throw rdma_error{"fi_enable", res};
        }
    }
};

// Definition from libfabric sources: prov/efa/src/rdm/efa_rdm_protocol.h
struct efa_ep_addr
{
    std::uint8_t  raw[16];
    std::uint16_t qpn;
    std::uint16_t pad;
    std::uint32_t qkey;
};

inline bool parse_fabric_address(const std::string& address,
                                 std::uint32_t      addr_format,
                                 void*              addr_buffer,
                                 size_t             addr_length)
{
    if (addr_format == FI_SOCKADDR_IN)
    {
        if (addr_length < sizeof(sockaddr_in))
        {
            return false; // Buffer too small
        }
        sockaddr_in* addr = reinterpret_cast<sockaddr_in*>(addr_buffer);
        memset(addr, 0, sizeof(sockaddr_in));
        addr->sin_family = AF_INET;
        if (inet_pton(AF_INET, address.c_str(), &addr->sin_addr) != 1)
        {
            return false;
        }
        addr->sin_port = htons(0); // Port is not specified
    }
    else if (addr_format == FI_SOCKADDR_IN6)
    {
        if (addr_length < sizeof(sockaddr_in6))
        {
            return false; // Buffer too small
        }
        sockaddr_in6* addr = reinterpret_cast<sockaddr_in6*>(addr_buffer);
        memset(addr, 0, sizeof(sockaddr_in6));
        addr->sin6_family = AF_INET6;
        if (inet_pton(AF_INET6, address.c_str(), &addr->sin6_addr) != 1)
        {
            return false;
        }
        addr->sin6_port = htons(0); // Port is not specified
    }
    else if (addr_format == FI_ADDR_EFA)
    {
        if (addr_length < sizeof(efa_ep_addr))
        {
            return false; // Not enough space for EFA address
        }
        if (address.rfind("efa://[", 0) != 0)
        {
            return false;
        }
        const auto end_pos = address.find(L']', 7);
        if (end_pos == std::string::npos)
        {
            return false; // Missing closing bracket
        }
        efa_ep_addr* efa_addr     = reinterpret_cast<efa_ep_addr*>(addr_buffer);
        const auto   ipv6_address = address.substr(7, end_pos - 7);
        if (!inet_pton(AF_INET6, ipv6_address.c_str(), efa_addr->raw))
        {
            return false; // Invalid IPv6 address
        }
#ifndef _WIN32
        if (std::sscanf(
                address.substr(end_pos + 1).c_str(), ":%hu:%u", &efa_addr->qpn, &efa_addr->qkey) !=
            2)
#else
        if (sscanf_s(
                address.substr(end_pos + 1).c_str(), ":%hu:%u", &efa_addr->qpn, &efa_addr->qkey) !=
            2)
#endif
        {
            return false; // Invalid format for qpn and qkey
        }
        return true;
    }
    else if (addr_format == FI_ADDR_STR)
    {
        strncpy(reinterpret_cast<char*>(addr_buffer), address.c_str(), addr_length - 1);
        reinterpret_cast<char*>(addr_buffer)[addr_length - 1] = '\0'; // Ensure null-termination
    }
    else
    {
        return false; // Unsupported address format
    }

    return true;
}

inline std::string get_address_as_string(const void* addr, uint32_t addr_format)
{
    switch (addr_format)
    {
    case FI_SOCKADDR_IN:
    {
        const sockaddr_in* sockaddr = reinterpret_cast<const sockaddr_in*>(addr);
        char               buf[INET_ADDRSTRLEN + 1];
        inet_ntop(AF_INET, &sockaddr->sin_addr, buf, INET_ADDRSTRLEN);
        return std::string(buf);
    }
    case FI_SOCKADDR_IN6:
    {
        const sockaddr_in6* sockaddr = reinterpret_cast<const sockaddr_in6*>(addr);
        char                buf[INET6_ADDRSTRLEN + 1];
        inet_ntop(AF_INET6, &sockaddr->sin6_addr, buf, INET6_ADDRSTRLEN);
        return std::string(buf);
    }
    case FI_ADDR_EFA:
    {
        // EFA 'sock' address is an IPv6 (128 bits for address and 16-bits for port) with an
        // additional 32-bit qkey EFA addresses are not supposed to be human-readable but we need to
        // tell return something stringy on /status request

        char buf[INET6_ADDRSTRLEN + 1];
        if (!inet_ntop(AF_INET6, addr, buf, INET6_ADDRSTRLEN))
        {
            throw std::runtime_error("Error calling inet_ntop");
        }
        const efa_ep_addr* efa_addr = reinterpret_cast<const efa_ep_addr*>(addr);
        std::stringstream  ss;
        ss << "efa://[" << buf << "]:" << efa_addr->qpn << ':' << efa_addr->qkey;
        return ss.str();
    }
    case FI_ADDR_STR: // Address as a null-terminated string
        return std::string{reinterpret_cast<const char*>(addr)};
    case FI_SOCKADDR_IB: throw std::runtime_error("Unsupported IB address format");
    }

    throw std::runtime_error("Unsupported address format");
}

inline std::string get_fabric_local_address_as_string(const fi_info& info)
{
    return get_address_as_string(info.src_addr, info.addr_format);
}
