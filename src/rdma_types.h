#pragma once

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>

#include <cassert>
#include <memory>
#include <stdexcept>
#include <sstream>

// Deleter that works for any type from libfabric but fi_info
template <typename T>
struct FabricInterfaceDeleter
{
    void operator()(T* in_pointer)
    {
        if (in_pointer)
        {
            int res = fi_close(&in_pointer->fid);
            assert(res == 0); (void) res;
        }
    } 
};

template <>
struct FabricInterfaceDeleter<fi_info>
{
    void operator()(fi_info* in_pointer)
    {
        if (in_pointer)
        {
            fi_freeinfo(in_pointer);
        }
    }
};

namespace std {

template <> struct default_delete<fi_info>    : FabricInterfaceDeleter<fi_info> {};
template <> struct default_delete<fid_fabric> : FabricInterfaceDeleter<fid_fabric> {};
template <> struct default_delete<fid_domain> : FabricInterfaceDeleter<fid_domain> {};
template <> struct default_delete<fid_eq>     : FabricInterfaceDeleter<fid_eq> {};
template <> struct default_delete<fid_pep>    : FabricInterfaceDeleter<fid_pep> {};
template <> struct default_delete<fid_ep>     : FabricInterfaceDeleter<fid_ep> {};
template <> struct default_delete<fid_cq>     : FabricInterfaceDeleter<fid_cq> {};
template <> struct default_delete<fid_mr>     : FabricInterfaceDeleter<fid_mr> {};

} // namespace std

// Utility class for passing out pointer to constructor-like functions.
// Shouldn't be used directly but with `makeOutPointer` wrapper function
// that does the type deduction.
template <typename SmartPointer>
class OutPointer
{
public:
    using element_type = typename SmartPointer::element_type;
    using pointer_type = std::add_pointer_t<element_type>;

    explicit OutPointer(SmartPointer& inout_pointer)
        : _smartPointer{inout_pointer}
        , _rawPointer{nullptr}
    {
        _smartPointer.reset();
    }

    ~OutPointer()
    {
        if (_rawPointer)
        {
            _smartPointer.reset(_rawPointer);
        }
    }

    OutPointer(const OutPointer&) = delete;
    OutPointer& operator=(const OutPointer&) = delete;

    operator pointer_type*() { return &_rawPointer; }
    operator void**() { return reinterpret_cast<void**>(&_rawPointer); }

private:
    SmartPointer& _smartPointer;
    pointer_type _rawPointer;
};

template <typename SmartPointer, typename Deleter>
class OutPointerWithDynamicDeleter
{
public:
    using element_type = typename SmartPointer::element_type;
    using pointer_type = std::add_pointer_t<element_type>;

    explicit OutPointerWithDynamicDeleter(SmartPointer& inout_pointer, Deleter in_deleter)
        : _smartPointer{inout_pointer}
        , _rawPointer{nullptr}
        , _deleter{std::move(in_deleter)}
    {
        _smartPointer.reset();
    }

    ~OutPointerWithDynamicDeleter()
    {
        if (_rawPointer)
        {
            _smartPointer.reset(_rawPointer, std::move(_deleter));
        }
    }

    OutPointerWithDynamicDeleter(const OutPointerWithDynamicDeleter&) = delete;
    OutPointerWithDynamicDeleter& operator=(const OutPointerWithDynamicDeleter&) = delete;

    operator pointer_type*() { return &_rawPointer; }
    operator void**() { return reinterpret_cast<void**>(&_rawPointer); }

private:
    SmartPointer& _smartPointer;
    pointer_type _rawPointer;
    Deleter _deleter;
};

template <typename SmartPointer>
OutPointer<SmartPointer>
makeOutPointer(SmartPointer& inout_pointer)
{
    return OutPointer<SmartPointer>(inout_pointer);
}

template <typename SmartPointer, typename Deleter>
OutPointerWithDynamicDeleter<SmartPointer, Deleter>
makeOutPointer(SmartPointer& inout_pointer, Deleter in_deleter)
{
    return OutPointerWithDynamicDeleter<SmartPointer, Deleter>(inout_pointer, std::move(in_deleter));
}

template <typename T>
auto
makeOutPointer(std::shared_ptr<T>& inout_pointer)
{
    return makeOutPointer(inout_pointer, FabricInterfaceDeleter<T>{});
}

struct rdma_error : std::runtime_error
{
    explicit rdma_error(const char* in_functionName, int in_errorCode)
        : std::runtime_error{formatErrorMessage(in_functionName, in_errorCode)}
    {
    }

private:
    static std::string formatErrorMessage(const char* in_functionName,
                                          int in_errorCode)
    {
        std::stringstream ss;
        ss << in_functionName << " returned " << fi_strerror(in_errorCode)
           << " (" << in_errorCode << ")";
        return ss.str();
    }
};

template <typename T>
fid_t toFid(const T& in_fabricInterface)
{
    return &in_fabricInterface.get()->fid;
}

struct RdmaAdapter
{
    std::shared_ptr<fi_info>    _fabricInfo;
    std::shared_ptr<fid_fabric> _fabric;
    std::shared_ptr<fid_domain> _domain;

    RdmaAdapter(const std::string& in_providerName, 
                const std::string& in_adapterAddress,
                const std::string& in_service,
                bool isListener)
    {
        std::unique_ptr<fi_info> hints{fi_allocinfo()};
        if (!hints)
        {
            throw std::runtime_error{"hints is null"};
        }

        hints->fabric_attr->prov_name = strdup(in_providerName.c_str());
        hints->ep_attr->type = FI_EP_MSG;
        hints->caps = FI_MSG;
        hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
        hints->addr_format = FI_SOCKADDR_IN;

        int res = fi_getinfo(FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION), in_adapterAddress.c_str(), in_service.c_str(),
                             isListener ? FI_SOURCE : 0U, hints.get(), makeOutPointer(_fabricInfo));
        if (res != 0)
        {
            throw rdma_error{"fi_getinfo", res};
        }

        res = fi_fabric(_fabricInfo->fabric_attr, makeOutPointer(_fabric), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_fabric", res};
        }

        std::shared_ptr<fid_domain> domain;
        res = fi_domain(_fabric.get(), _fabricInfo.get(), makeOutPointer(_domain), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_domain", res};
        }
    }
};

struct RdmaEndpoint
{
    std::shared_ptr<fid_domain> _domain;
    std::shared_ptr<fid_fabric> _fabric;

    std::unique_ptr<fid_eq> _eventQueue;
    std::unique_ptr<fid_cq> _inboundQueue;
    std::unique_ptr<fid_cq> _outboundQueue;
    std::unique_ptr<fid_ep> _endpoint;

    RdmaEndpoint(const RdmaAdapter& in_adapter)
        : RdmaEndpoint(in_adapter, *in_adapter._fabricInfo)
    {
    }

    RdmaEndpoint(const RdmaAdapter& in_adapter, const fi_info& in_fabricInfo)
        : _domain{in_adapter._domain}
        , _fabric{in_adapter._fabric}
    {
        int res = fi_endpoint(_domain.get(), const_cast<fi_info*>(&in_fabricInfo), makeOutPointer(_endpoint), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_endpoint", res};
        }

        fi_eq_attr eq_attr = {};
        eq_attr.wait_obj = FI_WAIT_UNSPEC;
        eq_attr.flags = FI_WRITE; // We'll insert custom events into EQ
        res = fi_eq_open(_fabric.get(), &eq_attr, makeOutPointer(_eventQueue), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_eq_open", res};
        }

        fi_cq_attr cq_attr = {};
        cq_attr.size = in_fabricInfo.tx_attr->size; // mozna uproscic do 2x tyle co oczekujemy
        cq_attr.wait_obj = FI_WAIT_UNSPEC;
        cq_attr.format = FI_CQ_FORMAT_MSG;
        res = fi_cq_open(_domain.get(), &cq_attr, makeOutPointer(_inboundQueue), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_cq_open", res};
        }

        res = fi_cq_open(_domain.get(), &cq_attr, makeOutPointer(_outboundQueue), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_cq_open", res};
        }

        res = fi_ep_bind(_endpoint.get(), toFid(_eventQueue), 0);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind 1", res};
        }

        res = fi_ep_bind(_endpoint.get(), toFid(_inboundQueue), FI_RECV);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind 2", res};
        }

        res = fi_ep_bind(_endpoint.get(), toFid(_outboundQueue), FI_SEND);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind 3", res};
        }
    }

    void receiveEmptyMessage()
    {
        ssize_t res = fi_recv(_endpoint.get(), nullptr, 0, nullptr, FI_ADDR_UNSPEC, nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_recv", static_cast<int>(res)};
        }
    }

    void sendEmptyMessage()
    {
        ssize_t res = fi_send(_endpoint.get(), nullptr, 0, nullptr, FI_ADDR_UNSPEC, nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_send", static_cast<int>(res)};
        }
    }

    size_t getMaxConnectionDataSize() const
    {
        size_t maxConnectionDataSize = 0;
        size_t maxConnectionDataSizeLength = sizeof(maxConnectionDataSize);
        int res = fi_getopt(toFid(_endpoint), FI_OPT_ENDPOINT,
                            FI_OPT_CM_DATA_SIZE, &maxConnectionDataSize, &maxConnectionDataSizeLength);
        if (res != 0)
        {
            throw rdma_error{"fi_getopt", res};
        }
        return maxConnectionDataSize;
    }
};

struct RdmaListeningEndpoint
{
    std::shared_ptr<fid_fabric> _fabric;

    std::unique_ptr<fid_eq> _eventQueue;
    std::unique_ptr<fid_pep> _passiveEndpoint;

    RdmaListeningEndpoint(const RdmaAdapter& in_adapter)
        : _fabric{in_adapter._fabric}
    {
        int res = fi_passive_ep(_fabric.get(), in_adapter._fabricInfo.get(), makeOutPointer(_passiveEndpoint), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_passive_ep", res};
        }

        fi_eq_attr eq_attr = {};
        eq_attr.wait_obj = FI_WAIT_UNSPEC;
        eq_attr.flags = FI_WRITE; // We'll insert custom events into EQ
        res = fi_eq_open(_fabric.get(), &eq_attr, makeOutPointer(_eventQueue), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_eq_open", res};
        }

        res = fi_pep_bind(_passiveEndpoint.get(), toFid(_eventQueue), 0);
        if (res != 0)
        {
            throw rdma_error{"fi_pep_bind", res};
        }

        /*
        sockaddr_in boundAddr;
        size_t boundAddrLen = sizeof(boundAddr);
        res = fi_getname(reinterpret_cast<fid_t>(listeningEndpoint._passiveEndpoint.get()), &boundAddr, &boundAddrLen);
        if (res != 0)
        {
            throw rdma_error{"fi_getname", res};
        }
        if (ntohs(boundAddr.sin_port) != atoi(in_cfg.port.c_str()))
        {
            throw std::runtime_error{"bad port given"};
        }
        */

        res = fi_listen(_passiveEndpoint.get());
        if (res != 0)
        {
            throw rdma_error{"fi_listen", res};
        }
    }

    size_t getMaxConnectionDataSize() const
    {
        size_t maxConnectionDataSize = 0;
        size_t maxConnectionDataSizeLength = sizeof(maxConnectionDataSize);
        int res = fi_getopt(toFid(_passiveEndpoint), FI_OPT_ENDPOINT,
                            FI_OPT_CM_DATA_SIZE, &maxConnectionDataSize, &maxConnectionDataSizeLength);
        if (res != 0)
        {
            throw rdma_error{"fi_getopt", res};
        }
        return maxConnectionDataSize;
    }
};
