#pragma once

#include <rdma/fabric.h>
#include <rdma/fi_cm.h>
#include <rdma/fi_domain.h>
#include <rdma/fi_endpoint.h>
#include <rdma/fi_eq.h>
#include <rdma/fi_errno.h>

#ifndef _WIN32
#  include <sys/types.h>
#  include <arpa/inet.h>
#  include <netinet/in.h>
#  include <sys/socket.h>
#else
#include <ws2tcpip.h>
#endif

#include <cstdint>
#include <cstdio>
#include <string.h>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>

#define DEBUG_LOG(...)                                                                                                 \
    do                                                                                                                 \
    {                                                                                                                  \
        using namespace std::chrono;                                                                                   \
        const auto tp = system_clock::now();                                                                           \
        const auto millis = duration_cast<milliseconds>(tp.time_since_epoch()).count() % 1000LL;                       \
        std::time_t time_tt = system_clock::to_time_t(tp);                                                             \
        std::tm t{};                                                                                                   \
        ::localtime_r(&time_tt, &t);                                                                                   \
        char buffer[512];                                                                                              \
        auto len = strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", &t);                                          \
        len += std::sprintf(buffer + len, ".%03u ", static_cast<unsigned>(millis));                                    \
        std::sprintf(buffer + len, __VA_ARGS__);                                                                       \
        std::fprintf(stdout, "%s\n", buffer);                                                                          \
        std::fflush(stdout);                                                                                           \
    } while (0);

inline const auto FABRIC_VERSION = FI_VERSION(FI_MAJOR_VERSION, FI_MINOR_VERSION);

// Deleter that works for any type from libfabric but fi_info
template <typename T>
struct FabricInterfaceDeleter
{
    void operator()(T* in_pointer)
    {
        if (in_pointer)
        {
            int res = fi_close(&in_pointer->fid);
            if (res != 0)
            {
                DEBUG_LOG("fi_close failed: %s (%d)", fi_strerror(res), res);
            }
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
template <> struct default_delete<fid_av>     : FabricInterfaceDeleter<fid_av> {};
template <> struct default_delete<fid_cntr>   : FabricInterfaceDeleter<fid_cntr> {};

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
        ss << in_functionName << " returned: '" << fi_strerror(in_errorCode)
           << " (" << in_errorCode << ")'";
        return ss.str();
    }
};

template <typename T>
fid_t toFid(const T& in_fabricInterface)
{
    return &in_fabricInterface.get()->fid;
}

inline std::shared_ptr<fi_info> createFabricInfoHints(const std::string& in_providerName,
                                                      const std::string& in_srcAddress)
{
    fi_info* rawHints = fi_allocinfo();
    if ( !rawHints )
    {
        throw rdma_error{"hints is null", -FI_ENOMEM};
    }
    std::shared_ptr<fi_info> hints{rawHints, fi_freeinfo};

    if (!in_providerName.empty())
    {
        hints->fabric_attr->prov_name = strdup(in_providerName.c_str());
    }
    hints->ep_attr->type = FI_EP_MSG;
    hints->caps = FI_MSG;
    hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    hints->rx_attr->iov_limit = 4;
    hints->tx_attr->iov_limit = 4;

    if (!in_srcAddress.empty())
    {
        in_addr srcAddr{};
        if (inet_pton(AF_INET, in_srcAddress.c_str(), &srcAddr) == 1)
        {
            sockaddr_in* addr = reinterpret_cast<sockaddr_in*>(malloc(sizeof(sockaddr_in)));
            memset(addr, 0, sizeof(sockaddr_in));
            addr->sin_addr = srcAddr;
            addr->sin_family = AF_INET;
            hints->addr_format = FI_SOCKADDR_IN;
            hints->src_addr = addr; // libfabric release it by calling free()
            hints->src_addrlen = sizeof(sockaddr_in);
        }
        else
        {
            // TODO ipv6
            throw rdma_error{"Invalid source address", -FI_ENODATA};
        }
    }

    return hints;
}

inline std::shared_ptr<fi_info> createFabricInfoHintsRdm(const std::string& in_providerName)
{
    fi_info* rawHints = fi_allocinfo();
    if ( !rawHints )
    {
        throw rdma_error{"hints is null", -FI_ENOMEM};
    }
    std::shared_ptr<fi_info> hints{rawHints, fi_freeinfo};

    if (!in_providerName.empty())
    {
        hints->fabric_attr->prov_name = strdup(in_providerName.c_str());
    }
    hints->ep_attr->type = FI_EP_RDM;
    hints->caps = FI_MSG | FI_RECV | FI_SEND | FI_REMOTE_COMM; // | FI_TAGGED
    hints->domain_attr->mr_mode = FI_MR_LOCAL | FI_MR_ALLOCATED | FI_MR_PROV_KEY | FI_MR_VIRT_ADDR;
    hints->domain_attr->progress = FI_PROGRESS_MANUAL;
    hints->domain_attr->threading = FI_THREAD_COMPLETION;
    hints->rx_attr->iov_limit = 4;
    hints->tx_attr->iov_limit = 4;

    return hints;
}


inline std::shared_ptr<fi_info> getFabricInfo(const std::string& in_providerName,
                                              const std::string& in_destAddress,
                                              const std::string& in_destPort,
                                              const std::string& in_srcAddress)
{
    std::shared_ptr<fi_info> hints = createFabricInfoHints(in_providerName, in_srcAddress);
    std::shared_ptr<fi_info> fabricInfo;
    int res = fi_getinfo(FABRIC_VERSION, in_destAddress.c_str(), in_destPort.c_str(),
                         0U, hints.get(), makeOutPointer(fabricInfo));
    if (res != 0)
    {
        throw rdma_error{"fi_getinfo", res};
    }

    if (!strcmp(fabricInfo->fabric_attr->prov_name, "tcp"))
    {
        fabricInfo->domain_attr->mr_mode |= FI_MR_PROV_KEY;
    }

    return fabricInfo;
}

inline std::shared_ptr<fi_info> getFabricInfo(const std::string& in_providerName,
                                              const std::string& in_srcAddress,
                                              const std::string& in_srcPort)
{
    std::shared_ptr<fi_info> hints = createFabricInfoHints(in_providerName, "");
    std::shared_ptr<fi_info> fabricInfo;
    int res = fi_getinfo(FABRIC_VERSION, in_srcAddress.c_str(), in_srcPort.c_str(),
                         FI_SOURCE, hints.get(), makeOutPointer(fabricInfo));
    if (res != 0)
    {
        throw rdma_error{"fi_getinfo", res};
    }

    if (!strcmp(fabricInfo->fabric_attr->prov_name, "tcp"))
    {
        fabricInfo->domain_attr->mr_mode |= FI_MR_PROV_KEY;
    }

    return fabricInfo;
}

struct RdmaAdapter
{
    std::shared_ptr<fi_info>    _fabricInfo;
    std::shared_ptr<fid_fabric> _fabric;
    std::shared_ptr<fid_domain> _domain;

    RdmaAdapter(std::shared_ptr<fi_info> in_fabricInfo)
        : _fabricInfo{std::move(in_fabricInfo)}
    {
        int res = fi_fabric(_fabricInfo->fabric_attr, makeOutPointer(_fabric), nullptr);
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

    std::unique_ptr<fid_eq>     _eventQueue;
    std::unique_ptr<fid_cq>     _completionQueue;
    std::unique_ptr<fid_ep>     _endpoint; // must be before EQ and CQ

    RdmaEndpoint(const RdmaAdapter& in_adapter)
        : RdmaEndpoint(in_adapter, *in_adapter._fabricInfo)
    {
    }

    RdmaEndpoint(const RdmaAdapter& in_adapter, const fi_info& in_fabricInfo)
        : _domain{in_adapter._domain}
        , _fabric{in_adapter._fabric}
    {
        assert(in_fabricInfo.ep_attr->type == FI_EP_MSG);

        int res = fi_endpoint(_domain.get(), const_cast<fi_info*>(&in_fabricInfo), makeOutPointer(_endpoint), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_endpoint", res};
        }

        fi_eq_attr eqAttrs = {};
        eqAttrs.wait_obj = FI_WAIT_UNSPEC;
        res = fi_eq_open(_fabric.get(), &eqAttrs, makeOutPointer(_eventQueue), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_eq_open", res};
        }

        fi_cq_attr cqAttrs = {};
        cqAttrs.size = 4; // Derived from the protocol requirements
                          // (we expect max two completions at given time) times two
        cqAttrs.wait_obj = FI_WAIT_UNSPEC;
        cqAttrs.format = FI_CQ_FORMAT_MSG;
        res = fi_cq_open(_domain.get(), &cqAttrs, makeOutPointer(_completionQueue), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_cq_open", res};
        }

        res = fi_ep_bind(_endpoint.get(), toFid(_eventQueue), 0);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind to EQ", res};
        }
        res = fi_ep_bind(_endpoint.get(), toFid(_completionQueue), FI_RECV | FI_SEND);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind to CQ", res};
        }
        // We want to post receive before accepting a connection.
        res = fi_enable(_endpoint.get());
        if (res != 0)
        {
            throw rdma_error{"fi_enable", res};
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

    std::unique_ptr<fid_eq>  _eventQueue;
    std::unique_ptr<fid_pep> _passiveEndpoint; // must be before EQ

    RdmaListeningEndpoint(const RdmaAdapter& in_adapter)
        : _fabric{in_adapter._fabric}
    {
        assert(in_adapter._fabricInfo->ep_attr->type == FI_EP_MSG);

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

struct RdmEndpoint
{
    std::shared_ptr<fid_domain> _domain;
    std::shared_ptr<fid_fabric> _fabric;

    std::unique_ptr<fid_cq>     _completionQueue;
    std::unique_ptr<fid_av>     _addressVector;
    std::unique_ptr<fid_ep>     _endpoint; // must be destroyed before anything that binds to it (so EQ, CQ and CNTR)

    RdmEndpoint(const RdmaAdapter& in_adapter)
        : RdmEndpoint(in_adapter, *in_adapter._fabricInfo)
    {
    }

    RdmEndpoint(const RdmaAdapter& in_adapter, const fi_info& in_fabricInfo)
        : _domain{in_adapter._domain}
        , _fabric{in_adapter._fabric}
    {
        assert(in_fabricInfo.ep_attr->type == FI_EP_RDM);

        int res = fi_endpoint(_domain.get(), const_cast<fi_info*>(&in_fabricInfo), makeOutPointer(_endpoint), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_endpoint", res};
        }
        fi_av_attr avAttrs = {};
        avAttrs.type = in_fabricInfo.domain_attr->av_type;
        avAttrs.count = 1;
        res = fi_av_open(_domain.get(), &avAttrs, makeOutPointer(_addressVector), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_av_open", res};
        }

        fi_cq_attr cqAttrs = {};
        cqAttrs.size = in_fabricInfo.tx_attr->size; // We can limit it later, for now use the full capability
        cqAttrs.wait_obj = FI_WAIT_NONE;
        cqAttrs.format = FI_CQ_FORMAT_MSG;
        res = fi_cq_open(_domain.get(), &cqAttrs, makeOutPointer(_completionQueue), nullptr);
        if (res != 0)
        {
            throw rdma_error{"fi_cq_open", res};
        }

        res = fi_ep_bind(_endpoint.get(), toFid(_completionQueue), FI_RECV | FI_SEND);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind to CQ", res};
        }
        res = fi_ep_bind(_endpoint.get(), toFid(_addressVector), 0);
        if (res != 0)
        {
            throw rdma_error{"fi_ep_bind to AV", res};
        }
        // We want to post receive before accepting a connection.
        res = fi_enable(_endpoint.get());
        if (res != 0)
        {
            throw rdma_error{"fi_enable", res};
        }
    }
};

// Definition from libfabric sources: prov/efa/src/rdm/efa_rdm_protocol.h
struct efa_ep_addr
{
    std::uint8_t raw[16];
    std::uint16_t qpn;
    std::uint16_t pad;
    std::uint32_t qkey;
};

inline bool parseFabricAddress(const std::string& in_address, std::uint32_t in_addrFormat, void* out_addrBuffer, size_t in_addrLength)
{
    if (in_addrFormat == FI_SOCKADDR_IN)
    {
        if (in_addrLength < sizeof(sockaddr_in))
        {
            return false; // Buffer too small
        }
        sockaddr_in* addr = reinterpret_cast<sockaddr_in*>(out_addrBuffer);
        memset(addr, 0, sizeof(sockaddr_in));
        addr->sin_family = AF_INET;
        if (inet_pton(AF_INET, in_address.c_str(), &addr->sin_addr) != 1)
        {
            return false;
        }
        addr->sin_port = htons(0); // Port is not specified
    }
    else if (in_addrFormat == FI_SOCKADDR_IN6)
    {
        if (in_addrLength < sizeof(sockaddr_in6))
        {
            return false; // Buffer too small
        }
        sockaddr_in6* addr = reinterpret_cast<sockaddr_in6*>(out_addrBuffer);
        memset(addr, 0, sizeof(sockaddr_in6));
        addr->sin6_family = AF_INET6;
        if (inet_pton(AF_INET6, in_address.c_str(), &addr->sin6_addr) != 1)
        {
            return false;
        }
        addr->sin6_port = htons(0); // Port is not specified
    }
    else if (in_addrFormat == FI_ADDR_EFA)
    {
        if ( in_addrLength < sizeof(efa_ep_addr) )
        {
            return false; // Not enough space for EFA address
        }
        if ( in_address.rfind( "efa://[", 0 ) != 0 )
        {
            return false;
        }
        const auto endPos = in_address.find( L']', 7 );
        if ( endPos == std::string::npos )
        {
            return false; // Missing closing bracket
        }
        efa_ep_addr* efaAddr = reinterpret_cast<efa_ep_addr*>(out_addrBuffer);
        const auto ipv6Address = in_address.substr(7, endPos - 7);
        if (!inet_pton( AF_INET6, ipv6Address.c_str(), efaAddr->raw ))
        {
            return false; // Invalid IPv6 address
        }
#ifndef _WIN32
        if ( std::sscanf(in_address.substr(endPos + 1).c_str(), ":%hu:%u", &efaAddr->qpn, &efaAddr->qkey) != 2 )
#else
        if ( sscanf_s(in_address.substr(endPos + 1).c_str(), ":%hu:%u", &efaAddr->qpn, &efaAddr->qkey) != 2 )
#endif
        {
            return false; // Invalid format for qpn and qkey
        }
        return true;
    }
    else if (in_addrFormat == FI_ADDR_STR)
    {
        strncpy(reinterpret_cast<char*>(out_addrBuffer), in_address.c_str(), in_addrLength - 1);
        reinterpret_cast<char*>(out_addrBuffer)[in_addrLength - 1] = '\0'; // Ensure null-termination
    }
    else
    {
        return false; // Unsupported address format
    }

    return true;
}

inline std::string getAddressAsString(const void* in_addr, uint32_t in_addrFormat)
{
    switch (in_addrFormat)
    {
    case FI_SOCKADDR_IN:
    {
        const sockaddr_in* addr = reinterpret_cast<const sockaddr_in*>(in_addr);
        char buf[INET_ADDRSTRLEN + 1];
        inet_ntop(AF_INET, &addr->sin_addr, buf, INET_ADDRSTRLEN);
        return std::string(buf);
    }
    case FI_SOCKADDR_IN6:
    {
        const sockaddr_in6* addr = reinterpret_cast<const sockaddr_in6*>(in_addr);
        char buf[INET6_ADDRSTRLEN + 1];
        inet_ntop(AF_INET6, &addr->sin6_addr, buf, INET6_ADDRSTRLEN);
        return std::string(buf);
    }
    case FI_ADDR_EFA:
    {
        // EFA 'sock' address is an IPv6 (128 bits for address and 16-bits for port) with an additional 32-bit qkey
        // EFA addresses are not supposed to be human-readable but we need to tell return something stringy on /status request

        char buf[INET6_ADDRSTRLEN + 1];
        if (!inet_ntop(AF_INET6, in_addr, buf, INET6_ADDRSTRLEN))
        {
            throw std::runtime_error("Error calling inet_ntop");
        }
        const efa_ep_addr* efa_addr = reinterpret_cast<const efa_ep_addr*>(in_addr);
        std::stringstream ss;
        ss << "efa://[" << buf << "]:" << efa_addr->qpn << ':' << efa_addr->qkey;
        return ss.str();
    }
    case FI_ADDR_STR: // Address as a null-terminated string
        return std::string{reinterpret_cast<const char*>(in_addr)};
    case FI_SOCKADDR_IB:
        throw std::runtime_error("Unsupported IB address format");
    }

    throw std::runtime_error("Unsupported address format");
}

inline std::string getFabricLocalAddressAsString(const fi_info& in_info)
{
    return getAddressAsString(in_info.src_addr, in_info.addr_format);
}
