#pragma once

#include <atomic>
#include <cstdint>

struct ClientConnection
{
    /// Identifier of the protocol
    std::uint32_t _identifier;
};

/// Identifier of this RDMA protocol
inline constexpr std::uint32_t PROTOCOL_IDENTIFIER = 1;

struct ClientConnectionFlowV1 : ClientConnection
{
    /// Identifier of the flow that the client want to open.
    std::uint8_t _flowIdentifier[16];
};

/**
 * Addition made to ClientConnectionFlowV1 to support frame level Metadata.
 */
struct ClientConnectionFlowV1B : ClientConnectionFlowV1
{
    /// Does the client want to receive frame metadata.
    bool _wantsFrameMetadata = true;
};

struct ServerConnectionFlowV1
{
    /// Size (in bytes) of the frame the server will send.
    std::uint32_t _frameSize = 0;
    /// Fabric clock time on the server at the moment of accepting the
    /// connection
    std::uint64_t _acceptConnectionTime = 0;
    /// At the moment of establishing connection, does the flow has active
    /// producers?
    bool _hasActiveProducers = false;
};

/**
 * Addition made to ServerConnectionFlowV1 to support frame level Metadata.
 */
struct ServerConnectionFlowV1B : ServerConnectionFlowV1
{
    /// Size (in bytes) of the frame metadata the server will send.
    std::uint32_t _frameMetadataSize = 0;
};

inline constexpr std::uint32_t FRAME_INFORMATION_FLAG_REPEATED = 1 << 0;
inline constexpr std::uint32_t FRAME_INFORMATION_FLAG_KEEP_ALIVE = 1 << 1;
inline constexpr std::uint32_t FRAME_INFORMATION_FLAG_FLOW_HAS_ACTIVE_PRODUCERS = 1 << 2;
inline constexpr std::uint32_t FRAME_INFORMATION_FLAG_FLOW_METADATA = 1 << 3;

struct FrameInformation
{
    std::uint64_t _frameIndex;
    std::uint32_t _bufferUsageCount;
    std::uint32_t _flags;
};

struct FrameBufferHeader
{
    std::atomic<std::uint32_t> _usageCounter;
    std::uint32_t _padding[7];
};
