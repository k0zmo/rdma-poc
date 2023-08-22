#pragma once

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
