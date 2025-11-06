#pragma once

#include <atomic>
#include <cstdint>

struct client_connection
{
    /// Identifier of the protocol
    std::uint32_t identifier;
};

/// Identifier of this RDMA protocol
inline constexpr std::uint32_t PROTOCOL_IDENTIFIER = 1;

struct client_connection_flow_v1 : client_connection
{
    /// Identifier of the flow that the client want to open.
    std::uint8_t flow_id[16];
};

/**
 * Addition made to ClientConnectionFlowV1 to support frame level Metadata.
 */
struct client_connection_flow_v1b : client_connection_flow_v1
{
    /// Does the client want to receive frame metadata.
    bool wants_frame_metadata = true;
};

struct client_connection_flow_v1c : client_connection_flow_v1b
{
    std::uint64_t max_chunk_size = std::uint64_t(-1);
};

struct server_connection_flow_v1
{
    /// Size (in bytes) of the frame the server will send.
    std::uint32_t frame_size = 0;
    /// Fabric clock time on the server at the moment of accepting the connection
    std::uint64_t accept_connection_time = 0;
    /// At the moment of establishing connection, does the flow has active producers?
    bool has_active_producers = false;
};

/**
 * Addition made to ServerConnectionFlowV1 to support frame level Metadata.
 */
struct server_connection_flow_v1b : server_connection_flow_v1
{
    /// Size (in bytes) of the frame metadata the server will send.
    std::uint32_t frame_metadata_size = 0;
};

struct server_connection_flow_v1c : server_connection_flow_v1b
{
    std::uint64_t max_chunk_size = std::uint64_t(-1);
};

inline constexpr std::uint32_t FRAME_INFORMATION_FLAG_REPEATED = 1 << 0;
inline constexpr std::uint32_t FRAME_INFORMATION_FLAG_KEEP_ALIVE = 1 << 1;
inline constexpr std::uint32_t FRAME_INFORMATION_FLAG_FLOW_HAS_ACTIVE_PRODUCERS = 1 << 2;
inline constexpr std::uint32_t FRAME_INFORMATION_FLAG_FLOW_METADATA = 1 << 3;

inline constexpr std::uint8_t PROTOCOL_VERSION  = 1;

struct frame_information
{
    std::uint64_t frame_index;
    std::uint32_t buffer_usage_count;
    std::uint32_t flags;
};

struct frame_buffer_header
{
    std::atomic<std::uint32_t> usage_counter;
    std::uint32_t padding[7];
};

namespace efa {

enum class control_message_type : std::uint8_t
{
    UNKNOWN,
    CLIENT_CONNECT_V1,
    CLIENT_SHUTDOWN_V1,
    SERVER_ACCEPT_V1,
    SERVER_REJECT_V1
};

struct control_message_header
{
    std::uint8_t          protocol_version = PROTOCOL_VERSION;
    control_message_type  type             = control_message_type::UNKNOWN;
    std::uint16_t         length           = 0;
};

struct client_connect_v1
{
    std::uint16_t address_format           = 0;
    std::uint16_t address_length           = 0;
    std::uint8_t  source_address_bytes[64] = {};
    std::uint8_t  dest_address_bytes[64]   = {};
    std::uint8_t  flow_id[16]              = {};
    bool          wants_frame_metadata     = true;
};

struct client_shutdown_v1
{
};

struct server_reject_v1
{
    char error_message[128];
};

struct server_accept_v1
{
    std::uint32_t frame_size             = 0;
    std::uint32_t frame_metadata_size    = 0;
    std::uint64_t accept_connection_time = 0;
    bool          has_active_producers   = false;
};

} // namespace efa
