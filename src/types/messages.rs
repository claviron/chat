use std::sync::Arc;

use claviron::helpers::uuid as uuid_binary;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::config::ChatHistoryConfig;
use crate::models::users::ChatUser;

/// Messages sent from client to server
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    /// Join a room/channel
    JoinRoom { room: String },

    /// Leave a room/channel
    LeaveRoom { room: String },

    /// Send message to a room
    SendToRoom {
        #[serde(with = "uuid_binary")]
        uuid: Uuid, // Client-generated message UUID
        room: String,
        content: String,
    },

    /// Send direct message to another user
    SendDirect {
        #[serde(with = "uuid_binary")]
        uuid: Uuid, // Client-generated message UUID
        #[serde(with = "uuid_binary")]
        to_user_uuid: Uuid, // Target user's UUID
        content: String,
    },

    /// Indicate typing in a room
    Typing { room: String },

    /// Indicate typing in direct message
    TypingDirect {
        #[serde(with = "uuid_binary")]
        to_user_uuid: Uuid,
    },

    /// Get room message history
    GetHistory {
        room: String,
        #[serde(default)]
        limit: Option<i64>,
        #[serde(default, with = "time::serde::timestamp::option")]
        since: Option<OffsetDateTime>, // RFC3339 timestamp - get messages since this time
        #[serde(default, with = "time::serde::timestamp::option")]
        before: Option<OffsetDateTime>, // RFC3339 timestamp - get messages before this time (for pagination)
    },

    /// Get direct message history with another user
    GetDirectHistory {
        #[serde(with = "uuid_binary")]
        with_user_uuid: Uuid, // User's UUID
        #[serde(default)]
        limit: Option<i64>,
        #[serde(default, with = "time::serde::timestamp::option")]
        since: Option<OffsetDateTime>, // RFC3339 timestamp - get messages since this time
        #[serde(default, with = "time::serde::timestamp::option")]
        before: Option<OffsetDateTime>, // RFC3339 timestamp - get messages before this time (for pagination)
    },

    /// List available rooms
    ListRooms,

    /// List users in a room
    ListUsers { room: String },

    /// Mark message as delivered (client → server)
    MarkDelivered {
        #[serde(with = "uuid_binary")]
        uuid: Uuid,
        /// Room name if this is a room message, None if private message
        #[serde(default)]
        room: Option<String>,
    },

    /// Mark message as read (client → server)
    MarkRead {
        #[serde(with = "uuid_binary")]
        uuid: Uuid,
        /// Room name if this is a room message, None if private message
        #[serde(default)]
        room: Option<String>,
    },

    /// Get detailed unread private messages (on-demand)
    GetUnreadPrivate,

    /// Get unread messages for a specific room (on-demand)
    GetUnreadRoom { room: String },

    /// Get list of online users
    GetOnlineUsers,
}

/// Messages sent from server to client
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    /// Welcome message with connection_id and unread counts (sent on connect)
    Welcome {
        #[serde(with = "uuid_binary")]
        connection_id: Uuid,
        #[serde(with = "uuid_binary")]
        user_uuid: Uuid, // User's UUID (not numeric ID)
        username: Arc<str>,
        name: Arc<str>, // User's display name
        message: String,
        unread: UnreadSummary, // All unread message counts
        history: ChatHistoryConfig,
    },

    /// Message in a room
    RoomMessage {
        #[serde(with = "uuid_binary")]
        uuid: Uuid, // Message UUID (client-generated, globally unique)
        room: String,
        #[serde(with = "uuid_binary")]
        from_user_uuid: Uuid, // Sender's UUID (not numeric ID)
        from_username: Arc<str>,
        from_avatar_url: Arc<str>,
        content: String,
        #[serde(with = "time::serde::timestamp")]
        timestamp: OffsetDateTime,
    },

    /// Direct message from another user
    DirectMessage {
        #[serde(with = "uuid_binary")]
        uuid: Uuid, // Message UUID (client-generated, globally unique)
        #[serde(with = "uuid_binary")]
        from_user_uuid: Uuid, // Sender's UUID (not numeric ID)
        from_username: Arc<str>,
        from_avatar_url: Arc<str>,
        content: String,
        #[serde(with = "time::serde::timestamp")]
        timestamp: OffsetDateTime,
    },

    /// User joined a room
    UserJoined {
        room: String,
        #[serde(with = "uuid_binary")]
        user_uuid: Uuid,
        username: Arc<str>,
    },

    /// User left a room
    UserLeft {
        room: String,
        #[serde(with = "uuid_binary")]
        user_uuid: Uuid,
        username: Arc<str>,
    },

    /// User came online (connected to svc)
    UserOnline {
        #[serde(with = "uuid_binary")]
        user_uuid: Uuid,
        username: Arc<str>,
    },

    /// User went offline (disconnected from svc)
    UserOffline {
        #[serde(with = "uuid_binary")]
        user_uuid: Uuid,
        username: Arc<str>,
    },

    /// User is typing in a room
    UserTyping {
        room: String,
        #[serde(with = "uuid_binary")]
        user_uuid: Uuid,
        username: Arc<str>,
    },

    /// User is typing in direct message
    UserTypingDirect {
        #[serde(with = "uuid_binary")]
        from_user_uuid: Uuid,
        from_username: Arc<str>,
    },

    /// Room message history response
    MessageHistory { room: String, messages: Vec<HistoryMessage> },

    /// Direct message history response
    DirectMessageHistory {
        #[serde(with = "uuid_binary")]
        with_user_uuid: Uuid, // Other user's UUID (not numeric ID)
        messages: Vec<HistoryMessage>,
    },

    /// List of available rooms
    RoomList { rooms: Vec<RoomInfo> },

    /// List of users in a room
    UserList { room: String, users: Vec<UserInfo> },

    /// System message (info, warnings, etc.)
    System { content: String },

    /// Error message
    Error { message: String },

    /// Confirmation of successful action
    Success { message: String },

    /// Message status update (sent/delivered/read/failed)
    MessageStatus {
        #[serde(with = "uuid_binary")]
        uuid: Uuid,
        status: MessageStatusType,
        #[serde(with = "time::serde::timestamp")]
        timestamp: OffsetDateTime,
        /// List of users who have read the message (for group svc read tracking)
        #[serde(skip_serializing_if = "Option::is_none")]
        read_by: Option<Vec<ReadReceipt>>,
        /// Total number of recipients in the group/room (for "Read by X of Y" display)
        #[serde(skip_serializing_if = "Option::is_none")]
        total_recipients: Option<usize>,
    },

    /// Detailed unread private messages response (on-demand)
    UnreadPrivateMessages { messages: Vec<UnreadPrivateDetail> },

    /// Unread messages for a room response (on-demand)
    UnreadRoomMessages { room: String, count: i64 },

    /// Online users list (sent on connect or request)
    OnlineUsers { users: Vec<OnlineUserInfo> },
}

/// Message status types for tracking delivery and read receipts
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageStatusType {
    /// Server confirmed receipt and broadcast
    Sent,
    /// Message delivered to recipient's connection (future)
    Delivered,
    /// Message read by recipient (future)
    Read,
    /// Message failed to send
    Failed,
}

#[derive(Debug, Clone, Serialize)]
pub struct RoomInfo {
    pub name: String,
    pub member_count: usize,
    pub crated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct UserInfo {
    #[serde(with = "uuid_binary")]
    pub uuid: Uuid, // User's UUID from auth system
    pub username: Arc<str>,
    pub name: Arc<str>,
    pub avatar: Arc<str>,
    pub online: bool,
}

impl From<(&ChatUser, bool)> for UserInfo {
    fn from((user, online): (&ChatUser, bool)) -> Self {
        Self {
            uuid: user.uuid(),
            name: user.name_arc().clone(),
            username: user.username_arc().clone(),
            avatar: user.avatar_arc().clone(),
            online,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ReadReceipt {
    #[serde(with = "uuid_binary")]
    pub user_uuid: Uuid, // User's UUID (not numeric ID)
    pub username: String,
}

/// Unread message summary sent in Welcome message (lightweight)
#[derive(Debug, Clone, Serialize)]
pub struct UnreadSummary {
    /// Total unread private message count (all senders combined)
    pub private_messages_count: i64,
    /// Unread room messages grouped by room name (just counts)
    pub room_messages: Vec<UnreadRoomCount>,
}

/// Unread count for a specific room
#[derive(Debug, Clone, Serialize)]
pub struct UnreadRoomCount {
    pub room: String,
    pub count: i64,
}

/// Detailed unread private message info (sent on-demand)
#[derive(Debug, Clone, Serialize)]
pub struct UnreadPrivateDetail {
    #[serde(with = "uuid_binary")]
    pub sender_uuid: Uuid,
    pub sender_username: String,
    pub sender_avatar: String,
    pub count: i64,
}

/// Online user info (minimal - for presence updates)
#[derive(Debug, Clone, Serialize)]
pub struct OnlineUserInfo {
    #[serde(with = "uuid_binary")]
    pub user_uuid: Uuid,
    pub username: Arc<str>,
}

#[derive(Debug, Clone, Serialize)]
pub struct HistoryMessage {
    #[serde(with = "uuid_binary")]
    pub uuid: Uuid, // Message UUID (for tracking read/seen/replies)
    #[serde(with = "uuid_binary")]
    pub from_uuid: Uuid, // Sender's UUID - client looks up username/avatar from local cache
    pub content: String,
    #[serde(with = "time::serde::timestamp")]
    pub timestamp: OffsetDateTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<MessageStatusType>,
}

impl ServerMessage {
    /// Create a welcome message with connection_id and unread counts
    pub fn welcome(
        connection_id: Uuid,
        uuid: Uuid,
        username: impl Into<Arc<str>>,
        name: impl Into<Arc<str>>,
        message: impl Into<String>,
        unread: UnreadSummary,
        history: ChatHistoryConfig,
    ) -> Self {
        Self::Welcome {
            connection_id,
            user_uuid: uuid,
            username: username.into(),
            name: name.into(),
            message: message.into(),
            unread,
            history,
        }
    }

    /// Convenience: welcome message using ChatUser (cheap Arc clones)
    pub fn welcome_from_user(
        connection_id: Uuid,
        user: &ChatUser,
        message: impl Into<String>,
        unread: UnreadSummary,
        history: ChatHistoryConfig,
    ) -> Self {
        Self::Welcome {
            connection_id,
            user_uuid: user.uuid(),
            username: user.username_arc().clone(),
            name: user.name_arc().clone(),
            message: message.into(),
            unread,
            history,
        }
    }

    /// Create an error message
    pub fn error(message: impl Into<String>) -> Self {
        Self::Error { message: message.into() }
    }

    /// Create a system message
    pub fn system(content: impl Into<String>) -> Self {
        Self::System { content: content.into() }
    }

    /// Create a success message
    pub fn success(message: impl Into<String>) -> Self {
        Self::Success { message: message.into() }
    }

    /// Create a message status update
    pub fn message_status(
        uuid: Uuid,
        status: MessageStatusType,
    ) -> Self {
        Self::MessageStatus {
            uuid,
            status,
            timestamp: time::OffsetDateTime::now_utc(),
            read_by: None,
            total_recipients: None,
        }
    }

    /// Create a message status update with read receipts (for group chats)
    pub fn message_status_with_receipts(
        uuid: Uuid,
        status: MessageStatusType,
        read_by: Vec<ReadReceipt>,
        total_recipients: usize,
    ) -> Self {
        Self::MessageStatus {
            uuid,
            status,
            timestamp: time::OffsetDateTime::now_utc(),
            read_by: Some(read_by),
            total_recipients: Some(total_recipients),
        }
    }
}
