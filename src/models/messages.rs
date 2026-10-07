//! SQLx-backed chat messages using `time::OffsetDateTime` at the database boundary.

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

/// Unread private message summary keyed only by chat's local sender id.
/// Identity/profile hydration belongs to the application's `UserProvider`.
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct UnreadPrivateSummary {
    pub sender_user_id: i32,
    pub unread_count: i64,
}

/// Unread room message summary (room + count)
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct UnreadRoomSummary {
    pub room: String,
    pub unread_count: i64,
}

/// Room message in database
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct RoomMessage {
    pub uuid: Uuid,
    pub room: String,
    pub from_user_id: i32,
    pub content: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
}

/// Private message in database
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct PrivateMessage {
    pub uuid: Uuid,
    pub from_user_id: i32,
    pub to_user_id: i32,
    pub content: String,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    #[serde(with = "time::serde::rfc3339::option")]
    pub delivered_at: Option<OffsetDateTime>,
    #[serde(with = "time::serde::rfc3339::option")]
    pub read_at: Option<OffsetDateTime>,
}

/// Message storage interface
#[derive(Clone)]
pub struct MessageStore {
    pool: PgPool,
}

impl MessageStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Save a new room message to database
    pub async fn save_room_message(
        &self,
        uuid: &Uuid,
        room: &str,
        from_user_id: i32,
        content: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO chat.room_messages (uuid, room, from_user_id, content)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (uuid) DO NOTHING
            "#,
        )
        .bind(uuid)
        .bind(room)
        .bind(from_user_id)
        .bind(content)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Save a new private message to database
    pub async fn save_private_message(
        &self,
        uuid: &Uuid,
        from_user_id: i32,
        to_user_id: i32,
        content: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO chat.private_messages (uuid, from_user_id, to_user_id, content)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (uuid) DO NOTHING
            "#
        )
        .bind(uuid)
        .bind(from_user_id)
        .bind(to_user_id)
        .bind(content)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Get recent room messages
    pub async fn get_recent_room_messages(
        &self,
        room: &str,
        limit: i64,
    ) -> Result<Vec<RoomMessage>, sqlx::Error> {
        let messages = sqlx::query_as::<_, RoomMessage>(
            r#"
            SELECT uuid, room, from_user_id, content, created_at
            FROM chat.room_messages
            WHERE room = $1
            ORDER BY created_at DESC
            LIMIT $2
            "#,
        )
        .bind(room)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(messages)
    }

    /// Get room messages since a specific time
    pub async fn get_room_messages_since(
        &self,
        room: &str,
        since: OffsetDateTime,
    ) -> Result<Vec<RoomMessage>, sqlx::Error> {
        let messages = sqlx::query_as::<_, RoomMessage>(
            r#"
            SELECT uuid, room, from_user_id, content, created_at
            FROM chat.room_messages
            WHERE room = $1 AND created_at > $2
            ORDER BY created_at ASC
            "#,
        )
        .bind(room)
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        Ok(messages)
    }

    /// Get room messages before a specific time (for pagination)
    pub async fn get_room_messages_before(
        &self,
        room: &str,
        before: OffsetDateTime,
        limit: i64,
    ) -> Result<Vec<RoomMessage>, sqlx::Error> {
        let messages = sqlx::query_as::<_, RoomMessage>(
            r#"
            SELECT uuid, room, from_user_id, content, created_at
            FROM chat.room_messages
            WHERE room = $1 AND created_at < $2
            ORDER BY created_at DESC
            LIMIT $3
            "#,
        )
        .bind(room)
        .bind(before)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(messages)
    }

    /// Get recent private messages between two users
    pub async fn get_recent_private_messages(
        &self,
        user1_id: i32,
        user2_id: i32,
        limit: i64,
    ) -> Result<Vec<PrivateMessage>, sqlx::Error> {
        let messages = sqlx::query_as::<_, PrivateMessage>(
            r#"
            SELECT uuid, from_user_id, to_user_id, content, created_at, delivered_at, read_at
            FROM chat.private_messages
            WHERE (from_user_id = $1 AND to_user_id = $2)
               OR (from_user_id = $2 AND to_user_id = $1)
            ORDER BY created_at DESC
            LIMIT $3
            "#
        )
        .bind(user1_id)
        .bind(user2_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(messages)
    }

    /// Get private messages since a specific time
    pub async fn get_private_messages_since(
        &self,
        user1_id: i32,
        user2_id: i32,
        since: OffsetDateTime,
    ) -> Result<Vec<PrivateMessage>, sqlx::Error> {
        let messages = sqlx::query_as::<_, PrivateMessage>(
            r#"
            SELECT uuid, from_user_id, to_user_id, content, created_at, delivered_at, read_at
            FROM chat.private_messages
            WHERE ((from_user_id = $1 AND to_user_id = $2)
                OR (from_user_id = $2 AND to_user_id = $1))
              AND created_at > $3
            ORDER BY created_at ASC
            "#
        )
        .bind(user1_id)
        .bind(user2_id)
        .bind(since)
        .fetch_all(&self.pool)
        .await?;

        Ok(messages)
    }

    /// Get private messages before a specific time (for pagination)
    pub async fn get_private_messages_before(
        &self,
        user1_id: i32,
        user2_id: i32,
        before: OffsetDateTime,
        limit: i64,
    ) -> Result<Vec<PrivateMessage>, sqlx::Error> {
        let messages = sqlx::query_as::<_, PrivateMessage>(
            r#"
            SELECT uuid, from_user_id, to_user_id, content, created_at, delivered_at, read_at
            FROM chat.private_messages
            WHERE ((from_user_id = $1 AND to_user_id = $2)
                OR (from_user_id = $2 AND to_user_id = $1))
              AND created_at < $3
            ORDER BY created_at DESC
            LIMIT $4
            "#
        )
        .bind(user1_id)
        .bind(user2_id)
        .bind(before)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(messages)
    }

    /// Mark private message as delivered
    /// Returns true if actually updated (was not delivered before)
    pub async fn mark_private_message_delivered(
        &self,
        uuid: &Uuid,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE chat.private_messages
            SET delivered_at = NOW()
            WHERE uuid = $1 AND delivered_at IS NULL
            "#,
        )
        .bind(uuid)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Mark private message as read
    /// Returns true if actually updated (was not read before)
    pub async fn mark_private_message_read(
        &self,
        uuid: &Uuid,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE chat.private_messages
            SET read_at = NOW()
            WHERE uuid = $1 AND read_at IS NULL
            "#,
        )
        .bind(uuid)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Get room message by UUID
    pub async fn get_room_message_by_uuid(
        &self,
        uuid: &Uuid,
    ) -> Result<Option<RoomMessage>, sqlx::Error> {
        let message = sqlx::query_as::<_, RoomMessage>(
            r#"
            SELECT uuid, room, from_user_id, content, created_at
            FROM chat.room_messages
            WHERE uuid = $1
            "#,
        )
        .bind(uuid)
        .fetch_optional(&self.pool)
        .await?;

        Ok(message)
    }

    /// Get private message by UUID
    pub async fn get_private_message_by_uuid(
        &self,
        uuid: &Uuid,
    ) -> Result<Option<PrivateMessage>, sqlx::Error> {
        let message = sqlx::query_as::<_, PrivateMessage>(
            r#"
            SELECT uuid, from_user_id, to_user_id, content, created_at, delivered_at, read_at
            FROM chat.private_messages
            WHERE uuid = $1
            "#
        )
        .bind(uuid)
        .fetch_optional(&self.pool)
        .await?;

        Ok(message)
    }

    /// Search room messages by content (full-text search)
    #[allow(unused)]
    pub async fn search_room_messages(
        &self,
        room: &str,
        query: &str,
        limit: i64,
    ) -> Result<Vec<RoomMessage>, sqlx::Error> {
        let messages = sqlx::query_as::<_, RoomMessage>(
            r#"
            SELECT uuid, room, from_user_id, content, created_at
            FROM chat.room_messages
            WHERE room = $1
              AND to_tsvector('english', content) @@ plainto_tsquery('english', $2)
            ORDER BY created_at DESC
            LIMIT $3
            "#
        )
        .bind(room)
        .bind(query)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(messages)
    }

    /// Get room message count
    pub async fn count_room_messages(
        &self,
        room: &str,
    ) -> Result<i64, sqlx::Error> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*) as "count"
            FROM chat.room_messages
            WHERE room = $1
            "#,
        )
        .bind(room)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    /// Get unread room messages for a user (messages without read receipt)
    pub async fn get_unread_room_messages(
        &self,
        user_id: i32,
    ) -> Result<Vec<RoomMessage>, sqlx::Error> {
        let messages = sqlx::query_as::<_, RoomMessage>(
            r#"
            SELECT rm.uuid, rm.room, rm.from_user_id, rm.content, rm.created_at
            FROM chat.room_messages rm
            LEFT JOIN chat.recipients r ON r.message_uuid = rm.uuid AND r.user_id = $1
            WHERE rm.from_user_id != $1
              AND (r.read_at IS NULL OR r.user_id IS NULL)
            ORDER BY rm.created_at DESC
            "#
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(messages)
    }

    /// Count unread room messages for a user by room
    pub async fn count_unread_room_messages_by_room(
        &self,
        user_id: i32,
        room: &str,
    ) -> Result<i64, sqlx::Error> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*) as "count"
            FROM chat.room_messages rm
            LEFT JOIN chat.recipients r ON r.message_uuid = rm.uuid AND r.user_id = $1
            WHERE rm.room = $2
              AND rm.from_user_id != $1
              AND (r.read_at IS NULL OR r.user_id IS NULL)
            "#
        )
        .bind(user_id)
        .bind(room)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    /// Get unread private message counts grouped by the chat-local sender id.
    pub async fn get_unread_private_counts_by_sender(
        &self,
        user_id: i32,
    ) -> Result<Vec<UnreadPrivateSummary>, sqlx::Error> {
        let summaries = sqlx::query_as::<_, UnreadPrivateSummary>(
            r#"
            SELECT
                pm.from_user_id as "sender_user_id",
                COUNT(pm.uuid)::bigint as "unread_count"
            FROM chat.private_messages pm
            WHERE pm.to_user_id = $1
              AND pm.read_at IS NULL
            GROUP BY pm.from_user_id
            ORDER BY MAX(pm.created_at) DESC
            "#,
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(summaries)
    }

    /// Get unread room message counts grouped by room (minimal info)
    pub async fn get_unread_room_counts_by_room(
        &self,
        user_id: i32,
    ) -> Result<Vec<UnreadRoomSummary>, sqlx::Error> {
        let summaries = sqlx::query_as::<_, UnreadRoomSummary>(
            r#"
            SELECT
                rm.room as "room",
                COUNT(rm.uuid)::bigint as "unread_count"
            FROM chat.room_messages rm
            LEFT JOIN chat.recipients r ON r.message_uuid = rm.uuid AND r.user_id = $1
            WHERE rm.from_user_id != $1
              AND (r.read_at IS NULL OR r.user_id IS NULL)
            GROUP BY rm.room
            ORDER BY MAX(rm.created_at) DESC
            "#
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(summaries)
    }

    /// Count total unread private messages for a user
    pub async fn count_unread_private_messages(
        &self,
        user_id: i32,
    ) -> Result<i64, sqlx::Error> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*) as "count"
            FROM chat.private_messages
            WHERE to_user_id = $1
              AND read_at IS NULL
            "#,
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    /// Count unread private messages from a specific user
    pub async fn count_unread_private_messages_from(
        &self,
        user_id: i32,
        from_user_id: i32,
    ) -> Result<i64, sqlx::Error> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*) as "count"
            FROM chat.private_messages
            WHERE to_user_id = $1
              AND from_user_id = $2
              AND read_at IS NULL
            "#,
        )
        .bind(user_id)
        .bind(from_user_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }
}
