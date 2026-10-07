//! SQLx-backed delivery receipts using `time::OffsetDateTime` at the database boundary.

use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

/// Message recipient record (delivery/read tracking for room messages)
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct MessageRecipient {
    pub message_uuid: Uuid,
    pub user_id: i32,
    pub delivered_at: Option<OffsetDateTime>,
    pub read_at: Option<OffsetDateTime>,
}

/// Recipient storage interface
#[derive(Clone)]
pub struct ReceiptStore {
    pool: PgPool,
}

impl ReceiptStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Add a recipient for a room message
    pub async fn add_recipient(
        &self,
        message_uuid: &Uuid,
        user_id: i32,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            r#"
            INSERT INTO chat.recipients (message_uuid, user_id)
            VALUES ($1, $2)
            ON CONFLICT (message_uuid, user_id) DO NOTHING
            "#,
        )
        .bind(message_uuid)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Mark message as delivered for a user
    /// Returns true if actually updated (wasn't delivered before)
    pub async fn mark_delivered(
        &self,
        message_uuid: &Uuid,
        user_id: i32,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            INSERT INTO chat.recipients (message_uuid, user_id, delivered_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (message_uuid, user_id)
            DO UPDATE SET delivered_at = NOW()
            WHERE chat.recipients.delivered_at IS NULL
            "#,
        )
        .bind(message_uuid)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Mark message as read for a user
    /// Returns true if actually updated (wasn't read before)
    pub async fn mark_read(
        &self,
        message_uuid: &Uuid,
        user_id: i32,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            r#"
            INSERT INTO chat.recipients (message_uuid, user_id, read_at)
            VALUES ($1, $2, NOW())
            ON CONFLICT (message_uuid, user_id)
            DO UPDATE SET read_at = NOW()
            WHERE chat.recipients.read_at IS NULL
            "#,
        )
        .bind(message_uuid)
        .bind(user_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Get all recipients for a message
    pub async fn get_recipients(
        &self,
        message_uuid: &Uuid,
    ) -> Result<Vec<MessageRecipient>, sqlx::Error> {
        let recipients = sqlx::query_as::<_, MessageRecipient>(
            r#"
            SELECT message_uuid, user_id, delivered_at, read_at
            FROM chat.recipients
            WHERE message_uuid = $1
            ORDER BY delivered_at ASC NULLS LAST
            "#,
        )
        .bind(message_uuid)
        .fetch_all(&self.pool)
        .await?;

        Ok(recipients)
    }

    /// Get delivery count for a message
    pub async fn get_delivered_count(
        &self,
        message_uuid: &Uuid,
    ) -> Result<i64, sqlx::Error> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::bigint as "count"
            FROM chat.recipients
            WHERE message_uuid = $1 AND delivered_at IS NOT NULL
            "#,
        )
        .bind(message_uuid)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    /// Get read count for a message
    pub async fn get_read_count(
        &self,
        message_uuid: &Uuid,
    ) -> Result<i64, sqlx::Error> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::bigint as "count"
            FROM chat.recipients
            WHERE message_uuid = $1 AND read_at IS NOT NULL
            "#,
        )
        .bind(message_uuid)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }

    /// Get users who read a message
    pub async fn get_readers(
        &self,
        message_uuid: &Uuid,
    ) -> Result<Vec<i32>, sqlx::Error> {
        let user_ids = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT user_id
            FROM chat.recipients
            WHERE message_uuid = $1 AND read_at IS NOT NULL
            ORDER BY read_at ASC
            "#,
        )
        .bind(message_uuid)
        .fetch_all(&self.pool)
        .await?;

        Ok(user_ids)
    }

    /// Get total recipient count for a message
    pub async fn get_total_recipients(
        &self,
        message_uuid: &Uuid,
    ) -> Result<i64, sqlx::Error> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)::bigint as "count"
            FROM chat.recipients
            WHERE message_uuid = $1
            "#,
        )
        .bind(message_uuid)
        .fetch_one(&self.pool)
        .await?;

        Ok(count)
    }
}
