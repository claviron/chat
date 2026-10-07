use sqlx::postgres::PgPoolOptions;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

use super::messages::MessageStore;
use super::receipts::ReceiptStore;

#[tokio::test]
#[ignore = "Requires CLAVIRON_CHAT_TEST_DATABASE_URL pointing to an isolated empty PostgreSQL database"]
async fn runtime_queries_preserve_chat_storage_contracts()
-> Result<(), sqlx::Error> {
    let url = std::env::var("CLAVIRON_CHAT_TEST_DATABASE_URL").expect(
        "set CLAVIRON_CHAT_TEST_DATABASE_URL to an isolated empty database",
    );
    // Every store query uses this one connection, inside the test's transaction.
    let pool = PgPoolOptions::new().max_connections(1).connect(&url).await?;
    sqlx::raw_sql(
        r#"
        BEGIN;
        CREATE SCHEMA chat;
        CREATE TABLE chat.room_messages (
            uuid uuid PRIMARY KEY, room text NOT NULL, from_user_id integer NOT NULL,
            content text NOT NULL, created_at timestamptz NOT NULL DEFAULT NOW()
        );
        CREATE TABLE chat.private_messages (
            uuid uuid PRIMARY KEY, from_user_id integer NOT NULL, to_user_id integer NOT NULL,
            content text NOT NULL, created_at timestamptz NOT NULL DEFAULT NOW(),
            delivered_at timestamptz, read_at timestamptz
        );
        CREATE TABLE chat.recipients (
            message_uuid uuid NOT NULL, user_id integer NOT NULL,
            delivered_at timestamptz, read_at timestamptz,
            PRIMARY KEY (message_uuid, user_id)
        );
        "#
    )
    .execute(&pool)
    .await?;
    let messages = MessageStore::new(pool.clone());
    let receipts = ReceiptStore::new(pool.clone());
    let room_id = Uuid::from_u128(1);
    let private_id = Uuid::from_u128(2);
    let reply_id = Uuid::from_u128(3);
    let missing_id = Uuid::from_u128(4);
    let since = OffsetDateTime::now_utc() - Duration::days(1);
    let before = OffsetDateTime::now_utc() + Duration::days(1);

    messages
        .save_room_message(&room_id, "wallet", 1, "wallet transfer")
        .await?;
    messages.save_room_message(&room_id, "wallet", 1, "duplicate").await?;
    let room = messages.get_room_message_by_uuid(&room_id).await?.unwrap();
    assert_eq!(
        (room.room.as_str(), room.from_user_id, room.content.as_str()),
        ("wallet", 1, "wallet transfer")
    );
    assert!(messages.get_room_message_by_uuid(&missing_id).await?.is_none());
    assert_eq!(messages.count_room_messages("wallet").await?, 1);
    assert_eq!(messages.count_room_messages("missing").await?, 0);
    assert_eq!(
        messages.get_recent_room_messages("wallet", 1).await?[0].uuid,
        room_id
    );
    assert_eq!(
        messages.get_room_messages_since("wallet", since).await?[0].uuid,
        room_id
    );
    assert_eq!(
        messages.get_room_messages_before("wallet", before, 1).await?[0].uuid,
        room_id
    );
    assert_eq!(
        messages.search_room_messages("wallet", "transfer", 1).await?[0].uuid,
        room_id
    );
    assert_eq!(messages.get_unread_room_messages(2).await?[0].uuid, room_id);
    assert!(messages.get_unread_room_messages(1).await?.is_empty());
    assert_eq!(
        messages.count_unread_room_messages_by_room(2, "wallet").await?,
        1
    );
    let summaries = messages.get_unread_room_counts_by_room(2).await?;
    assert_eq!(
        (summaries[0].room.as_str(), summaries[0].unread_count),
        ("wallet", 1)
    );

    receipts.add_recipient(&room_id, 2).await?;
    receipts.add_recipient(&room_id, 2).await?;
    let recipients = receipts.get_recipients(&room_id).await?;
    assert_eq!(recipients.len(), 1);
    assert_eq!(
        (recipients[0].message_uuid, recipients[0].user_id),
        (room_id, 2)
    );
    assert!(
        recipients[0].delivered_at.is_none() && recipients[0].read_at.is_none()
    );
    assert_eq!(receipts.get_total_recipients(&room_id).await?, 1);
    assert_eq!(receipts.get_delivered_count(&room_id).await?, 0);
    assert_eq!(receipts.get_read_count(&room_id).await?, 0);
    assert!(receipts.mark_delivered(&room_id, 2).await?);
    assert!(!receipts.mark_delivered(&room_id, 2).await?);
    assert_eq!(receipts.get_delivered_count(&room_id).await?, 1);
    assert!(receipts.mark_read(&room_id, 2).await?);
    assert!(!receipts.mark_read(&room_id, 2).await?);
    assert_eq!(receipts.get_read_count(&room_id).await?, 1);
    assert_eq!(receipts.get_readers(&room_id).await?, vec![2]);
    let recipient = receipts.get_recipients(&room_id).await?.remove(0);
    assert!(recipient.delivered_at.is_some() && recipient.read_at.is_some());
    assert_eq!(
        messages.count_unread_room_messages_by_room(2, "wallet").await?,
        0
    );
    assert!(messages.get_unread_room_counts_by_room(2).await?.is_empty());

    messages.save_private_message(&private_id, 1, 2, "incoming").await?;
    messages.save_private_message(&private_id, 1, 2, "duplicate").await?;
    messages.save_private_message(&reply_id, 2, 1, "outgoing").await?;
    let private =
        messages.get_private_message_by_uuid(&private_id).await?.unwrap();
    assert_eq!(
        (private.from_user_id, private.to_user_id, private.content.as_str()),
        (1, 2, "incoming")
    );
    assert!(private.delivered_at.is_none() && private.read_at.is_none());
    assert!(messages.get_private_message_by_uuid(&missing_id).await?.is_none());
    assert_eq!(messages.get_recent_private_messages(1, 2, 10).await?.len(), 2);
    assert_eq!(
        messages.get_private_messages_since(1, 2, since).await?.len(),
        2
    );
    assert_eq!(
        messages.get_private_messages_before(1, 2, before, 10).await?.len(),
        2
    );
    assert!(messages.get_recent_private_messages(1, 3, 10).await?.is_empty());
    assert_eq!(messages.count_unread_private_messages(2).await?, 1);
    assert_eq!(messages.count_unread_private_messages_from(2, 1).await?, 1);
    assert_eq!(messages.count_unread_private_messages_from(2, 3).await?, 0);
    let summaries = messages.get_unread_private_counts_by_sender(2).await?;
    assert_eq!(
        (summaries[0].sender_user_id, summaries[0].unread_count),
        (1, 1)
    );
    assert!(messages.mark_private_message_delivered(&private_id).await?);
    assert!(!messages.mark_private_message_delivered(&private_id).await?);
    assert!(messages.mark_private_message_read(&private_id).await?);
    assert!(!messages.mark_private_message_read(&private_id).await?);
    let private =
        messages.get_private_message_by_uuid(&private_id).await?.unwrap();
    assert!(private.delivered_at.is_some() && private.read_at.is_some());
    assert_eq!(messages.count_unread_private_messages(2).await?, 0);
    assert!(messages.get_unread_private_counts_by_sender(2).await?.is_empty());
    sqlx::query("ROLLBACK").execute(&pool).await?;
    pool.close().await;
    Ok(())
}
