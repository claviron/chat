use super::*;
use crate::svc::rate_limit::{ChatRateLimitError, ChatRateLimitKey};
use std::num::NonZeroU64;

fn config() -> ChatRateLimitConfig {
    let mut config = ChatRateLimitConfig::default();
    config.quota.units = NonZeroU64::MIN;
    config.quota.period = std::time::Duration::from_secs(3600);
    config.quota.burst = NonZeroU64::MIN;
    config
}

fn client(uuid: Uuid) -> ChatClient {
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    ChatClient::new(
        ChatUser::from_trusted_snapshot(1, uuid, "alice", "Alice", ""),
        tx,
    )
}

#[test]
fn unchanged_reload_and_reconnect_retain_the_existing_user_budget() {
    let server = ChatServer::empty();
    let config = config();
    let uuid = Uuid::new_v4();
    server.prepare_rate_limit(&config, "runtime").unwrap();
    assert!(server.check_rate_limit(&client(uuid)).is_ok());
    server.prepare_rate_limit(&config, "runtime").unwrap();
    assert!(matches!(
        server.check_rate_limit(&client(uuid)),
        Err(ChatRateLimitError::Limited { .. })
    ));
}

#[test]
fn failed_preparation_preserves_the_previous_budget_and_missing_state_denies() {
    let server = ChatServer::empty();
    let connection = client(Uuid::new_v4());
    assert!(matches!(
        server.check_rate_limit(&connection),
        Err(ChatRateLimitError::Unavailable)
    ));
    let config = config();
    server.prepare_rate_limit(&config, "runtime").unwrap();
    assert!(server.check_rate_limit(&connection).is_ok());
    let mut invalid = config;
    invalid.quota.period = std::time::Duration::ZERO;
    assert!(server.prepare_rate_limit(&invalid, "runtime").is_err());
    assert!(matches!(
        server.check_rate_limit(&connection),
        Err(ChatRateLimitError::Limited { .. })
    ));
}

#[test]
fn fleet_quota_cannot_open_resources_in_the_runtime_namespace() {
    use claviron::orbit::quota::{QuotaBacking, QuotaResource};
    use std::num::NonZeroUsize;
    let server = ChatServer::empty();
    let mut config = config();
    config.quota.store = QuotaBacking::FleetGuard {
        resource: QuotaResource { fleet: "runtime".parse().unwrap(), kind: 0 },
        owners: NonZeroUsize::new(4).unwrap(),
        recovery_attempts: 0,
    };
    assert!(server.prepare_rate_limit(&config, "runtime").is_err());
    assert!(matches!(
        server.check_rate_limit(&client(Uuid::new_v4())),
        Err(ChatRateLimitError::Unavailable)
    ));
}

#[tokio::test]
async fn sessionless_messages_are_refused_before_room_or_direct_storage() {
    let server = ChatServer::empty();
    let mut config = config();
    config.key = ChatRateLimitKey::Session;
    server.prepare_rate_limit(&config, "runtime").unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel(2);
    let user = ChatUser::from_trusted_snapshot(
        1,
        Uuid::new_v4(),
        "alice",
        "Alice",
        "",
    );
    let connection = ChatClient::new(user, tx);
    let message_id = Uuid::new_v4();
    for message in [
        crate::types::messages::ClientMessage::SendToRoom {
            uuid: message_id,
            room: "general".into(),
            content: "hello".into(),
        },
        crate::types::messages::ClientMessage::SendDirect {
            uuid: message_id,
            to_user_uuid: Uuid::new_v4(),
            content: "hello".into(),
        },
    ] {
        crate::handle_client_message(&server, &connection, message).await;
        assert!(
            matches!(rx.try_recv(), Ok(ServerMessage::MessageStatus { status: crate::types::messages::MessageStatusType::Failed, uuid, .. }) if uuid == message_id)
        );
        assert!(
            matches!(rx.try_recv(), Ok(ServerMessage::Error { message }) if message == "Chat rate limit requires a verified session")
        );
        assert!(rx.try_recv().is_err());
    }
}
