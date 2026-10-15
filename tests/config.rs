use super::{
    ChatAuthConfig, ChatAuthGuard, ChatConfig, ChatHistoryConfig,
    join_route_path,
};

#[test]
fn validates_history_windows_and_bounds_every_request() {
    let policy: ChatHistoryConfig = serde_yaml::from_str(
        "initial_limit: 3\npage_limit: 2\nmax_limit: 4\nmax_messages_per_conversation: 8\nmax_conversations: 3\n",
    ).unwrap();
    assert!(policy.validate().is_ok());
    assert_eq!(policy.request_limit(None).unwrap(), 3);
    assert_eq!(policy.request_limit(Some(900)).unwrap(), 4);
    assert!(policy.request_limit(Some(0)).is_err());
    assert!(policy.request_limit(Some(-1)).is_err());
    let invalid = ChatHistoryConfig { initial_limit: 5, ..policy.clone() };
    assert!(invalid.validate().is_err());
    let invalid = ChatHistoryConfig {
        max_messages_per_conversation: 1,
        ..policy.clone()
    };
    assert!(invalid.validate().is_err());
    let invalid = ChatHistoryConfig { max_conversations: 1, ..policy };
    assert!(invalid.validate().is_err());
}

#[test]
fn joins_chat_transport_paths() {
    assert_eq!(&*join_route_path("/chat", "/ws"), "/chat/ws");
    assert_eq!(&*join_route_path("/chat/", "h2"), "/chat/h2");
    assert_eq!(&*join_route_path("", "/ws"), "/ws");
    assert_eq!(&*join_route_path("/", ""), "/");
}

#[test]
fn accepts_multiple_auth_guards() {
    let auth: ChatAuthConfig = serde_yaml::from_str(
        r#"
guards: [orbit, ticket]
default: orbit
"#,
    )
    .expect("multi-guard auth config should parse");

    assert_eq!(auth.default_guard, ChatAuthGuard::Orbit);
    assert!(auth.guard_enabled(ChatAuthGuard::Orbit));
    assert!(auth.guard_enabled(ChatAuthGuard::Ticket));
}

#[test]
fn legacy_guard_remains_single_guard() {
    let auth: ChatAuthConfig = serde_yaml::from_str("guard: ticket\n")
        .expect("legacy guard should parse");

    assert_eq!(auth.default_guard, ChatAuthGuard::Ticket);
    assert!(auth.guard_enabled(ChatAuthGuard::Ticket));
    assert!(!auth.guard_enabled(ChatAuthGuard::Orbit));
}

#[test]
fn session_guard_requires_an_origin_and_rejects_fallback_guards() {
    let mut config: ChatConfig = serde_yaml::from_str(
        "auth:\n  guards: [session]\n  default: session\n",
    )
    .unwrap();
    assert!(config.validate().is_err());
    config.allowed_origins = vec!["https://example.test".into()];
    assert!(config.validate().is_ok());
    config.auth.guards.push(ChatAuthGuard::Ticket);
    assert!(config.validate().is_err());
}

#[test]
fn parses_presence_durations() {
    let config: ChatConfig = serde_yaml::from_str(
        r#"
presence:
  sync_interval: 3s
  stale_after: 15s
"#,
    )
    .expect("presence config should parse");

    assert_eq!(config.presence.sync_interval.as_secs(), 3);
    assert_eq!(config.presence.stale_after.as_secs(), 15);
    assert!(config.validate().is_ok());
}

#[test]
fn rejects_presence_timeout_shorter_than_two_refreshes() {
    let config: ChatConfig = serde_yaml::from_str(
        r#"
presence:
  sync_interval: 5s
  stale_after: 9s
"#,
    )
    .expect("presence config should parse before semantic validation");

    assert!(config.validate().is_err());
}
