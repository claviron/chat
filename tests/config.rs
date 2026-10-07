use super::{ChatAuthConfig, ChatAuthGuard, ChatConfig, join_route_path};

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
