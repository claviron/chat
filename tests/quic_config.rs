use super::*;

fn listener(extra: &str) -> ChatQuicListenerConfig {
    serde_yaml::from_str(&format!(
        "listen: '127.0.0.1:9444'\nhosts: [chat.example.test]\n{extra}"
    ))
    .unwrap()
}

#[test]
fn absence_is_opt_out_and_declared_listener_owns_transport_and_drain() {
    let absent: ChatQuicConfig = serde_yaml::from_str("{}").unwrap();
    assert!(absent.listener.is_none());
    assert!(absent.validate().is_ok());
    let config = listener(
        "max_connections: 10\ngraceful_timeout: null\nhandshake_timeout: 2s\ntransport: {keep_alive_interval: 10s, idle_timeout: 60s}",
    );
    assert_eq!(config.max_connections.unwrap().get(), 10);
    assert_eq!(config.handshake_timeout, Duration::from_secs(2));
    assert_eq!(config.graceful_timeout, None);
    assert_eq!(config.scope, Scope::Worker);
    assert_eq!(config.dispatch, Dispatch::Each);
    assert!(config.validate().is_ok());
}

#[test]
fn rejects_http_fields_and_duplicate_tls_sources_and_requires_a_bind() {
    for extra in [
        "max_request_body_size: 1024",
        "max_connections: 0",
        "keep_alive_interval: 10s",
        "transport: {bogus: true}",
        "ca_path: /example/client-ca.pem",
        "policy: {}",
    ] {
        assert!(
            serde_yaml::from_str::<ChatQuicListenerConfig>(&format!(
                "listen: '127.0.0.1:9444'\nhosts: [chat.example.test]\n{extra}"
            ))
            .is_err(),
            "{extra}"
        );
    }
    assert!(
        serde_yaml::from_str::<ChatQuicListenerConfig>(
            "hosts: [chat.example.test]"
        )
        .is_err()
    );
    assert!(
        serde_yaml::from_str::<ChatQuicListenerConfig>(
            "listen: '127.0.0.1:9444'"
        )
        .is_err()
    );
}

#[test]
fn validates_dispatch_and_rejects_invalid_transport_or_master_placement() {
    for extra in [
        "transport: {keep_alive_interval: 0s}",
        "transport: {keep_alive_interval: 60s, idle_timeout: 60s}",
        "transport: {keep_alive_interval: 10s, enable_early_data: true}",
        "scope: master\ntransport: {keep_alive_interval: 10s}",
        "dispatch: pool\ntransport: {keep_alive_interval: 10s}",
        "dispatch: one\ntransport: {keep_alive_interval: 10s}",
    ] {
        assert!(listener(extra).validate().is_err(), "{extra}");
    }
    let pool = listener(
        "dispatch: pool\ndispatch_policy: {capacity: 2, lease_ttl: 10s, renewal_interval: 3s}\ntransport: {keep_alive_interval: 10s}",
    );
    assert!(pool.validate().is_ok());
}
