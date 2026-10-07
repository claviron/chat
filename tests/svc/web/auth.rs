use super::{authorization_guard, websocket_guard, websocket_guard_name};
use crate::config::ChatAuthGuard;

#[test]
fn websocket_prefix_selects_guard() {
    assert_eq!(
        websocket_guard("bearer.payload").unwrap(),
        ChatAuthGuard::Orbit
    );
    assert_eq!(
        websocket_guard("ticket.payload").unwrap(),
        ChatAuthGuard::Ticket
    );
    assert!(websocket_guard("unknown.payload").is_err());
    assert_eq!(websocket_guard_name("bearer.payload"), "orbit");
    assert_eq!(websocket_guard_name("ticket.payload"), "ticket");
    assert_eq!(websocket_guard_name("unknown.payload"), "unknown");
}

#[test]
fn authorization_scheme_selects_guard() {
    assert_eq!(
        authorization_guard("Bearer orbit", ChatAuthGuard::Orbit).unwrap(),
        (ChatAuthGuard::Orbit, "orbit")
    );
    assert_eq!(
        authorization_guard("Ticket opaque", ChatAuthGuard::Orbit).unwrap(),
        (ChatAuthGuard::Ticket, "opaque")
    );
    assert_eq!(
        authorization_guard("opaque", ChatAuthGuard::Ticket).unwrap(),
        (ChatAuthGuard::Ticket, "opaque")
    );
}
