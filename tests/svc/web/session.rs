use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use claviron::auth::session::auth::{
    Audience, Authority, Blake3State, Claims, FleetAuth, Keyring, Purpose,
    SecretKey, Validation,
};
use claviron::auth::session::token::{OrbitAuth, TokenAuthenticator};
use claviron::auth::{AnyAuthenticated, UserProvider, UserProviderError};
use claviron::orbitive::Fleet;
use fastwebsockets::{Frame, Payload};
use tokio::sync::mpsc;
use uuid::Uuid;

use super::handle_ws_frame;
use crate::models::users::ChatUser;
use crate::{ChatClient, ChatServer};

struct NoLookup;
#[async_trait]
impl UserProvider for NoLookup {
    async fn find_by_subject(
        &self,
        _: &str,
    ) -> Result<Option<AnyAuthenticated>, UserProviderError> {
        panic!("live connection must not resolve its user again")
    }
}

#[tokio::test]
async fn peer_logout_rejects_an_already_connected_client_before_frame_dispatch()
{
    let authority = Arc::new(
        Authority::new(
            "chat-test",
            "issuer",
            Keyring::new(1, [(1, SecretKey::from_bytes([3; 32]))]).unwrap(),
        )
        .unwrap(),
    );
    let fleet = Arc::new(Fleet::join("chat-revoke-test", 1).unwrap());
    let make_state = || {
        Arc::new(
            FleetAuth::new(
                fleet.clone(),
                &authority,
                SecretKey::from_bytes([4; 32]),
                NonZeroUsize::new(8).unwrap(),
                Blake3State,
            )
            .unwrap(),
        )
    };
    let issuer_state = make_state();
    let receiver_state = make_state();
    let now = crate::types::client::auth_now().unwrap();
    let uuid = Uuid::new_v4();
    let session =
        issuer_state.create_session(uuid.to_string(), now, now + 60).unwrap();
    let token = authority
        .issue_session(
            Claims {
                subject: uuid.to_string(),
                audience: Audience::new("chat").unwrap(),
                purpose: Purpose::Access,
                capabilities: Default::default(),
                issued_at: now,
                not_before: now,
                expires_at: now + 60,
            },
            &session,
        )
        .unwrap();
    let adapter = OrbitAuth::new(
        authority,
        Validation {
            audience: Audience::new("chat").unwrap(),
            purpose: Purpose::Access,
            required_capabilities: Default::default(),
        },
        receiver_state,
        Arc::new(NoLookup),
        Duration::from_secs(60),
    );
    let verified = adapter.validate(token.expose(), now).unwrap();
    let user = ChatUser::from_trusted_snapshot(1, uuid, "alice", "Alice", "");
    let (tx, _rx) = mpsc::channel(1);
    let client = ChatClient::new(user, tx).with_session(Some(verified));
    assert!(client.auth_valid());
    issuer_state.revoke_session(session.id(), now).unwrap();
    assert!(!client.auth_valid());
    // Empty server is intentional: revoked input must never reach its DB-backed handlers.
    assert!(
        !handle_ws_frame(
            &ChatServer::empty(),
            &client,
            Frame::binary(Payload::Borrowed(&[]))
        )
        .await
    );
}
