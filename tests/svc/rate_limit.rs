use std::sync::Arc;

use async_trait::async_trait;
use claviron::auth::session::VerifiedSession;
use claviron::auth::session::auth::{
    Audience, Authority, Blake3State, Claims, FleetAuth, Keyring, Purpose,
    SecretKey, Session, Validation,
};
use claviron::auth::session::token::{OrbitAuth, TokenAuthenticator};
use claviron::auth::{AnyAuthenticated, UserProvider, UserProviderError};
use claviron::orbitive::Fleet;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::*;
use crate::models::users::ChatUser;

fn config(key: ChatRateLimitKey) -> ChatRateLimitConfig {
    ChatRateLimitConfig {
        key,
        quota: QuotaConfig {
            units: NonZeroU64::MIN,
            period: Duration::from_secs(3600),
            burst: NonZeroU64::MIN,
            table: QuotaTable {
                slots: NonZeroUsize::new(8).unwrap(),
                shards: NonZeroUsize::MIN,
                key_bytes: NonZeroUsize::new(32).unwrap(),
                probes: NonZeroUsize::new(8).unwrap(),
            },
            cas_attempts: NonZeroUsize::new(64).unwrap(),
            store: QuotaBacking::LocalGuard {},
        },
    }
}

fn client(
    uuid: Uuid,
    session: Option<VerifiedSession>,
) -> ChatClient {
    let (tx, _rx) = mpsc::channel(1);
    ChatClient::new(
        ChatUser::from_trusted_snapshot(1, uuid, "alice", "Alice", ""),
        tx,
    )
    .with_session(session)
}

fn assert_limited(result: Result<(), ChatRateLimitError>) {
    assert!(
        matches!(result, Err(ChatRateLimitError::Limited { retry_after_seconds }) if retry_after_seconds > 0)
    );
}

#[test]
fn user_budget_is_shared_by_connections_clones_and_reconnects_not_other_users()
{
    let limiter =
        ChatRateLimiter::new(&config(ChatRateLimitKey::User)).unwrap();
    let uuid = Uuid::new_v4();
    let first = client(uuid, None);
    let other_connection = client(uuid, None);
    assert_ne!(first.connection_id(), other_connection.connection_id());
    assert!(limiter.check(&first).is_ok());
    assert_limited(limiter.check(&other_connection));
    drop(first);
    drop(other_connection);
    assert_limited(limiter.clone().check(&client(uuid, None)));
    assert!(limiter.check(&client(Uuid::new_v4(), None)).is_ok());
}

#[test]
fn simultaneous_connections_cannot_multiply_the_same_user_budget() {
    let limiter =
        ChatRateLimiter::new(&config(ChatRateLimitKey::User)).unwrap();
    let barrier = std::sync::Barrier::new(8);
    let uuid = Uuid::new_v4();
    let allowed = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let connection = client(uuid, None);
                let limiter = &limiter;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    limiter.check(&connection).is_ok()
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| usize::from(handle.join().unwrap()))
            .sum::<usize>()
    });
    assert_eq!(allowed, 1);
}

struct NoLookup;
#[async_trait]
impl UserProvider for NoLookup {
    async fn find_by_subject(
        &self,
        _: &str,
    ) -> Result<Option<AnyAuthenticated>, UserProviderError> {
        panic!("quota admission must not resolve users")
    }
}

struct Sessions {
    authority: Arc<Authority>,
    state: Arc<FleetAuth<Blake3State>>,
    auth: OrbitAuth<Blake3State>,
    now: u64,
}

impl Sessions {
    fn new() -> Self {
        let authority = Arc::new(
            Authority::new(
                "chat-test",
                "issuer",
                Keyring::new(1, [(1, SecretKey::from_bytes([3; 32]))]).unwrap(),
            )
            .unwrap(),
        );
        let fleet = Arc::new(Fleet::join("ctquota", 1).unwrap());
        let state = Arc::new(
            FleetAuth::new(
                fleet,
                &authority,
                SecretKey::from_bytes([4; 32]),
                NonZeroUsize::new(8).unwrap(),
                Blake3State,
            )
            .unwrap(),
        );
        let auth = OrbitAuth::new(
            authority.clone(),
            Validation {
                audience: Audience::new("chat").unwrap(),
                purpose: Purpose::Access,
                required_capabilities: Default::default(),
            },
            state.clone(),
            Arc::new(NoLookup),
            Duration::from_secs(60),
        );
        Self {
            authority,
            state,
            auth,
            now: crate::types::client::auth_now().unwrap(),
        }
    }

    fn login(
        &self,
        uuid: Uuid,
    ) -> Session {
        self.state
            .create_session(uuid.to_string(), self.now, self.now + 3600)
            .unwrap()
    }

    fn credential(
        &self,
        session: &Session,
    ) -> VerifiedSession {
        let token = self
            .authority
            .issue_session(
                Claims {
                    subject: session.subject().to_owned(),
                    audience: Audience::new("chat").unwrap(),
                    purpose: Purpose::Access,
                    capabilities: Default::default(),
                    issued_at: self.now,
                    not_before: self.now,
                    expires_at: self.now + 3600,
                },
                session,
            )
            .unwrap();
        self.auth.validate(token.expose(), self.now).unwrap()
    }
}

#[test]
fn session_budget_survives_reconnect_and_new_credentials_but_separates_logins()
{
    let sessions = Sessions::new();
    let uuid = Uuid::new_v4();
    let login = sessions.login(uuid);
    let first = client(uuid, Some(sessions.credential(&login)));
    let reconnect = client(uuid, Some(sessions.credential(&login)));
    assert_ne!(first.connection_id(), reconnect.connection_id());
    let limiter =
        ChatRateLimiter::new(&config(ChatRateLimitKey::Session)).unwrap();
    assert!(limiter.check(&first).is_ok());
    assert_limited(limiter.check(&reconnect));
    let second_login = sessions.login(uuid);
    assert!(
        limiter
            .check(&client(uuid, Some(sessions.credential(&second_login))))
            .is_ok()
    );

    let users = ChatRateLimiter::new(&config(ChatRateLimitKey::User)).unwrap();
    assert!(users.check(&first).is_ok());
    assert_limited(
        users.check(&client(uuid, Some(sessions.credential(&second_login)))),
    );
}

#[test]
fn session_key_requires_verified_session_and_never_falls_back_to_user_or_socket()
 {
    let config = config(ChatRateLimitKey::Session);
    let limiter = ChatRateLimiter::new(&config).unwrap();
    let uuid = Uuid::new_v4();
    for connection in [client(uuid, None), client(uuid, None)] {
        assert!(matches!(
            limiter.check(&connection),
            Err(ChatRateLimitError::MissingSession)
        ));
    }
}

#[test]
fn store_pressure_is_not_reported_as_identity_quota_exhaustion() {
    let mut config = config(ChatRateLimitKey::User);
    config.quota.cas_attempts = NonZeroUsize::MIN;
    let limiter = ChatRateLimiter::new(&config).unwrap();
    assert!(matches!(
        limiter.check(&client(Uuid::new_v4(), None)),
        Err(ChatRateLimitError::Store(QuotaError::Busy))
    ));
}

#[cfg(unix)]
struct ShmFixture {
    reservation: tempfile::TempDir,
    resource: claviron::orbit::quota::QuotaResource,
}

#[cfg(unix)]
impl ShmFixture {
    fn new() -> Self {
        let reservation = tempfile::Builder::new()
            .prefix("q")
            .rand_bytes(7)
            .tempdir()
            .unwrap();
        let fleet = reservation
            .path()
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        Self {
            reservation,
            resource: claviron::orbit::quota::QuotaResource { fleet, kind: 0 },
        }
    }

    fn config(
        &self,
        key: ChatRateLimitKey,
    ) -> ChatRateLimitConfig {
        let mut config = config(key);
        config.quota.store = QuotaBacking::FleetGuard {
            resource: self.resource,
            owners: NonZeroUsize::new(4).unwrap(),
            recovery_attempts: 0,
        };
        config
    }

    fn cleanup(&self) -> std::io::Result<()> {
        use claviron::orbitive::shm;
        use std::os::unix::fs::MetadataExt;
        let uid = std::fs::metadata(self.reservation.path())?.uid();
        let _lock =
            shm::try_lock_fleet_exclusive(self.resource.fleet.as_str(), uid)?
                .ok_or_else(|| {
                std::io::Error::other("quota fixture still has live owners")
            })?;
        let name = shm::ring_segment_name(
            self.resource.fleet.as_str(),
            self.resource.kind,
        );
        match shm::ShmRegion::open_existing_read_write(
            &name,
            1,
            uid,
            Default::default(),
        ) {
            Ok(region) => region.unlink()?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        std::fs::remove_file(shm::fleet_lock_path_for_uid(
            self.resource.fleet.as_str(),
            uid,
        ))?;
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for ShmFixture {
    fn drop(&mut self) {
        let result = self.cleanup();
        if !std::thread::panicking() {
            result.expect("isolated quota fixture cleanup");
        }
    }
}

#[cfg(unix)]
#[test]
fn fleet_guard_shares_user_and_session_debt_across_opens_and_preserves_reopen()
{
    let sessions = Sessions::new();
    let uuid = Uuid::new_v4();
    let login = sessions.login(uuid);
    for key in [ChatRateLimitKey::User, ChatRateLimitKey::Session] {
        let fixture = ShmFixture::new();
        let config = fixture.config(key);
        let first = ChatRateLimiter::new(&config).unwrap();
        let second = ChatRateLimiter::new(&config).unwrap();
        assert!(
            first
                .check(&client(uuid, Some(sessions.credential(&login))))
                .is_ok()
        );
        assert_limited(
            second.check(&client(uuid, Some(sessions.credential(&login)))),
        );
        drop(first);
        drop(second);
        let reopened = ChatRateLimiter::new(&config).unwrap();
        assert_limited(
            reopened.check(&client(uuid, Some(sessions.credential(&login)))),
        );
        assert_eq!(
            reopened.check(&client(Uuid::new_v4(), None)).is_ok(),
            key == ChatRateLimitKey::User
        );
    }
}
