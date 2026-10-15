use super::{same_client_auth, validate_client_auth};
use claviron::tls::auth::{ClientAuthMode, ClientAuthPolicy};

fn required() -> ClientAuthPolicy {
    ClientAuthPolicy {
        mode: ClientAuthMode::Required,
        ca_bundle: Some("/example/client-ca.pem".into()),
        organization: Some("Example".into()),
        unit: Some("Chat".into()),
    }
}

#[test]
fn inherited_policy_requires_mtls_ca_and_complete_subject_constraints() {
    assert!(validate_client_auth(&required()).is_ok());
    assert!(validate_client_auth(&ClientAuthPolicy::default()).is_err());
    assert!(
        validate_client_auth(&ClientAuthPolicy {
            ca_bundle: None,
            ..required()
        })
        .is_err()
    );
    assert!(
        validate_client_auth(&ClientAuthPolicy { unit: None, ..required() })
            .is_err()
    );
}

#[test]
fn one_quic_listener_cannot_merge_different_ca_or_subject_policies() {
    let policy = required();
    assert!(same_client_auth(&policy, &policy.clone()));
    assert!(!same_client_auth(
        &policy,
        &ClientAuthPolicy {
            ca_bundle: Some("/example/other-ca.pem".into()),
            ..required()
        }
    ));
    assert!(!same_client_auth(
        &policy,
        &ClientAuthPolicy { unit: Some("Other".into()), ..required() }
    ));
}
