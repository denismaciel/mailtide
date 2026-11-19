//! Tests ported by name from Zero's `gmail-push.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use mailtide::ids::{HistoryId, Timestamp};
use mailtide_gmail::push::{Jwks, PushError, PushExpectations, verify};
use serde_json::{Value, json};

const KEY: &str = include_str!("../testdata/push-test-key.pem");
const JWKS: &str = include_str!("../testdata/push-test-jwks.json");
const NOW: i64 = 1_700_000_000;
const AUDIENCE: &str = "https://api.example.com/push/test-project";
const SIGNER: &str = "push@test-project.iam.gserviceaccount.com";

fn expect() -> PushExpectations {
    PushExpectations {
        audience: AUDIENCE.into(),
        service_account: SIGNER.into(),
    }
}

fn authorization(audience: &str) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test-key".into());
    let claims = json!({
        "iss": "https://accounts.google.com",
        "aud": audience,
        "email": SIGNER,
        "email_verified": true,
        "iat": NOW,
        "exp": NOW + 300,
    });
    let key = EncodingKey::from_rsa_pem(KEY.as_bytes()).expect("test key");
    format!("Bearer {}", encode(&header, &claims, &key).expect("signs"))
}

/// A Pub/Sub push envelope wrapping a Gmail notification.
fn envelope(payload: &Value) -> Vec<u8> {
    json!({
        "subscription": "projects/test-project/subscriptions/gmail",
        "message": {"data": STANDARD.encode(payload.to_string()), "messageId": "1"},
    })
    .to_string()
    .into_bytes()
}

fn push(audience: &str, body: &[u8]) -> Result<mailtide_gmail::push::Notification, PushError> {
    let jwks = Jwks::parse(JWKS.as_bytes()).expect("jwks");
    verify(
        &authorization(audience),
        body,
        &jwks,
        &expect(),
        Timestamp(NOW * 1000),
    )
}

/// Zero: gmail-push > Gmail push admission > accepts wrapped and unwrapped notifications
#[test]
fn gmail_push_admission_accepts_wrapped_and_unwrapped_notifications() {
    // Mailtide only takes pushes from Pub/Sub, so only the wrapped form is
    // accepted; the mailbox comes from the payload, not the subscription.
    let notification = push(
        AUDIENCE,
        &envelope(&json!({"emailAddress": "user@example.com", "historyId": "123"})),
    )
    .expect("wrapped notification");
    assert_eq!(notification.account.as_str(), "user@example.com");
    assert_eq!(notification.history_id, HistoryId(123));
}

/// Zero: gmail-push > Gmail push admission > rejects malformed history IDs and subscriptions from another project
#[test]
fn gmail_push_admission_rejects_malformed_history_ids_and_subscriptions_from_another_project() {
    for history_id in [json!(1.5), json!("12x"), json!(-3), json!(null)] {
        assert_eq!(
            push(
                AUDIENCE,
                &envelope(&json!({"emailAddress": "user@example.com", "historyId": history_id}))
            ),
            Err(PushError::Malformed("historyId".into())),
            "{history_id}"
        );
    }
    // A push for another project's subscription carries that project's
    // audience, which doesn't verify here.
    let other = push(
        "https://api.example.com/push/other-project",
        &envelope(&json!({"emailAddress": "user@example.com", "historyId": "1"})),
    );
    assert!(
        matches!(other, Err(PushError::InvalidToken(_))),
        "{other:?}"
    );
}

/// Zero: gmail-push > Gmail push admission > accepts the numeric history IDs emitted in actual Gmail push messages
#[test]
fn gmail_push_admission_accepts_the_numeric_history_ids_emitted_in_actual_gmail_push_messages() {
    let notification = push(
        AUDIENCE,
        &envelope(&json!({"emailAddress": "test@example.com", "historyId": 4_910_605})),
    )
    .expect("numeric history id");
    assert_eq!(notification.account.as_str(), "test@example.com");
    assert_eq!(notification.history_id, HistoryId(4_910_605));
}
