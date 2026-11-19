//! Tests ported by name from Zero's `gmail-push-auth.test.ts`
//! (Mail-0/Zero, MIT, commit 4b642dfd9890).
//! Managed by `scripts/zero_tests.py`: stubs are added, never rewritten.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use mailtide::ids::{HistoryId, Timestamp};
use mailtide_gmail::push::{Jwks, Notification, PushError, PushExpectations, verify};
use serde_json::json;

const KEY: &str = include_str!("../testdata/push-test-key.pem");
const JWKS: &str = include_str!("../testdata/push-test-jwks.json");
const NOW: i64 = 1_700_000_000;
const AUDIENCE: &str = "https://api.example.com/a8n/notify/google";
const EMAIL: &str = "push@example.iam.gserviceaccount.com";

fn token(audience: &str, identity: &str, verified: bool) -> String {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test-key".into());
    let claims = json!({
        "iss": "https://accounts.google.com",
        "aud": audience,
        "email": identity,
        "email_verified": verified,
        "iat": NOW,
        "exp": NOW + 300,
    });
    let key = EncodingKey::from_rsa_pem(KEY.as_bytes()).expect("test key");
    encode(&header, &claims, &key).expect("signs")
}

fn check(token: &str) -> Result<Notification, PushError> {
    let data =
        STANDARD.encode(json!({"emailAddress": "user@example.com", "historyId": "7"}).to_string());
    let body = json!({"message": {"data": data, "messageId": "1"}, "subscription": "s"})
        .to_string()
        .into_bytes();
    let jwks = Jwks::parse(JWKS.as_bytes()).expect("jwks");
    let expect = PushExpectations {
        audience: AUDIENCE.into(),
        service_account: EMAIL.into(),
    };
    verify(
        &format!("Bearer {token}"),
        &body,
        &jwks,
        &expect,
        Timestamp(NOW * 1000),
    )
}

/// Zero: gmail-push-auth > signed Pub/Sub identity verification > accepts a correctly signed token for the configured audience and service account
#[test]
fn signed_pub_sub_identity_verification_accepts_a_correctly_signed_token_for_the_configured_audience_and_service_account()
 {
    let notification = check(&token(AUDIENCE, EMAIL, true)).expect("valid push");
    assert_eq!(notification.history_id, HistoryId(7));
}

/// Zero: gmail-push-auth > signed Pub/Sub identity verification > rejects an audience mismatch and unverified or different email identity
#[test]
fn signed_pub_sub_identity_verification_rejects_an_audience_mismatch_and_unverified_or_different_email_identity()
 {
    let mismatch = check(&token("https://other.example.com", EMAIL, true));
    assert!(
        matches!(mismatch, Err(PushError::InvalidToken(_))),
        "{mismatch:?}"
    );
    assert_eq!(
        check(&token(AUDIENCE, "other@example.com", true)),
        Err(PushError::WrongSigner)
    );
    assert_eq!(
        check(&token(AUDIENCE, EMAIL, false)),
        Err(PushError::WrongSigner)
    );
}

/// Zero: gmail-push-auth > signed Pub/Sub identity verification > rejects invalid signatures
#[test]
fn signed_pub_sub_identity_verification_rejects_invalid_signatures() {
    let valid = token(AUDIENCE, EMAIL, true);
    let mut parts: Vec<String> = valid.split('.').map(str::to_owned).collect();
    let signature = &parts[2];
    let flipped = if signature.starts_with('A') { "B" } else { "A" };
    parts[2] = format!("{flipped}{}", &signature[1..]);
    let tampered = check(&parts.join("."));
    assert!(
        matches!(tampered, Err(PushError::InvalidToken(_))),
        "{tampered:?}"
    );
    // A payload re-signed by nobody: swap claims under the original signature.
    let other = token(AUDIENCE, "other@example.com", true);
    let other_claims = other.split('.').nth(1).expect("claims");
    let forged = format!(
        "{}.{other_claims}.{}",
        parts[0],
        valid.split('.').nth(2).expect("sig")
    );
    let forged = check(&forged);
    assert!(
        matches!(forged, Err(PushError::InvalidToken(_))),
        "{forged:?}"
    );
}
