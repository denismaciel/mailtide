//! Gmail push via Pub/Sub: bearer-token verification and notification parsing.
//!
//! Pub/Sub signs each push with a Google OIDC token (RS256). The host fetches
//! and caches Google's keys; [`Jwks::parse`] validates them once, and
//! [`verify`] checks signature, issuer, audience, the push service account
//! and expiry against the time it is given (no clock is read here). A
//! verified push names an account and a history id: a hint, never data.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use mailtide::ids::{AccountId, HistoryId, Timestamp};
use serde::Deserialize;

/// Google's signing keys, parsed and validated.
#[derive(Debug, Clone)]
pub struct Jwks(JwkSet);

impl Jwks {
    /// Parse Google's JWKS document.
    pub fn parse(document: &[u8]) -> Result<Self, PushError> {
        serde_json::from_slice(document)
            .map(Self)
            .map_err(|e| PushError::Malformed(format!("jwks: {e}")))
    }
}

/// What a valid push must claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PushExpectations {
    /// The token audience (as configured on the Pub/Sub subscription).
    pub audience: String,
    /// The push service account's email.
    pub service_account: String,
}

/// A verified notification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notification {
    /// The mailbox.
    pub account: AccountId,
    /// The mailbox revision.
    pub history_id: HistoryId,
}

/// Why a push was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PushError {
    /// Missing or malformed `Authorization`.
    MissingToken,
    /// Signature, key, issuer or audience didn't verify.
    InvalidToken(String),
    /// Signed by an account other than the push service account.
    WrongSigner,
    /// The token expired.
    Expired,
    /// The envelope or payload isn't a Gmail notification.
    Malformed(String),
}

#[derive(Deserialize)]
struct Claims {
    email: Option<String>,
    email_verified: Option<bool>,
    exp: i64,
}

#[derive(Deserialize)]
struct Envelope {
    message: PubsubMessage,
}

#[derive(Deserialize)]
struct PubsubMessage {
    data: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Payload {
    email_address: String,
    history_id: serde_json::Value,
}

const ISSUERS: [&str; 2] = ["https://accounts.google.com", "accounts.google.com"];

/// Verify a push's bearer token and parse its notification.
pub fn verify(
    authorization: &str,
    body: &[u8],
    jwks: &Jwks,
    expect: &PushExpectations,
    now: Timestamp,
) -> Result<Notification, PushError> {
    let token = authorization
        .strip_prefix("Bearer ")
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or(PushError::MissingToken)?;
    let header = decode_header(token).map_err(|e| PushError::InvalidToken(e.to_string()))?;
    let kid = header
        .kid
        .ok_or_else(|| PushError::InvalidToken("no key id".into()))?;
    let jwk = jwks
        .0
        .find(&kid)
        .ok_or_else(|| PushError::InvalidToken(format!("unknown key {kid}")))?;
    let key = DecodingKey::from_jwk(jwk).map_err(|e| PushError::InvalidToken(e.to_string()))?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&[expect.audience.as_str()]);
    validation.set_issuer(&ISSUERS);
    // Expiry is checked against the time we were given, not a clock.
    validation.validate_exp = false;
    let claims = decode::<Claims>(token, &key, &validation)
        .map_err(|e| PushError::InvalidToken(e.to_string()))?
        .claims;
    if claims.exp.saturating_mul(1000) <= now.0 {
        return Err(PushError::Expired);
    }
    if claims.email.as_deref() != Some(expect.service_account.as_str())
        || claims.email_verified != Some(true)
    {
        return Err(PushError::WrongSigner);
    }
    let envelope: Envelope =
        serde_json::from_slice(body).map_err(|e| PushError::Malformed(format!("envelope: {e}")))?;
    let data = STANDARD
        .decode(envelope.message.data.as_bytes())
        .map_err(|e| PushError::Malformed(format!("data: {e}")))?;
    let payload: Payload =
        serde_json::from_slice(&data).map_err(|e| PushError::Malformed(format!("payload: {e}")))?;
    let history_id = match payload.history_id {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
    .ok_or_else(|| PushError::Malformed("historyId".into()))?;
    let account = AccountId::parse(&payload.email_address)
        .map_err(|_| PushError::Malformed("emailAddress".into()))?;
    Ok(Notification {
        account,
        history_id: HistoryId(history_id),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use serde_json::json;

    const KEY: &str = include_str!("../testdata/push-test-key.pem");
    const JWKS: &str = include_str!("../testdata/push-test-jwks.json");
    const NOW: i64 = 1_700_000_000;

    fn expect() -> PushExpectations {
        PushExpectations {
            audience: "https://example.com/push".into(),
            service_account: "push@project.iam.gserviceaccount.com".into(),
        }
    }

    fn token(claims: &serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some("test-key".into());
        let key = EncodingKey::from_rsa_pem(KEY.as_bytes()).expect("test key");
        encode(&header, claims, &key).expect("signs")
    }

    fn claims() -> serde_json::Value {
        json!({
            "iss": "https://accounts.google.com",
            "aud": "https://example.com/push",
            "email": "push@project.iam.gserviceaccount.com",
            "email_verified": true,
            "iat": NOW,
            "exp": NOW + 3600,
        })
    }

    fn body(history_id: &serde_json::Value) -> Vec<u8> {
        let data = STANDARD.encode(
            json!({"emailAddress": "Alice@Example.com", "historyId": history_id}).to_string(),
        );
        json!({"message": {"data": data, "messageId": "1"}, "subscription": "s"})
            .to_string()
            .into_bytes()
    }

    fn jwks() -> Jwks {
        Jwks::parse(JWKS.as_bytes()).expect("jwks")
    }

    #[test]
    fn a_valid_push_names_the_account_and_history_id() {
        let auth = format!("Bearer {}", token(&claims()));
        let notification = verify(
            &auth,
            &body(&json!(4242)),
            &jwks(),
            &expect(),
            Timestamp(NOW * 1000),
        )
        .expect("valid");
        assert_eq!(notification.account.as_str(), "alice@example.com");
        assert_eq!(notification.history_id, HistoryId(4242));
        let text = verify(
            &auth,
            &body(&json!("4243")),
            &jwks(),
            &expect(),
            Timestamp(NOW * 1000),
        )
        .expect("valid");
        assert_eq!(text.history_id, HistoryId(4243));
    }

    #[test]
    fn expired_wrong_audience_and_wrong_signer_are_refused() {
        let auth = format!("Bearer {}", token(&claims()));
        let later = Timestamp((NOW + 7200) * 1000);
        assert_eq!(
            verify(&auth, &body(&json!(1)), &jwks(), &expect(), later),
            Err(PushError::Expired)
        );
        let mut other = claims();
        other["aud"] = json!("https://elsewhere");
        let auth = format!("Bearer {}", token(&other));
        assert!(matches!(
            verify(
                &auth,
                &body(&json!(1)),
                &jwks(),
                &expect(),
                Timestamp(NOW * 1000)
            ),
            Err(PushError::InvalidToken(_))
        ));
        let mut stranger = claims();
        stranger["email"] = json!("someone@else.com");
        let auth = format!("Bearer {}", token(&stranger));
        assert_eq!(
            verify(
                &auth,
                &body(&json!(1)),
                &jwks(),
                &expect(),
                Timestamp(NOW * 1000)
            ),
            Err(PushError::WrongSigner)
        );
    }

    #[test]
    fn missing_tokens_are_refused() {
        assert_eq!(
            verify("", b"{}", &jwks(), &expect(), Timestamp(0)),
            Err(PushError::MissingToken)
        );
    }
}
