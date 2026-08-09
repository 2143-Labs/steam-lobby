//! TURN REST-auth credential minting (coturn `static-auth-secret` scheme).
//!
//! The wire format is the coturn long-term-credentials REST form:
//! `username = "<unix-expiry>:steam-lobby"`,
//! `password = base64(HMAC-SHA1(secret, username))` with the shared secret
//! used as the literal HMAC key bytes. Verified end-to-end against coturn
//! 4.17.0. Pure module — no axum, no DB.
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use sha1::Sha1;

use serde::Serialize;

/// Credentials handed to a client so its `RTCPeerConnection` can use the
/// coturn relay. Field names are the JSON contract the demo consumes.
#[derive(Serialize)]
pub struct TurnCredentials {
    pub username: String,
    pub password: String,
    pub ttl: u64,
    pub uris: Vec<String>,
}

/// Mint a credential set valid from `now_secs` for `ttl_secs` seconds.
/// Pure and deterministic for a given input — the only testable unit in the
/// TURN path.
pub fn mint_turn_credentials(
    secret: &str,
    ttl_secs: u64,
    now_secs: u64,
    uris: &[String],
) -> TurnCredentials {
    let expiry = now_secs + ttl_secs;
    let username = format!("{expiry}:steam-lobby");
    let mut mac =
        Hmac::<Sha1>::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(username.as_bytes());
    let password = STANDARD.encode(&mac.finalize().into_bytes()[..]);
    TurnCredentials {
        username,
        password,
        ttl: ttl_secs,
        uris: uris.to_vec(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    /// RFC 2202 §2 test case 2: key "Jefe", data "what do ya want for nothing?"
    /// -> HMAC-SHA1 effcdf6ae5eb2fa2d27416d5f184df9c259a7c79. Guards the
    /// hmac/sha1/base64 crate wiring, not just our formatting.
    #[test]
    fn rfc2202_vector() {
        let mut mac = Hmac::<Sha1>::new_from_slice(b"Jefe").unwrap();
        mac.update(b"what do ya want for nothing?");
        let digest = mac.finalize().into_bytes();
        assert_eq!(
            hex(&digest),
            "effcdf6ae5eb2fa2d27416d5f184df9c259a7c79",
            "HMAC-SHA1 mismatch — crate API drift?"
        );
        assert_eq!(
            STANDARD.encode(digest),
            "7/zfauXrL6LSdBbV8YTfnCWafHk=",
            "base64 encode mismatch"
        );
    }

    #[test]
    fn username_has_expiry_and_suffix() {
        let c = mint_turn_credentials("s3cret", 3600, 1_700_000_000, &[]);
        assert_eq!(c.username, "1700003600:steam-lobby");
        assert_eq!(c.ttl, 3600);
    }

    #[test]
    fn uris_passthrough() {
        let uris = vec!["turn:turn.example.com:3478?transport=udp".to_string()];
        let c = mint_turn_credentials("s3cret", 3600, 0, &uris);
        assert_eq!(c.uris, uris);
    }

    #[test]
    fn empty_secret_is_deterministic() {
        let a = mint_turn_credentials("", 3600, 1_700_000_000, &[]);
        let b = mint_turn_credentials("", 3600, 1_700_000_000, &[]);
        assert_eq!(a.password, b.password);
        assert!(!a.password.is_empty(), "empty key is a valid HMAC key");
    }
}
