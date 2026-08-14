//! KMS session encryption (AES-256-GCM).
//!
//! When the account's *Session Manager preferences* document enables "Encrypt
//! session data", the SSM agent asks the client to negotiate a data key during
//! the handshake.  Every stream-data payload is then encrypted end-to-end
//! between the client and the agent, so the Amazon Message Gateway Service
//! carries only ciphertext.
//!
//! # Key agreement
//!
//! 1. The agent sends a `KMSEncryption` handshake action carrying the KMS key ID
//!    configured in the session preferences.
//! 2. The client calls `kms:GenerateDataKey` for **64 bytes** with the
//!    encryption context `{"aws:ssm:SessionId": …, "aws:ssm:TargetId": …}`.
//! 3. The 64-byte plaintext key is split in half: the **first** 32 bytes are the
//!    client's *decryption* key and the **last** 32 bytes are its *encryption*
//!    key.  The agent decrypts the same ciphertext blob and applies the halves
//!    the other way round, giving each direction its own key.
//! 4. The client returns the ciphertext blob in its handshake response so the
//!    agent can call `kms:Decrypt` on it.
//!
//! Because the client calls `GenerateDataKey` and the agent calls `Decrypt`,
//! **both** principals need the corresponding KMS grants — a session that fails
//! here with `AccessDeniedException` usually means the caller's IAM policy is
//! missing `kms:GenerateDataKey` on the configured key.
//!
//! # Frame layout
//!
//! ```text
//! ┌──────────────┬──────────────────────────────────┐
//! │ nonce (12 B) │ AES-256-GCM ciphertext ‖ tag(16) │
//! └──────────────┴──────────────────────────────────┘
//! ```
//!
//! A fresh random nonce is generated per message and prepended, matching the
//! reference implementation in `session-manager-plugin/src/encryption`.
//!
//! # Without the `kms` feature
//!
//! [`SessionCrypto`] still exists but is an uninhabited type, so
//! `Option<Arc<SessionCrypto>>` is statically always `None` and every call site
//! compiles unchanged with no `cfg` branching.  A session whose agent demands
//! encryption then fails the handshake with an actionable message instead of
//! silently continuing in the clear.

#[cfg(feature = "kms")]
use aes_gcm::aead::{Aead, KeyInit, OsRng};
#[cfg(feature = "kms")]
use aes_gcm::{AeadCore, Aes256Gcm, Key, Nonce};
use bytes::Bytes;
#[cfg(feature = "kms")]
use std::collections::HashMap;
#[cfg(feature = "kms")]
use zeroize::Zeroizing;

#[cfg(feature = "kms")]
use crate::errors::Error;
use crate::errors::Result;

/// Session encryption is not compiled into this build.
///
/// This type is uninhabited, so no value of it can exist and every
/// encrypt/decrypt path is statically unreachable.
#[cfg(not(feature = "kms"))]
#[derive(Debug)]
pub enum SessionCrypto {}

#[cfg(not(feature = "kms"))]
impl SessionCrypto {
    /// Unreachable: no `SessionCrypto` value can be constructed in this build.
    pub fn encrypt(&self, _plaintext: &[u8]) -> Result<Bytes> {
        match *self {}
    }

    /// Unreachable: no `SessionCrypto` value can be constructed in this build.
    pub fn decrypt(&self, _frame: &[u8]) -> Result<Bytes> {
        match *self {}
    }
}

/// Bytes requested from `kms:GenerateDataKey`; split into two AES-256 keys.
#[cfg(feature = "kms")]
const DATA_KEY_BYTES: i32 = 64;
/// AES-256-GCM nonce length.
#[cfg(feature = "kms")]
const NONCE_LEN: usize = 12;
/// AES-256-GCM authentication tag length.
#[cfg(feature = "kms")]
const TAG_LEN: usize = 16;

/// A negotiated pair of directional AES-256-GCM keys for one session.
///
/// The KMS key material is scrubbed as soon as the ciphers are built.  The
/// struct deliberately does not implement `Clone`, and its `Debug` is opaque, so
/// key state can be neither duplicated nor accidentally logged.
#[cfg(feature = "kms")]
pub struct SessionCrypto {
    /// Ciphertext blob to hand back to the agent so it can derive the same keys.
    cipher_text_blob: Bytes,
    encrypt: Aes256Gcm,
    decrypt: Aes256Gcm,
}

#[cfg(feature = "kms")]
impl std::fmt::Debug for SessionCrypto {
    /// Deliberately opaque: a derived `Debug` on key state is one stray
    /// `dbg!` away from printing session keys into a log aggregator.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionCrypto(AES-256-GCM, keys redacted)")
    }
}

#[cfg(feature = "kms")]
impl SessionCrypto {
    /// Negotiate a data key with KMS for this session.
    ///
    /// `target_id` is the session's target as passed to `StartSession`; it is
    /// part of the KMS encryption context and must match what the agent uses,
    /// otherwise the agent's `kms:Decrypt` call fails.
    pub async fn negotiate(
        kms: &aws_sdk_kms::Client,
        kms_key_id: &str,
        session_id: &str,
        target_id: &str,
    ) -> Result<Self> {
        let context = HashMap::from([
            ("aws:ssm:SessionId".to_owned(), session_id.to_owned()),
            ("aws:ssm:TargetId".to_owned(), target_id.to_owned()),
        ]);

        let output = kms
            .generate_data_key()
            .key_id(kms_key_id)
            .number_of_bytes(DATA_KEY_BYTES)
            .set_encryption_context(Some(context))
            .send()
            .await
            .map_err(Error::from)?;

        let plaintext = output
            .plaintext()
            .ok_or_else(|| Error::Crypto("KMS returned no plaintext data key".into()))?;
        let blob = output
            .ciphertext_blob()
            .ok_or_else(|| Error::Crypto("KMS returned no ciphertext blob".into()))?;

        let material = Zeroizing::new(plaintext.as_ref().to_vec());
        Self::from_key_material(&material, Bytes::copy_from_slice(blob.as_ref()))
    }

    /// Build the directional ciphers from raw 64-byte key material.
    ///
    /// Split out from [`negotiate`](Self::negotiate) so the split-and-assign
    /// rule can be tested without calling KMS.
    fn from_key_material(material: &[u8], cipher_text_blob: Bytes) -> Result<Self> {
        if material.len() != DATA_KEY_BYTES as usize {
            return Err(Error::Crypto(format!(
                "expected a {DATA_KEY_BYTES}-byte data key, got {}",
                material.len()
            )));
        }
        let (decrypt_half, encrypt_half) = material.split_at(material.len() / 2);
        Ok(Self {
            cipher_text_blob,
            encrypt: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(encrypt_half)),
            decrypt: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(decrypt_half)),
        })
    }

    /// The KMS ciphertext blob to send to the agent in the handshake response.
    pub fn cipher_text_blob(&self) -> &Bytes {
        &self.cipher_text_blob
    }

    /// Encrypt an outbound payload, prepending a fresh random nonce.
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Bytes> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let ciphertext = self
            .encrypt
            .encrypt(&nonce, plaintext)
            .map_err(|_| Error::Crypto("AES-GCM encryption failed".into()))?;

        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(&ciphertext);
        Ok(Bytes::from(out))
    }

    /// Decrypt an inbound payload whose first 12 bytes are the nonce.
    ///
    /// A failure here means the frame was tampered with, truncated, or the two
    /// sides disagree about the key — all of which are fatal for the session.
    pub fn decrypt(&self, frame: &[u8]) -> Result<Bytes> {
        if frame.len() < NONCE_LEN + TAG_LEN {
            return Err(Error::Crypto(format!(
                "encrypted frame is {} bytes, shorter than the {}-byte minimum",
                frame.len(),
                NONCE_LEN + TAG_LEN
            )));
        }
        let (nonce, ciphertext) = frame.split_at(NONCE_LEN);
        let plaintext = self
            .decrypt
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|_| {
                Error::Crypto("AES-GCM authentication failed: frame corrupt or key mismatch".into())
            })?;
        Ok(Bytes::from(plaintext))
    }
}

#[cfg(all(test, feature = "kms"))]
mod tests {
    use super::*;

    /// Build the two peers of a session: the client, and the agent-side view
    /// that swaps the key halves.
    fn peers() -> (SessionCrypto, SessionCrypto) {
        let material: Vec<u8> = (0..64u8).collect();
        let client = SessionCrypto::from_key_material(&material, Bytes::from_static(b"blob"))
            .expect("valid key material");

        // The agent applies the halves the other way round.
        let mut swapped = material[32..].to_vec();
        swapped.extend_from_slice(&material[..32]);
        let agent = SessionCrypto::from_key_material(&swapped, Bytes::from_static(b"blob"))
            .expect("valid key material");

        (client, agent)
    }

    #[test]
    fn client_ciphertext_decrypts_on_the_agent_side() {
        let (client, agent) = peers();
        let plaintext = b"echo hello from the client\n";

        let frame = client.encrypt(plaintext).unwrap();
        assert_eq!(agent.decrypt(&frame).unwrap().as_ref(), plaintext);
    }

    #[test]
    fn agent_ciphertext_decrypts_on_the_client_side() {
        let (client, agent) = peers();
        let plaintext = b"total 0\r\n";

        let frame = agent.encrypt(plaintext).unwrap();
        assert_eq!(client.decrypt(&frame).unwrap().as_ref(), plaintext);
    }

    /// Each direction uses a different key, so a peer must not be able to read
    /// back its own ciphertext.  If this ever passes, the halves were assigned
    /// the same way on both sides and the split is wrong.
    #[test]
    fn a_peer_cannot_decrypt_its_own_ciphertext() {
        let (client, _agent) = peers();
        let frame = client.encrypt(b"secret").unwrap();
        assert!(client.decrypt(&frame).is_err());
    }

    #[test]
    fn frame_layout_is_nonce_then_ciphertext_and_tag() {
        let (client, _) = peers();
        let plaintext = b"1234567890";
        let frame = client.encrypt(plaintext).unwrap();
        assert_eq!(frame.len(), NONCE_LEN + plaintext.len() + TAG_LEN);
    }

    #[test]
    fn nonces_are_unique_per_message() {
        let (client, _) = peers();
        let a = client.encrypt(b"same plaintext").unwrap();
        let b = client.encrypt(b"same plaintext").unwrap();
        assert_ne!(a[..NONCE_LEN], b[..NONCE_LEN], "nonce must not repeat");
        assert_ne!(a, b);
    }

    #[test]
    fn tampering_is_detected() {
        let (client, agent) = peers();
        let mut frame = client.encrypt(b"transfer $10").unwrap().to_vec();
        let last = frame.len() - 1;
        frame[last] ^= 0x01;
        assert!(
            agent.decrypt(&frame).is_err(),
            "GCM tag must reject tampering"
        );
    }

    #[test]
    fn short_frames_are_rejected_without_panicking() {
        let (_, agent) = peers();
        for len in 0..(NONCE_LEN + TAG_LEN) {
            assert!(agent.decrypt(&vec![0u8; len]).is_err(), "len {len}");
        }
    }

    #[test]
    fn empty_payload_round_trips() {
        let (client, agent) = peers();
        let frame = client.encrypt(b"").unwrap();
        assert!(agent.decrypt(&frame).unwrap().is_empty());
    }

    #[test]
    fn wrong_key_length_is_rejected() {
        // `SessionCrypto` has no `Debug` by design (it holds key state), so
        // match on the result rather than unwrapping it.
        let Err(err) = SessionCrypto::from_key_material(&[0u8; 32], Bytes::new()) else {
            panic!("a 32-byte key must be rejected");
        };
        assert!(err.to_string().contains("64-byte data key"), "{err}");
    }
}
