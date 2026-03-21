//! At-rest page encryption using XChaCha20-Poly1305.
//!
//! # Physical page layout on disk
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────┐
//! │  nonce (24 bytes)  │  ciphertext + auth tag (page_size + 16 bytes)  │
//! └─────────────────────────────────────────────────────────────────────┘
//!   total on disk: page_size + 40 bytes  ("encrypted stride")
//! ```
//!
//! The nonce is generated fresh from the OS CSPRNG on every write.
//! It is stored inline in the page so decryption is self-contained.
//!
//! The `page_id` is passed as AEAD Associated Authenticated Data (AAD) on
//! both encrypt and decrypt.  This cryptographically binds each ciphertext
//! to its logical page location: swapping the encrypted blob of page A into
//! slot B will cause the AEAD tag to fail, defeating page-swap / relocation
//! attacks without any additional nonce comparison.
//!
//! # Passthrough mode
//!
//! When constructed with [`EncryptionContext::none`], all operations are
//! identity functions — plaintext in, plaintext out.

use chacha20poly1305::{
    aead::{Aead, KeyInit, Payload},
    XChaCha20Poly1305, XNonce,
};
use rand::{rngs::OsRng, RngCore};
use sparrowdb_common::{Error, Result};

/// Encryption context — holds the 32-byte master key for an open database.
///
/// When the key is `None` the context operates in passthrough mode and pages
/// are read/written without any encryption.
pub struct EncryptionContext {
    cipher: Option<XChaCha20Poly1305>,
}

impl EncryptionContext {
    /// Create a context that performs no encryption (passthrough mode).
    pub fn none() -> Self {
        Self { cipher: None }
    }

    /// Create a context that encrypts all pages with the given 32-byte key.
    pub fn with_key(key: [u8; 32]) -> Self {
        let cipher = XChaCha20Poly1305::new(&key.into());
        Self {
            cipher: Some(cipher),
        }
    }

    /// Returns `true` if this context has an encryption key (non-passthrough).
    pub fn is_encrypted(&self) -> bool {
        self.cipher.is_some()
    }

    /// Encrypt a WAL record payload.
    ///
    /// `lsn` is used as the AEAD AAD, binding the ciphertext to its log position.
    ///
    /// Output layout: `[nonce: 24 bytes][ciphertext+tag: plaintext.len()+16 bytes]`
    ///
    /// In passthrough mode the plaintext is returned as-is.
    ///
    /// # Errors
    /// Returns [`Error::Corruption`] if the underlying AEAD encrypt fails.
    pub fn encrypt_wal_payload(&self, lsn: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        let cipher = match &self.cipher {
            None => return Ok(plaintext.to_vec()),
            Some(c) => c,
        };

        let mut nonce_bytes = [0u8; 24];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = *XNonce::from_slice(&nonce_bytes);
        let aad = lsn.to_le_bytes();

        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| Error::Corruption("XChaCha20-Poly1305 WAL encrypt failed".into()))?;

        let mut output = Vec::with_capacity(24 + ciphertext.len());
        output.extend_from_slice(nonce.as_slice());
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    /// Decrypt a WAL record payload encrypted with [`encrypt_wal_payload`].
    ///
    /// `lsn` is used as AEAD AAD — must match the value used during encryption.
    ///
    /// In passthrough mode the data is returned as-is.
    ///
    /// # Errors
    /// - [`Error::EncryptionAuthFailed`] — wrong key or the LSN does not match.
    /// - [`Error::InvalidArgument`] — `encrypted` is shorter than 40 bytes.
    pub fn decrypt_wal_payload(&self, lsn: u64, encrypted: &[u8]) -> Result<Vec<u8>> {
        let cipher = match &self.cipher {
            None => return Ok(encrypted.to_vec()),
            Some(c) => c,
        };

        if encrypted.len() < 40 {
            return Err(Error::InvalidArgument(format!(
                "encrypted WAL payload is {} bytes; minimum is 40 (24-byte nonce + 16-byte tag)",
                encrypted.len()
            )));
        }

        let nonce = XNonce::from_slice(&encrypted[..24]);
        let aad = lsn.to_le_bytes();
        let plaintext = cipher
            .decrypt(
                nonce,
                Payload {
                    msg: &encrypted[24..],
                    aad: &aad,
                },
            )
            .map_err(|_| Error::EncryptionAuthFailed)?;

        Ok(plaintext)
    }

    /// Encrypt a plaintext page and return the on-disk representation.
    ///
    /// A fresh 24-byte nonce is generated from the OS CSPRNG on every call.
    /// `page_id` is passed as AEAD AAD so the ciphertext is cryptographically
    /// bound to its logical page location.
    ///
    /// Output layout: `[nonce: 24 bytes][ciphertext+tag: plaintext.len()+16 bytes]`
    /// Total length: `plaintext.len() + 40`.
    ///
    /// In passthrough mode the plaintext is returned as-is (no overhead bytes).
    ///
    /// # Errors
    /// Returns [`Error::Corruption`] if the underlying AEAD encrypt fails
    /// (extremely unlikely — only possible if plaintext is too large for the
    /// AEAD to handle, which the chacha20poly1305 crate does not bound in
    /// normal usage).
    pub fn encrypt_page(&self, page_id: u64, plaintext: &[u8]) -> Result<Vec<u8>> {
        let cipher = match &self.cipher {
            None => return Ok(plaintext.to_vec()),
            Some(c) => c,
        };

        // Generate a fresh random nonce for every write.
        let mut nonce_bytes = [0u8; 24];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = *XNonce::from_slice(&nonce_bytes);

        let aad = page_id.to_le_bytes();
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| Error::Corruption("XChaCha20-Poly1305 encrypt failed".into()))?;

        // Prepend the nonce so the on-disk page is self-describing.
        let mut output = Vec::with_capacity(24 + ciphertext.len());
        output.extend_from_slice(nonce.as_slice());
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    /// Decrypt an on-disk page back to plaintext.
    ///
    /// Expects `encrypted` to be at least 40 bytes (`24` nonce + `16` tag).
    ///
    /// `page_id` is passed as AEAD AAD — the AEAD authentication tag will
    /// reject ciphertexts encrypted under a different `page_id`, defeating
    /// page-swap / relocation attacks.
    ///
    /// In passthrough mode the data is returned as-is.
    ///
    /// # Errors
    /// - [`Error::EncryptionAuthFailed`] — the AEAD authentication tag was
    ///   rejected (wrong key, corrupted data, or page-swap attack detected).
    /// - [`Error::InvalidArgument`] — `encrypted` is shorter than 40 bytes.
    pub fn decrypt_page(&self, page_id: u64, encrypted: &[u8]) -> Result<Vec<u8>> {
        let cipher = match &self.cipher {
            None => return Ok(encrypted.to_vec()),
            Some(c) => c,
        };

        if encrypted.len() < 40 {
            return Err(Error::InvalidArgument(format!(
                "encrypted page is {} bytes; minimum is 40 (24-byte nonce + 16-byte tag)",
                encrypted.len()
            )));
        }

        let nonce = XNonce::from_slice(&encrypted[..24]);
        let aad = page_id.to_le_bytes();
        let plaintext = cipher
            .decrypt(
                nonce,
                Payload {
                    msg: &encrypted[24..],
                    aad: &aad,
                },
            )
            .map_err(|_| Error::EncryptionAuthFailed)?;

        Ok(plaintext)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn nonces_are_random() {
        // Two encryptions of the same page must produce different nonces.
        let ctx = EncryptionContext::with_key([0x01; 32]);
        let pt = vec![0u8; 32];
        let ct0 = ctx.encrypt_page(0, &pt).unwrap();
        let ct1 = ctx.encrypt_page(0, &pt).unwrap();
        // The first 24 bytes are the nonces — they must differ.
        assert_ne!(
            &ct0[..24],
            &ct1[..24],
            "nonces must be random, not deterministic"
        );
    }

    #[test]
    fn encrypt_output_length() {
        let ctx = EncryptionContext::with_key([0x01; 32]);
        let pt = vec![0u8; 512];
        let ct = ctx.encrypt_page(0, &pt).unwrap();
        assert_eq!(ct.len(), 512 + 40);
    }

    #[test]
    fn passthrough_is_identity() {
        let ctx = EncryptionContext::none();
        let data = vec![0xFFu8; 256];
        assert_eq!(ctx.encrypt_page(5, &data).unwrap(), data);
        assert_eq!(ctx.decrypt_page(5, &data).unwrap(), data);
    }

    #[test]
    fn too_short_ciphertext_is_rejected() {
        let ctx = EncryptionContext::with_key([0x01; 32]);
        let result = ctx.decrypt_page(0, &[0u8; 39]);
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }
}
