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
//! The nonce is derived deterministically per page:
//! `nonce[0..8] = page_id.to_le_bytes()`, `nonce[8..24] = 0x00`.
//!
//! This is safe because a fresh 32-byte key is generated per database file, so
//! the (key, nonce) pair is unique across all pages of all databases. The nonce
//! is still stored inline in the page for forward compatibility (future versions
//! may use random per-write nonces).
//!
//! # Passthrough mode
//!
//! When constructed with [`EncryptionContext::none`], all operations are
//! identity functions — plaintext in, plaintext out.

use chacha20poly1305::{
    aead::{Aead, KeyInit},
    XChaCha20Poly1305, XNonce,
};
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

    /// Derive the 24-byte XChaCha20 nonce for a page.
    ///
    /// Layout: `[page_id LE u64 (8 bytes)][zeros (16 bytes)]`
    fn nonce_for(page_id: u64) -> XNonce {
        let mut nonce = [0u8; 24];
        nonce[..8].copy_from_slice(&page_id.to_le_bytes());
        *XNonce::from_slice(&nonce)
    }

    /// Encrypt a plaintext page and return the on-disk representation.
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

        let nonce = Self::nonce_for(page_id);
        let ciphertext = cipher
            .encrypt(&nonce, plaintext)
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
    /// In passthrough mode the data is returned as-is.
    ///
    /// # Errors
    /// - [`Error::DecryptionFailed`] — AEAD authentication tag rejected.
    ///   This means the wrong key was supplied or the page is corrupted.
    /// - [`Error::InvalidArgument`] — `encrypted` is shorter than 40 bytes.
    pub fn decrypt_page(&self, _page_id: u64, encrypted: &[u8]) -> Result<Vec<u8>> {
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
        let plaintext = cipher
            .decrypt(nonce, &encrypted[24..])
            .map_err(|_| Error::DecryptionFailed)?;

        Ok(plaintext)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn nonce_is_deterministic() {
        let n0a = EncryptionContext::nonce_for(0);
        let n0b = EncryptionContext::nonce_for(0);
        assert_eq!(n0a, n0b);
    }

    #[test]
    fn nonce_encodes_page_id() {
        let n = EncryptionContext::nonce_for(0xDEAD_BEEF_CAFE_1234);
        let expected = 0xDEAD_BEEF_CAFE_1234u64.to_le_bytes();
        assert_eq!(&n.as_slice()[..8], &expected);
        // Trailing bytes must be zero.
        assert!(n.as_slice()[8..].iter().all(|&b| b == 0));
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
