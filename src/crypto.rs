use std::fmt::Debug;
use std::sync::Arc;

pub type CryptoProviderRef = Arc<dyn CryptoProvider>;

/// TODO(jakedern): Docs
pub trait CryptoProvider: Send + Sync + Debug + 'static {
    fn digest_sha256(&self, bytes: &[u8]) -> crate::Result<Digest>;
    fn hmac_sha256(&self, secret: &[u8], bytes: &[u8]) -> crate::Result<Tag>;
}

#[derive(Debug)]
pub struct Digest(Vec<u8>);

impl From<&[u8]> for Digest {
    fn from(bytes: &[u8]) -> Self {
        Digest(bytes.to_vec())
    }
}

impl AsRef<[u8]> for Digest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug)]
pub struct Tag(Vec<u8>);

impl AsRef<[u8]> for Tag {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for Tag {
    fn from(bytes: &[u8]) -> Self {
        Tag(bytes.to_vec())
    }
}

impl From<Vec<u8>> for Tag {
    fn from(bytes: Vec<u8>) -> Self {
        Tag(bytes)
    }
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// TODO(jakedern): Docs
#[cfg(feature = "ring")]
pub mod ring_crypto {
    use super::{CryptoProvider, Digest, Tag};

    #[derive(Debug, Clone, Copy)]
    pub struct RingProvider;

    impl CryptoProvider for RingProvider {
        fn digest_sha256(&self, bytes: &[u8]) -> crate::Result<Digest> {
            let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
            Ok(digest.as_ref().into())
        }

        fn hmac_sha256(&self, secret: &[u8], bytes: &[u8]) -> crate::Result<Tag> {
            let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
            let tag = ring::hmac::sign(&key, bytes);
            Ok(tag.as_ref().into())
        }
    }

    impl Default for RingProvider {
        fn default() -> Self {
            RingProvider
        }
    }
}

pub mod openssl_crypto {
    use openssl::hash::MessageDigest;
    use openssl::pkey::PKey;
    use openssl::sign::Signer;

    use super::{CryptoProvider, Digest, Tag};

    #[derive(Debug, Clone, Copy)]
    pub struct OpenSslCrypto;

    impl CryptoProvider for OpenSslCrypto {
        fn digest_sha256(&self, bytes: &[u8]) -> crate::Result<Digest> {
            let digest = openssl::hash::hash(MessageDigest::sha256(), bytes)?;
            Ok(digest.as_ref().into())
        }

        fn hmac_sha256(&self, secret: &[u8], bytes: &[u8]) -> crate::Result<Tag> {
            let key = PKey::hmac(secret)?;
            let mut signer = Signer::new(MessageDigest::sha256(), &key)?;
            signer.update(bytes)?;
            let hmac = signer.sign_to_vec()?;
            Ok(hmac.into())
        }
    }

    impl From<openssl::error::ErrorStack> for crate::Error {
        fn from(value: openssl::error::ErrorStack) -> Self {
            // TODO(jakedern)
            todo!()
        }
    }
}
