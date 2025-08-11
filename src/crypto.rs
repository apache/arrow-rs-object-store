use std::fmt::{Debug, Display};

type DynError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub struct Error {
    inner: DynError,
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Crypto error: {}", self.inner)
    }
}

impl std::error::Error for Error {}

impl From<DynError> for Error {
    fn from(err: DynError) -> Self {
        Error { inner: err }
    }
}

/// TODO(jakedern): Docs
pub trait CryptoProvider: Send + Sync + Debug + 'static {
    fn digest_sha256(bytes: &[u8]) -> Result<impl AsRef<[u8]>, Error>;
    fn hmac_sha256(secret: &[u8], bytes: &[u8]) -> Result<impl AsRef<[u8]>, Error>;
    fn hex_digest(bytes: &[u8]) -> Result<String, Error> {
        let digest = Self::digest_sha256(bytes)?;
        Ok(hex_encode(digest.as_ref()))
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
    use super::{CryptoProvider, Error};

    #[derive(Debug, Clone, Copy)]
    pub struct RingProvider;

    impl CryptoProvider for RingProvider {
        fn digest_sha256(bytes: &[u8]) -> Result<impl AsRef<[u8]>, Error> {
            let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
            Ok(digest)
        }

        fn hmac_sha256(secret: &[u8], bytes: &[u8]) -> Result<impl AsRef<[u8]>, Error> {
            let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret);
            let tag = ring::hmac::sign(&key, bytes);
            Ok(tag)
        }
    }

    impl Default for RingProvider {
        fn default() -> Self {
            RingProvider
        }
    }
}

// pub mod openssl_crypto {
//     use openssl::hash::MessageDigest;
//     use openssl::pkey::PKey;
//     use openssl::sign::Signer;
//
//     use super::{CryptoError, CryptoProvider, DynError};
//
//     pub struct OpenSslCrypto;
//
//     impl CryptoProvider for OpenSslCrypto {
//         fn digest_sha256(bytes: &[u8]) -> Result<impl AsRef<[u8]>, CryptoError> {
//             let digest = openssl::hash::hash(MessageDigest::sha256(), bytes)
//                 .map_err(|e| Box::new(e) as DynError)?;
//             Ok(digest)
//         }
//
//         fn hmac_sha256(secret: &[u8], bytes: &[u8]) -> Result<impl AsRef<[u8]>, CryptoError> {
//             let key = PKey::hmac(secret).map_err(|e| Box::new(e) as DynError)?;
//             let mut signer =
//                 Signer::new(MessageDigest::sha256(), &key).map_err(|e| Box::new(e) as DynError)?;
//             signer.update(bytes).map_err(|e| Box::new(e) as DynError)?;
//             let hmac = signer.sign_to_vec().map_err(|e| Box::new(e) as DynError)?;
//             Ok(hmac)
//         }
//     }
// }
