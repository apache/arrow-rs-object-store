// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

#[cfg(test)]
pub mod noop_crypto {
    use super::{CryptoProvider, Digest, Tag};
    #[derive(Debug, Clone, Copy)]
    pub struct NoopCrypto;

    impl CryptoProvider for NoopCrypto {
        fn digest_sha256(&self, bytes: &[u8]) -> crate::Result<Digest> {
            Ok(Digest(bytes.to_vec()))
        }
        fn hmac_sha256(&self, _secret: &[u8], bytes: &[u8]) -> crate::Result<Tag> {
            Ok(Tag(bytes.to_vec()))
        }
    }

    impl Default for NoopCrypto {
        fn default() -> Self {
            NoopCrypto
        }
    }
}

// pub mod openssl_crypto {
//     use openssl::hash::MessageDigest;
//     use openssl::pkey::PKey;
//     use openssl::sign::Signer;
//
//     use super::{CryptoProvider, Digest, Tag};
//
//     #[derive(Debug, Clone, Copy)]
//     pub struct OpenSslCrypto;
//
//     impl CryptoProvider for OpenSslCrypto {
//         fn digest_sha256(&self, bytes: &[u8]) -> crate::Result<Digest> {
//             let digest = openssl::hash::hash(MessageDigest::sha256(), bytes)?;
//             Ok(digest.as_ref().into())
//         }
//
//         fn hmac_sha256(&self, secret: &[u8], bytes: &[u8]) -> crate::Result<Tag> {
//             let key = PKey::hmac(secret)?;
//             let mut signer = Signer::new(MessageDigest::sha256(), &key)?;
//             signer.update(bytes)?;
//             let hmac = signer.sign_to_vec()?;
//             Ok(hmac.into())
//         }
//     }
//
//     impl From<openssl::error::ErrorStack> for crate::Error {
//         fn from(value: openssl::error::ErrorStack) -> Self {
//             // TODO(jakedern)
//             todo!()
//         }
//     }
// }
