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

//! Cryptographic backend abstraction
//!
//! This module provides a unified interface over different cryptographic backends
//! (ring and aws-lc-rs) to allow compile-time selection without code duplication.

#[cfg(all(feature = "ring", feature = "aws-lc"))]
compile_error!("Features 'ring' and 'aws-lc' are mutually exclusive");

/// Re-export the chosen cryptographic backend as `crypto`.
///
/// Since aws-lc-rs is API-compatible with ring for most operations, we can use
/// a simple alias pattern to switch between them at compile time.
#[cfg(feature = "ring")]
#[allow(unused_imports)]
pub(crate) use ring as crypto;

#[cfg(feature = "aws-lc")]
#[allow(unused_imports)]
pub(crate) use aws_lc_rs as crypto;

/// Get the modulus length for an RSA key pair.
///
/// This abstracts the API difference between ring and aws-lc-rs:
/// - ring: `key.public().modulus_len()`
/// - aws-lc-rs: `key.public_key().modulus_len()` (requires KeyPair trait)
///
/// Both libraries require a pre-allocated buffer for signing, but use different
/// methods to determine the required buffer size. This helper encapsulates that
/// difference so the main code doesn't need feature conditionals.
#[cfg(any(feature = "gcp", feature = "gcp-aws-lc"))]
pub(crate) fn rsa_key_modulus_len(key: &crypto::signature::RsaKeyPair) -> usize {
    #[cfg(feature = "ring")]
    {
        key.public().modulus_len()
    }
    #[cfg(feature = "aws-lc")]
    {
        use crypto::signature::KeyPair;
        key.public_key().modulus_len()
    }
}
