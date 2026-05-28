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

//! Cloudflare R2 credential handling

use crate::client::CredentialProvider;
use async_trait::async_trait;
use std::fmt::Debug;

/// Credential for authenticating with the Cloudflare R2 REST API.
///
/// Uses a Cloudflare API token (Bearer token) for authentication.
#[derive(Debug, Clone)]
pub struct CloudflareCredential {
    /// The Cloudflare API token used for Bearer authentication
    pub api_token: String,
}

/// A [`CredentialProvider`] that provides a static [`CloudflareCredential`]
#[derive(Debug)]
pub(crate) struct StaticCloudflareCredentialProvider {
    credential: CloudflareCredential,
}

impl StaticCloudflareCredentialProvider {
    /// Create a new [`StaticCloudflareCredentialProvider`]
    pub(crate) fn new(api_token: String) -> Self {
        Self {
            credential: CloudflareCredential { api_token },
        }
    }
}

#[async_trait]
impl CredentialProvider for StaticCloudflareCredentialProvider {
    type Credential = CloudflareCredential;

    async fn get_credential(&self) -> crate::Result<std::sync::Arc<Self::Credential>> {
        Ok(std::sync::Arc::new(self.credential.clone()))
    }
}
