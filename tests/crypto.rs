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

//! Tests combinations of crypto-related features. This module is meant to be
//! run both with and without the `ring` feature enabled to make sure that both
//! scenarios are covered.

#[test]
#[cfg(feature = "azure")]
fn test_azure_default_crypto() {
    let builder = object_store::azure::MicrosoftAzureBuilder::default()
        .with_container_name("testcontainer")
        .with_account("testaccount");

    #[cfg(feature = "ring")]
    {
        builder
            .build()
            .expect("default crypto should be configured");
    }

    #[cfg(not(feature = "ring"))]
    {
        let res = builder.build();
        assert!(
            res.is_err(),
            "Builder should fail without crypto configured"
        );
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Missing crypto provider."));
    }
}

#[test]
#[cfg(feature = "aws")]
fn test_aws_default_crypto() {
    let builder = object_store::aws::AmazonS3Builder::default().with_bucket_name("testbucket");

    #[cfg(feature = "ring")]
    {
        builder
            .build()
            .expect("default crypto should be configured");
    }

    #[cfg(not(feature = "ring"))]
    {
        let res = builder.build();
        assert!(
            res.is_err(),
            "Builder should fail without crypto configured"
        );
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Missing crypto provider."));
    }
}

#[test]
#[cfg(feature = "gcp")]
fn test_gcp_default_crypto() {
    let builder =
        object_store::gcp::GoogleCloudStorageBuilder::default().with_bucket_name("testbucket");

    #[cfg(feature = "ring")]
    {
        builder
            .build()
            .expect("default crypto should be configured");
    }

    #[cfg(not(feature = "ring"))]
    {
        let res = builder.build();
        assert!(
            res.is_err(),
            "Builder should fail without crypto configured"
        );
        assert!(res
            .unwrap_err()
            .to_string()
            .contains("Missing crypto provider."));
    }
}
