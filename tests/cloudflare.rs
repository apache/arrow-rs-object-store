//! Integration tests for Cloudflare R2 REST API
//!
//! Run with:
//! ```
//! CLOUDFLARE_ACCOUNT_ID=xxx CLOUDFLARE_R2_BUCKET=xxx CLOUDFLARE_API_TOKEN=xxx \
//!   cargo test --features cloudflare test_cloudflare_r2 -- --nocapture
//! ```

#[cfg(feature = "cloudflare")]
mod cloudflare_signed_url_tests {
    use object_store::cloudflare::CloudflareR2Builder;
    use object_store::path::Path;
    use object_store::signer::Signer;
    use object_store::{ObjectStore, ObjectStoreExt};
    use reqwest::Method;
    use std::time::Duration;

    #[tokio::test]
    async fn test_signed_url_generation() {
        let r2 = CloudflareR2Builder::new()
            .with_account_id("a01e84070e939a1184a89996889802fb")
            .with_bucket_name("test-bucket")
            .with_api_token("dummy-token")
            .with_access_key_id("88bc40f98a5a693902e77b815b57ec4f")
            .with_secret_access_key("aac1d4bbcc3a766e926d1ef8794ef8788347462368eb035162f11277d9b8c74f")
            .build()
            .expect("Failed to build CloudflareR2");

        let path = Path::from("test-file.txt");
        let url = r2.signed_url(Method::GET, &path, Duration::from_secs(3600)).await.unwrap();

        println!("Generated presigned URL: {}", url);

        // Verify URL structure
        assert!(url.as_str().contains("a01e84070e939a1184a89996889802fb.r2.cloudflarestorage.com"));
        assert!(url.as_str().contains("/test-bucket/test-file.txt"));
        assert!(url.as_str().contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
        assert!(url.as_str().contains("X-Amz-Credential=88bc40f98a5a693902e77b815b57ec4f"));
        assert!(url.as_str().contains("X-Amz-Expires=3600"));
        assert!(url.as_str().contains("X-Amz-SignedHeaders=host"));
        assert!(url.as_str().contains("X-Amz-Signature="));

        println!("All signed URL assertions passed!");
    }

    /// Integration test: PUT an object via REST API, then GET it via presigned URL (no auth headers).
    ///
    /// Requires env vars:
    /// ```
    /// CLOUDFLARE_ACCOUNT_ID=xxx
    /// CLOUDFLARE_R2_BUCKET=xxx
    /// CLOUDFLARE_API_TOKEN=xxx
    /// CLOUDFLARE_R2_ACCESS_KEY_ID=xxx
    /// CF_R2_SECRET_ACCESS_KEY=xxx
    /// cargo test --features cloudflare test_signed_url_with_real_data -- --nocapture
    /// ```
    #[tokio::test]
    async fn test_signed_url_with_real_data() {
        let account_id = match std::env::var("CLOUDFLARE_ACCOUNT_ID") {
            Ok(v) => v,
            Err(_) => {
                eprintln!("Skipping: CLOUDFLARE env vars not set");
                return;
            }
        };
        let bucket = std::env::var("CLOUDFLARE_R2_BUCKET").unwrap();
        let access_key_id = std::env::var("CLOUDFLARE_R2_ACCESS_KEY_ID").unwrap();
        let secret_access_key = std::env::var("CF_R2_SECRET_ACCESS_KEY").unwrap();

        let r2 = CloudflareR2Builder::new()
            .with_account_id(&account_id)
            .with_bucket_name(&bucket)
            .with_api_token("unused-for-this-test")
            .with_access_key_id(&access_key_id)
            .with_secret_access_key(&secret_access_key)
            .build()
            .expect("Failed to build CloudflareR2");

        let client = reqwest::Client::new();

        // 1. PUT test data via presigned PUT URL (S3-compatible endpoint)
        let path = Path::from("signed-url-test/hello.txt");
        let payload = bytes::Bytes::from("Hello from presigned URL test!");
        println!("Generating presigned PUT URL...");
        let put_url = r2
            .signed_url(Method::PUT, &path, Duration::from_secs(3600))
            .await
            .expect("Failed to generate signed PUT URL");
        println!("  PUT URL: {}", put_url);

        let put_response = client
            .put(put_url.as_str())
            .body(payload.clone())
            .send()
            .await
            .expect("PUT via presigned URL failed");
        println!("  PUT response status: {}", put_response.status());
        assert!(
            put_response.status().is_success(),
            "PUT failed with status: {} body: {:?}",
            put_response.status(),
            put_response.text().await.unwrap_or_default()
        );
        println!("  PUT success via presigned URL");

        // 2. Generate a presigned GET URL and fetch
        let get_url = r2
            .signed_url(Method::GET, &path, Duration::from_secs(3600))
            .await
            .expect("Failed to generate signed GET URL");
        println!("Presigned GET URL: {}", get_url);

        let response = client.get(get_url.as_str()).send().await.expect("GET via presigned URL failed");
        println!("  GET response status: {}", response.status());
        assert!(
            response.status().is_success(),
            "Expected 200 OK, got: {}",
            response.status()
        );

        let body = response.bytes().await.expect("Failed to read response body");
        assert_eq!(body, payload, "Body content mismatch!");
        println!("  Body matches: {:?}", std::str::from_utf8(&body).unwrap());

        // 3. Cleanup via presigned DELETE URL
        let delete_url = r2
            .signed_url(Method::DELETE, &path, Duration::from_secs(3600))
            .await
            .expect("Failed to generate signed DELETE URL");
        let del_response = client.delete(delete_url.as_str()).send().await.expect("DELETE failed");
        assert!(
            del_response.status().is_success() || del_response.status().as_u16() == 204,
            "DELETE failed with status: {}",
            del_response.status()
        );
        println!("  Cleanup done via presigned DELETE");

        println!("\nPresigned URL integration test passed!");
    }
}

#[cfg(feature = "cloudflare")]
mod cloudflare_tests {
    use bytes::Bytes;
    use futures_util::TryStreamExt;
    use object_store::cloudflare::CloudflareR2Builder;
    use object_store::path::Path;
    use object_store::{ObjectStore, ObjectStoreExt};

    fn get_store() -> Option<Box<dyn ObjectStore>> {
        let account_id = std::env::var("CLOUDFLARE_ACCOUNT_ID").ok()?;
        let bucket = std::env::var("CLOUDFLARE_R2_BUCKET").ok()?;
        let token = std::env::var("CLOUDFLARE_API_TOKEN").ok()?;

        let store = CloudflareR2Builder::new()
            .with_account_id(account_id)
            .with_bucket_name(bucket)
            .with_api_token(token)
            .build()
            .expect("Failed to build CloudflareR2");

        Some(Box::new(store))
    }

    #[tokio::test]
    async fn test_cloudflare_r2_put_get_delete() {
        let Some(store) = get_store() else {
            eprintln!("Skipping: CLOUDFLARE env vars not set");
            return;
        };

        let path = Path::from("integration-test/hello.txt");
        let payload = Bytes::from("hello cloudflare r2!");

        // PUT
        println!("Testing PUT...");
        let put_result = store
            .put_opts(&path, payload.clone().into(), Default::default())
            .await;
        match &put_result {
            Ok(r) => println!("  PUT success: etag={:?}", r.e_tag),
            Err(e) => panic!("  PUT failed: {}", e),
        }

        // GET
        println!("Testing GET...");
        let get_result = store.get_opts(&path, Default::default()).await;
        match get_result {
            Ok(r) => {
                let bytes = r.bytes().await.unwrap();
                println!("  GET success: {} bytes", bytes.len());
                assert_eq!(bytes, payload);
            }
            Err(e) => panic!("  GET failed: {}", e),
        }

        // LIST
        println!("Testing LIST...");
        let prefix = Path::from("integration-test");
        let list_result: Vec<_> = store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        println!("  LIST found {} objects", list_result.len());
        assert!(list_result.iter().any(|m| m.location == path));

        // DELETE
        println!("Testing DELETE...");
        store.delete(&path).await.unwrap();
        println!("  DELETE success");

        // Verify deleted
        let get_after_delete = store.get_opts(&path, Default::default()).await;
        assert!(get_after_delete.is_err(), "Object should be deleted");
        println!("  Verified: object no longer exists");

        println!("\nAll tests passed!");
    }

    #[tokio::test]
    async fn test_cloudflare_r2_list_with_delimiter() {
        let Some(store) = get_store() else {
            return;
        };

        let files = vec![
            Path::from("delim-test/a/1.txt"),
            Path::from("delim-test/a/2.txt"),
            Path::from("delim-test/b/3.txt"),
            Path::from("delim-test/root.txt"),
        ];

        // Put test files
        for f in &files {
            store
                .put_opts(f, Bytes::from("x").into(), Default::default())
                .await
                .unwrap();
        }

        // List with delimiter
        let prefix = Path::from("delim-test");
        let result = store.list_with_delimiter(Some(&prefix)).await.unwrap();
        println!("Objects: {:?}", result.objects.iter().map(|o| &o.location).collect::<Vec<_>>());
        println!("Common prefixes: {:?}", result.common_prefixes);

        // Should have root.txt as object and a/, b/ as common prefixes
        assert!(result.objects.iter().any(|o| o.location == Path::from("delim-test/root.txt")));
        assert!(result.common_prefixes.len() >= 2);

        // Cleanup
        for f in &files {
            let _ = store.delete(f).await;
        }

        println!("list_with_delimiter test passed!");
    }

    #[tokio::test]
    async fn test_cloudflare_r2_multipart_not_supported() {
        // NOTE: The Cloudflare R2 REST API (v4) does NOT support multipart uploads.
        // Multipart is only available via the S3-compatible API or Workers bindings.
        // This test verifies the expected behavior (graceful error).
        let Some(store) = get_store() else {
            return;
        };

        let path = Path::from("integration-test/multipart.bin");

        println!("Testing multipart upload (expected to fail on REST API)...");
        let result = store.put_multipart_opts(&path, Default::default()).await;

        assert!(result.is_err(), "Multipart should not be supported on R2 REST API");
        println!("  Confirmed: multipart not supported on REST API (as expected)");
    }
}
