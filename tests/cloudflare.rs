//! Integration tests for Cloudflare R2 REST API
//!
//! Run with:
//! ```
//! CLOUDFLARE_ACCOUNT_ID=xxx CLOUDFLARE_R2_BUCKET=xxx CLOUDFLARE_API_TOKEN=xxx \
//!   cargo test --features cloudflare test_cloudflare_r2 -- --nocapture
//! ```

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
