// Throwaway smoke test for the Hugging Face S3-compatible gateway (issue #706).
// NOT committed. Verifies that the *existing* object_store AmazonS3 config talks to
// https://s3.hf.co/<namespace> with no code changes (the plan's "Tier B").
//
// Run from this directory after exporting credentials (see README.md):
//   cargo run
//
// Env vars:
//   HF_NAMESPACE            HF username or org (used to build the endpoint)   [required]
//   HF_BUCKET               existing bucket name under that namespace         [required]
//   AWS_ACCESS_KEY_ID       HF-generated S3 key, "HFAK..."                     [required]
//   AWS_SECRET_ACCESS_KEY   HF-generated S3 secret                             [required]
//   HF_S3_ENDPOINT          override endpoint (default https://s3.hf.co/<HF_NAMESPACE>)
//   HF_REGION               default "us-east-1"
//   HF_SMOKE_RESULTS        results markdown path (default ./live-results.md)

use bytes::Bytes;
use futures::StreamExt;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path;
use object_store::signer::Signer;
use object_store::{
    ClientOptions, Error, GetOptions, GetRange, ObjectStore, ObjectStoreExt, PutMode, PutOptions,
    WriteMultipart,
};
use std::env;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type OsResult<T> = std::result::Result<T, Error>;

fn generic(msg: impl Into<String>) -> Error {
    Error::Generic {
        store: "hf-smoke",
        source: msg.into().into(),
    }
}

fn env_req(key: &str) -> String {
    env::var(key).unwrap_or_else(|_| {
        eprintln!("ERROR: required env var {key} is not set");
        std::process::exit(2);
    })
}

fn build_store(disable_bulk_delete: bool) -> OsResult<AmazonS3> {
    let namespace = env_req("HF_NAMESPACE");
    let bucket = env_req("HF_BUCKET");
    let endpoint =
        env::var("HF_S3_ENDPOINT").unwrap_or_else(|_| format!("https://s3.hf.co/{namespace}"));
    let region = env::var("HF_REGION").unwrap_or_else(|_| "us-east-1".to_string());

    // Only the standard config surface — no custom keys, no code changes.
    // Defaults left as-is: path-style, no checksum algorithm, signed payload,
    // conditional_put = ETagMatch.
    let mut builder = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_region(region)
        .with_bucket_name(bucket)
        .with_access_key_id(env_req("AWS_ACCESS_KEY_ID"))
        .with_secret_access_key(env_req("AWS_SECRET_ACCESS_KEY"))
        .with_disable_bulk_delete(disable_bulk_delete);

    // Optional User-Agent override. Set HF_USER_AGENT=aws-sdk-rust to make the HF gateway
    // proxy GETs (HTTP 200) instead of 302-redirecting to the CDN — a config-only workaround
    // for the cross-host-redirect download failure.
    if let Ok(ua) = env::var("HF_USER_AGENT") {
        println!("(using User-Agent override: {ua})");
        builder = builder.with_client_options(ClientOptions::default().with_user_agent(
            ua.parse().expect("invalid HF_USER_AGENT header value"),
        ));
    }

    builder.build()
}

#[tokio::main]
async fn main() {
    let store = match build_store(false) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("failed to build store: {e}");
            std::process::exit(1);
        }
    };

    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let prefix = format!("object-store-hf-smoke/{ts}");
    println!("== HF S3 gateway smoke test ==");
    println!("test prefix: {prefix}\n");

    let mut results: Vec<(String, Result<String, String>)> = Vec::new();

    macro_rules! step {
        ($name:expr, $body:block) => {{
            let name: &str = $name;
            // Plain `async` (not `async move`): capture `store` and locals by reference;
            // the future is awaited immediately on the next line.
            let r: OsResult<String> = async $body.await;
            let r = r.map_err(|e| e.to_string());
            match &r {
                Ok(detail) => println!("[PASS] {name}\n       {detail}"),
                Err(e) => println!("[FAIL] {name}\n       {e}"),
            }
            results.push((name.to_string(), r));
        }};
    }

    // Read-only mode: exercise only GET/HEAD/LIST against an existing object. This still proves the
    // one behavior unprovable from source and from boto3 — that object_store's own HTTP client
    // follows the HF 302 -> CDN redirect (and preserves Range) — without needing write access.
    let readonly = env::var("HF_SMOKE_READONLY")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if readonly {
        println!("read-only mode (HF_SMOKE_READONLY set): no writes will be attempted.\n");
        // Discover an existing object to read.
        let mut listing = store.list(None);
        let mut found: Option<(Path, u64)> = None;
        let mut list_err: Option<String> = None;
        while let Some(item) = listing.next().await {
            match item {
                Ok(m) => {
                    found = Some((m.location.clone(), m.size));
                    break;
                }
                Err(e) => {
                    list_err = Some(e.to_string());
                    break;
                }
            }
        }
        drop(listing);
        match (&found, &list_err) {
            (Some((p, sz)), _) => {
                println!("[PASS] list (found readable object)\n       {p} ({sz} bytes)");
                results.push(("list (find object)".into(), Ok(format!("{p} ({sz} bytes)"))));
            }
            (None, Some(e)) => {
                println!("[FAIL] list\n       {e}");
                results.push(("list (find object)".into(), Err(e.clone())));
            }
            (None, None) => {
                let m = "bucket has no objects; add one (HF UI or `hf buckets cp`) \
                         or use a Write token for the full suite"
                    .to_string();
                println!("[FAIL] list — {m}");
                results.push(("list (find object)".into(), Err(m)));
            }
        }

        if let Some((path, size)) = found {
            step!("head (existing object)", {
                let m = store.head(&path).await?;
                Ok(format!("size={} e_tag={:?}", m.size, m.e_tag))
            });
            step!("get (full object) [cross-host 302 -> CDN]", {
                let b = store.get(&path).await?.bytes().await?;
                if b.len() as u64 != size {
                    return Err(generic(format!("got {} bytes, expected {size}", b.len())));
                }
                Ok(format!("downloaded {} bytes", b.len()))
            });
            if size > 0 {
                step!("get_opts (ranged) [cross-host 302 + Range]", {
                    let n = size.min(5);
                    let b = store
                        .get_opts(
                            &path,
                            GetOptions {
                                range: Some(GetRange::Bounded(0..n)),
                                ..Default::default()
                            },
                        )
                        .await?
                        .bytes()
                        .await?;
                    if b.len() as u64 != n {
                        return Err(generic(format!("range 0..{n} returned {} bytes", b.len())));
                    }
                    Ok(format!("range 0..{n} = {} bytes", b.len()))
                });
            }

            // Goal #2: object_store generates a presigned URL (query-string SigV4 via the Signer
            // trait — a separate code path from the header-signing fix). A plain HTTP client
            // downloads it, following the gateway's 302 -> CDN. Verify CONTENT correctness by
            // comparing the presigned download byte-for-byte against object_store's own get().
            step!("signed_url download == get() [presigned URL, content verified]", {
                let url = store
                    .signed_url(reqwest::Method::GET, &path, Duration::from_secs(600))
                    .await?;
                // Sanity: the presigned URL is against the gateway host, not the CDN.
                let signed_host = url.host_str().unwrap_or("").to_string();

                let client = reqwest::Client::builder()
                    .build()
                    .map_err(|e| generic(e.to_string()))?;
                let resp = client
                    .get(url)
                    .send()
                    .await
                    .map_err(|e| generic(format!("send: {e}")))?;
                let st = resp.status();
                let presigned = resp
                    .bytes()
                    .await
                    .map_err(|e| generic(format!("body: {e}")))?;
                if !st.is_success() {
                    return Err(generic(format!("status {st}")));
                }
                if presigned.len() as u64 != size {
                    return Err(generic(format!(
                        "presigned got {} bytes, expected {size}",
                        presigned.len()
                    )));
                }
                // Byte-for-byte against the direct API download.
                let direct = store.get(&path).await?.bytes().await?;
                if presigned != direct {
                    return Err(generic(format!(
                        "presigned bytes ({}) differ from get() bytes ({})",
                        presigned.len(),
                        direct.len()
                    )));
                }
                Ok(format!(
                    "presigned host={signed_host}, {} bytes, identical to get()",
                    presigned.len()
                ))
            });
        }

        step!("list_with_delimiter", {
            let r = store.list_with_delimiter(None).await?;
            Ok(format!(
                "{} objects, {} common_prefixes",
                r.objects.len(),
                r.common_prefixes.len()
            ))
        });
    } else {
    let small = Path::from(format!("{prefix}/small.txt"));
    let large = Path::from(format!("{prefix}/large.bin"));
    let copy = Path::from(format!("{prefix}/small-copy.txt"));
    let create = Path::from(format!("{prefix}/create.txt"));
    let data = Bytes::from_static(b"hello huggingface s3 gateway");

    // 1. put (small, single PutObject)
    step!("put (small object)", {
        store.put(&small, data.clone().into()).await?;
        Ok(format!("put {} bytes", data.len()))
    });

    // 2. get + verify
    step!("get (full object, verify bytes)", {
        let got = store.get(&small).await?.bytes().await?;
        if got != data {
            return Err(generic("content mismatch"));
        }
        Ok(format!("got {} bytes, matches", got.len()))
    });

    // 3. get_opts with Range — exercises the cross-host 302 -> CDN + Range preservation
    step!("get_opts (ranged, 0..5) [cross-host 302 path]", {
        let opts = GetOptions {
            range: Some(GetRange::Bounded(0..5)),
            ..Default::default()
        };
        let got = store.get_opts(&small, opts).await?.bytes().await?;
        if got.as_ref() != &data[0..5] {
            return Err(generic(format!("range mismatch: got {got:?}")));
        }
        Ok(format!("range 0..5 = {got:?}"))
    });

    // 4. head
    step!("head (metadata + size)", {
        let meta = store.head(&small).await?;
        if meta.size != data.len() as u64 {
            return Err(generic(format!(
                "size {} != expected {}",
                meta.size,
                data.len()
            )));
        }
        Ok(format!("size={} e_tag={:?}", meta.size, meta.e_tag))
    });

    // 5. put_multipart (>1 part: 11 MiB at 5 MiB default chunk -> 3 parts)
    let mp_total = 11 * 1024 * 1024usize;
    step!("put_multipart (11 MiB -> 3 parts)", {
        let upload = store.put_multipart(&large).await?;
        let mut w = WriteMultipart::new(upload); // 5 MiB default chunk
        w.write(&vec![7u8; mp_total]);
        w.finish().await?;
        Ok(format!("uploaded {mp_total} bytes via multipart"))
    });

    // 6. head the multipart object
    step!("head (multipart object size)", {
        let meta = store.head(&large).await?;
        if meta.size != mp_total as u64 {
            return Err(generic(format!(
                "multipart size {} != expected {}",
                meta.size, mp_total
            )));
        }
        Ok(format!("size={}", meta.size))
    });

    // 7. list() — recursive, no delimiter (ListObjectsV2)
    let prefix_path = Path::from(prefix.clone());
    step!("list (recursive, no delimiter)", {
        let mut s = store.list(Some(&prefix_path));
        let mut names = Vec::new();
        while let Some(m) = s.next().await {
            names.push(m?.location.to_string());
        }
        if names.is_empty() {
            return Err(generic("list returned no objects"));
        }
        Ok(format!("{} objects: {names:?}", names.len()))
    });

    // 8. list_with_delimiter("/")
    step!("list_with_delimiter", {
        let res = store.list_with_delimiter(Some(&prefix_path)).await?;
        Ok(format!(
            "{} objects, {} common_prefixes",
            res.objects.len(),
            res.common_prefixes.len()
        ))
    });

    // 9. copy (same namespace) + verify
    step!("copy (same namespace) + verify", {
        store.copy(&small, &copy).await?;
        let got = store.get(&copy).await?.bytes().await?;
        if got != data {
            return Err(generic("copied content mismatch"));
        }
        Ok("copied and verified".into())
    });

    // 10. put_opts PutMode::Create on a fresh key (If-None-Match) — should succeed
    step!("put_opts PutMode::Create (fresh key)", {
        store
            .put_opts(
                &create,
                Bytes::from_static(b"v1").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await?;
        Ok("create succeeded on fresh key".into())
    });

    // 11. put_opts PutMode::Create again — must fail AlreadyExists
    step!("put_opts PutMode::Create (duplicate -> AlreadyExists)", {
        match store
            .put_opts(
                &create,
                Bytes::from_static(b"v2").into(),
                PutOptions {
                    mode: PutMode::Create,
                    ..Default::default()
                },
            )
            .await
        {
            Err(Error::AlreadyExists { .. }) => {
                Ok("correctly rejected duplicate (AlreadyExists)".into())
            }
            Err(e) => Err(generic(format!("expected AlreadyExists, got: {e}"))),
            Ok(_) => Err(generic("expected AlreadyExists, but create succeeded")),
        }
    });

    // 12. delete (single object)
    step!("delete (single object)", {
        store.delete(&copy).await?;
        // Confirm it's gone.
        match store.head(&copy).await {
            Err(Error::NotFound { .. }) => Ok("deleted single object".into()),
            Ok(_) => Err(generic("object still present after delete")),
            Err(e) => Err(generic(format!("unexpected error after delete: {e}"))),
        }
    });

    // 13. bulk delete via delete_stream (POST ?delete, with Content-MD5)
    let bulk_keys = vec![small.clone(), large.clone(), create.clone()];
    let mut bulk_failed = false;
    step!("bulk delete (delete_stream / POST ?delete)", {
        let stream = futures::stream::iter(bulk_keys.clone().into_iter().map(Ok)).boxed();
        let mut out = store.delete_stream(stream);
        let mut deleted = 0usize;
        let mut first_err: Option<Error> = None;
        while let Some(r) = out.next().await {
            match r {
                Ok(_) => deleted += 1,
                Err(e) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
            }
        }
        match first_err {
            Some(e) => Err(generic(format!(
                "bulk delete errored after {deleted} ok: {e}"
            ))),
            None => Ok(format!("bulk-deleted {deleted} objects")),
        }
    });
    // Detect whether the bulk step failed so we can exercise the escape hatch.
    if let Some((_, Err(_))) = results.last() {
        bulk_failed = true;
    }

    // 14. Escape-hatch: if bulk delete failed, retry per-object with disable_bulk_delete=true
    if bulk_failed {
        println!("\n-- bulk delete failed; testing AWS_DISABLE_BULK_DELETE escape hatch --");
        match build_store(true) {
            Ok(store_nobulk) => {
                step!("delete (per-object, disable_bulk_delete=true)", {
                    let stream =
                        futures::stream::iter(bulk_keys.clone().into_iter().map(Ok)).boxed();
                    let mut out = store_nobulk.delete_stream(stream);
                    let mut deleted = 0usize;
                    let mut first_err: Option<Error> = None;
                    while let Some(r) = out.next().await {
                        match r {
                            Ok(_) => deleted += 1,
                            // A key already removed by the partial bulk attempt is fine.
                            Err(Error::NotFound { .. }) => deleted += 1,
                            Err(e) => {
                                if first_err.is_none() {
                                    first_err = Some(e);
                                }
                            }
                        }
                    }
                    match first_err {
                        Some(e) => Err(generic(format!("per-object delete errored: {e}"))),
                        None => Ok(format!(
                            "per-object deleted {deleted} objects (escape hatch works)"
                        )),
                    }
                });
            }
            Err(e) => println!("[FAIL] build disable_bulk_delete store\n       {e}"),
        }
    }

        // Best-effort cleanup of anything left behind (ignore NotFound).
        for k in [&small, &large, &copy, &create] {
            let _ = store.delete(k).await;
        }
    }

    // Summary + results file.
    let passed = results.iter().filter(|(_, r)| r.is_ok()).count();
    let total = results.len();
    println!("\n== {passed}/{total} steps passed ==");

    let results_path =
        env::var("HF_SMOKE_RESULTS").unwrap_or_else(|_| "live-results.md".to_string());
    let mut md = String::new();
    md.push_str("# HF S3 gateway — live smoke test results\n\n");
    md.push_str(&format!("Test prefix: `{prefix}`\n\n"));
    md.push_str(&format!("**{passed}/{total} steps passed.**\n\n"));
    md.push_str("| Step | Result | Detail |\n|---|---|---|\n");
    for (name, r) in &results {
        let (res, detail) = match r {
            Ok(d) => ("PASS", d.replace('|', "\\|")),
            Err(e) => ("FAIL", e.replace('|', "\\|").replace('\n', " ")),
        };
        md.push_str(&format!("| {name} | {res} | {detail} |\n"));
    }
    match std::fs::write(&results_path, md) {
        Ok(_) => println!("results written to {results_path}"),
        Err(e) => eprintln!("could not write {results_path}: {e}"),
    }

    if passed != total {
        std::process::exit(1);
    }
}
