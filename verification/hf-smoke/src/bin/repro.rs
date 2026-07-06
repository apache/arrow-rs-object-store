// Reproduce object_store's GET-through-302 with a reqwest client configured like
// object_store's (src/client/mod.rs:835-939), to surface the real error chain.
use std::env;

#[tokio::main]
async fn main() {
    let url = env::args().nth(1).expect("usage: repro <url>");

    // Mirror object_store's client() builder.
    let client = reqwest::Client::builder()
        .user_agent("object_store/0.14.0")
        .no_gzip()
        .no_brotli()
        .no_zstd()
        .no_deflate()
        .https_only(true) // object_store default (allow_http = false)
        .build()
        .unwrap();

    // Optionally add the headers object_store attaches to its signed request, to see which
    // one (if any) breaks following the cross-host 302. Toggle via env.
    let mut req = client.get(&url);
    if env::var("REPRO_HOST").is_ok() {
        println!("[+] adding manual `host: s3.hf.co` header (mimics credential.rs:232)");
        req = req.header("host", "s3.hf.co");
    }
    if env::var("REPRO_AMZ").is_ok() {
        println!("[+] adding x-amz-content-sha256 / x-amz-date / authorization headers");
        req = req
            .header(
                "x-amz-content-sha256",
                "UNSIGNED-PAYLOAD",
            )
            .header("x-amz-date", "20260706T000000Z")
            .header(
                "authorization",
                "AWS4-HMAC-SHA256 Credential=x/20260706/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=deadbeef",
            );
    }

    println!("GET {url}\n(default redirect policy: follow up to 10)\n");
    match req.send().await {
        Ok(resp) => {
            println!("status: {}", resp.status());
            println!("final url: {}", resp.url());
            match resp.bytes().await {
                Ok(b) => println!("downloaded {} bytes", b.len()),
                Err(e) => print_chain("body error", &e),
            }
        }
        Err(e) => {
            println!(
                "reqwest classification: is_connect={} is_timeout={} is_redirect={} is_request={} is_body={}",
                e.is_connect(), e.is_timeout(), e.is_redirect(), e.is_request(), e.is_body()
            );
            print_chain("send error", &e);
        }
    }
}

fn print_chain(label: &str, e: &(dyn std::error::Error)) {
    println!("{label}: {e}");
    let mut src = e.source();
    let mut i = 0;
    while let Some(s) = src {
        println!("  caused by [{i}]: {s}");
        src = s.source();
        i += 1;
    }
}
