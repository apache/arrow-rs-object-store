# HF S3 gateway smoke test (throwaway — issue #706)

> **Why this is in the repo:** this directory is committed only so reviewers can see and reproduce how
> the "don't pin the Host header when signing" fix was verified end-to-end against a real Hugging Face
> bucket. It is removed again by the immediately-following revert commit and is not part of the
> `object_store` crate. `live-results.sample.md` shows an actual passing run.

Exercises `object_store`'s `AmazonS3` against the Hugging Face S3-compatible gateway. Path-depends on
the crate in this repo (`../..`) with the `aws` feature. With the fix, downloads (`get`, ranged `get`)
succeed against the gateway's cross-host 302 → CDN redirect; without it they fail.

## Prereqs — generate the token, S3 credentials, and a bucket

1. **HF access token (Write scope).** Go to <https://huggingface.co/settings/tokens> → *Create new
   token* → set type/permission to **Write** (Read gives read-only S3 access) → *Create* → copy it.
2. **S3 credentials from that token.** Back on the tokens page, find the token, open its *⋮* dropdown
   → **Generate S3 credentials**. Copy the **access key ID** (starts `HFAK…`) and the **secret**
   (shown only once).
3. **A bucket.** Buckets live at `https://huggingface.co/buckets/<owner>/<bucket>`. Create one at
   <https://huggingface.co/new-bucket> or with `hf buckets create <bucket>` — the gateway won't
   create it for you.

## Easiest path: `gather.py` (single `uv` command)

Parses your bucket URL, validates the credentials against the gateway with boto3 (an independent
cross-check), does a tiny write round-trip, and writes a ready-to-source `.env`:

```bash
cd verification/hf-smoke
uv run gather.py "https://huggingface.co/buckets/<owner>/<bucket>"   # paste your bucket URL
# it prompts for the HFAK access key + secret, validates, and writes .env

set -a && . ./.env && set +a && cargo run                            # run the Rust smoke test
```

## Or set the env vars yourself
```bash
cd verification/hf-smoke
export HF_NAMESPACE=<your-username-or-org>
export HF_BUCKET=<existing-bucket-name>
export AWS_ACCESS_KEY_ID=HFAK...
export AWS_SECRET_ACCESS_KEY=...
# optional: export HF_S3_ENDPOINT=https://s3.hf.co/$HF_NAMESPACE   (default)
# optional: export HF_REGION=us-east-1                             (default)
cargo run
```

Objects are written under the prefix `object-store-hf-smoke/<timestamp>/` and cleaned up at the end.
Results print to the terminal and are written to `live-results.md` in the current directory.

## What it exercises
put (single) · get + verify · **ranged get (cross-host 302 → CDN path)** · head · put_multipart
(11 MiB → 3 parts) · list (recursive) · list_with_delimiter · copy (same namespace) ·
PutMode::Create success + duplicate→AlreadyExists · single delete · **bulk delete (POST ?delete,
Content-MD5)** and, if that fails, the `disable_bulk_delete` per-object escape hatch.

Exit code 0 = all steps passed; 1 = at least one failed (see the table in `live-results.md`).

## Read-only mode

If you only have a **Read** token, set `HF_SMOKE_READONLY=1` (gather.py adds this to `.env`
automatically when it detects read-only creds). The test then skips all writes and instead discovers
an existing object and runs head / full get / ranged get / list — which still proves the one thing
that can't be verified from source or via boto3: that **object_store's own HTTP client follows the HF
302 → CDN redirect** (and preserves `Range`). The read-only path needs at least one object already in
the bucket; if it's empty, upload one via the HF UI or `hf buckets cp`.
