# /// script
# requires-python = ">=3.9"
# dependencies = ["boto3>=1.36"]
# ///
"""
Gather + validate the details needed to run the Rust HF S3 smoke test (issue #706).

Single command:
    uv run gather.py "https://huggingface.co/buckets/<owner>/<bucket>"

What it does:
  1. Parses your bucket URL into namespace + bucket.
  2. Prompts for the S3 credentials you generated from an HF token (access key "HFAK...",
     secret). See the header of README.md for how to create these.
  3. Validates them against the HF S3 gateway with boto3, using the exact settings from
     https://huggingface.co/docs/hub/storage-buckets-s3 (endpoint https://s3.hf.co/<namespace>,
     region us-east-1, path-style, checksums "when_required"). This is an independent Python
     cross-check that the config works before you run the Rust test.
  4. Does a tiny put/get/delete round-trip to confirm write access too.
  5. Writes a `.env` (chmod 600) in this directory that the Rust smoke test can source.

Nothing is generated on the HF side here (S3 credentials are created once via the web UI); this
script only collects, validates, and stashes the values.
"""

import getpass
import os
import stat
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, EndpointConnectionError


def parse_bucket(raw: str):
    """Accept any of:
    https://huggingface.co/buckets/<ns>/<bucket>[/...]
    hf://buckets/<ns>/<bucket>[/...]
    <ns>/<bucket>[/...]
    Returns (namespace, bucket) or None.
    """
    s = raw.strip()
    for token in ("huggingface.co/buckets/", "hf://buckets/"):
        if token in s:
            s = s.split(token, 1)[1]
            break
    else:
        # Strip a leading scheme if it's a plain URL without /buckets/
        if "://" in s:
            s = s.split("://", 1)[1]
            if "/" in s:
                s = s.split("/", 1)[1]
    parts = [p for p in s.split("/") if p]
    if len(parts) < 2:
        return None
    return parts[0], parts[1]


def prompt(msg: str, default: str = "") -> str:
    suffix = f" [{default}]" if default else ""
    val = input(f"{msg}{suffix}: ").strip()
    return val or default


def main() -> int:
    print("== HF S3 gateway — detail gatherer (issue #706) ==\n")

    # 1. Bucket URL -> namespace / bucket
    raw = sys.argv[1] if len(sys.argv) > 1 else prompt(
        "Paste your bucket URL (e.g. https://huggingface.co/buckets/<owner>/<bucket>)"
    )
    parsed = parse_bucket(raw)
    if not parsed:
        print(f"\nERROR: could not parse a '<namespace>/<bucket>' out of: {raw!r}")
        print("Expected something like https://huggingface.co/buckets/my-org/my-bucket")
        return 2
    namespace, bucket = parsed
    namespace = prompt("Namespace (owner: your username or org)", namespace)
    bucket = prompt("Bucket name", bucket)
    endpoint = f"https://s3.hf.co/{namespace}"
    region = os.environ.get("HF_REGION", "us-east-1")

    print(f"\n  namespace : {namespace}")
    print(f"  bucket    : {bucket}")
    print(f"  endpoint  : {endpoint}")
    print(f"  region    : {region}\n")

    # 2. Credentials (generated once via the HF web UI; see README.md).
    # NOTE: we deliberately do NOT silently read AWS_SECRET_ACCESS_KEY from the environment —
    # stale/real AWS creds in your shell are the classic cause of SignatureDoesNotMatch here.
    print("Enter the S3 credentials generated from your HF token")
    print("(HF settings -> Access Tokens -> token menu -> 'Generate S3 credentials'):")
    if os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("AWS_SESSION_TOKEN"):
        print("  (heads up: AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN are set in your shell;")
        print("   this script ignores them and uses only what you type below.)")
    env_ak = (os.environ.get("AWS_ACCESS_KEY_ID") or "").strip()
    access_key = prompt("  Access key ID (HFAK...)", env_ak).strip()
    if not access_key:
        print("ERROR: access key ID is required.")
        return 2
    if not access_key.startswith("HFAK"):
        print("  note: HF S3 access keys normally start with 'HFAK' — double-check if this fails.")
    secret_key = getpass.getpass("  Secret access key (hidden): ").strip()
    if not secret_key:
        print("ERROR: secret access key is required.")
        return 2
    print(f"  using access key {access_key[:8]}…{access_key[-2:]} "
          f"(len {len(access_key)}), secret len {len(secret_key)}")

    # 3. Build a boto3 client with the HF-documented settings and validate.
    cfg = Config(
        region_name=region,
        signature_version="s3v4",
        s3={"addressing_style": "path"},
        request_checksum_calculation="when_required",
        response_checksum_validation="when_required",
    )
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=cfg,
    )

    print("\nValidating against the gateway ...")
    try:
        s3.list_objects_v2(Bucket=bucket, MaxKeys=1)
        print("  [ok] ListObjectsV2 — credentials, endpoint, and bucket all resolve.")
    except EndpointConnectionError as e:
        print(f"  [FAIL] cannot reach {endpoint}: {e}")
        return 1
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "?")
        print(f"  [FAIL] ListObjectsV2 rejected ({code}): {e}")
        if code == "SignatureDoesNotMatch":
            print("         The access key is recognized but the SECRET doesn't match. Usually:")
            print("           - the secret was mistyped / partially pasted (it's shown only once)")
            print("           - the secret belongs to a different access key (regenerate the pair)")
            print("           - stray whitespace was included")
            print("         Fix: HF settings -> Access Tokens -> token -> 'Generate S3 credentials',")
            print("         copy BOTH the new HFAK id and its secret, and re-run.")
        elif code in ("InvalidAccessKeyId", "AccessDenied"):
            print("         The access key isn't recognized or lacks access to this bucket/namespace.")
            print("         Check the HFAK id, the namespace, and that the token has the right scope.")
        elif code in ("NoSuchBucket", "404"):
            print(f"         Bucket '{bucket}' not found under namespace '{namespace}'. Create it first.")
        else:
            print("         Check the bucket exists under this namespace and the key has access.")
        return 1

    # 4. Tiny write round-trip (confirms the token had Write scope).
    writable = False
    key = f"object-store-hf-smoke/pyverify-{int(time.time())}.txt"
    body = b"hf-smoke gather.py write check"
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body)
        got = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        s3.delete_object(Bucket=bucket, Key=key)
        if got == body:
            writable = True
            print("  [ok] put/get/delete round-trip — write access confirmed.")
        else:
            print("  [warn] round-trip content mismatch (unexpected); continuing.")
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "?")
        print(f"  [info] write round-trip failed ({code}) — looks like a Read-only token.")
        print("         That's fine: the Rust test will run in READ-ONLY mode (get/head/list),")
        print("         which still proves object_store follows the HF 302 -> CDN redirect.")
        print("         For the full write/multipart/delete suite, use a Write-scoped token.")

    # 5. Stash a .env for the Rust smoke test.
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    lines = [
        f"HF_NAMESPACE={namespace}",
        f"HF_BUCKET={bucket}",
        f"HF_REGION={region}",
        f"AWS_ACCESS_KEY_ID={access_key}",
        f"AWS_SECRET_ACCESS_KEY={secret_key}",
    ]
    if not writable:
        # Read-only creds: tell the Rust test to skip writes.
        lines.append("HF_SMOKE_READONLY=1")
    lines.append("")
    with open(env_path, "w") as f:
        f.write("\n".join(lines))
    os.chmod(env_path, stat.S_IRUSR | stat.S_IWUSR)  # 600

    print(f"\nWrote {env_path} (permissions 600).")
    if not writable:
        print("Set HF_SMOKE_READONLY=1 in .env (read-only creds -> read-only Rust test).")
    print("\nNow run the Rust smoke test from this directory:")
    print("    set -a && . ./.env && set +a && cargo run")
    print("\n(.env lives in the git-excluded scratch tree, so it won't be committed.)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
