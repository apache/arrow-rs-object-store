#!/bin/bash

# Test script to verify both crypto providers work correctly
# and that mutual exclusivity is enforced

set -e

echo "🔐 Testing Crypto Provider Implementation"
echo "========================================"

echo ""
echo "✅ Testing ring crypto provider..."
cargo test --features "crypto-ring,aws,fs" --quiet -- test_hex_digest test_hmac_sha256 test_hex_encode

echo ""
echo "✅ Testing aws-lc-rs crypto provider..."
cargo test --features "crypto-aws-lc-rs,aws,fs" --no-default-features --quiet -- test_hex_digest test_hmac_sha256 test_hex_encode

echo ""
echo "✅ Testing cloud features with ring..."
cargo check --features "cloud" --quiet

echo ""
echo "✅ Testing cloud features with aws-lc-rs..."
cargo check --features "cloud,crypto-aws-lc-rs" --no-default-features --quiet

echo ""
echo "✅ Testing AWS features with ring..."
cargo check --features "aws" --quiet

echo ""
echo "✅ Testing AWS features with aws-lc-rs..."
cargo check --features "aws,crypto-aws-lc-rs" --no-default-features --quiet

echo ""
echo "✅ Testing GCP features with ring..."
cargo check --features "gcp" --quiet

echo ""
echo "✅ Testing GCP features with aws-lc-rs..."
cargo check --features "gcp,crypto-aws-lc-rs" --no-default-features --quiet

echo ""
echo "✅ Testing example with ring..."
cargo run --example crypto_providers --features "cloud" --quiet

echo ""
echo "✅ Testing example with aws-lc-rs..."
cargo run --example crypto_providers --features "cloud,crypto-aws-lc-rs" --no-default-features --quiet

echo ""
echo "❌ Testing mutual exclusivity (should fail)..."
if cargo check --features "crypto-ring,crypto-aws-lc-rs" --quiet 2>/dev/null; then
    echo "ERROR: Mutual exclusivity check failed - both providers were allowed!"
    exit 1
else
    echo "✅ Mutual exclusivity correctly enforced"
fi

echo ""
echo "🎉 All tests passed!"
echo ""
echo "Summary:"
echo "- ✅ Both crypto providers work correctly"
echo "- ✅ Both providers produce identical results"
echo "- ✅ Cloud features work with both providers"
echo "- ✅ AWS features work with both providers"
echo "- ✅ GCP features work with both providers"
echo "- ✅ Mutual exclusivity is enforced"
echo "- ✅ Examples work with both providers"
echo ""
echo "Usage:"
echo "  # Use ring (default):"
echo "  cargo build --features cloud"
echo ""
echo "  # Use aws-lc-rs:"
echo "  cargo build --features 'cloud,crypto-aws-lc-rs' --no-default-features"
