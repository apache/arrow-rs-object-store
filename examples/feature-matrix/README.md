<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Downstream HTTP and crypto examples

Each directory is a separate Cargo workspace. Each manifest selects its own `object_store` features and direct dependencies. Run an example with:

```sh
cargo run --manifest-path examples/feature-matrix/reqwest-ring/Cargo.toml
```

The examples cover these dependency choices:

| Directory | HTTP transport | Object request signing | TLS provider |
| --- | --- | --- | --- |
| `reqwest-ring` | reqwest | bundled Ring | explicit Rustls Ring |
| `reqwest-external-crypto` | reqwest | external SHA-256 and HMAC | native TLS |
| `custom-http-aws-lc` | custom connector | bundled AWS-LC | no TLS transport |
| `custom-http-ring` | custom connector | bundled Ring | no TLS transport |
| `custom-http-external-crypto` | custom connector | external SHA-256 and HMAC | no TLS transport |
| `batteries-included` | reqwest | bundled AWS-LC | default Rustls AWS-LC |

All five custom manifests enable the AWS, Azure, GCP, and HTTP base features. The batteries-included manifest enables all four complete features. Each example sends a signed S3 PUT with fake credentials. The custom connector records the request in memory. The reqwest examples use a local plain-HTTP listener. These checks do not test HTTPS certificate validation or a live cloud service.

Run `cargo fmt --check`, `cargo clippy -- -D warnings`, `cargo run`, and `cargo tree --edges normal` with each manifest path. The CI workflow runs these commands and checks the selected normal dependencies.
