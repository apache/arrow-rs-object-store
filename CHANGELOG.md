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

# Changelog

## [v0.14.2](https://github.com/apache/arrow-rs-object-store/tree/v0.14.2) (2026-09-11)

[Full Changelog](https://github.com/apache/arrow-rs-object-store/compare/v0.14.1...v0.14.2)

**Implemented enhancements:**

- Implement `Clone` for `MicrosoftAzure` [\#824](https://github.com/apache/arrow-rs-object-store/issues/824)
- Support URLs for jurisdiction-restricted buckets on R2 [\#814](https://github.com/apache/arrow-rs-object-store/issues/814)
- Lightweight DNS cache with shuffle to prevent DNS flooding [\#726](https://github.com/apache/arrow-rs-object-store/issues/726)
- Allow presigning object\_store multipart uploads  [\#271](https://github.com/apache/arrow-rs-object-store/issues/271)
- Allow specifying `Content-Type` when signing object\_store PUT requests [\#270](https://github.com/apache/arrow-rs-object-store/issues/270)

**Fixed bugs:**

- Incomplete ASF license header in `src/client/s3.rs` \(caused -1 vote on 0.14.2 RC1\) [\#853](https://github.com/apache/arrow-rs-object-store/issues/853)
- WriteMultipart::finish should abort after part upload failure [\#818](https://github.com/apache/arrow-rs-object-store/issues/818)
- The 1.85 MSRV is inaccurate with all features [\#811](https://github.com/apache/arrow-rs-object-store/issues/811)
- When the backend service returns an HTTP 500, the object store panics [\#414](https://github.com/apache/arrow-rs-object-store/issues/414)

**Documentation updates:**

- Docs: point users uploading large objects at the multipart API [\#839](https://github.com/apache/arrow-rs-object-store/pull/839) ([alamb](https://github.com/alamb))
- \[object-store\]: update release schedule [\#834](https://github.com/apache/arrow-rs-object-store/pull/834) ([alamb](https://github.com/alamb))
- Add doc example for multipart upload to `GoogleCloudStorage::create_multipart` [\#803](https://github.com/apache/arrow-rs-object-store/pull/803) ([alamb](https://github.com/alamb))
- Add doc example for multipart upload to `MicrosoftAzure::create_multipart` [\#802](https://github.com/apache/arrow-rs-object-store/pull/802) ([alamb](https://github.com/alamb))
- Add doc example for multipart upload to AmazonS3::create\_multipart [\#801](https://github.com/apache/arrow-rs-object-store/pull/801) ([alamb](https://github.com/alamb))

**Closed issues:**

- Regression: `ClientOptions` no longer `UnwindSafe`/`RefUnwindSafe` after custom DNS resolver support \(\#728\) [\#835](https://github.com/apache/arrow-rs-object-store/issues/835)
- Using `PutMode::Create` on Azure puts doesn't return `AlreadyExists` on failure [\#829](https://github.com/apache/arrow-rs-object-store/issues/829)
- Release object store `0.14.1` \(non-breaking\) - Target August 2026 [\#761](https://github.com/apache/arrow-rs-object-store/issues/761)
- Expose underlying object store capabilities \(e.g. ordered listing, negative ranges\) [\#675](https://github.com/apache/arrow-rs-object-store/issues/675)

**Merged pull requests:**

- Restore missing first line of ASF license header in `src/client/s3.rs` [\#854](https://github.com/apache/arrow-rs-object-store/pull/854) ([alamb](https://github.com/alamb))
- feat: retry failed multipart part uploads [\#849](https://github.com/apache/arrow-rs-object-store/pull/849) ([criccomini](https://github.com/criccomini))
- build\(deps\): bump taiki-e/install-action from 2.85.5 to 2.86.1 [\#842](https://github.com/apache/arrow-rs-object-store/pull/842) ([dependabot[bot]](https://github.com/apps/dependabot))
- Restore `UnwindSafe`/`RefUnwindSafe` on `ClientOptions` by bounding `DnsResolver` [\#836](https://github.com/apache/arrow-rs-object-store/pull/836) ([alamb](https://github.com/alamb))
- Return AlreadyExists in azure backend when using PutMode::Create and precondition fails [\#830](https://github.com/apache/arrow-rs-object-store/pull/830) ([itsjunetime](https://github.com/itsjunetime))
- Signer: reexport `url::Url` and `http::Method` types in the `signer` module [\#827](https://github.com/apache/arrow-rs-object-store/pull/827) ([Tpt](https://github.com/Tpt))
- Derive `Clone` for `MicrosoftAzure` [\#825](https://github.com/apache/arrow-rs-object-store/pull/825) ([kylebarron](https://github.com/kylebarron))
- build\(deps\): bump taiki-e/install-action from 2 to 2.85.5 [\#823](https://github.com/apache/arrow-rs-object-store/pull/823) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix \#818 - WriteMultipart::finish should abort after part upload failure [\#819](https://github.com/apache/arrow-rs-object-store/pull/819) ([anson-vandoren](https://github.com/anson-vandoren))
- Support R2 URLs with jurisdictions [\#815](https://github.com/apache/arrow-rs-object-store/pull/815) ([Kharacternyk](https://github.com/Kharacternyk))
- build\(deps\): update base64 requirement from 0.22 to 0.23 [\#813](https://github.com/apache/arrow-rs-object-store/pull/813) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix: restore 1.85 MSRV and fix MSRV CI [\#812](https://github.com/apache/arrow-rs-object-store/pull/812) ([LDeakin](https://github.com/LDeakin))
- build\(deps\): bump actions/setup-python from 6 to 7 [\#809](https://github.com/apache/arrow-rs-object-store/pull/809) ([dependabot[bot]](https://github.com/apps/dependabot))
- build\(deps\): bump actions/setup-node from 6 to 7 [\#804](https://github.com/apache/arrow-rs-object-store/pull/804) ([dependabot[bot]](https://github.com/apps/dependabot))
- feat: presigned URLs with extra query params and signed headers \(SignedUrlOptions\) [\#771](https://github.com/apache/arrow-rs-object-store/pull/771) ([zfarrell](https://github.com/zfarrell))
- feat: Support custom DNS resolver [\#728](https://github.com/apache/arrow-rs-object-store/pull/728) ([kdn36](https://github.com/kdn36))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
