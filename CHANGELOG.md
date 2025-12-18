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

## [v0.13.0](https://github.com/apache/arrow-rs-object-store/tree/v0.13.0) (2025-12-18)

[Full Changelog](https://github.com/apache/arrow-rs-object-store/compare/v0.12.4...v0.13.0)

**Breaking changes:**

- fix: Add fields to `Error::NotImplemented` with more details on what isn't implemented [\#575](https://github.com/apache/arrow-rs-object-store/pull/575) ([carols10cents](https://github.com/carols10cents))
- refactor!: `copy` & `copy_if_not_exists` =\> `copy_opts` [\#548](https://github.com/apache/arrow-rs-object-store/pull/548) ([crepererum](https://github.com/crepererum))
- refactor!: move `head` to `ObjectStoreExt` [\#543](https://github.com/apache/arrow-rs-object-store/pull/543) ([crepererum](https://github.com/crepererum))
- fix: wrappers and default trait methods [\#537](https://github.com/apache/arrow-rs-object-store/pull/537) ([crepererum](https://github.com/crepererum))
- refactor!: move `get_range` to `ObjectStoreExt` [\#536](https://github.com/apache/arrow-rs-object-store/pull/536) ([crepererum](https://github.com/crepererum))
- refactor!: move `get` to `ObjectStoreExt` [\#532](https://github.com/apache/arrow-rs-object-store/pull/532) ([crepererum](https://github.com/crepererum))
- refactor: move `put_multipart` to `ObjectStoreExt` [\#530](https://github.com/apache/arrow-rs-object-store/pull/530) ([crepererum](https://github.com/crepererum))
- feat!: use `'static` lifetime for `delete_stream` [\#524](https://github.com/apache/arrow-rs-object-store/pull/524) ([linhr](https://github.com/linhr))
- Bump rust edition to 2024, rust version to 1.85 [\#515](https://github.com/apache/arrow-rs-object-store/pull/515) ([AdamGS](https://github.com/AdamGS))
- reapply: remove AWS dynamo integration \(\#407\), try 2 [\#494](https://github.com/apache/arrow-rs-object-store/pull/494) ([alamb](https://github.com/alamb))
- refactor: introduce `ObjectStoreExt` trait [\#405](https://github.com/apache/arrow-rs-object-store/pull/405) ([crepererum](https://github.com/crepererum))

**Implemented enhancements:**

- `NotImplemented` error should say what method wasn't implemented by what implementation [\#572](https://github.com/apache/arrow-rs-object-store/issues/572)
- Allow explicitly specifying the GCS base URL [\#566](https://github.com/apache/arrow-rs-object-store/issues/566)
- Detailed error messages for Generic [\#560](https://github.com/apache/arrow-rs-object-store/issues/560)
- Include reqwest/hyper error sources in error messages [\#554](https://github.com/apache/arrow-rs-object-store/issues/554)
- Improve `Path` ergonomics [\#545](https://github.com/apache/arrow-rs-object-store/issues/545)
- \[Suggestion\] Move ObjectStore API to use arrow-rs' Buffer [\#544](https://github.com/apache/arrow-rs-object-store/issues/544)
- Implement ObjectStore for Arc\<T\> and Box\<T\> [\#525](https://github.com/apache/arrow-rs-object-store/issues/525)
- Refactor GetOptions with a builder pattern [\#516](https://github.com/apache/arrow-rs-object-store/issues/516)
- Better support for Tags [\#508](https://github.com/apache/arrow-rs-object-store/issues/508)
- Error 411 \(Length Required\) when using Multipart PUT on GCP with S3Store [\#495](https://github.com/apache/arrow-rs-object-store/issues/495)
- Deprecate and Remove DynamoCommit [\#373](https://github.com/apache/arrow-rs-object-store/issues/373)
- Add CopyOptions [\#116](https://github.com/apache/arrow-rs-object-store/issues/116)

**Fixed bugs:**

- RequestError exposes underlying reqwest::Error rather than the nicer HttpError [\#579](https://github.com/apache/arrow-rs-object-store/issues/579)
- SpawnService panics tokio-runtime-worker threads [\#578](https://github.com/apache/arrow-rs-object-store/issues/578)
- local path filtering has different semantics [\#573](https://github.com/apache/arrow-rs-object-store/issues/573)
- AWS: using Checksum::SHA256 causes copy\_if\_not\_exists with S3CopyIfNotExists::Multipart to fail [\#568](https://github.com/apache/arrow-rs-object-store/issues/568)
- Cargo Audit Produces a Warning for rustls-pemfile being unmaintained [\#564](https://github.com/apache/arrow-rs-object-store/issues/564)
- AmazonS3ConfigKey::WebIdentityTokenFile is ignored [\#538](https://github.com/apache/arrow-rs-object-store/issues/538)
- Cannot parse AWS S3 HTTP URLs without region [\#522](https://github.com/apache/arrow-rs-object-store/issues/522)
- Signature mismatch \(sigv4\) when using attribute values with `double whitespace` [\#510](https://github.com/apache/arrow-rs-object-store/issues/510)
- Generic S3 error: Metadata value for ""x-amz-meta-.."" contained non UTF-8 characters [\#509](https://github.com/apache/arrow-rs-object-store/issues/509)
- Inconsistent documentation of "Supported Keys" [\#497](https://github.com/apache/arrow-rs-object-store/issues/497)

**Documentation updates:**

- Update release date for version 0.13.0 in README [\#521](https://github.com/apache/arrow-rs-object-store/pull/521) ([alamb](https://github.com/alamb))
- feat: refactor GetOptions with builder, add binary examples [\#517](https://github.com/apache/arrow-rs-object-store/pull/517) ([peasee](https://github.com/peasee))
- docs: document missing config keys [\#498](https://github.com/apache/arrow-rs-object-store/pull/498) ([CommanderStorm](https://github.com/CommanderStorm))

**Closed issues:**

- Add documentation for what backends support bulk delete \(`ObjectStore::delete_stream`\) [\#527](https://github.com/apache/arrow-rs-object-store/issues/527)
- Use `'static` lifetime for `delete_stream` [\#518](https://github.com/apache/arrow-rs-object-store/issues/518)
- Release object store  `0.12.4` \(non breaking API\) Release September 2025 [\#489](https://github.com/apache/arrow-rs-object-store/issues/489)

**Merged pull requests:**

- correctly expose HttpError through RetryError::source [\#580](https://github.com/apache/arrow-rs-object-store/pull/580) ([carlsverre](https://github.com/carlsverre))
- Remove dev dependency on openssl [\#577](https://github.com/apache/arrow-rs-object-store/pull/577) ([tustvold](https://github.com/tustvold))
- Documentation for backend support of bulk delete [\#571](https://github.com/apache/arrow-rs-object-store/pull/571) ([jayvdb](https://github.com/jayvdb))
- aws: fix bug in multipart copy when SHA256 checksum is used [\#569](https://github.com/apache/arrow-rs-object-store/pull/569) ([james-rms](https://github.com/james-rms))
- Allow explicitly specifying GCS base URL [\#567](https://github.com/apache/arrow-rs-object-store/pull/567) ([rgehan](https://github.com/rgehan))
- fix: cargo audit warning for rustls-pemfile [\#565](https://github.com/apache/arrow-rs-object-store/pull/565) ([mgattozzi](https://github.com/mgattozzi))
-  refactor!:  Consolidate `ObjectStore`: `rename` & `rename_if_not_exists` =\> `rename_opts` [\#555](https://github.com/apache/arrow-rs-object-store/pull/555) ([crepererum](https://github.com/crepererum))
- Improve Documentation on `ObjectStoreExt` [\#551](https://github.com/apache/arrow-rs-object-store/pull/551) ([alamb](https://github.com/alamb))
- build\(deps\): bump actions/checkout from 5 to 6 [\#550](https://github.com/apache/arrow-rs-object-store/pull/550) ([dependabot[bot]](https://github.com/apps/dependabot))
- refactor!: move `delete` to `ObjectStoreExt` [\#549](https://github.com/apache/arrow-rs-object-store/pull/549) ([crepererum](https://github.com/crepererum))
- Use builder fields instead of env vars for determining credential provider [\#547](https://github.com/apache/arrow-rs-object-store/pull/547) ([Friede80](https://github.com/Friede80))
- `Path` improvements [\#546](https://github.com/apache/arrow-rs-object-store/pull/546) ([Kinrany](https://github.com/Kinrany))
- docs\(client\): improve warnings and links in client doc comments [\#540](https://github.com/apache/arrow-rs-object-store/pull/540) ([CommanderStorm](https://github.com/CommanderStorm))
- fix\(gcp\): ignore ADC errors when explicit credentials are provided [\#531](https://github.com/apache/arrow-rs-object-store/pull/531) ([nazq](https://github.com/nazq))
- Implement ObjectStore for Arc\<T\> and Box\<T\> [\#526](https://github.com/apache/arrow-rs-object-store/pull/526) ([wyatt-herkamp](https://github.com/wyatt-herkamp))
- Allow parsing S3 URLs without region [\#523](https://github.com/apache/arrow-rs-object-store/pull/523) ([kylebarron](https://github.com/kylebarron))
- chore: fix msrv check by pinning syn [\#519](https://github.com/apache/arrow-rs-object-store/pull/519) ([mbrobbel](https://github.com/mbrobbel))
- fix: canonicalize multiple whitespaces in SigV4 [\#512](https://github.com/apache/arrow-rs-object-store/pull/512) ([ion-elgreco](https://github.com/ion-elgreco))
- build\(deps\): bump actions/setup-node from 5 to 6 [\#506](https://github.com/apache/arrow-rs-object-store/pull/506) ([dependabot[bot]](https://github.com/apps/dependabot))
- Remove unneeded files from published package [\#505](https://github.com/apache/arrow-rs-object-store/pull/505) ([weiznich](https://github.com/weiznich))
- minor: Fix MSRV CI workflow [\#502](https://github.com/apache/arrow-rs-object-store/pull/502) ([kylebarron](https://github.com/kylebarron))
- Allow more settings without vendor prefix [\#500](https://github.com/apache/arrow-rs-object-store/pull/500) ([jonashaag](https://github.com/jonashaag))
- Add Content\_length header to S3 create\_multipart [\#496](https://github.com/apache/arrow-rs-object-store/pull/496) ([dreamtalen](https://github.com/dreamtalen))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
