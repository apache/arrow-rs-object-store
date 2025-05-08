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

## [v0.12.1](https://github.com/apache/arrow-rs-object-store/tree/v0.12.1) (2025-05-08)

[Full Changelog](https://github.com/apache/arrow-rs-object-store/compare/v0.12.0...v0.12.1)

**Implemented enhancements:**

- Support Alibaba OSS Object Storage [\#323](https://github.com/apache/arrow-rs-object-store/issues/323)
- Enable anonymous access to GCS buckets [\#302](https://github.com/apache/arrow-rs-object-store/issues/302)
- \[object\_store\] Run requests on a different tokio runtime [\#13](https://github.com/apache/arrow-rs-object-store/issues/13)
- \[object\_store\] consider migrating `humantime` to `jiff` [\#292](https://github.com/apache/arrow-rs-object-store/issues/292)
- Support EKS Pod Identity \(alternative to IRSA\) [\#282](https://github.com/apache/arrow-rs-object-store/issues/282)
- Object\_store: Create an upload method that handles concurrency [\#279](https://github.com/apache/arrow-rs-object-store/issues/279)
- object\_store: Retry on connection duration timeouts \(retry / recover after partially reading a streaming response\) [\#53](https://github.com/apache/arrow-rs-object-store/issues/53)
- \[object-store\] re-export `hyper` [\#293](https://github.com/apache/arrow-rs-object-store/issues/293)
- object\_store: abort\_multipart\(\) should return NotFound error if not found [\#146](https://github.com/apache/arrow-rs-object-store/issues/146)
- Make `GetOptionsExt` publicly usable [\#261](https://github.com/apache/arrow-rs-object-store/issues/261)

**Fixed bugs:**

- Incorrect token sent as part of url signing function. [\#337](https://github.com/apache/arrow-rs-object-store/issues/337)
- Azure Gen2 broken on latest [\#320](https://github.com/apache/arrow-rs-object-store/issues/320)
- object\_store: Azure brokenness on 0.12.0 [\#326](https://github.com/apache/arrow-rs-object-store/issues/326)
- Generic S3 error: Client error with status 411 Length Required [\#278](https://github.com/apache/arrow-rs-object-store/issues/278)

**Closed issues:**

- CI doesn't run on PRs [\#335](https://github.com/apache/arrow-rs-object-store/issues/335)
- Some Inconsistencies in the Path and List [\#327](https://github.com/apache/arrow-rs-object-store/issues/327)
- Add allow-list to restrict access to local files with LocalFileSystem [\#312](https://github.com/apache/arrow-rs-object-store/issues/312)
- Query on usage of experimental package ring [\#310](https://github.com/apache/arrow-rs-object-store/issues/310)
- \[Object Store\] Make the service account used when interacting with the metadata url more flexible [\#265](https://github.com/apache/arrow-rs-object-store/issues/265)

**Merged pull requests:**

- chore: Add anda\_object\_store to README [\#346](https://github.com/apache/arrow-rs-object-store/pull/346) ([zensh](https://github.com/zensh))
- Update nix requirement from 0.29.0 to 0.30.0 [\#344](https://github.com/apache/arrow-rs-object-store/pull/344) ([dependabot[bot]](https://github.com/apps/dependabot))
- Fix GCP signing token [\#338](https://github.com/apache/arrow-rs-object-store/pull/338) ([jackm-mimica](https://github.com/jackm-mimica))
- Fix query parameter signing in Azure [\#334](https://github.com/apache/arrow-rs-object-store/pull/334) ([AdamGS](https://github.com/AdamGS))
- feat: add EKS Pod Identity support \(\#282\) [\#333](https://github.com/apache/arrow-rs-object-store/pull/333) ([andreasbros](https://github.com/andreasbros))
- feat: Add `SpawnService` and `SpawnedReqwestConnector` for running requests on a different runtime [\#332](https://github.com/apache/arrow-rs-object-store/pull/332) ([ion-elgreco](https://github.com/ion-elgreco))
- Support `object_store` with wasm: Default wasm32-unknown-unknown HttpConnector [\#329](https://github.com/apache/arrow-rs-object-store/pull/329) ([H-Plus-Time](https://github.com/H-Plus-Time))
- Enable anonymous access to GCS buckets [\#322](https://github.com/apache/arrow-rs-object-store/pull/322) ([kylebarron](https://github.com/kylebarron))
- Fix semantic versioning link in README.md [\#317](https://github.com/apache/arrow-rs-object-store/pull/317) ([lewiszlw](https://github.com/lewiszlw))
- feat: make some helpers/utils public [\#316](https://github.com/apache/arrow-rs-object-store/pull/316) ([crepererum](https://github.com/crepererum))
- chore: fix `integration` feature [\#314](https://github.com/apache/arrow-rs-object-store/pull/314) ([crepererum](https://github.com/crepererum))
- Bump `rand` to 0.9 [\#303](https://github.com/apache/arrow-rs-object-store/pull/303) ([mbrobbel](https://github.com/mbrobbel))
- Add content length to PUT GCP multipart complete [\#257](https://github.com/apache/arrow-rs-object-store/pull/257) ([jkosh44](https://github.com/jkosh44))
- Update README.md and Contributing guidelines [\#8](https://github.com/apache/arrow-rs-object-store/pull/8) ([alamb](https://github.com/alamb))
- Tweaks: homepage and fix RAT [\#7](https://github.com/apache/arrow-rs-object-store/pull/7) ([alamb](https://github.com/alamb))
- Import `object_store`, with history, from arrow-rs [\#3](https://github.com/apache/arrow-rs-object-store/pull/3) ([alamb](https://github.com/alamb))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
