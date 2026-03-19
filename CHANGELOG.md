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

## [v0.13.2](https://github.com/apache/arrow-rs-object-store/tree/v0.13.2) (2026-03-19)

[Full Changelog](https://github.com/apache/arrow-rs-object-store/compare/v0.12.5...v0.13.2)

**Implemented enhancements:**

- `Path::join(Self, &PathPart) -> Self` [\#665](https://github.com/apache/arrow-rs-object-store/issues/665)
- Support for AWS Encryption Client encryption [\#647](https://github.com/apache/arrow-rs-object-store/issues/647)
- `LocalFileSystem`: use `read_at` instead of seek + read [\#622](https://github.com/apache/arrow-rs-object-store/issues/622)
- Avoid reading metadata for `LocalFileSystem::read_ranges` \(and other methods\) [\#614](https://github.com/apache/arrow-rs-object-store/issues/614)
- expose `Inner` from `HttpRequestBody` [\#606](https://github.com/apache/arrow-rs-object-store/issues/606)
- Release object store `0.13.1` \(maintenance\) - Target Jan 2026 [\#598](https://github.com/apache/arrow-rs-object-store/issues/598)
- Support AWS\_ENDPOINT\_URL\_S3 in aws backend [\#589](https://github.com/apache/arrow-rs-object-store/issues/589)
- Release object store `0.12.5` \(maintenance\) - Target Dec 2025 [\#582](https://github.com/apache/arrow-rs-object-store/issues/582)
- Support upper-case configuration options in parse\_url\_opts [\#529](https://github.com/apache/arrow-rs-object-store/issues/529)
- object\_store: Support `{az,abfs,abfss}://container@account.blob.{core.windows.net,fabric.microsoft.com}` URLs [\#430](https://github.com/apache/arrow-rs-object-store/issues/430)
- Support `Transfer-Encoding: chunked` responses in HttpStore [\#340](https://github.com/apache/arrow-rs-object-store/issues/340)
- Use reconstructed ListBlobs marker to provide list offset support in `MicrosoftAzure` store [\#461](https://github.com/apache/arrow-rs-object-store/issues/461)

**Fixed bugs:**

- Azure Fabric: Unsigned integer underflow when fetching token causes integer overflow panic [\#640](https://github.com/apache/arrow-rs-object-store/issues/640)
- Error body missing for 5xx errors after retry exhausted [\#617](https://github.com/apache/arrow-rs-object-store/issues/617)
- Heavy contention on credentials cache [\#541](https://github.com/apache/arrow-rs-object-store/issues/541)
- AWS/S3 Default Headers are not considered for signature calculation [\#484](https://github.com/apache/arrow-rs-object-store/issues/484)
- az:// \<container\> not work as expected [\#443](https://github.com/apache/arrow-rs-object-store/issues/443)

**Performance improvements:**

- Preallocate single `Vec` in `get_ranges` for LocalFilesystem [\#634](https://github.com/apache/arrow-rs-object-store/issues/634)
- Use platform specific `read_at` when available [\#628](https://github.com/apache/arrow-rs-object-store/pull/628) ([AdamGS](https://github.com/AdamGS))
- Avoid metadata lookup for `LocalFileSystem::read_ranges` and `chunked_stream` [\#621](https://github.com/apache/arrow-rs-object-store/pull/621) ([Dandandan](https://github.com/Dandandan))

**Closed issues:**

- \[Security Alert\] Exposed API key\(s\) detected: AWS Access Key [\#659](https://github.com/apache/arrow-rs-object-store/issues/659)
- AWS S3 token expired on multi-threaded app with Arc usage [\#655](https://github.com/apache/arrow-rs-object-store/issues/655)
- Emulator tests fail in CI due to an unsupported service version header in Azurite [\#626](https://github.com/apache/arrow-rs-object-store/issues/626)

**Merged pull requests:**

- Replace `Path::child` with `Path::join` [\#666](https://github.com/apache/arrow-rs-object-store/pull/666) ([Kinrany](https://github.com/Kinrany))
- Support --xa-s3 suffix for S3 Express One Zone bucket access points [\#663](https://github.com/apache/arrow-rs-object-store/pull/663) ([pdeva](https://github.com/pdeva))
- docs: clarify `Clone` behavior [\#656](https://github.com/apache/arrow-rs-object-store/pull/656) ([crepererum](https://github.com/crepererum))
- Implement Clone for local and memory stores [\#653](https://github.com/apache/arrow-rs-object-store/pull/653) ([DoumanAsh](https://github.com/DoumanAsh))
- Unify `from_env` behaviours [\#652](https://github.com/apache/arrow-rs-object-store/pull/652) ([miraclx](https://github.com/miraclx))
- docs: add examples to the aws docs where appropriate [\#651](https://github.com/apache/arrow-rs-object-store/pull/651) ([CommanderStorm](https://github.com/CommanderStorm))
- Switch TokenCache to RWLock [\#648](https://github.com/apache/arrow-rs-object-store/pull/648) ([tustvold](https://github.com/tustvold))
- Minimize futures dependency into relevant sub-crates [\#646](https://github.com/apache/arrow-rs-object-store/pull/646) ([AdamGS](https://github.com/AdamGS))
- Clarify ShuffleResolver doc-comments [\#645](https://github.com/apache/arrow-rs-object-store/pull/645) ([jkosh44](https://github.com/jkosh44))
- Introduce a "tokio" to allow pulling a trait-only build [\#644](https://github.com/apache/arrow-rs-object-store/pull/644) ([AdamGS](https://github.com/AdamGS))
- fix\(azure\): fix integer overflow in Fabric token expiry check [\#641](https://github.com/apache/arrow-rs-object-store/pull/641) ([desmondcheongzx](https://github.com/desmondcheongzx))
- chore: upgrade to `rand` 0.10 [\#637](https://github.com/apache/arrow-rs-object-store/pull/637) ([crepererum](https://github.com/crepererum))
- fix\(aws\): Include default headers in signature calculation \(\#484\) [\#636](https://github.com/apache/arrow-rs-object-store/pull/636) ([singhsaabir](https://github.com/singhsaabir))
- fix\(azure\): correct Microsoft Fabric blob endpoint domain [\#631](https://github.com/apache/arrow-rs-object-store/pull/631) ([kevinjqliu](https://github.com/kevinjqliu))
- Unblock emulater based tests [\#627](https://github.com/apache/arrow-rs-object-store/pull/627) ([AdamGS](https://github.com/AdamGS))
- Azure ADLS list\_with\_offset support [\#623](https://github.com/apache/arrow-rs-object-store/pull/623) ([omar](https://github.com/omar))
- Implement tests for range and partial content responses [\#619](https://github.com/apache/arrow-rs-object-store/pull/619) ([vitoordaz](https://github.com/vitoordaz))
- fix: missing 5xx error body when retry exhausted [\#618](https://github.com/apache/arrow-rs-object-store/pull/618) ([jackye1995](https://github.com/jackye1995))
- build\(deps\): update nix requirement from 0.30.0 to 0.31.1 [\#616](https://github.com/apache/arrow-rs-object-store/pull/616) ([dependabot[bot]](https://github.com/apps/dependabot))
- Clarify behavior of `parse_url_opts` with regards to case sensitivity  [\#613](https://github.com/apache/arrow-rs-object-store/pull/613) ([AdamGS](https://github.com/AdamGS))
- Fix logical format conflict [\#605](https://github.com/apache/arrow-rs-object-store/pull/605) ([tustvold](https://github.com/tustvold))
- Fix Azure URL parsing [\#604](https://github.com/apache/arrow-rs-object-store/pull/604) ([tustvold](https://github.com/tustvold))
- build\(deps\): update quick-xml requirement from 0.38.0 to 0.39.0 [\#602](https://github.com/apache/arrow-rs-object-store/pull/602) ([dependabot[bot]](https://github.com/apps/dependabot))
- Only read file metadata once in `LocalFileSystem::read_ranges` [\#595](https://github.com/apache/arrow-rs-object-store/pull/595) ([AdamGS](https://github.com/AdamGS))
- feat: Add support for AWS\_ENDPOINT\_URL\_S3 environment variable [\#590](https://github.com/apache/arrow-rs-object-store/pull/590) ([rajatgoel](https://github.com/rajatgoel))
- feat: impl MultipartStore for PrefixStore [\#587](https://github.com/apache/arrow-rs-object-store/pull/587) ([ddupg](https://github.com/ddupg))
- Implement typos-cli [\#570](https://github.com/apache/arrow-rs-object-store/pull/570) ([jayvdb](https://github.com/jayvdb))
- feat \(azure\): support for '.blob.core.windows.net' in "az://" scheme [\#431](https://github.com/apache/arrow-rs-object-store/pull/431) ([vladidobro](https://github.com/vladidobro))


\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
