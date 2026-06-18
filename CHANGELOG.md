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

## [v0.14.0](https://github.com/apache/arrow-rs-object-store/tree/v0.14.0) (2026-06-18)

[Full Changelog](https://github.com/apache/arrow-rs-object-store/compare/v0.13.2...v0.14.0)

**Breaking changes:**

- refactor!: remove `cloud` feature alias [\#751](https://github.com/apache/arrow-rs-object-store/pull/751) ([kevinjqliu](https://github.com/kevinjqliu))
- Add `Extensions` in `*Result` objects [\#743](https://github.com/apache/arrow-rs-object-store/pull/743) ([criccomini](https://github.com/criccomini))
- Make `reqwest` optional [\#724](https://github.com/apache/arrow-rs-object-store/pull/724) ([awesterb](https://github.com/awesterb))
- Pluggable Crypto / Update reqwest 0.13 [\#707](https://github.com/apache/arrow-rs-object-store/pull/707) ([goffrie](https://github.com/goffrie))
- Add CRC64NVME checksum support [\#633](https://github.com/apache/arrow-rs-object-store/pull/633) ([kdn36](https://github.com/kdn36))

**Implemented enhancements:**

- Support PutMultipartOptions for MultipartStore::create\_multipart [\#745](https://github.com/apache/arrow-rs-object-store/issues/745)
- Supporting CPKs for Azure Blob Requests [\#741](https://github.com/apache/arrow-rs-object-store/issues/741)
- Support `Extensions` in `*Result` objects [\#740](https://github.com/apache/arrow-rs-object-store/issues/740)
- Implement `Signer` on `PrefixStore` [\#738](https://github.com/apache/arrow-rs-object-store/issues/738)
- Make `reqwest` optional [\#723](https://github.com/apache/arrow-rs-object-store/issues/723)
- Expose azure::split\_sas for custom SAS credential providers [\#720](https://github.com/apache/arrow-rs-object-store/issues/720)
- GCS: support bearer-token auth in parse\_url\_opts without bypassing default option handling [\#716](https://github.com/apache/arrow-rs-object-store/issues/716)
- Custom credentials for signing cannot be provided to GCP stores [\#698](https://github.com/apache/arrow-rs-object-store/issues/698)
- Read timeouts are not supported [\#680](https://github.com/apache/arrow-rs-object-store/issues/680)
- AWS\_REQUEST\_PAYER="requester" vs Boolean [\#668](https://github.com/apache/arrow-rs-object-store/issues/668)
- Reliable file writes for LocalFileSystem: explicit close error checking and opt-in sync [\#661](https://github.com/apache/arrow-rs-object-store/issues/661)
- infer `use_fabric_endpoint` based on URI containing "dfs.fabric.microsoft.com" [\#658](https://github.com/apache/arrow-rs-object-store/issues/658)
- Support CRC checksum [\#611](https://github.com/apache/arrow-rs-object-store/issues/611)
- Change crypto provider from ring to aws-rust-lc [\#413](https://github.com/apache/arrow-rs-object-store/issues/413)

**Fixed bugs:**

- `resolve_bucket_region` fails TLS handshake for bucket names containing dots [\#747](https://github.com/apache/arrow-rs-object-store/issues/747)
- Extensions not passed in `BufWriter` single put case [\#735](https://github.com/apache/arrow-rs-object-store/issues/735)
- GoogleCloudStorage::copy overwrite semantics are flipped. [\#712](https://github.com/apache/arrow-rs-object-store/issues/712)
- BUG: in-memory storage loss part on upload [\#682](https://github.com/apache/arrow-rs-object-store/issues/682)
- PrefixStore::head doesn't strip prefix from meta.location [\#664](https://github.com/apache/arrow-rs-object-store/issues/664)
- Invalid range in read with `get` from object\_store [\#654](https://github.com/apache/arrow-rs-object-store/issues/654)
- newline\_delimited\_stream incorrect processing `Generic { store: "LineDelimiter", source: UnterminatedString }` on valid CSVs \(impacts datafusion\) [\#650](https://github.com/apache/arrow-rs-object-store/issues/650)
- When the backend service returns an HTTP 500, the object store panics [\#414](https://github.com/apache/arrow-rs-object-store/issues/414)
- Object store failing during bulk delete in S3 [\#277](https://github.com/apache/arrow-rs-object-store/issues/277)

**Closed issues:**

- Document new feature flag system [\#749](https://github.com/apache/arrow-rs-object-store/issues/749)
- Add CI check to ensure reqwest is not accidentally added [\#748](https://github.com/apache/arrow-rs-object-store/issues/748)
- Discuss feature naming for optional transport and crypto support [\#744](https://github.com/apache/arrow-rs-object-store/issues/744)
- S3: delete\(\) now requires DeleteObjects \(bulk delete\) support, breaks S3-compatible providers \(Alibaba Cloud OSS, etc.\) [\#731](https://github.com/apache/arrow-rs-object-store/issues/731)
- use `startFrom` when listing blobs from OneLake endpoints [\#697](https://github.com/apache/arrow-rs-object-store/issues/697)
- MicrosoftAzure::list\_with\_offset returns empty on OneLake since 0.13.0 \(regression from \#623\) [\#695](https://github.com/apache/arrow-rs-object-store/issues/695)
- Release object store 0.14.0 \(breaking API changes\), target May 2026 [\#673](https://github.com/apache/arrow-rs-object-store/issues/673)
- Release object store 0.13.3 \(non-breaking API changes\), Target May 2026 [\#672](https://github.com/apache/arrow-rs-object-store/issues/672)

**Merged pull requests:**

- Clarify aws-lc-rs feature docs and fix CI comment URL [\#764](https://github.com/apache/arrow-rs-object-store/pull/764) ([alamb](https://github.com/alamb))
- fix\(local\): fsync create-mode rename source delete [\#758](https://github.com/apache/arrow-rs-object-store/pull/758) ([kevinjqliu](https://github.com/kevinjqliu))
- Fix flaky local close-file tests [\#757](https://github.com/apache/arrow-rs-object-store/pull/757) ([kevinjqliu](https://github.com/kevinjqliu))
- build\(deps\): update itertools requirement from 0.14.0 to 0.15.0 [\#755](https://github.com/apache/arrow-rs-object-store/pull/755) ([dependabot[bot]](https://github.com/apps/dependabot))
- Support PutMultipartOptions for MultipartStore::create\_multipart for AWS/GCP. [\#754](https://github.com/apache/arrow-rs-object-store/pull/754) ([BearMinimum98](https://github.com/BearMinimum98))
- Use path-style URL in `resolve_bucket_region` for dotted bucket names [\#752](https://github.com/apache/arrow-rs-object-store/pull/752) ([brankogrb-db](https://github.com/brankogrb-db))
- docs: explain `*-base` features and document the full feature flag set [\#750](https://github.com/apache/arrow-rs-object-store/pull/750) ([kevinjqliu](https://github.com/kevinjqliu))
- ci: use --locked when installing cargo-msrv and cargo-audit [\#746](https://github.com/apache/arrow-rs-object-store/pull/746) ([kevinjqliu](https://github.com/kevinjqliu))
- Supporting CPK \(Customer Provided Keys\) in Azure Blob Storage requests [\#742](https://github.com/apache/arrow-rs-object-store/pull/742) ([Braedon-Wooding-Displayr](https://github.com/Braedon-Wooding-Displayr))
- Add `impl<T: Signed> Signed for PrefixStore<T>` [\#739](https://github.com/apache/arrow-rs-object-store/pull/739) ([Tpt](https://github.com/Tpt))
- feat: add option to disable bulk delete for aws [\#734](https://github.com/apache/arrow-rs-object-store/pull/734) ([hengfeiyang](https://github.com/hengfeiyang))
- fix\(aws\): set retry\_error\_body on bulk\_delete\_request [\#733](https://github.com/apache/arrow-rs-object-store/pull/733) ([rustyprimus](https://github.com/rustyprimus))
- build\(deps\): update quick-xml requirement from 0.39.0 to 0.40.1 [\#727](https://github.com/apache/arrow-rs-object-store/pull/727) ([dependabot[bot]](https://github.com/apps/dependabot))
- feat\(azure\): expose `split_sas` for custom SAS credential providers [\#721](https://github.com/apache/arrow-rs-object-store/pull/721) ([dbatomic](https://github.com/dbatomic))
- refactor: unify internal naming of `allow_invalid_certificates` [\#718](https://github.com/apache/arrow-rs-object-store/pull/718) ([CommanderStorm](https://github.com/CommanderStorm))
- feat\(gcp\): support explicit bearer token config in parse flow [\#717](https://github.com/apache/arrow-rs-object-store/pull/717) ([siddharthmittal13](https://github.com/siddharthmittal13))
- GCP: fix copy \(no\) overwrite semantics [\#713](https://github.com/apache/arrow-rs-object-store/pull/713) ([james-rms](https://github.com/james-rms))
- Whitelisting Onelake API & Workspace PL FQDNs [\#711](https://github.com/apache/arrow-rs-object-store/pull/711) ([SmritiAgrawal04](https://github.com/SmritiAgrawal04))
- feat: add AzureConfigKey::CredentialType to select Azure credential method [\#710](https://github.com/apache/arrow-rs-object-store/pull/710) ([jackye1995](https://github.com/jackye1995))
- fix: fix incorrect splitting with line delimited streaming [\#700](https://github.com/apache/arrow-rs-object-store/pull/700) ([bboissin](https://github.com/bboissin))
- Add `with_signing_credentials` to GCP builder [\#699](https://github.com/apache/arrow-rs-object-store/pull/699) ([ilya-zlobintsev](https://github.com/ilya-zlobintsev))
- fix: clippy [\#694](https://github.com/apache/arrow-rs-object-store/pull/694) ([crepererum](https://github.com/crepererum))
- build\(deps\): bump actions/github-script from 8 to 9 [\#692](https://github.com/apache/arrow-rs-object-store/pull/692) ([dependabot[bot]](https://github.com/apps/dependabot))
- fix\(core\): add missing version assignment [\#691](https://github.com/apache/arrow-rs-object-store/pull/691) ([dentiny](https://github.com/dentiny))
- fix\(retry\): respect range header on retry [\#690](https://github.com/apache/arrow-rs-object-store/pull/690) ([dentiny](https://github.com/dentiny))
- fix\(core\): add missing extension assignment [\#689](https://github.com/apache/arrow-rs-object-store/pull/689) ([dentiny](https://github.com/dentiny))
- fix\(aws\): populate default header for complete multipart request [\#688](https://github.com/apache/arrow-rs-object-store/pull/688) ([dentiny](https://github.com/dentiny))
- fix\[prefix\]: strip\_meta from get\_opts result in PrefixStore [\#686](https://github.com/apache/arrow-rs-object-store/pull/686) ([asubiotto](https://github.com/asubiotto))
- fix\(memory\): fix out-of-order multipart upload for in-memory store [\#683](https://github.com/apache/arrow-rs-object-store/pull/683) ([dentiny](https://github.com/dentiny))
- Expose `read_timeout` from `reqwest` [\#681](https://github.com/apache/arrow-rs-object-store/pull/681) ([Muon](https://github.com/Muon))
- build\(deps\): update md-5 requirement from 0.10.6 to 0.11.0 [\#679](https://github.com/apache/arrow-rs-object-store/pull/679) ([dependabot[bot]](https://github.com/apps/dependabot))
- feat\(local\): explicit close with error checking for LocalFileSystem [\#676](https://github.com/apache/arrow-rs-object-store/pull/676) ([jgiannuzzi](https://github.com/jgiannuzzi))
- Update release schedule on README.md [\#674](https://github.com/apache/arrow-rs-object-store/pull/674) ([alamb](https://github.com/alamb))
- fix: support `AWS_REQUEST_PAYER=requester` [\#669](https://github.com/apache/arrow-rs-object-store/pull/669) ([ljstrnadiii](https://github.com/ljstrnadiii))
- feat\(local\): add fsync to LocalFileSystem for durability [\#643](https://github.com/apache/arrow-rs-object-store/pull/643) ([Barre](https://github.com/Barre))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
