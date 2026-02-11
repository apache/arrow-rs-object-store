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

#### Google specific configuration

| configuration                               | description                                                                                                                                                                                        | example                                                                                                                   |
|---------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `base_url`                                  | Sets the base URL for communicating with GCS.<br>If not explicitly set, it will be:<br>1. Derived from the service account credentials, if provided<br>2. Otherwise, uses the default GCS endpoint | `https://storage.googleapis.com`                                                                                          |
| `service_account`<br>`service_account_path` | Path to the service account file                                                                                                                                                                   | `some/path/to/file`                                                                                                       |
| `service_account_key`                       | The serialized service account key                                                                                                                                                                 | `{"private_key": "private_key", "private_key_id": "private_key_id", "client_email":"client_email", "disable_oauth":true}` |
| `bucket`<br>`bucket_name`                   | Bucket name                                                                                                                                                                                        | `foobar-abc`                                                                                                              |
| `application_credentials`                   | Set the path to the [application credentials file](https://cloud.google.com/docs/authentication/provide-credentials-adc)                                                                           | `some/path/to/file`                                                                                                       |
| `skip_signature`                            | Skip signing request                                                                                                                                                                               | `true`                                                                                                                    |
