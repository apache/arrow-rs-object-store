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

#### Security options

| configuration                | description                                                                                                                                                                                                                                                                                                                                                    | example |
|------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|
| `allow_http`                 | Allow non-TLS, i.e. non-HTTPS connections<br><br>**Security warning:**<br>If you enable this option, attackers may be able to read the data you request                                                                                                                                                                                                                        | `true`  |
| `allow_invalid_certificates` | Skip certificate validation on https connections<br><br>**Security warning:**<br>You should think very carefully before using this method. If invalid certificates are trusted, any certificate for any site will be trusted for use. This includes expired certificates. This introduces significant vulnerabilities, and should only be used as a last resort or for testing | `true`  |


#### Connection options

| configuration                 | description                                                                                                        | example        |
|-------------------------------|--------------------------------------------------------------------------------------------------------------------|----------------|
| `user_agent`                  | User-Agent header to be used by this client                                                                        | `martin 1.0.0` |
| `randomize_addresses`         | Randomize order addresses that the DNS resolution yields.<br>This will spread the connections across more servers. | `true`         |
| `connect_timeout`             | Timeout for only the connect phase of a Client                                                                     | `5s`           |
| `timeout`                     | The timeout is applied from when the request starts connecting until the response body has finished                | `10s`          |
| `pool_idle_timeout`           | The pool max idle timeout                                                                                          | `5m`           |
| `pool_max_idle_per_host`      | maximum number of idle connections per host                                                                        | `10`           |
| `http1_only`                  | Only use http1 connections                                                                                         | `false`        |
| `http2_only`                  | Only use http2 connections                                                                                         | `false`        |
| `http2_keep_alive_interval`   | Interval for HTTP2 Ping frames should be sent to keep a connection alive.                                          | `15s`          |
| `http2_keep_alive_timeout`    | Timeout for receiving an acknowledgement of the keep-alive ping.                                                   | `15s`          |
| `http2_keep_alive_while_idle` | Enable HTTP2 keep alive pings for idle connections                                                                 | `true`         |
| `http2_max_frame_size`        | Sets the maximum frame size to use for HTTP2.                                                                      |                |


#### Proxy settings

| configuration          | description                                        | example                         |
|------------------------|----------------------------------------------------|---------------------------------|
| `proxy_url`            | HTTP proxy to use for requests                     | `http://proxy.example.com:8080` |
| `proxy_ca_certificate` | PEM-formatted CA certificate for proxy connections | `-----BEGIN CERTIFICATE-----`<br>...<br>`-----END CERTIFICATE-----`                              |
| `proxy_excludes`       | List of hosts that bypass proxy                    | `example.com`, `apache.org`                   |
