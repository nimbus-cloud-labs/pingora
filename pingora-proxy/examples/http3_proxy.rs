// Copyright 2026 Cloudflare, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[cfg(not(feature = "http3"))]
fn main() {
    panic!("run this example with `--features http3`");
}

#[cfg(feature = "http3")]
mod example {
    use async_trait::async_trait;
    use bytes::Bytes;
    use log::info;
    use pingora_core::server::configuration::Opt;
    use pingora_core::server::Server;
    use pingora_core::upstreams::peer::{Http3Peer, HttpPeer, HttpUpstreamTransport};
    use pingora_core::Result;
    use pingora_http::ResponseHeader;
    use pingora_proxy::{
        Http3Negotiation, Http3RetryClassifier, Http3RetryPolicy, ProxyHttp, Session,
    };
    use std::env;
    use std::time::Duration;

    pub struct Http3AwareProxy {
        downstream_http3: Http3Negotiation,
        upstream_retry: Http3RetryClassifier,
    }

    fn upstream_host() -> String {
        env::var("PINGORA_HTTP3_UPSTREAM_HOST").unwrap_or_else(|_| "www.google.com".to_string())
    }

    fn upstream_port() -> u16 {
        env::var("PINGORA_HTTP3_UPSTREAM_PORT")
            .ok()
            .and_then(|port| port.parse().ok())
            .unwrap_or(443)
    }

    #[async_trait]
    impl ProxyHttp for Http3AwareProxy {
        type CTX = ();

        fn new_ctx(&self) -> Self::CTX {}

        async fn request_filter(
            &self,
            session: &mut Session,
            _ctx: &mut Self::CTX,
        ) -> Result<bool> {
            if session.req_header().uri.path() == "/about" {
                let body = Bytes::from_static(
                    b"HTTP/3 foundations example: downstream alt-svc + upstream h3 peer selection",
                );
                session.respond_error_with_body(200, body).await?;
                return Ok(true);
            }

            if session.req_header().uri.path() == "/echo" {
                let mut body = Vec::new();
                while let Some(chunk) = session.as_downstream_mut().read_request_body().await? {
                    body.extend_from_slice(&chunk);
                }
                session
                    .respond_error_with_body(200, Bytes::from(body))
                    .await?;
                return Ok(true);
            }

            Ok(false)
        }

        async fn upstream_peer(
            &self,
            _session: &mut Session,
            _ctx: &mut Self::CTX,
        ) -> Result<Box<HttpPeer>> {
            let host = upstream_host();
            let port = upstream_port();
            Ok(Box::new(HttpPeer::new(
                (host.as_str(), port),
                true,
                host.clone(),
            )))
        }

        async fn upstream_transport(
            &self,
            _session: &mut Session,
            _ctx: &mut Self::CTX,
        ) -> Result<HttpUpstreamTransport> {
            Ok(HttpUpstreamTransport::Http3)
        }

        async fn upstream_http3_peer(
            &self,
            _session: &mut Session,
            _ctx: &mut Self::CTX,
        ) -> Result<Option<Box<Http3Peer>>> {
            let host = upstream_host();
            let port = upstream_port();
            let mut peer = Http3Peer::new((host.as_str(), port), host.clone());
            peer.options = peer
                .options
                .clone()
                .with_connect_timeout(Some(Duration::from_secs(5)))
                .with_idle_timeout(Some(Duration::from_secs(30)));
            Ok(Some(Box::new(peer)))
        }

        async fn response_filter(
            &self,
            _session: &mut Session,
            upstream_response: &mut ResponseHeader,
            _ctx: &mut Self::CTX,
        ) -> Result<()>
        where
            Self::CTX: Send + Sync,
        {
            upstream_response.insert_header("Server", "Http3AwareProxy")?;
            self.downstream_http3.apply_alt_svc(upstream_response)?;
            Ok(())
        }

        async fn logging(
            &self,
            session: &mut Session,
            _e: Option<&pingora_core::Error>,
            _ctx: &mut Self::CTX,
        ) {
            info!(
                "request: {}, upstream retry policy: {:?}",
                self.request_summary(session, &()),
                self.upstream_retry.policy()
            );
        }
    }

    // RUST_LOG=INFO cargo run -p pingora-proxy --example http3_proxy --features http3
    // curl -k https://127.0.0.1:6191/about
    // curl -k https://127.0.0.1:6191/robots.txt -H "Host: www.google.com"
    // curl --http3-only -k https://127.0.0.1:6191/robots.txt -H "Host: www.google.com"
    // curl --http3-only -k https://127.0.0.1:6191/echo -d 'pingora-http3'
    // PINGORA_HTTP3_UPSTREAM_HOST=www.google.com PINGORA_HTTP3_UPSTREAM_PORT=443 \
    //   cargo run -p pingora-proxy --example http3_proxy --features http3
    pub fn run() {
        env_logger::init();

        let opt = Opt::parse_args();
        let mut server = Server::new(Some(opt)).unwrap();
        server.bootstrap();

        let mut proxy = pingora_proxy::http_proxy_service(
            &server.configuration,
            Http3AwareProxy {
                downstream_http3: Http3Negotiation {
                    enabled: true,
                    advertised_port: 6191,
                    max_age: Some(86_400),
                },
                upstream_retry: Http3RetryClassifier::new(Http3RetryPolicy {
                    allow_h2_fallback: true,
                    allow_h1_fallback: false,
                }),
            },
        );
        let cert_path = format!(
            "{}/../pingora-core/tests/certs/server.crt",
            env!("CARGO_MANIFEST_DIR")
        );
        let key_path = format!(
            "{}/../pingora-core/tests/certs/server.key",
            env!("CARGO_MANIFEST_DIR")
        );
        proxy
            .add_tls("0.0.0.0:6191", &cert_path, &key_path)
            .expect("valid TLS listener");
        proxy
            .add_http3("0.0.0.0:6191", &cert_path, &key_path)
            .expect("valid HTTP/3 listener");
        server.add_service(proxy);
        server.run_forever();
    }
}

#[cfg(feature = "http3")]
fn main() {
    example::run();
}
