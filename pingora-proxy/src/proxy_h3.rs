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

//! Downstream HTTP/3 bridging on top of the QUIC transport layer.

use http::header::{self, CONNECTION, HOST, UPGRADE};
use http::Version;
use pingora_error::{Error, ErrorType, Result};
use pingora_http::{Method, RequestHeader};
use pingora_quic::{QuicIncomingDatagram, QuicSessionEvent};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;

/// Compatibility status for a proxy phase when used with downstream HTTP/3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Http3PhaseCompatibility {
    /// The phase operates on generic request/response state and is expected to work unchanged.
    Supported,
    /// The phase relies on downstream HTTP/1.x semantics and is not currently supported.
    Unsupported(&'static str),
}

/// Compatibility report for the current downstream HTTP/3 bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http3CompatibilityReport {
    /// `early_request_filter()`
    pub early_request_filter: Http3PhaseCompatibility,
    /// `request_filter()`
    pub request_filter: Http3PhaseCompatibility,
    /// `request_body_filter()`
    pub request_body_filter: Http3PhaseCompatibility,
    /// `proxy_upstream_filter()`
    pub proxy_upstream_filter: Http3PhaseCompatibility,
    /// `upstream_peer()`
    pub upstream_peer: Http3PhaseCompatibility,
    /// `upstream_request_filter()`
    pub upstream_request_filter: Http3PhaseCompatibility,
    /// `upstream_response_filter()` and body/trailer variants
    pub upstream_response_filters: Http3PhaseCompatibility,
    /// `response_filter()` and body/trailer variants
    pub downstream_response_filters: Http3PhaseCompatibility,
    /// `logging()`
    pub logging: Http3PhaseCompatibility,
    /// Downstream HTTP upgrade / websocket-over-upgrade semantics
    pub downstream_upgrade: Http3PhaseCompatibility,
}

impl Http3CompatibilityReport {
    /// Compatibility report for the currently implemented HTTP/3 bridge.
    pub const fn current() -> Self {
        Self {
            early_request_filter: Http3PhaseCompatibility::Supported,
            request_filter: Http3PhaseCompatibility::Supported,
            request_body_filter: Http3PhaseCompatibility::Supported,
            proxy_upstream_filter: Http3PhaseCompatibility::Supported,
            upstream_peer: Http3PhaseCompatibility::Supported,
            upstream_request_filter: Http3PhaseCompatibility::Supported,
            upstream_response_filters: Http3PhaseCompatibility::Supported,
            downstream_response_filters: Http3PhaseCompatibility::Supported,
            logging: Http3PhaseCompatibility::Supported,
            downstream_upgrade: Http3PhaseCompatibility::Unsupported(
                "HTTP/3 does not support HTTP/1.x Upgrade/Connection semantics",
            ),
        }
    }
}

/// Downstream protocol negotiation preference exposed by the HTTP/3 bridge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownstreamHttpVersion {
    /// HTTP/1.1
    Http1,
    /// HTTP/2
    Http2,
    /// HTTP/3
    Http3,
}

/// Feature-gated downstream HTTP/3 negotiation settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http3Negotiation {
    /// Whether downstream HTTP/3 is enabled for this proxy.
    pub enabled: bool,
    /// Preferred HTTP/3 UDP port to advertise.
    pub advertised_port: u16,
    /// Optional `ma` value for `alt-svc`.
    pub max_age: Option<u32>,
}

impl Default for Http3Negotiation {
    fn default() -> Self {
        Self {
            enabled: false,
            advertised_port: 443,
            max_age: Some(86_400),
        }
    }
}

impl Http3Negotiation {
    /// Build an `alt-svc` value for downstream advertisement.
    pub fn alt_svc_value(&self) -> Option<String> {
        if !self.enabled {
            return None;
        }

        let mut value = format!("h3=\":{}\"", self.advertised_port);
        if let Some(max_age) = self.max_age {
            value.push_str(&format!("; ma={max_age}"));
        }
        Some(value)
    }

    /// Apply the current HTTP/3 advertisement policy to a downstream response header.
    pub fn apply_alt_svc(&self, response: &mut pingora_http::ResponseHeader) -> Result<()> {
        if let Some(value) = self.alt_svc_value() {
            response.insert_header(header::ALT_SVC, value)?;
        } else {
            response.remove_header(&header::ALT_SVC);
        }
        Ok(())
    }

    /// Report the protocol preference order exposed to downstream clients.
    pub fn preferred_downstream_versions(&self) -> Vec<DownstreamHttpVersion> {
        if self.enabled {
            vec![
                DownstreamHttpVersion::Http3,
                DownstreamHttpVersion::Http2,
                DownstreamHttpVersion::Http1,
            ]
        } else {
            vec![DownstreamHttpVersion::Http2, DownstreamHttpVersion::Http1]
        }
    }
}

/// A single HTTP/3 stream accepted by the QUIC transport layer.
#[derive(Debug, Clone)]
pub struct Http3AcceptedStream {
    /// The underlying transport event that accepted or refreshed the QUIC session.
    pub transport: QuicIncomingDatagram,
    /// The bidirectional stream identifier.
    pub stream_id: u64,
    /// The decoded request method for this HTTP/3 stream.
    pub method: Method,
    /// The decoded request path or absolute-form URI path bytes.
    pub path: Vec<u8>,
    /// Optional `:authority` pseudo-header value.
    pub authority: Option<String>,
    /// Additional decoded header fields for the request.
    pub headers: Vec<(String, String)>,
}

/// HTTP/3-aware downstream session state tracked by the proxy bridge.
#[derive(Debug, Clone)]
pub struct Http3DownstreamSession {
    /// Underlying QUIC transport session state.
    pub transport: pingora_quic::QuicDownstreamSession,
    /// Number of HTTP/3 requests observed on this downstream session.
    pub requests_seen: u64,
}

/// A downstream HTTP/3 request prepared for the proxy phase model.
#[derive(Debug)]
pub struct Http3DownstreamRequest {
    /// The HTTP/3-aware session associated with this request.
    pub session: Http3DownstreamSession,
    /// The HTTP/3 stream identifier.
    pub stream_id: u64,
    /// Request headers represented using Pingora's existing request type.
    pub request_header: RequestHeader,
}

/// Bookkeeping counters for the HTTP/3 bridge.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Http3BridgeStats {
    /// Number of streams accepted through the bridge.
    pub accepted_streams: u64,
    /// Number of newly accepted downstream QUIC sessions.
    pub accepted_sessions: u64,
    /// Number of requests mapped onto existing downstream sessions.
    pub reused_sessions: u64,
}

#[derive(Debug, Default)]
struct Http3BridgeStatsInner {
    accepted_streams: AtomicU64,
    accepted_sessions: AtomicU64,
    reused_sessions: AtomicU64,
}

impl Http3BridgeStatsInner {
    fn snapshot(&self) -> Http3BridgeStats {
        Http3BridgeStats {
            accepted_streams: self.accepted_streams.load(Ordering::Relaxed),
            accepted_sessions: self.accepted_sessions.load(Ordering::Relaxed),
            reused_sessions: self.reused_sessions.load(Ordering::Relaxed),
        }
    }
}

/// Bridge from accepted QUIC streams into Pingora's downstream request model.
#[derive(Debug, Default)]
pub struct Http3ProxyBridge {
    sessions: Mutex<
        HashMap<pingora_core::protocols::l4::datagram::DatagramFlowKey, Http3DownstreamSession>,
    >,
    stats: Http3BridgeStatsInner,
}

impl Http3ProxyBridge {
    /// Create a new HTTP/3 proxy bridge.
    pub fn new() -> Self {
        Self::default()
    }

    /// Return a snapshot of bridge counters.
    pub fn stats(&self) -> Http3BridgeStats {
        self.stats.snapshot()
    }

    /// Return the currently supported downstream HTTP/3 phase behavior.
    pub const fn compatibility_report(&self) -> Http3CompatibilityReport {
        Http3CompatibilityReport::current()
    }

    /// Map an accepted HTTP/3 stream into a Pingora request header.
    pub fn accept_stream(&self, accepted: Http3AcceptedStream) -> Result<Http3DownstreamRequest> {
        let request_header = self.build_request_header(&accepted)?;
        let flow_key = accepted.transport.session.flow_key.clone();

        let mut sessions = self.sessions.lock().expect("http3 bridge mutex poisoned");
        let session = sessions
            .entry(flow_key)
            .or_insert_with(|| Http3DownstreamSession {
                transport: accepted.transport.session.clone(),
                requests_seen: 0,
            });

        session.transport = accepted.transport.session.clone();
        session.requests_seen += 1;

        self.stats.accepted_streams.fetch_add(1, Ordering::Relaxed);
        match accepted.transport.event {
            QuicSessionEvent::Accepted => {
                self.stats.accepted_sessions.fetch_add(1, Ordering::Relaxed);
            }
            QuicSessionEvent::Reused => {
                self.stats.reused_sessions.fetch_add(1, Ordering::Relaxed);
            }
        }

        Ok(Http3DownstreamRequest {
            session: session.clone(),
            stream_id: accepted.stream_id,
            request_header,
        })
    }

    fn build_request_header(&self, accepted: &Http3AcceptedStream) -> Result<RequestHeader> {
        self.ensure_compatible_headers(accepted)?;
        let mut request = RequestHeader::build(
            accepted.method.clone(),
            &accepted.path,
            Some(accepted.headers.len() + 1),
        )?;
        request.version = Version::HTTP_3;

        if let Some(authority) = &accepted.authority {
            request.insert_header(HOST, authority)?;
        }

        for (name, value) in &accepted.headers {
            request.append_header(name.clone(), value.clone())?;
        }

        Ok(request)
    }

    fn ensure_compatible_headers(&self, accepted: &Http3AcceptedStream) -> Result<()> {
        for (name, _) in &accepted.headers {
            if name.eq_ignore_ascii_case(CONNECTION.as_str())
                || name.eq_ignore_ascii_case(UPGRADE.as_str())
            {
                return Error::e_explain(
                    ErrorType::InvalidHTTPHeader,
                    "HTTP/3 downstream bridge does not support Connection or Upgrade headers",
                );
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Http3AcceptedStream, Http3PhaseCompatibility, Http3ProxyBridge};
    use http::Version;
    use pingora_core::protocols::l4::datagram::{Datagram, DatagramFlowKey, DatagramMeta};
    use pingora_core::protocols::l4::socket::SocketAddr;
    use pingora_error::ErrorType;
    use pingora_http::{Method, ResponseHeader};
    use pingora_quic::{
        QuicConnectionMeta, QuicDownstreamSession, QuicIncomingDatagram, QuicSessionEvent,
    };
    use std::sync::Arc;
    use std::time::Instant;

    fn accepted_stream(event: QuicSessionEvent, stream_id: u64) -> Http3AcceptedStream {
        let local_addr = SocketAddr::Inet("127.0.0.1:4433".parse().unwrap());
        let peer_addr = SocketAddr::Inet("127.0.0.1:50000".parse().unwrap());

        Http3AcceptedStream {
            transport: QuicIncomingDatagram {
                session: QuicDownstreamSession {
                    flow_key: DatagramFlowKey {
                        listener_id: Arc::<str>::from("h3"),
                        local_addr: local_addr.clone(),
                        peer_addr: peer_addr.clone(),
                    },
                    meta: QuicConnectionMeta {
                        local_addr: local_addr.clone(),
                        peer_addr: peer_addr.clone(),
                        alpn_protocol: Some(b"h3".to_vec()),
                        server_name: None,
                        resumed: false,
                    },
                    established_at: Instant::now(),
                    last_seen: Instant::now(),
                    packets_received: 1,
                },
                datagram: Datagram::new(
                    DatagramMeta {
                        local_addr,
                        peer_addr,
                    },
                    b"headers".to_vec(),
                ),
                event,
            },
            stream_id,
            method: Method::GET,
            path: b"/health".to_vec(),
            authority: Some("example.com".to_string()),
            headers: vec![("x-test".to_string(), "1".to_string())],
        }
    }

    #[test]
    fn bridge_maps_http3_stream_into_request_header() {
        let bridge = Http3ProxyBridge::new();
        let request = bridge
            .accept_stream(accepted_stream(QuicSessionEvent::Accepted, 0))
            .unwrap();

        assert_eq!(request.stream_id, 0);
        assert_eq!(request.request_header.version, Version::HTTP_3);
        assert_eq!(request.request_header.method, Method::GET);
        assert_eq!(request.request_header.uri.path(), "/health");
        assert_eq!(
            request
                .request_header
                .headers
                .get(http::header::HOST)
                .unwrap(),
            "example.com"
        );
        assert_eq!(request.session.requests_seen, 1);
    }

    #[test]
    fn bridge_reuses_downstream_session_across_streams() {
        let bridge = Http3ProxyBridge::new();

        let first = bridge
            .accept_stream(accepted_stream(QuicSessionEvent::Accepted, 0))
            .unwrap();
        let second = bridge
            .accept_stream(accepted_stream(QuicSessionEvent::Reused, 4))
            .unwrap();

        assert_eq!(
            first.session.transport.flow_key,
            second.session.transport.flow_key
        );
        assert_eq!(second.session.requests_seen, 2);

        let stats = bridge.stats();
        assert_eq!(stats.accepted_streams, 2);
        assert_eq!(stats.accepted_sessions, 1);
        assert_eq!(stats.reused_sessions, 1);
    }

    #[test]
    fn bridge_reports_current_http3_phase_compatibility() {
        let bridge = Http3ProxyBridge::new();
        let report = bridge.compatibility_report();

        assert_eq!(report.request_filter, Http3PhaseCompatibility::Supported);
        assert_eq!(
            report.downstream_upgrade,
            Http3PhaseCompatibility::Unsupported(
                "HTTP/3 does not support HTTP/1.x Upgrade/Connection semantics"
            )
        );
    }

    #[test]
    fn bridge_rejects_upgrade_style_headers() {
        let bridge = Http3ProxyBridge::new();
        let mut accepted = accepted_stream(QuicSessionEvent::Accepted, 0);
        accepted
            .headers
            .push(("connection".to_string(), "upgrade".to_string()));

        let error = bridge.accept_stream(accepted).unwrap_err();
        assert_eq!(error.etype, ErrorType::InvalidHTTPHeader);
    }

    #[test]
    fn negotiation_disables_or_advertises_alt_svc() {
        let mut response = ResponseHeader::build(200, None).unwrap();
        let negotiation = super::Http3Negotiation::default();
        negotiation.apply_alt_svc(&mut response).unwrap();
        assert!(response.headers.get(http::header::ALT_SVC).is_none());

        let mut response = ResponseHeader::build(200, None).unwrap();
        let enabled = super::Http3Negotiation {
            enabled: true,
            advertised_port: 8443,
            max_age: Some(60),
        };
        enabled.apply_alt_svc(&mut response).unwrap();
        assert_eq!(
            response.headers.get(http::header::ALT_SVC).unwrap(),
            "h3=\":8443\"; ma=60"
        );
    }

    #[test]
    fn negotiation_reports_predictable_fallback_order() {
        let disabled = super::Http3Negotiation::default();
        assert_eq!(
            disabled.preferred_downstream_versions(),
            vec![
                super::DownstreamHttpVersion::Http2,
                super::DownstreamHttpVersion::Http1
            ]
        );

        let enabled = super::Http3Negotiation {
            enabled: true,
            advertised_port: 443,
            max_age: Some(86_400),
        };
        assert_eq!(
            enabled.preferred_downstream_versions(),
            vec![
                super::DownstreamHttpVersion::Http3,
                super::DownstreamHttpVersion::Http2,
                super::DownstreamHttpVersion::Http1
            ]
        );
    }
}
