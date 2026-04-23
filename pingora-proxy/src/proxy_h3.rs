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

use futures::SinkExt;
use http::header::{self, CONNECTION, HOST, UPGRADE};
use http::Version;
use pingora_core::connectors::http::{custom, Connector};
use pingora_core::protocols::http::subrequest::server::HttpSession as SubrequestSession;
use pingora_core::protocols::http::{HttpTask, ServerSession};
use pingora_core::server::ShutdownWatch;
use pingora_core::upstreams::peer::{Http3Peer, HttpPeer, HttpUpstreamTransport, Peer};
use pingora_error::{Error, ErrorType, Result};
use pingora_http::{Method, RequestHeader, ResponseHeader};
use pingora_quic::{
    Http3UpstreamPool, Http3UpstreamSession, QuicConnectorConfig, QuicDownstreamSession,
    QuicIncomingDatagram, QuicSessionEvent, QuicUpstreamPoolStats,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tokio::sync::oneshot;
use tokio_quiche::buf_factory::BufFactory;
use tokio_quiche::http3::driver::{
    ClientH3Event, H3Event, InboundFrame, InboundFrameStream, IncomingH3Headers, NewClientRequest,
    OutboundFrame, OutboundFrameSender,
};
use tokio_quiche::quiche::h3::{Header as H3Header, NameValue};

use crate::{HttpProxy, ProxyHttp};
use bytes::Bytes;
use pingora_core::apps::HttpServerApp;

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

/// Transport choice for an upstream HTTP request as resolved by the proxy layer.
#[derive(Debug, Clone)]
pub enum SelectedHttpUpstream {
    /// HTTP/1.x or HTTP/2 stream-based upstream.
    Stream(Box<HttpPeer>),
    /// HTTP/3 QUIC-backed upstream.
    Http3(Box<Http3Peer>),
}

impl SelectedHttpUpstream {
    /// Report the transport category selected for the upstream.
    pub fn transport(&self) -> HttpUpstreamTransport {
        match self {
            SelectedHttpUpstream::Stream(peer) => match peer.get_alpn() {
                Some(alpn) if alpn.get_min_http_version() >= 2 => HttpUpstreamTransport::Http2,
                _ => HttpUpstreamTransport::Http1,
            },
            SelectedHttpUpstream::Http3(_) => HttpUpstreamTransport::Http3,
        }
    }
}

/// Retry action chosen for an HTTP/3 upstream failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Http3RetryAction {
    /// Do not retry the request.
    Fail,
    /// Retry the request against the same logical upstream peer.
    RetrySamePeer,
    /// Retry the request after re-running upstream selection.
    RetryNextPeer,
    /// Fall back to a stream-based upstream transport.
    Fallback(HttpUpstreamTransport),
}

/// Policy knobs for HTTP/3 upstream retry and failover behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http3RetryPolicy {
    /// Whether connect-time QUIC failures may fall back to HTTP/2.
    pub allow_h2_fallback: bool,
    /// Whether connect-time QUIC failures may fall back to HTTP/1.1.
    pub allow_h1_fallback: bool,
}

impl Default for Http3RetryPolicy {
    fn default() -> Self {
        Self {
            allow_h2_fallback: true,
            allow_h1_fallback: false,
        }
    }
}

/// Request-scoped context used when classifying HTTP/3 upstream failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Http3RetryContext {
    /// Whether the request is safe to replay, e.g. idempotent or fully buffered.
    pub request_can_retry: bool,
}

/// Normalized decision for HTTP/3 upstream retry, failover, and timeout handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Http3RetryDecision {
    /// What the caller should do next.
    pub action: Http3RetryAction,
    /// Whether the proxy should mark the underlying error as retryable.
    pub mark_retryable: bool,
    /// Whether a fresh QUIC session must be used for the next attempt.
    pub fresh_session_required: bool,
    /// Whether upstream peer selection may choose another backend.
    pub allow_backend_remap: bool,
}

impl Http3RetryDecision {
    const fn fail() -> Self {
        Self {
            action: Http3RetryAction::Fail,
            mark_retryable: false,
            fresh_session_required: false,
            allow_backend_remap: false,
        }
    }
}

/// Classifies HTTP/3 upstream failures into retry, failover, and fallback actions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Http3RetryClassifier {
    policy: Http3RetryPolicy,
}

impl Http3RetryClassifier {
    /// Create a new classifier with the given policy.
    pub fn new(policy: Http3RetryPolicy) -> Self {
        Self { policy }
    }

    /// Return the currently configured policy.
    pub fn policy(&self) -> Http3RetryPolicy {
        self.policy
    }

    /// Classify a failure that happened before the upstream request was sent.
    pub fn classify_connect_error(&self, error: &Error) -> Http3RetryDecision {
        match error.etype() {
            ErrorType::ConnectTimedout
            | ErrorType::ConnectRefused
            | ErrorType::ConnectNoRoute
            | ErrorType::ConnectError
            | ErrorType::TLSHandshakeFailure
            | ErrorType::TLSHandshakeTimedout
            | ErrorType::HandshakeError => {
                if self.policy.allow_h2_fallback {
                    Http3RetryDecision {
                        action: Http3RetryAction::Fallback(HttpUpstreamTransport::Http2),
                        mark_retryable: true,
                        fresh_session_required: true,
                        allow_backend_remap: true,
                    }
                } else if self.policy.allow_h1_fallback {
                    Http3RetryDecision {
                        action: Http3RetryAction::Fallback(HttpUpstreamTransport::Http1),
                        mark_retryable: true,
                        fresh_session_required: true,
                        allow_backend_remap: true,
                    }
                } else {
                    Http3RetryDecision {
                        action: Http3RetryAction::RetryNextPeer,
                        mark_retryable: true,
                        fresh_session_required: true,
                        allow_backend_remap: true,
                    }
                }
            }
            _ => Http3RetryDecision::fail(),
        }
    }

    /// Classify a failure that happened after an upstream HTTP/3 session was selected.
    pub fn classify_proxy_error(
        &self,
        error: &Error,
        context: Http3RetryContext,
    ) -> Http3RetryDecision {
        match error.etype() {
            ErrorType::ConnectTimedout
            | ErrorType::ConnectRefused
            | ErrorType::ConnectNoRoute
            | ErrorType::ConnectError
            | ErrorType::TLSHandshakeFailure
            | ErrorType::TLSHandshakeTimedout
            | ErrorType::HandshakeError => self.classify_connect_error(error),
            ErrorType::ConnectionClosed
            | ErrorType::ReadTimedout
            | ErrorType::WriteTimedout
            | ErrorType::ReadError
            | ErrorType::WriteError => {
                if context.request_can_retry {
                    Http3RetryDecision {
                        action: Http3RetryAction::RetrySamePeer,
                        mark_retryable: true,
                        fresh_session_required: true,
                        allow_backend_remap: false,
                    }
                } else {
                    Http3RetryDecision::fail()
                }
            }
            _ => Http3RetryDecision::fail(),
        }
    }
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
    /// Request body reader for the real downstream HTTP/3 stream, when available.
    pub body_reader: Option<Http3BodyReader>,
    /// Response writer for the same downstream HTTP/3 stream, when available.
    pub response_writer: Option<Http3ResponseWriter>,
    /// Whether downstream request trailers are surfaced by the current backend.
    pub request_trailers_supported: bool,
}

/// Body data observed on a downstream HTTP/3 request stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http3BodyChunk {
    /// Bytes received for this chunk.
    pub data: Vec<u8>,
    /// Whether this chunk closed the receive side of the stream.
    pub fin: bool,
}

/// Read-side handle for a downstream HTTP/3 request body.
#[derive(Debug)]
pub struct Http3BodyReader {
    recv: InboundFrameStream,
}

impl Http3BodyReader {
    fn new(recv: InboundFrameStream) -> Self {
        Self { recv }
    }

    /// Read the next request body chunk.
    ///
    /// Request trailers are not surfaced by the current tokio-quiche server
    /// driver API, so `None` means the body stream has ended.
    pub async fn recv_chunk(&mut self) -> Result<Option<Http3BodyChunk>> {
        let Some(frame) = self.recv.recv().await else {
            return Ok(None);
        };

        match frame {
            InboundFrame::Body(buf, fin) => Ok(Some(Http3BodyChunk {
                data: buf.into_inner().into_vec(),
                fin,
            })),
            InboundFrame::Datagram(_) => Error::e_explain(
                ErrorType::InternalError,
                "HTTP/3 request body path does not accept DATAGRAM frames",
            ),
        }
    }
}

/// Write-side handle for a downstream HTTP/3 response stream.
#[derive(Debug)]
pub struct Http3ResponseWriter {
    send: OutboundFrameSender,
    headers_sent: bool,
}

impl Http3ResponseWriter {
    fn new(send: OutboundFrameSender) -> Self {
        Self {
            send,
            headers_sent: false,
        }
    }

    /// Write HTTP/3 response headers to the downstream stream.
    pub async fn send_response_header(&mut self, response: &ResponseHeader) -> Result<()> {
        if self.headers_sent {
            return Error::e_explain(
                ErrorType::InternalError,
                "HTTP/3 response headers were already sent on this stream",
            );
        }

        self.send
            .send(OutboundFrame::Headers(
                response_to_h3_headers(response),
                None,
            ))
            .await
            .map_err(|_| {
                Error::explain(
                    ErrorType::ConnectionClosed,
                    "HTTP/3 stream closed before response headers could be sent",
                )
            })?;
        self.headers_sent = true;
        Ok(())
    }

    /// Write response body bytes to the downstream stream.
    pub async fn send_body(&mut self, body: &[u8], fin: bool) -> Result<()> {
        self.send
            .send(OutboundFrame::body(BufFactory::buf_from_slice(body), fin))
            .await
            .map_err(|_| {
                Error::explain(
                    ErrorType::ConnectionClosed,
                    "HTTP/3 stream closed before response body could be sent",
                )
            })
    }

    /// Write response trailers to the downstream stream.
    pub async fn send_trailers(
        &mut self,
        trailers: &[(impl AsRef<str>, impl AsRef<str>)],
    ) -> Result<()> {
        let trailers = trailers
            .iter()
            .map(|(name, value)| H3Header::new(name.as_ref().as_bytes(), value.as_ref().as_bytes()))
            .collect();
        self.send
            .send(OutboundFrame::Trailers(trailers, None))
            .await
            .map_err(|_| {
                Error::explain(
                    ErrorType::ConnectionClosed,
                    "HTTP/3 stream closed before response trailers could be sent",
                )
            })
    }
}

/// Buffered body chunk to send to an upstream HTTP/3 origin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Http3UpstreamBodyChunk {
    /// Request body bytes for this chunk.
    pub data: Vec<u8>,
    /// Whether this chunk finishes the request body stream.
    pub fin: bool,
}

/// Upstream HTTP/3 request prepared for real origin execution.
#[derive(Debug)]
pub struct Http3UpstreamRequest {
    /// Target request header to send upstream.
    pub request_header: RequestHeader,
    /// Buffered request body chunks to send after the headers.
    pub body: Vec<Http3UpstreamBodyChunk>,
    /// Whether this request is safe to retry after transport failure.
    pub replay_safe: bool,
}

impl Http3UpstreamRequest {
    /// Create an upstream HTTP/3 request without a body.
    pub fn new(request_header: RequestHeader) -> Self {
        Self {
            request_header,
            body: Vec::new(),
            replay_safe: false,
        }
    }

    /// Attach body chunks to this request.
    pub fn with_body(mut self, body: Vec<Http3UpstreamBodyChunk>) -> Self {
        self.body = body;
        self
    }

    /// Mark whether the request is safe to replay on retry.
    pub fn with_replay_safe(mut self, replay_safe: bool) -> Self {
        self.replay_safe = replay_safe;
        self
    }
}

/// Real upstream HTTP/3 response returned by an origin.
#[derive(Debug)]
pub struct Http3UpstreamResponse {
    /// Response headers mapped into Pingora's response type.
    pub response_header: ResponseHeader,
    /// Buffered response body chunks received from the origin.
    pub body: Vec<Http3BodyChunk>,
    /// Whether response trailers are surfaced by the current boundary.
    pub response_trailers_supported: bool,
}

/// Result of executing an upstream HTTP/3 request.
#[derive(Debug)]
pub enum Http3UpstreamOutcome {
    /// The upstream request completed successfully.
    Response(Box<Http3UpstreamResponse>),
    /// Retry, failover, or fallback should be applied by the caller.
    Retry(Http3RetryDecision),
}

/// Real upstream HTTP/3 executor backed by pooled H3-over-QUIC sessions.
#[derive(Default)]
pub struct Http3UpstreamExecutor {
    pool: Http3UpstreamPool,
    next_request_id: AtomicU64,
}

impl Http3UpstreamExecutor {
    /// Create a new executor with an empty upstream H3 session pool.
    pub fn new() -> Self {
        Self {
            pool: Http3UpstreamPool::new(),
            next_request_id: AtomicU64::new(1),
        }
    }

    /// Return pool lifecycle counters for the upstream H3 session pool.
    pub fn pool_stats(&self) -> QuicUpstreamPoolStats {
        self.pool.stats()
    }

    /// Map transport selection and peers into a concrete upstream target.
    pub fn select_upstream(
        &self,
        transport: HttpUpstreamTransport,
        stream_peer: Box<HttpPeer>,
        http3_peer: Option<Box<Http3Peer>>,
    ) -> Result<SelectedHttpUpstream> {
        match transport {
            HttpUpstreamTransport::Http1 | HttpUpstreamTransport::Http2 => {
                Ok(SelectedHttpUpstream::Stream(stream_peer))
            }
            HttpUpstreamTransport::Http3 => Ok(SelectedHttpUpstream::Http3(
                http3_peer.ok_or_else(|| {
                    Error::explain(
                        ErrorType::InternalError,
                        "HTTP/3 upstream transport was selected without an Http3Peer",
                    )
                })?,
            )),
        }
    }

    /// Build a real QUIC connector config from an [`Http3Peer`].
    pub fn connector_config(&self, peer: &Http3Peer) -> QuicConnectorConfig {
        let mut config = QuicConnectorConfig::new(peer.authority.clone(), peer.address().clone())
            .with_server_name(Some(peer.authority.clone()))
            .with_alpn_protocols(peer.alpn.clone());

        if let Some(local_bind_addr) = peer.local_bind_addr().cloned() {
            config = config.with_local_bind_addr(Some(local_bind_addr));
        }
        if let Some(connect_timeout) = peer.connect_timeout() {
            config = config.with_connect_timeout(connect_timeout);
        }
        if let Some(idle_timeout) = peer.idle_timeout() {
            config = config.with_idle_timeout(idle_timeout);
        }

        config
    }

    /// Execute an upstream HTTP/3 request against the given origin.
    pub async fn execute(
        &self,
        peer: &Http3Peer,
        request: Http3UpstreamRequest,
        classifier: &Http3RetryClassifier,
    ) -> Result<Http3UpstreamOutcome> {
        let connector = self.connector_config(peer);
        let (session, _reused) = match self.pool.checkout(&connector).await {
            Ok(result) => result,
            Err(error) => {
                return Ok(Http3UpstreamOutcome::Retry(
                    classifier.classify_connect_error(&error),
                ))
            }
        };

        match self.execute_on_session(&session, peer, &request).await {
            Ok(response) => {
                self.pool.release(session);
                Ok(Http3UpstreamOutcome::Response(Box::new(response)))
            }
            Err(error) => Ok(Http3UpstreamOutcome::Retry(
                classifier.classify_proxy_error(
                    &error,
                    Http3RetryContext {
                        request_can_retry: request.replay_safe,
                    },
                ),
            )),
        }
    }

    async fn execute_on_session(
        &self,
        session: &Http3UpstreamSession,
        peer: &Http3Peer,
        request: &Http3UpstreamRequest,
    ) -> Result<Http3UpstreamResponse> {
        let request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        let headers = request_header_to_h3_headers(&request.request_header, peer)?;
        let has_body = !request.body.is_empty();
        let (body_writer_tx, body_writer_rx) = oneshot::channel();
        session
            .send_request(NewClientRequest {
                request_id,
                headers,
                body_writer: has_body.then_some(body_writer_tx),
            })
            .await?;

        if has_body {
            let mut body_writer = body_writer_rx.await.map_err(|_| {
                Error::explain(
                    ErrorType::ConnectionClosed,
                    "HTTP/3 upstream body writer dropped before request body could be sent",
                )
            })?;
            for chunk in &request.body {
                body_writer
                    .send(OutboundFrame::body(
                        BufFactory::buf_from_slice(&chunk.data),
                        chunk.fin,
                    ))
                    .await
                    .map_err(|_| {
                        Error::explain(
                            ErrorType::WriteError,
                            "HTTP/3 upstream body writer closed while sending request body",
                        )
                    })?;
            }
        }

        let response = self.recv_response(session, request_id).await?;
        Ok(response)
    }

    async fn recv_response(
        &self,
        session: &Http3UpstreamSession,
        request_id: u64,
    ) -> Result<Http3UpstreamResponse> {
        let mut saw_request_open = false;
        loop {
            let Some(event) = session.recv_event().await else {
                return Error::e_explain(
                    ErrorType::ConnectionClosed,
                    "HTTP/3 upstream controller closed before a response was received",
                );
            };

            match event {
                ClientH3Event::NewOutboundRequest {
                    request_id: seen_request_id,
                    ..
                } if seen_request_id == request_id => {
                    saw_request_open = true;
                }
                ClientH3Event::Core(H3Event::IncomingHeaders(headers)) => {
                    let response_header = response_header_from_h3_headers(&headers.headers)?;
                    let body = read_http3_body(headers.recv, headers.read_fin).await?;
                    return Ok(Http3UpstreamResponse {
                        response_header,
                        body,
                        response_trailers_supported: false,
                    });
                }
                ClientH3Event::Core(H3Event::ConnectionError(error)) => {
                    return Err(Error::because(
                        ErrorType::ConnectionClosed,
                        "HTTP/3 upstream connection errored before response completion",
                        error,
                    ));
                }
                ClientH3Event::Core(H3Event::ConnectionShutdown(_)) => {
                    return Error::e_explain(
                        ErrorType::ConnectionClosed,
                        if saw_request_open {
                            "HTTP/3 upstream connection shut down before response completion"
                        } else {
                            "HTTP/3 upstream connection shut down before the request stream opened"
                        },
                    );
                }
                ClientH3Event::Core(H3Event::ResetStream { .. })
                | ClientH3Event::Core(H3Event::StreamClosed { .. }) => {
                    return Error::e_explain(
                        ErrorType::ConnectionClosed,
                        "HTTP/3 upstream stream closed before a complete response was received",
                    );
                }
                _ => {}
            }
        }
    }
}

/// Execute a buffered upstream request against an HTTP/1.x or HTTP/2 origin.
#[cfg_attr(not(test), allow(dead_code))]
pub async fn execute_stream_upstream<C>(
    connector: &Connector<C>,
    peer: &HttpPeer,
    mut request: Http3UpstreamRequest,
) -> Result<Http3UpstreamResponse>
where
    C: custom::Connector,
{
    request.request_header.version = match peer.get_alpn() {
        Some(alpn) if alpn.get_min_http_version() >= 2 => Version::HTTP_2,
        _ => Version::HTTP_11,
    };
    if request.request_header.version == Version::HTTP_11
        && !request.body.is_empty()
        && request
            .request_header
            .headers
            .get(header::CONTENT_LENGTH)
            .is_none()
        && request
            .request_header
            .headers
            .get(header::TRANSFER_ENCODING)
            .is_none()
    {
        let body_len: usize = request.body.iter().map(|chunk| chunk.data.len()).sum();
        request
            .request_header
            .insert_header(header::CONTENT_LENGTH, body_len.to_string())?;
    }

    let (mut session, _reused) = connector.get_http_session(peer).await?;
    let response = async {
        session
            .write_request_header(Box::new(request.request_header))
            .await?;

        if request.body.is_empty() {
            session.finish_request_body().await?;
        } else {
            let mut finished = false;
            for chunk in request.body {
                finished |= chunk.fin;
                session
                    .write_request_body(bytes::Bytes::from(chunk.data), chunk.fin)
                    .await?;
            }
            if !finished {
                session.finish_request_body().await?;
            }
        }

        session.read_response_header().await?;
        let response_header = session.response_header().cloned().ok_or_else(|| {
            Error::explain(
                ErrorType::InvalidHTTPHeader,
                "stream upstream did not provide a response header",
            )
        })?;

        let mut body = Vec::new();
        while let Some(chunk) = session.read_response_body().await? {
            let fin = session.response_done();
            body.push(Http3BodyChunk {
                data: chunk.to_vec(),
                fin,
            });
            if fin {
                break;
            }
        }

        Ok(Http3UpstreamResponse {
            response_header,
            body,
            response_trailers_supported: false,
        })
    }
    .await;

    match response {
        Ok(response) => {
            if session.response_done() {
                connector
                    .release_http_session(session, peer, peer.idle_timeout())
                    .await;
            } else {
                session.shutdown().await;
            }
            Ok(response)
        }
        Err(error) => {
            session.shutdown().await;
            Err(error)
        }
    }
}

/// Write a buffered upstream response onto a downstream HTTP/3 stream.
#[cfg_attr(not(test), allow(dead_code))]
pub async fn write_downstream_http3_response(
    writer: &mut Http3ResponseWriter,
    response: &Http3UpstreamResponse,
) -> Result<()> {
    writer
        .send_response_header(&response.response_header)
        .await?;

    if response.body.is_empty() {
        writer.send_body(&[], true).await?;
        return Ok(());
    }

    if !response.response_trailers_supported {
        let total_len: usize = response.body.iter().map(|chunk| chunk.data.len()).sum();
        let mut body = Vec::with_capacity(total_len);
        for chunk in &response.body {
            body.extend_from_slice(&chunk.data);
        }
        writer.send_body(&body, true).await?;
        return Ok(());
    }

    let has_terminal_chunk = response.body.iter().any(|chunk| chunk.fin);
    for (idx, chunk) in response.body.iter().enumerate() {
        let is_last = idx + 1 == response.body.len();
        let fin = chunk.fin || (is_last && !has_terminal_chunk);
        writer.send_body(&chunk.data, fin).await?;
    }

    Ok(())
}

/// Process one downstream HTTP/3 request through the standard `HttpProxy` request lifecycle.
#[allow(dead_code)]
pub async fn proxy_downstream_http3_request<SV, C>(
    proxy: std::sync::Arc<HttpProxy<SV, C>>,
    mut request: Http3DownstreamRequest,
    shutdown: &ShutdownWatch,
) -> Result<()>
where
    SV: ProxyHttp + Send + Sync + 'static,
    SV::CTX: Send + Sync,
    C: custom::Connector,
{
    let mut writer = request.response_writer.take().ok_or_else(|| {
        Error::explain(
            ErrorType::InternalError,
            "downstream HTTP/3 request is missing a response writer",
        )
    })?;
    let (subrequest, mut handle) =
        SubrequestSession::new_from_request_header(request.request_header.clone());
    let mut body_reader = request.body_reader.take();
    let body_tx = handle.tx.clone();
    let mut wants_body = handle.subreq_wants_body;
    let proxy_error_rx = handle.subreq_proxy_error;
    let proxy_task = {
        let proxy = proxy.clone();
        let shutdown = shutdown.clone();
        tokio::spawn(async move {
            let session = ServerSession::new_subrequest(subrequest);
            let _ = proxy.process_new_http(session, &shutdown).await;
        })
    };
    let body_task = tokio::spawn(async move {
        let wants_body = (&mut wants_body).await;
        if wants_body.is_err() {
            return Ok(());
        }
        if let Some(reader) = body_reader.as_mut() {
            while let Some(chunk) = reader.recv_chunk().await? {
                let fin = chunk.fin;
                body_tx
                    .send(HttpTask::Body(Some(Bytes::from(chunk.data)), fin))
                    .await
                    .map_err(|error| {
                        Error::because(
                            ErrorType::WriteError,
                            "feeding HTTP/3 request body into subrequest session",
                            error,
                        )
                    })?;
                if fin {
                    return Ok(());
                }
            }
            body_tx.send(HttpTask::Done).await.map_err(|error| {
                Error::because(
                    ErrorType::WriteError,
                    "closing HTTP/3 request body stream for subrequest session",
                    error,
                )
            })?;
        }
        Ok::<(), Box<Error>>(())
    });

    let mut response_finished = false;
    while let Some(task) = handle.rx.recv().await {
        match task {
            HttpTask::Header(header, end_stream) => {
                writer.send_response_header(&header).await?;
                if end_stream {
                    writer.send_body(&[], true).await?;
                    response_finished = true;
                    break;
                }
            }
            HttpTask::Body(data, end_stream) | HttpTask::UpgradedBody(data, end_stream) => {
                let body = data.unwrap_or_default();
                writer.send_body(&body, end_stream).await?;
                if end_stream {
                    response_finished = true;
                    break;
                }
            }
            HttpTask::Trailer(Some(trailers)) => {
                let trailers = trailers
                    .iter()
                    .map(|(name, value)| {
                        (
                            name.as_str().to_string(),
                            String::from_utf8_lossy(value.as_bytes()).into_owned(),
                        )
                    })
                    .collect::<Vec<_>>();
                writer.send_trailers(&trailers).await?;
                response_finished = true;
                break;
            }
            HttpTask::Trailer(None) => {
                response_finished = true;
                break;
            }
            HttpTask::Done => {
                if !response_finished {
                    writer.send_body(&[], true).await?;
                }
                response_finished = true;
                break;
            }
            HttpTask::Failed(error) => return Err(error),
        }
    }

    body_task.await.map_err(|error| {
        Error::because(
            ErrorType::InternalError,
            "joining HTTP/3 subrequest body task",
            error,
        )
    })??;
    proxy_task.await.map_err(|error| {
        Error::because(ErrorType::InternalError, "joining HTTP/3 proxy task", error)
    })?;

    if !response_finished {
        if let Ok(error) = proxy_error_rx.await {
            return Err(error);
        }
        return Error::e_explain(
            ErrorType::ConnectionClosed,
            "HTTP/3 proxy lifecycle ended without producing a downstream response",
        );
    }

    Ok(())
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
            body_reader: None,
            response_writer: None,
            request_trailers_supported: false,
        })
    }

    /// Map real downstream HTTP/3 headers into a Pingora request.
    pub fn accept_incoming_headers(
        &self,
        transport: QuicDownstreamSession,
        incoming_headers: IncomingH3Headers,
    ) -> Result<Http3DownstreamRequest> {
        let request_header = self.build_request_from_h3_headers(&incoming_headers.headers)?;
        let stream_id = incoming_headers.stream_id;
        let flow_key = transport.flow_key.clone();

        let mut sessions = self.sessions.lock().expect("http3 bridge mutex poisoned");
        let session = sessions
            .entry(flow_key)
            .or_insert_with(|| Http3DownstreamSession {
                transport: transport.clone(),
                requests_seen: 0,
            });

        session.transport = transport;
        session.requests_seen += 1;
        self.stats.accepted_streams.fetch_add(1, Ordering::Relaxed);

        Ok(Http3DownstreamRequest {
            session: session.clone(),
            stream_id,
            request_header,
            body_reader: Some(Http3BodyReader::new(incoming_headers.recv)),
            response_writer: Some(Http3ResponseWriter::new(incoming_headers.send)),
            request_trailers_supported: false,
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

    fn build_request_from_h3_headers(&self, headers: &[H3Header]) -> Result<RequestHeader> {
        let mut method = None;
        let mut path = None;
        let mut authority = None;
        let mut regular_headers: Vec<(String, Vec<u8>)> = Vec::new();

        for header in headers {
            let name = std::str::from_utf8(header.name()).map_err(|_| {
                Error::explain(
                    ErrorType::InvalidHTTPHeader,
                    "HTTP/3 request header name is not valid UTF-8",
                )
            })?;
            let value = header.value();

            match name {
                ":method" => {
                    method = Some(Method::from_bytes(value).map_err(|_| {
                        Error::explain(
                            ErrorType::InvalidHTTPHeader,
                            "HTTP/3 :method pseudo-header is invalid",
                        )
                    })?);
                }
                ":path" => path = Some(value.to_vec()),
                ":authority" => {
                    authority = Some(std::str::from_utf8(value).map_err(|_| {
                        Error::explain(
                            ErrorType::InvalidHTTPHeader,
                            "HTTP/3 :authority pseudo-header is not valid UTF-8",
                        )
                    })?);
                }
                ":scheme" => {}
                _ if name.starts_with(':') => {
                    return Error::e_explain(
                        ErrorType::InvalidHTTPHeader,
                        format!("unsupported HTTP/3 pseudo-header {name}"),
                    );
                }
                _ => regular_headers.push((name.to_string(), value.to_vec())),
            }
        }

        let method = method.ok_or_else(|| {
            Error::explain(
                ErrorType::InvalidHTTPHeader,
                "HTTP/3 request is missing :method pseudo-header",
            )
        })?;
        let path = path.ok_or_else(|| {
            Error::explain(
                ErrorType::InvalidHTTPHeader,
                "HTTP/3 request is missing :path pseudo-header",
            )
        })?;
        let mut request = RequestHeader::build(method, &path, Some(regular_headers.len() + 1))?;
        request.version = Version::HTTP_3;

        if let Some(authority) = authority {
            request.insert_header(HOST, authority)?;
        }

        for (name, value) in regular_headers {
            if name.eq_ignore_ascii_case(CONNECTION.as_str())
                || name.eq_ignore_ascii_case(UPGRADE.as_str())
            {
                return Error::e_explain(
                    ErrorType::InvalidHTTPHeader,
                    "HTTP/3 downstream bridge does not support Connection or Upgrade headers",
                );
            }
            request.append_header(
                name,
                http::HeaderValue::from_bytes(&value).map_err(|_| {
                    Error::explain(
                        ErrorType::InvalidHTTPHeader,
                        "HTTP/3 request header value is invalid",
                    )
                })?,
            )?;
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

fn response_to_h3_headers(response: &ResponseHeader) -> Vec<H3Header> {
    let mut headers = Vec::with_capacity(response.headers.len() + 1);
    headers.push(H3Header::new(
        b":status",
        response.status.as_str().as_bytes(),
    ));
    for (name, value) in &response.headers {
        headers.push(H3Header::new(name.as_str().as_bytes(), value.as_bytes()));
    }
    headers
}

fn request_header_to_h3_headers(
    request: &RequestHeader,
    peer: &Http3Peer,
) -> Result<Vec<H3Header>> {
    let path = request
        .uri
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| "/".to_string());
    let authority = request
        .headers
        .get(HOST)
        .and_then(|value| value.to_str().ok())
        .unwrap_or(peer.authority());
    let mut headers = vec![
        H3Header::new(b":method", request.method.as_str().as_bytes()),
        H3Header::new(b":scheme", b"https"),
        H3Header::new(b":authority", authority.as_bytes()),
        H3Header::new(b":path", path.as_bytes()),
    ];

    for (name, value) in &request.headers {
        if name == HOST || name == CONNECTION || name == UPGRADE {
            continue;
        }
        headers.push(H3Header::new(name.as_str().as_bytes(), value.as_bytes()));
    }

    Ok(headers)
}

fn response_header_from_h3_headers(headers: &[H3Header]) -> Result<ResponseHeader> {
    let mut status = None;
    let mut regular_headers: Vec<(String, Vec<u8>)> = Vec::new();

    for header in headers {
        let name = std::str::from_utf8(header.name()).map_err(|_| {
            Error::explain(
                ErrorType::InvalidHTTPHeader,
                "HTTP/3 response header name is not valid UTF-8",
            )
        })?;
        match name {
            ":status" => {
                let code = std::str::from_utf8(header.value()).map_err(|_| {
                    Error::explain(
                        ErrorType::InvalidHTTPHeader,
                        "HTTP/3 response :status is not valid UTF-8",
                    )
                })?;
                status = Some(code.parse::<u16>().map_err(|_| {
                    Error::explain(
                        ErrorType::InvalidHTTPHeader,
                        "HTTP/3 response :status pseudo-header is invalid",
                    )
                })?);
            }
            _ if name.starts_with(':') => {
                return Error::e_explain(
                    ErrorType::InvalidHTTPHeader,
                    format!("unsupported HTTP/3 response pseudo-header {name}"),
                );
            }
            _ => regular_headers.push((name.to_string(), header.value().to_vec())),
        }
    }

    let mut response = ResponseHeader::build(
        status.ok_or_else(|| {
            Error::explain(
                ErrorType::InvalidHTTPHeader,
                "HTTP/3 response is missing :status pseudo-header",
            )
        })?,
        Some(regular_headers.len()),
    )?;
    response.version = Version::HTTP_3;
    for (name, value) in regular_headers {
        response.append_header(
            name,
            http::HeaderValue::from_bytes(&value).map_err(|_| {
                Error::explain(
                    ErrorType::InvalidHTTPHeader,
                    "HTTP/3 response header value is invalid",
                )
            })?,
        )?;
    }
    Ok(response)
}

async fn read_http3_body(
    mut recv: InboundFrameStream,
    read_fin: bool,
) -> Result<Vec<Http3BodyChunk>> {
    if read_fin {
        return Ok(Vec::new());
    }

    let mut body = Vec::new();
    while let Some(frame) = recv.recv().await {
        match frame {
            InboundFrame::Body(buf, fin) => {
                body.push(Http3BodyChunk {
                    data: buf.into_inner().into_vec(),
                    fin,
                });
                if fin {
                    break;
                }
            }
            InboundFrame::Datagram(_) => {
                return Error::e_explain(
                    ErrorType::InternalError,
                    "HTTP/3 upstream response path does not accept DATAGRAM frames",
                )
            }
        }
    }

    Ok(body)
}

#[cfg(test)]
mod tests {
    use super::{
        execute_stream_upstream, write_downstream_http3_response, Http3AcceptedStream,
        Http3PhaseCompatibility, Http3ProxyBridge, Http3RetryAction, Http3RetryClassifier,
        Http3RetryContext, Http3RetryPolicy, Http3UpstreamBodyChunk, Http3UpstreamExecutor,
        Http3UpstreamOutcome, Http3UpstreamRequest, SelectedHttpUpstream,
    };
    use futures::{SinkExt, StreamExt};
    use http::header::HOST;
    use http::Version;
    use hyper::service::{make_service_fn, service_fn};
    use hyper::{Body, Response, Server};
    use pingora_core::connectors::http::Connector;
    use pingora_core::protocols::l4::datagram::{Datagram, DatagramFlowKey, DatagramMeta};
    use pingora_core::protocols::l4::socket::SocketAddr;
    use pingora_core::upstreams::peer::{Http3Peer, HttpPeer, HttpUpstreamTransport};
    use pingora_error::ErrorType;
    use pingora_http::{Method, RequestHeader, ResponseHeader};
    use pingora_quic::{
        QuicConnectionMeta, QuicDownstreamSession, QuicIncomingDatagram, QuicSessionEvent,
    };
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Instant;
    use tokio::sync::mpsc;
    use tokio_quiche::buf_factory::BufFactory;
    use tokio_quiche::http3::driver::{H3Event, ServerH3Event};
    use tokio_quiche::http3::driver::{InboundFrame, IncomingH3Headers, OutboundFrame};
    use tokio_quiche::http3::settings::Http3Settings;
    use tokio_quiche::http3::H3AuditStats;
    use tokio_quiche::metrics::DefaultMetrics;
    use tokio_quiche::quiche::h3::{Header as H3Header, NameValue};
    use tokio_quiche::settings::{
        CertificateKind, ConnectionParams, Hooks, QuicSettings, TlsCertificatePaths,
    };
    use tokio_util::sync::PollSender;

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
                    stream_handle: None,
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

    fn downstream_session() -> QuicDownstreamSession {
        let local_addr = SocketAddr::Inet("127.0.0.1:4433".parse().unwrap());
        let peer_addr = SocketAddr::Inet("127.0.0.1:50000".parse().unwrap());
        QuicDownstreamSession {
            flow_key: DatagramFlowKey {
                listener_id: Arc::<str>::from("h3"),
                local_addr: local_addr.clone(),
                peer_addr: peer_addr.clone(),
            },
            meta: QuicConnectionMeta {
                local_addr,
                peer_addr,
                alpn_protocol: Some(b"h3".to_vec()),
                server_name: Some("example.com".to_string()),
                resumed: false,
            },
            established_at: Instant::now(),
            last_seen: Instant::now(),
            packets_received: 1,
            stream_handle: None,
        }
    }

    fn upstream_peer(port: u16) -> Http3Peer {
        let mut peer = Http3Peer::new(("127.0.0.1", port), "localhost".to_string());
        peer.options = peer
            .options
            .clone()
            .with_connect_timeout(Some(std::time::Duration::from_secs(2)))
            .with_idle_timeout(Some(std::time::Duration::from_secs(30)));
        peer
    }

    fn upstream_request(method: Method, path: &[u8]) -> Http3UpstreamRequest {
        let mut request = RequestHeader::build(method, path, Some(2)).unwrap();
        request.insert_header(HOST, "localhost").unwrap();
        Http3UpstreamRequest::new(request)
    }

    fn quic_test_cert_paths() -> (String, String) {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../pingora-core/tests/certs");
        (
            root.join("server.crt").display().to_string(),
            root.join("server.key").display().to_string(),
        )
    }

    async fn spawn_http3_origin() -> std::io::Result<std::net::SocketAddr> {
        let (cert_path, key_path) = quic_test_cert_paths();
        let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await?;
        let addr = socket.local_addr()?;
        let mut settings = QuicSettings::default();
        settings.alpn = vec![b"h3".to_vec()];
        settings.verify_peer = false;
        let params = ConnectionParams::new_server(
            settings,
            TlsCertificatePaths {
                cert: &cert_path,
                private_key: &key_path,
                kind: CertificateKind::X509,
            },
            Hooks::default(),
        );
        let mut listeners = tokio_quiche::listen([socket], params, DefaultMetrics)
            .map_err(|error| std::io::Error::other(error.to_string()))?;

        tokio::spawn(async move {
            let accept_stream = &mut listeners[0];
            while let Some(result) = accept_stream.next().await {
                let Ok(connection) = result else {
                    continue;
                };
                let (driver, mut controller) =
                    tokio_quiche::ServerH3Driver::new(Http3Settings::default());
                connection.start(driver);
                tokio::spawn(async move {
                    while let Some(event) = controller.event_receiver_mut().recv().await {
                        match event {
                            ServerH3Event::Headers {
                                incoming_headers, ..
                            } => {
                                let IncomingH3Headers {
                                    headers,
                                    mut send,
                                    mut recv,
                                    ..
                                } = incoming_headers;
                                let path = headers
                                    .iter()
                                    .find(|header| header.name() == b":path")
                                    .map(|header| {
                                        String::from_utf8_lossy(header.value()).into_owned()
                                    })
                                    .unwrap_or_else(|| "/".to_string());
                                let mut request_body = Vec::new();
                                while let Some(frame) = recv.recv().await {
                                    match frame {
                                        InboundFrame::Body(buf, fin) => {
                                            request_body.extend_from_slice(&buf);
                                            if fin {
                                                break;
                                            }
                                        }
                                        InboundFrame::Datagram(_) => {}
                                    }
                                }
                                let mut response_body = format!("{}|", path).into_bytes();
                                response_body.extend_from_slice(&request_body);
                                send.send(OutboundFrame::Headers(
                                    vec![
                                        H3Header::new(b":status", b"200"),
                                        H3Header::new(b"x-origin", b"tokio-quiche"),
                                    ],
                                    None,
                                ))
                                .await
                                .unwrap();
                                send.send(OutboundFrame::body(
                                    BufFactory::buf_from_slice(&response_body),
                                    true,
                                ))
                                .await
                                .unwrap();
                            }
                            ServerH3Event::Core(H3Event::ConnectionShutdown(_)) => break,
                            _ => {}
                        }
                    }
                });
            }
        });

        Ok(addr)
    }

    async fn spawn_http1_origin() -> std::io::Result<std::net::SocketAddr> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let addr = listener.local_addr()?;
        listener.set_nonblocking(true)?;

        let make_service = make_service_fn(|_| async {
            Ok::<_, std::convert::Infallible>(service_fn(
                |request: hyper::Request<Body>| async move {
                    let path = request.uri().path().to_string();
                    let body = hyper::body::to_bytes(request.into_body()).await.unwrap();
                    let mut response_body = format!("{path}|").into_bytes();
                    response_body.extend_from_slice(&body);
                    Ok::<_, std::convert::Infallible>(
                        Response::builder()
                            .status(200)
                            .header("x-origin", "http1")
                            .body(Body::from(response_body))
                            .unwrap(),
                    )
                },
            ))
        });

        tokio::spawn(async move {
            let _ = Server::from_tcp(listener)
                .unwrap()
                .http1_only(true)
                .serve(make_service)
                .await;
        });

        Ok(addr)
    }

    async fn spawn_http2_origin() -> std::io::Result<std::net::SocketAddr> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        tokio::spawn(async move {
            loop {
                let Ok((tcp, _addr)) = listener.accept().await else {
                    break;
                };
                tokio::spawn(async move {
                    let mut connection = match h2::server::handshake(tcp).await {
                        Ok(connection) => connection,
                        Err(_) => return,
                    };

                    while let Some(result) = connection.accept().await {
                        let Ok((request, mut respond)) = result else {
                            return;
                        };
                        tokio::spawn(async move {
                            let path = request.uri().path().to_string();
                            let mut body = request.into_body();
                            let mut request_body = Vec::new();
                            while let Some(chunk) = body.data().await {
                                let Ok(chunk) = chunk else {
                                    return;
                                };
                                request_body.extend_from_slice(&chunk);
                            }
                            let mut response_body = format!("{path}|").into_bytes();
                            response_body.extend_from_slice(&request_body);
                            let response = http::Response::builder()
                                .status(200)
                                .header("content-length", response_body.len())
                                .header("x-origin", "http2")
                                .body(())
                                .unwrap();
                            let Ok(mut send_stream) = respond.send_response(response, false) else {
                                return;
                            };
                            let _ = send_stream.send_data(bytes::Bytes::from(response_body), true);
                        });
                    }
                });
            }
        });

        Ok(addr)
    }

    async fn collect_http3_request_body(
        request: &mut super::Http3DownstreamRequest,
    ) -> Vec<Http3UpstreamBodyChunk> {
        let mut body = Vec::new();
        if let Some(reader) = request.body_reader.as_mut() {
            while let Some(chunk) = reader.recv_chunk().await.unwrap() {
                let fin = chunk.fin;
                body.push(Http3UpstreamBodyChunk {
                    data: chunk.data,
                    fin,
                });
                if fin {
                    break;
                }
            }
        }
        body
    }

    async fn collect_response_frames(
        response_rx: &mut mpsc::Receiver<OutboundFrame>,
    ) -> Vec<OutboundFrame> {
        let mut frames = Vec::new();
        while let Some(frame) = response_rx.recv().await {
            let terminal = matches!(
                frame,
                OutboundFrame::Body(_, true) | OutboundFrame::Trailers(_, _)
            );
            frames.push(frame);
            if terminal {
                break;
            }
        }
        frames
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

    #[tokio::test]
    async fn bridge_maps_real_http3_headers_and_reads_request_body() {
        let bridge = Http3ProxyBridge::new();
        let (body_tx, body_rx) = mpsc::channel(4);
        let (response_tx, _response_rx) = mpsc::channel(4);
        body_tx
            .send(InboundFrame::Body(
                BufFactory::buf_from_slice(b"ping"),
                false,
            ))
            .await
            .unwrap();
        body_tx
            .send(InboundFrame::Body(
                BufFactory::buf_from_slice(b"pong"),
                true,
            ))
            .await
            .unwrap();
        drop(body_tx);

        let incoming_headers = IncomingH3Headers {
            stream_id: 4,
            headers: vec![
                H3Header::new(b":method", b"POST"),
                H3Header::new(b":path", b"/submit"),
                H3Header::new(b":authority", b"example.com"),
                H3Header::new(b"x-test", b"1"),
            ],
            send: PollSender::new(response_tx),
            recv: body_rx,
            read_fin: false,
            h3_audit_stats: Arc::new(H3AuditStats::new(4)),
        };

        let mut request = bridge
            .accept_incoming_headers(downstream_session(), incoming_headers)
            .unwrap();

        assert_eq!(request.stream_id, 4);
        assert_eq!(request.request_header.version, Version::HTTP_3);
        assert_eq!(request.request_header.method, Method::POST);
        assert_eq!(request.request_header.uri.path(), "/submit");
        assert_eq!(
            request
                .request_header
                .headers
                .get(http::header::HOST)
                .unwrap(),
            "example.com"
        );
        assert!(!request.request_trailers_supported);

        let mut body = request.body_reader.take().unwrap();
        let first = body.recv_chunk().await.unwrap().unwrap();
        assert_eq!(first.data, b"ping");
        assert!(!first.fin);
        let second = body.recv_chunk().await.unwrap().unwrap();
        assert_eq!(second.data, b"pong");
        assert!(second.fin);
        assert!(body.recv_chunk().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn bridge_writes_real_http3_response_frames() {
        let bridge = Http3ProxyBridge::new();
        let (_body_tx, body_rx) = mpsc::channel(1);
        let (response_tx, mut response_rx) = mpsc::channel(4);
        let incoming_headers = IncomingH3Headers {
            stream_id: 8,
            headers: vec![
                H3Header::new(b":method", b"GET"),
                H3Header::new(b":path", b"/health"),
            ],
            send: PollSender::new(response_tx),
            recv: body_rx,
            read_fin: true,
            h3_audit_stats: Arc::new(H3AuditStats::new(8)),
        };

        let mut request = bridge
            .accept_incoming_headers(downstream_session(), incoming_headers)
            .unwrap();
        let mut writer = request.response_writer.take().unwrap();
        let mut response = ResponseHeader::build(200, None).unwrap();
        response.insert_header("x-served-by", "pingora").unwrap();

        writer.send_response_header(&response).await.unwrap();
        writer.send_body(b"ok", false).await.unwrap();
        writer
            .send_trailers(&[("x-finished", "true")])
            .await
            .unwrap();

        let headers = response_rx.recv().await.unwrap();
        match headers {
            OutboundFrame::Headers(headers, _) => {
                assert!(headers
                    .iter()
                    .any(|header| header.name() == b":status" && header.value() == b"200"));
                assert!(headers
                    .iter()
                    .any(|header| header.name() == b"x-served-by" && header.value() == b"pingora"));
            }
            other => panic!("unexpected frame: {other:?}"),
        }

        let body = response_rx.recv().await.unwrap();
        match body {
            OutboundFrame::Body(buf, fin) => {
                assert_eq!(&buf[..], b"ok");
                assert!(!fin);
            }
            other => panic!("unexpected frame: {other:?}"),
        }

        let trailers = response_rx.recv().await.unwrap();
        match trailers {
            OutboundFrame::Trailers(headers, _) => {
                assert_eq!(headers.len(), 1);
                assert_eq!(headers[0].name(), b"x-finished");
                assert_eq!(headers[0].value(), b"true");
            }
            other => panic!("unexpected frame: {other:?}"),
        }
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

    #[test]
    fn selected_upstream_reports_transport_choice() {
        let stream = SelectedHttpUpstream::Stream(Box::new(HttpPeer::new(
            ("127.0.0.1", 8080),
            false,
            "example.com".into(),
        )));
        assert_eq!(stream.transport(), HttpUpstreamTransport::Http1);

        let http3 = SelectedHttpUpstream::Http3(Box::new(Http3Peer::new(
            ("127.0.0.1", 8443),
            "example.com".into(),
        )));
        assert_eq!(http3.transport(), HttpUpstreamTransport::Http3);
    }

    #[test]
    fn http3_connect_failures_prefer_h2_fallback() {
        let classifier = Http3RetryClassifier::default();
        let error = pingora_error::Error::new(ErrorType::ConnectTimedout);

        let decision = classifier.classify_connect_error(&error);
        assert_eq!(
            decision.action,
            Http3RetryAction::Fallback(HttpUpstreamTransport::Http2)
        );
        assert!(decision.mark_retryable);
        assert!(decision.fresh_session_required);
        assert!(decision.allow_backend_remap);
    }

    #[test]
    fn http3_connect_failures_retry_next_peer_without_fallback() {
        let classifier = Http3RetryClassifier::new(Http3RetryPolicy {
            allow_h2_fallback: false,
            allow_h1_fallback: false,
        });
        let error = pingora_error::Error::new(ErrorType::ConnectRefused);

        let decision = classifier.classify_connect_error(&error);
        assert_eq!(decision.action, Http3RetryAction::RetryNextPeer);
        assert!(decision.mark_retryable);
        assert!(decision.allow_backend_remap);
    }

    #[test]
    fn http3_proxy_timeouts_retry_same_peer_only_when_request_is_safe() {
        let classifier = Http3RetryClassifier::default();
        let error = pingora_error::Error::new(ErrorType::ReadTimedout);

        let safe = classifier.classify_proxy_error(
            &error,
            Http3RetryContext {
                request_can_retry: true,
            },
        );
        assert_eq!(safe.action, Http3RetryAction::RetrySamePeer);
        assert!(safe.mark_retryable);
        assert!(safe.fresh_session_required);
        assert!(!safe.allow_backend_remap);

        let unsafe_decision = classifier.classify_proxy_error(
            &error,
            Http3RetryContext {
                request_can_retry: false,
            },
        );
        assert_eq!(unsafe_decision.action, Http3RetryAction::Fail);
        assert!(!unsafe_decision.mark_retryable);
    }

    #[test]
    fn upstream_executor_maps_http3_peer_into_connector_config() {
        let executor = Http3UpstreamExecutor::new();
        let peer = upstream_peer(8443);
        let config = executor.connector_config(&peer);

        assert_eq!(config.peer_addr, *peer.address());
        assert_eq!(config.server_name.as_deref(), Some("localhost"));
        assert_eq!(config.alpn_protocols, vec![b"h3".to_vec()]);
        assert_eq!(config.connect_timeout, std::time::Duration::from_secs(2));
        assert_eq!(config.idle_timeout, std::time::Duration::from_secs(30));
    }

    #[test]
    fn upstream_executor_selects_requested_transport() {
        let executor = Http3UpstreamExecutor::new();
        let stream_peer = Box::new(HttpPeer::new(
            ("127.0.0.1", 8080),
            false,
            "example.com".into(),
        ));
        let http3_peer = Box::new(Http3Peer::new(("127.0.0.1", 8443), "example.com".into()));

        let selected = executor
            .select_upstream(
                HttpUpstreamTransport::Http3,
                stream_peer.clone(),
                Some(http3_peer),
            )
            .unwrap();
        assert_eq!(selected.transport(), HttpUpstreamTransport::Http3);

        let selected = executor
            .select_upstream(HttpUpstreamTransport::Http1, stream_peer, None)
            .unwrap();
        assert_eq!(selected.transport(), HttpUpstreamTransport::Http1);
    }

    #[tokio::test]
    async fn upstream_executor_round_trips_real_http3_requests() {
        let origin_addr = match spawn_http3_origin().await {
            Ok(addr) => addr,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("failed to start HTTP/3 origin: {error}"),
        };
        let executor = Http3UpstreamExecutor::new();
        let peer = upstream_peer(origin_addr.port());
        let request = upstream_request(Method::POST, b"/echo").with_body(vec![
            Http3UpstreamBodyChunk {
                data: b"ping".to_vec(),
                fin: false,
            },
            Http3UpstreamBodyChunk {
                data: b"pong".to_vec(),
                fin: true,
            },
        ]);
        let classifier = Http3RetryClassifier::default();

        let outcome = executor.execute(&peer, request, &classifier).await.unwrap();
        let Http3UpstreamOutcome::Response(response) = outcome else {
            panic!("expected upstream response");
        };
        assert_eq!(response.response_header.status, 200);
        assert_eq!(
            response.response_header.headers.get("x-origin").unwrap(),
            "tokio-quiche"
        );
        let mut body = Vec::new();
        let mut saw_fin = false;
        for chunk in &response.body {
            body.extend_from_slice(&chunk.data);
            saw_fin |= chunk.fin;
        }
        assert_eq!(body, b"/echo|pingpong");
        assert!(saw_fin);
        assert!(!response.response_trailers_supported);
    }

    #[tokio::test]
    async fn upstream_executor_returns_retry_decision_on_connect_failure() {
        let executor = Http3UpstreamExecutor::new();
        let peer = upstream_peer(9);
        let classifier = Http3RetryClassifier::default();
        let request = upstream_request(Method::GET, b"/health").with_replay_safe(true);

        let outcome = executor.execute(&peer, request, &classifier).await.unwrap();
        let Http3UpstreamOutcome::Retry(decision) = outcome else {
            panic!("expected retry decision");
        };
        assert_eq!(
            decision.action,
            Http3RetryAction::Fallback(HttpUpstreamTransport::Http2)
        );
        assert!(decision.mark_retryable);
    }

    #[tokio::test]
    async fn upstream_executor_reuses_pooled_http3_sessions() {
        let origin_addr = match spawn_http3_origin().await {
            Ok(addr) => addr,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("failed to start HTTP/3 origin: {error}"),
        };
        let executor = Http3UpstreamExecutor::new();
        let peer = upstream_peer(origin_addr.port());
        let classifier = Http3RetryClassifier::default();

        let first = executor
            .execute(&peer, upstream_request(Method::GET, b"/one"), &classifier)
            .await
            .unwrap();
        assert!(matches!(first, Http3UpstreamOutcome::Response(_)));
        assert_eq!(executor.pool_stats().released_sessions, 1);

        let second = executor
            .execute(&peer, upstream_request(Method::GET, b"/two"), &classifier)
            .await
            .unwrap();
        let Http3UpstreamOutcome::Response(response) = second else {
            panic!("expected second upstream response");
        };
        assert_eq!(response.body[0].data, b"/two|");
        let stats = executor.pool_stats();
        assert_eq!(stats.reused_sessions, 1);
        assert_eq!(stats.released_sessions, 2);
    }

    #[tokio::test]
    async fn mixed_mode_h3_downstream_to_h1_upstream() {
        let origin_addr = match spawn_http1_origin().await {
            Ok(addr) => addr,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("failed to start HTTP/1 origin: {error}"),
        };
        let bridge = Http3ProxyBridge::new();
        let (body_tx, body_rx) = mpsc::channel(4);
        let (response_tx, mut response_rx) = mpsc::channel(4);
        body_tx
            .send(InboundFrame::Body(
                BufFactory::buf_from_slice(b"ping"),
                false,
            ))
            .await
            .unwrap();
        body_tx
            .send(InboundFrame::Body(
                BufFactory::buf_from_slice(b"pong"),
                true,
            ))
            .await
            .unwrap();
        drop(body_tx);

        let incoming_headers = IncomingH3Headers {
            stream_id: 12,
            headers: vec![
                H3Header::new(b":method", b"POST"),
                H3Header::new(b":path", b"/echo"),
                H3Header::new(b":authority", b"localhost"),
            ],
            send: PollSender::new(response_tx),
            recv: body_rx,
            read_fin: false,
            h3_audit_stats: Arc::new(H3AuditStats::new(12)),
        };

        let mut request = bridge
            .accept_incoming_headers(downstream_session(), incoming_headers)
            .unwrap();
        let body = collect_http3_request_body(&mut request).await;
        let upstream_request =
            Http3UpstreamRequest::new(request.request_header.clone()).with_body(body);
        let mut peer = HttpPeer::new(("127.0.0.1", origin_addr.port()), false, String::new());
        peer.options.set_http_version(1, 1);
        let connector = Connector::new(None);
        let response = execute_stream_upstream(&connector, &peer, upstream_request)
            .await
            .unwrap();

        let mut writer = request.response_writer.take().unwrap();
        write_downstream_http3_response(&mut writer, &response)
            .await
            .unwrap();

        let frames = collect_response_frames(&mut response_rx).await;
        assert!(matches!(frames[0], OutboundFrame::Headers(_, _)));
        match &frames[1] {
            OutboundFrame::Body(buf, fin) => {
                assert_eq!(&buf[..], b"/echo|pingpong");
                assert!(*fin);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mixed_mode_h3_downstream_to_h2_upstream() {
        let origin_addr = match spawn_http2_origin().await {
            Ok(addr) => addr,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("failed to start HTTP/2 origin: {error}"),
        };
        let bridge = Http3ProxyBridge::new();
        let (body_tx, body_rx) = mpsc::channel(4);
        let (response_tx, mut response_rx) = mpsc::channel(4);
        body_tx
            .send(InboundFrame::Body(
                BufFactory::buf_from_slice(b"ping"),
                false,
            ))
            .await
            .unwrap();
        body_tx
            .send(InboundFrame::Body(
                BufFactory::buf_from_slice(b"pong"),
                true,
            ))
            .await
            .unwrap();
        drop(body_tx);

        let incoming_headers = IncomingH3Headers {
            stream_id: 16,
            headers: vec![
                H3Header::new(b":method", b"POST"),
                H3Header::new(b":path", b"/echo"),
                H3Header::new(b":authority", b"localhost"),
            ],
            send: PollSender::new(response_tx),
            recv: body_rx,
            read_fin: false,
            h3_audit_stats: Arc::new(H3AuditStats::new(16)),
        };

        let mut request = bridge
            .accept_incoming_headers(downstream_session(), incoming_headers)
            .unwrap();
        let body = collect_http3_request_body(&mut request).await;
        let upstream_request =
            Http3UpstreamRequest::new(request.request_header.clone()).with_body(body);
        let mut peer = HttpPeer::new(("127.0.0.1", origin_addr.port()), false, String::new());
        peer.options.set_http_version(2, 2);
        let connector = Connector::new(None);
        let response = execute_stream_upstream(&connector, &peer, upstream_request)
            .await
            .unwrap();

        let mut writer = request.response_writer.take().unwrap();
        write_downstream_http3_response(&mut writer, &response)
            .await
            .unwrap();

        let frames = collect_response_frames(&mut response_rx).await;
        assert!(matches!(frames[0], OutboundFrame::Headers(_, _)));
        match &frames[1] {
            OutboundFrame::Body(buf, fin) => {
                assert_eq!(&buf[..], b"/echo|pingpong");
                assert!(*fin);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }

    #[tokio::test]
    async fn mixed_mode_h3_downstream_to_h3_upstream() {
        let origin_addr = match spawn_http3_origin().await {
            Ok(addr) => addr,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("failed to start HTTP/3 origin: {error}"),
        };
        let bridge = Http3ProxyBridge::new();
        let executor = Http3UpstreamExecutor::new();
        let classifier = Http3RetryClassifier::default();
        let (body_tx, body_rx) = mpsc::channel(4);
        let (response_tx, mut response_rx) = mpsc::channel(4);
        body_tx
            .send(InboundFrame::Body(
                BufFactory::buf_from_slice(b"ping"),
                false,
            ))
            .await
            .unwrap();
        body_tx
            .send(InboundFrame::Body(
                BufFactory::buf_from_slice(b"pong"),
                true,
            ))
            .await
            .unwrap();
        drop(body_tx);

        let incoming_headers = IncomingH3Headers {
            stream_id: 20,
            headers: vec![
                H3Header::new(b":method", b"POST"),
                H3Header::new(b":path", b"/echo"),
                H3Header::new(b":authority", b"localhost"),
            ],
            send: PollSender::new(response_tx),
            recv: body_rx,
            read_fin: false,
            h3_audit_stats: Arc::new(H3AuditStats::new(20)),
        };

        let mut request = bridge
            .accept_incoming_headers(downstream_session(), incoming_headers)
            .unwrap();
        let body = collect_http3_request_body(&mut request).await;
        let upstream_request =
            Http3UpstreamRequest::new(request.request_header.clone()).with_body(body);
        let outcome = executor
            .execute(
                &upstream_peer(origin_addr.port()),
                upstream_request,
                &classifier,
            )
            .await
            .unwrap();
        let Http3UpstreamOutcome::Response(response) = outcome else {
            panic!("expected upstream response");
        };

        let mut writer = request.response_writer.take().unwrap();
        write_downstream_http3_response(&mut writer, &response)
            .await
            .unwrap();

        let frames = collect_response_frames(&mut response_rx).await;
        assert!(matches!(frames[0], OutboundFrame::Headers(_, _)));
        match &frames[1] {
            OutboundFrame::Body(buf, fin) => {
                assert_eq!(&buf[..], b"/echo|pingpong");
                assert!(*fin);
            }
            other => panic!("unexpected frame: {other:?}"),
        }
    }
}
