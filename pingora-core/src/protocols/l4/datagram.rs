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

//! Datagram transport primitives.

use std::io;
use std::sync::Arc;
#[cfg(unix)]
use std::{
    net::UdpSocket as StdUdpSocket,
    os::unix::io::{AsRawFd, FromRawFd},
};
#[cfg(windows)]
use std::{
    net::UdpSocket as StdUdpSocket,
    os::windows::io::{AsRawSocket, FromRawSocket},
};
use tokio::net::UdpSocket;

use crate::protocols::l4::socket::SocketAddr;

/// Metadata describing an inbound UDP datagram.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatagramMeta {
    /// The local listener address that received the datagram.
    pub local_addr: SocketAddr,
    /// The remote peer address that sent the datagram.
    pub peer_addr: SocketAddr,
}

/// A listener-scoped flow key for UDP traffic.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatagramFlowKey {
    /// Stable identity of the listener or service that received the packet.
    pub listener_id: Arc<str>,
    /// The local listener address that received the datagram.
    pub local_addr: SocketAddr,
    /// The remote peer address that sent the datagram.
    pub peer_addr: SocketAddr,
}

impl DatagramMeta {
    /// Derive a listener-scoped flow key from this metadata.
    pub fn flow_key(&self, listener_id: impl Into<Arc<str>>) -> DatagramFlowKey {
        DatagramFlowKey {
            listener_id: listener_id.into(),
            local_addr: self.local_addr.clone(),
            peer_addr: self.peer_addr.clone(),
        }
    }
}

/// An owned UDP datagram and its metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Datagram {
    /// Packet metadata describing the local and remote addresses.
    pub meta: DatagramMeta,
    payload: Vec<u8>,
    truncated: bool,
}

impl Datagram {
    /// Create a datagram from owned bytes and metadata.
    pub fn new(meta: DatagramMeta, payload: Vec<u8>) -> Self {
        Self::new_with_truncation(meta, payload, false)
    }

    /// Create a datagram from owned bytes and explicit truncation status.
    pub fn new_with_truncation(meta: DatagramMeta, payload: Vec<u8>, truncated: bool) -> Self {
        Self {
            meta,
            payload,
            truncated,
        }
    }

    /// Borrow the payload as bytes.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// Whether the datagram may have been truncated by the receive buffer.
    pub fn is_truncated(&self) -> bool {
        self.truncated
    }

    /// Consume the datagram and return the owned payload.
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}

/// A UDP listener that preserves datagram semantics.
#[derive(Debug)]
pub struct UdpListener {
    socket: UdpSocket,
}

impl UdpListener {
    /// Bind a UDP socket to the given address.
    pub async fn bind(addr: &str) -> io::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        Ok(Self { socket })
    }

    #[cfg(unix)]
    /// Create a UDP listener from a raw file descriptor.
    pub fn from_raw_fd(fd: std::os::unix::io::RawFd) -> io::Result<Self> {
        // SAFETY: the caller transfers ownership of a valid UDP socket fd.
        let socket = unsafe { StdUdpSocket::from_raw_fd(fd) };
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket: UdpSocket::from_std(socket)?,
        })
    }

    #[cfg(windows)]
    /// Create a UDP listener from a raw socket.
    pub fn from_raw_socket(sock: std::os::windows::io::RawSocket) -> io::Result<Self> {
        // SAFETY: the caller transfers ownership of a valid UDP socket.
        let socket = unsafe { StdUdpSocket::from_raw_socket(sock as _) };
        socket.set_nonblocking(true)?;
        Ok(Self {
            socket: UdpSocket::from_std(socket)?,
        })
    }

    /// Return the local address the listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr().map(Into::into)
    }

    /// Receive a datagram and return its size with local and peer addressing metadata.
    pub async fn recv_from(&self, buf: &mut [u8]) -> io::Result<(usize, DatagramMeta)> {
        let (size, peer_addr) = self.socket.recv_from(buf).await?;
        let local_addr = self.local_addr()?;
        Ok((
            size,
            DatagramMeta {
                local_addr,
                peer_addr: peer_addr.into(),
            },
        ))
    }

    /// Receive a datagram into an owned packet buffer.
    pub async fn recv_datagram(&self, max_size: usize) -> io::Result<Datagram> {
        let mut buf = vec![0; max_size];
        let (size, meta) = self.recv_from(&mut buf).await?;
        buf.truncate(size);
        // Without recvmsg/MSG_TRUNC we cannot distinguish an exact-fit datagram
        // from one truncated to the receive buffer, so exact fits are treated as
        // conservatively truncated at the service layer.
        let truncated = size == max_size;
        Ok(Datagram::new_with_truncation(meta, buf, truncated))
    }

    /// Send a datagram to the given remote address.
    pub async fn send_to(&self, buf: &[u8], addr: &SocketAddr) -> io::Result<usize> {
        match addr {
            SocketAddr::Inet(addr) => self.socket.send_to(buf, addr).await,
            #[cfg(unix)]
            SocketAddr::Unix(_) => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP listener does not support Unix domain socket addresses",
            )),
        }
    }

    /// Send an owned datagram to its remote peer.
    pub async fn send_datagram(&self, datagram: &Datagram) -> io::Result<usize> {
        self.send_to(datagram.payload(), &datagram.meta.peer_addr)
            .await
    }
}

#[cfg(unix)]
impl AsRawFd for UdpListener {
    fn as_raw_fd(&self) -> std::os::unix::io::RawFd {
        self.socket.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsRawSocket for UdpListener {
    fn as_raw_socket(&self) -> std::os::windows::io::RawSocket {
        self.socket.as_raw_socket()
    }
}

#[cfg(test)]
mod tests {
    use super::{Datagram, DatagramFlowKey, DatagramMeta, UdpListener};
    use crate::protocols::l4::socket::SocketAddr;
    use std::sync::Arc;
    use tokio::net::UdpSocket;

    #[test]
    fn datagram_flow_key_is_listener_scoped() {
        let meta = DatagramMeta {
            local_addr: "127.0.0.1:8080".parse().map(SocketAddr::Inet).unwrap(),
            peer_addr: "127.0.0.1:50000".parse().map(SocketAddr::Inet).unwrap(),
        };

        let flow_a = meta.flow_key(Arc::<str>::from("udp-lb-a"));
        let flow_b = meta.flow_key(Arc::<str>::from("udp-lb-b"));

        assert_ne!(flow_a, flow_b);
        assert_eq!(
            flow_a,
            DatagramFlowKey {
                listener_id: Arc::<str>::from("udp-lb-a"),
                local_addr: meta.local_addr.clone(),
                peer_addr: meta.peer_addr.clone(),
            }
        );
    }

    #[test]
    fn datagram_preserves_owned_payload() {
        let meta = DatagramMeta {
            local_addr: "127.0.0.1:8080".parse().map(SocketAddr::Inet).unwrap(),
            peer_addr: "127.0.0.1:50000".parse().map(SocketAddr::Inet).unwrap(),
        };

        let datagram = Datagram::new(meta, b"payload".to_vec());
        assert_eq!(datagram.payload(), b"payload");
        assert_eq!(datagram.into_payload(), b"payload".to_vec());
    }

    #[test]
    fn datagram_tracks_truncation_status() {
        let meta = DatagramMeta {
            local_addr: "127.0.0.1:8080".parse().map(SocketAddr::Inet).unwrap(),
            peer_addr: "127.0.0.1:50000".parse().map(SocketAddr::Inet).unwrap(),
        };

        assert!(!Datagram::new(meta.clone(), b"payload".to_vec()).is_truncated());
        assert!(Datagram::new_with_truncation(meta, b"payload".to_vec(), true).is_truncated());
    }

    #[tokio::test]
    async fn udp_listener_receives_and_sends_datagrams() {
        let listener = UdpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let client_addr = client.local_addr().unwrap();

        client
            .send_to(
                b"ping",
                listener_addr
                    .as_inet()
                    .expect("listener should bind to an inet address"),
            )
            .await
            .unwrap();

        let mut buf = [0; 32];
        let (size, meta) = listener.recv_from(&mut buf).await.unwrap();

        assert_eq!(&buf[..size], b"ping");
        assert_eq!(meta.local_addr, listener_addr);
        assert_eq!(meta.peer_addr, SocketAddr::Inet(client_addr));

        listener.send_to(b"pong", &meta.peer_addr).await.unwrap();

        let mut recv = [0; 32];
        let (size, peer_addr) = client.recv_from(&mut recv).await.unwrap();
        assert_eq!(&recv[..size], b"pong");
        assert_eq!(peer_addr, listener_addr.as_inet().copied().unwrap());
    }

    #[tokio::test]
    async fn udp_listener_marks_exact_buffer_fill_as_truncated() {
        let listener = UdpListener::bind("127.0.0.1:0").await.unwrap();
        let listener_addr = listener.local_addr().unwrap();

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client
            .send_to(
                b"oversized",
                listener_addr
                    .as_inet()
                    .expect("listener should bind to an inet address"),
            )
            .await
            .unwrap();

        let datagram = listener.recv_datagram(4).await.unwrap();
        assert_eq!(datagram.payload(), b"over");
        assert!(datagram.is_truncated());
    }
}
