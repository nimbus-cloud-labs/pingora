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

//! UDP upstream selection primitives.

use parking_lot::RwLock;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::protocols::l4::datagram::DatagramFlowKey;
use crate::protocols::l4::socket::SocketAddr;
use crate::upstreams::peer::UdpPeer;

/// Baseline UDP backend selection modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UdpSelectionMode {
    /// Weighted round robin across all available backends.
    RoundRobin,
    /// Stable weighted hashing based on the listener-scoped flow key.
    FlowHash,
}

/// A tracked UDP flow entry mapped to an upstream peer.
#[derive(Debug, Clone)]
pub struct UdpFlowEntry {
    /// The selected upstream peer for this flow.
    pub peer: UdpPeer,
    /// The last time this flow was observed.
    pub last_seen: Instant,
}

/// The outcome of a flow-table lookup.
#[derive(Debug, Clone)]
pub enum UdpFlowLookup {
    /// A live flow entry was found and refreshed.
    Active(UdpFlowEntry),
    /// The flow entry had expired and was removed.
    Expired,
    /// No flow entry exists for the given key.
    Missing,
}

/// A bounded-lifetime UDP flow table keyed by listener-scoped flow IDs.
#[derive(Debug)]
pub struct UdpFlowTable {
    entries: HashMap<DatagramFlowKey, UdpFlowEntry>,
    idle_timeout: Duration,
}

impl UdpFlowTable {
    /// Create a flow table with the given idle timeout.
    pub fn new(idle_timeout: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            idle_timeout,
        }
    }

    /// Return the configured idle timeout.
    pub fn idle_timeout(&self) -> Duration {
        self.idle_timeout
    }

    /// Return the current number of tracked flow entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the flow table is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Insert or replace a flow entry and mark it as active now.
    pub fn upsert(&mut self, flow_key: DatagramFlowKey, peer: UdpPeer) {
        self.entries.insert(
            flow_key,
            UdpFlowEntry {
                peer,
                last_seen: Instant::now(),
            },
        );
    }

    /// Look up a flow entry, expiring it first if it is idle.
    pub fn lookup(&mut self, flow_key: &DatagramFlowKey) -> UdpFlowLookup {
        let expired = self
            .entries
            .get(flow_key)
            .map(|entry| entry.last_seen.elapsed() >= self.idle_timeout)
            .unwrap_or(false);

        if expired {
            self.entries.remove(flow_key);
            return UdpFlowLookup::Expired;
        }

        if let Some(entry) = self.entries.get_mut(flow_key) {
            entry.last_seen = Instant::now();
            return UdpFlowLookup::Active(entry.clone());
        }

        UdpFlowLookup::Missing
    }

    /// Remove any expired flow entries and return how many were removed.
    pub fn cleanup_expired(&mut self) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|_, entry| entry.last_seen.elapsed() < self.idle_timeout);
        before - self.entries.len()
    }

    /// Remove all flow entries mapped to the given peer address.
    pub fn invalidate_peer(&mut self, peer_addr: &SocketAddr) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|_, entry| entry.peer.address() != peer_addr);
        before - self.entries.len()
    }
}

/// A weighted collection of UDP peers with baseline selection methods.
#[derive(Debug)]
pub struct UdpPeerSet {
    peers: Vec<UdpPeer>,
    weighted_indexes: Vec<usize>,
    enabled: RwLock<HashMap<SocketAddr, bool>>,
    next_rr: AtomicUsize,
}

impl UdpPeerSet {
    /// Create a peer set from the given UDP peers.
    pub fn new(peers: Vec<UdpPeer>) -> Self {
        let enabled = peers
            .iter()
            .map(|peer| (peer.address().clone(), true))
            .collect();
        let weighted_indexes = peers
            .iter()
            .enumerate()
            .flat_map(|(index, peer)| std::iter::repeat_n(index, peer.weight().max(1)))
            .collect();

        Self {
            peers,
            weighted_indexes,
            enabled: RwLock::new(enabled),
            next_rr: AtomicUsize::new(0),
        }
    }

    /// Return the configured peers.
    pub fn peers(&self) -> &[UdpPeer] {
        &self.peers
    }

    /// Whether the set contains a peer with the given address.
    pub fn contains_addr(&self, addr: &SocketAddr) -> bool {
        self.peers.iter().any(|peer| peer.address() == addr)
    }

    /// Whether the given peer address is currently enabled for selection.
    pub fn is_enabled(&self, addr: &SocketAddr) -> bool {
        self.enabled.read().get(addr).copied().unwrap_or(false)
    }

    /// Enable or disable the given peer address. Returns `true` if the peer exists.
    pub fn set_enabled(&self, addr: &SocketAddr, enabled: bool) -> bool {
        let mut state = self.enabled.write();
        let Some(current) = state.get_mut(addr) else {
            return false;
        };
        *current = enabled;
        true
    }

    fn available_weighted_indexes(&self) -> Vec<usize> {
        let enabled = self.enabled.read();
        self.weighted_indexes
            .iter()
            .copied()
            .filter(|index| {
                enabled
                    .get(self.peers[*index].address())
                    .copied()
                    .unwrap_or(false)
            })
            .collect()
    }

    /// Whether the set contains no peers.
    pub fn is_empty(&self) -> bool {
        self.peers.is_empty()
    }

    /// Select the next peer using weighted round robin.
    pub fn select_round_robin(&self) -> Option<&UdpPeer> {
        let weighted_indexes = self.available_weighted_indexes();
        if weighted_indexes.is_empty() {
            return None;
        }

        let next = self.next_rr.fetch_add(1, Ordering::Relaxed);
        let peer_index = weighted_indexes[next % weighted_indexes.len()];
        self.peers.get(peer_index)
    }

    /// Select a peer deterministically for the given flow key.
    pub fn select_by_flow(&self, flow_key: &DatagramFlowKey) -> Option<&UdpPeer> {
        let weighted_indexes = self.available_weighted_indexes();
        if weighted_indexes.is_empty() {
            return None;
        }

        let mut hasher = DefaultHasher::new();
        flow_key.hash(&mut hasher);
        let slot = (hasher.finish() as usize) % weighted_indexes.len();
        self.peers.get(weighted_indexes[slot])
    }

    /// Select a peer according to the given mode.
    pub fn select(&self, mode: UdpSelectionMode, flow_key: &DatagramFlowKey) -> Option<&UdpPeer> {
        match mode {
            UdpSelectionMode::RoundRobin => self.select_round_robin(),
            UdpSelectionMode::FlowHash => self.select_by_flow(flow_key),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{UdpFlowLookup, UdpFlowTable, UdpPeerSet, UdpSelectionMode};
    use crate::protocols::l4::datagram::DatagramFlowKey;
    use crate::protocols::l4::socket::SocketAddr;
    use crate::upstreams::peer::UdpPeer;
    use std::collections::BTreeSet;
    use std::sync::Arc;
    use std::time::Duration;

    fn peer(addr: &str) -> UdpPeer {
        UdpPeer::new(addr)
    }

    fn flow(listener_id: &str, peer_addr: &str) -> DatagramFlowKey {
        DatagramFlowKey {
            listener_id: Arc::<str>::from(listener_id),
            local_addr: SocketAddr::Inet("127.0.0.1:7000".parse().unwrap()),
            peer_addr: SocketAddr::Inet(peer_addr.parse().unwrap()),
        }
    }

    #[test]
    fn flow_hash_selection_is_stable_for_same_key() {
        let peers = UdpPeerSet::new(vec![peer("127.0.0.1:5300"), peer("127.0.0.1:5301")]);
        let key = flow("udp-lb", "127.0.0.1:50000");

        let first = peers.select_by_flow(&key).unwrap().address().clone();
        let second = peers
            .select(UdpSelectionMode::FlowHash, &key)
            .unwrap()
            .address()
            .clone();

        assert_eq!(first, second);
    }

    #[test]
    fn round_robin_selection_visits_all_equal_weight_peers() {
        let peers = UdpPeerSet::new(vec![
            peer("127.0.0.1:5300"),
            peer("127.0.0.1:5301"),
            peer("127.0.0.1:5302"),
        ]);
        let key = flow("udp-lb", "127.0.0.1:50000");

        let seen: BTreeSet<_> = (0..6)
            .map(|_| {
                peers
                    .select(UdpSelectionMode::RoundRobin, &key)
                    .unwrap()
                    .address()
                    .clone()
            })
            .collect();

        assert_eq!(seen.len(), 3);
    }

    #[test]
    fn selection_skips_disabled_peers() {
        let peers = UdpPeerSet::new(vec![peer("127.0.0.1:5300"), peer("127.0.0.1:5301")]);
        let disabled = SocketAddr::Inet("127.0.0.1:5300".parse().unwrap());
        let key = flow("udp-lb", "127.0.0.1:50000");

        assert!(peers.set_enabled(&disabled, false));
        assert!(!peers.is_enabled(&disabled));

        let selected = peers.select(UdpSelectionMode::RoundRobin, &key).unwrap();
        assert_ne!(selected.address(), &disabled);
    }

    #[test]
    fn flow_table_lookup_refreshes_active_entries() {
        let mut table = UdpFlowTable::new(Duration::from_secs(1));
        let key = flow("udp-lb", "127.0.0.1:50000");
        let upstream = peer("127.0.0.1:5300");

        table.upsert(key.clone(), upstream.clone());
        let first_seen = match table.lookup(&key) {
            UdpFlowLookup::Active(entry) => entry.last_seen,
            other => panic!("expected active flow entry, got {other:?}"),
        };
        std::thread::sleep(Duration::from_millis(5));
        let refreshed = match table.lookup(&key) {
            UdpFlowLookup::Active(entry) => entry,
            other => panic!("expected active flow entry, got {other:?}"),
        };

        assert_eq!(refreshed.peer.address(), upstream.address());
        assert!(refreshed.last_seen >= first_seen);
    }

    #[test]
    fn flow_table_expires_idle_entries() {
        let mut table = UdpFlowTable::new(Duration::from_millis(10));
        let key = flow("udp-lb", "127.0.0.1:50000");

        table.upsert(key.clone(), peer("127.0.0.1:5300"));
        std::thread::sleep(Duration::from_millis(20));

        assert!(matches!(table.lookup(&key), UdpFlowLookup::Expired));
        assert!(table.is_empty());
    }

    #[test]
    fn flow_table_reports_missing_entries() {
        let mut table = UdpFlowTable::new(Duration::from_secs(1));
        let key = flow("udp-lb", "127.0.0.1:50000");

        assert!(matches!(table.lookup(&key), UdpFlowLookup::Missing));
    }

    #[test]
    fn flow_table_invalidates_entries_for_removed_peer() {
        let mut table = UdpFlowTable::new(Duration::from_secs(1));
        let flow_a = flow("udp-lb", "127.0.0.1:50000");
        let flow_b = flow("udp-lb", "127.0.0.1:50001");
        let peer_a = peer("127.0.0.1:5300");
        let peer_b = peer("127.0.0.1:5301");

        table.upsert(flow_a.clone(), peer_a.clone());
        table.upsert(flow_b.clone(), peer_b.clone());

        let removed = table.invalidate_peer(peer_a.address());

        assert_eq!(removed, 1);
        assert!(matches!(table.lookup(&flow_a), UdpFlowLookup::Missing));
        let flow_b = match table.lookup(&flow_b) {
            UdpFlowLookup::Active(entry) => entry,
            other => panic!("expected active flow entry, got {other:?}"),
        };
        assert_eq!(flow_b.peer.address(), peer_b.address());
    }
}
