package hatTopology

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// DefaultElectionTimeout is the maximum age of a heartbeat before a node is
// considered unavailable.
const DefaultElectionTimeout = 15 * time.Second

// TopologySnapshotProvider returns an immutable, normalized topology snapshot.
// Implementations may update their current topology between calls.
type TopologySnapshotProvider interface {
	TopologySnapshot() ClusterTopology
}

// ElectionOptions configures leader selection.
type ElectionOptions struct {
	Timeout time.Duration
	Now     func() time.Time
}

// ElectionStore tracks node liveness and elects the first available owner of
// each topology shard. The shard primary is always preferred.
type ElectionStore struct {
	mu       sync.RWMutex
	topology TopologySnapshotProvider
	timeout  time.Duration
	now      func() time.Time
	nodes    map[string]electionNodeRecord
}

type electionNodeRecord struct {
	lastSeen time.Time
	offline  bool
}

// ElectionStatus is the current liveness and leader-selection view.
type ElectionStatus struct {
	TimeoutMillis int64                `json:"timeout_ms"`
	Nodes         []ElectionNodeStatus `json:"nodes"`
	Leaders       []ElectionLeader     `json:"leaders"`
	OrphanNodes   []string             `json:"orphan_nodes,omitempty"`
}

type ElectionNodeStatus struct {
	ID       string     `json:"id"`
	Online   bool       `json:"online"`
	Reason   string     `json:"reason"`
	LastSeen *time.Time `json:"last_seen,omitempty"`
}

type ElectionLeader struct {
	Shard      uint32   `json:"shard"`
	Leader     string   `json:"leader,omitempty"`
	Available  bool     `json:"available"`
	Primary    string   `json:"primary"`
	Candidates []string `json:"candidates,omitempty"`
}

type ElectionKeyRoute struct {
	Key    string         `json:"key"`
	Route  TopologyRoute  `json:"route"`
	Leader ElectionLeader `json:"leader"`
}

// NewElectionStore creates an election store backed by topology snapshots.
func NewElectionStore(topology TopologySnapshotProvider, options ElectionOptions) *ElectionStore {
	timeout := options.Timeout
	if timeout == 0 {
		timeout = DefaultElectionTimeout
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &ElectionStore{topology: topology, timeout: timeout, now: now, nodes: map[string]electionNodeRecord{}}
}

func (store *ElectionStore) Heartbeat(nodeID string) error {
	return store.setNode(nodeID, false)
}

func (store *ElectionStore) MarkOffline(nodeID string) error {
	return store.setNode(nodeID, true)
}

// IsHealthy reports whether nodeID is currently eligible to serve a
// stale-sensitive read according to topology membership, maintenance state,
// heartbeat timeout, and explicit offline state.
func (store *ElectionStore) IsHealthy(nodeID string) bool {
	if store == nil {
		return false
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return false
	}
	topology, ok := store.topologySnapshot()
	if !ok {
		return false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.nodeActiveLocked(topology.Nodes, nodeID, store.now())
}

// OrphanNodes returns liveness records for node IDs that are no longer in the
// current topology. The returned IDs are sorted and independently owned.
func (store *ElectionStore) OrphanNodes() []string {
	if store == nil {
		return nil
	}
	topology, ok := store.topologySnapshot()
	if !ok {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.orphanNodeIDsLocked(topology)
}

// PruneOrphanNodes removes only stale liveness records whose node IDs are no
// longer present in the current topology. It never changes topology or cache
// data. The removed IDs are sorted and independently owned.
func (store *ElectionStore) PruneOrphanNodes() []string {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	topology, ok := store.topologySnapshot()
	if !ok {
		return nil
	}
	orphans := store.orphanNodeIDsLocked(topology)
	for _, nodeID := range orphans {
		delete(store.nodes, nodeID)
	}
	return orphans
}

// Status reports each node and elected leader for every current shard.
func (store *ElectionStore) Status() ElectionStatus {
	topology, ok := store.topologySnapshot()
	if !ok {
		return ElectionStatus{}
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	now := store.now()
	nodes := make([]ElectionNodeStatus, 0, len(topology.Nodes))
	for _, node := range topology.Nodes {
		nodes = append(nodes, store.nodeStatusLocked(node, now))
	}
	leaders := make([]ElectionLeader, 0, len(topology.Shards))
	if ModeFor(topology) == TopologyModeFullReplica {
		if shard, exists := topology.FullReplicaShard(); exists {
			leaders = append(leaders, store.electShardLeaderLocked(shard, topology.Nodes, now))
		}
	} else {
		for _, shard := range topology.Shards {
			leaders = append(leaders, store.electShardLeaderLocked(shard, topology.Nodes, now))
		}
	}
	return ElectionStatus{
		TimeoutMillis: store.timeout.Milliseconds(),
		Nodes:         nodes,
		Leaders:       leaders,
		OrphanNodes:   store.orphanNodeIDsLocked(topology),
	}
}

// LeaderForKey reports the selected route and its current available leader.
func (store *ElectionStore) LeaderForKey(key string) (ElectionKeyRoute, bool) {
	topology, ok := store.topologySnapshot()
	if !ok {
		return ElectionKeyRoute{}, false
	}
	route, ok := topology.RouteForKey(key)
	if !ok {
		return ElectionKeyRoute{}, false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return ElectionKeyRoute{Key: key, Route: route, Leader: store.electShardLeaderLocked(route.Shard, topology.Nodes, store.now())}, true
}

// ActiveNodes returns a snapshot of the current liveness state for topology.
func (store *ElectionStore) ActiveNodes(topology ClusterTopology) map[string]bool {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.activeNodesLocked(topology, store.now())
}

// InactiveNodes returns explicitly offline, timed-out, or maintenance nodes.
func (store *ElectionStore) InactiveNodes(topology ClusterTopology) map[string]bool {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	now := store.now()
	var inactive map[string]bool
	for _, node := range topology.Nodes {
		if store.nodeActiveLocked(topology.Nodes, node.ID, now) {
			continue
		}
		if inactive == nil {
			inactive = make(map[string]bool)
		}
		inactive[node.ID] = true
	}
	return inactive
}

// ElectShardLeader elects the first active candidate, preserving primary-first
// ordering. It is useful to callers which already hold a liveness snapshot.
func ElectShardLeader(shard TopologyShard, active map[string]bool) ElectionLeader {
	leader := ElectionLeader{Shard: shard.ID, Primary: shard.Primary, Candidates: Owners(shard)}
	for _, nodeID := range leader.Candidates {
		if active[nodeID] {
			leader.Leader, leader.Available = nodeID, true
			break
		}
	}
	return leader
}

func (store *ElectionStore) topologySnapshot() (ClusterTopology, bool) {
	if store == nil || store.topology == nil {
		return ClusterTopology{}, false
	}
	topology := store.topology.TopologySnapshot()
	return topology, len(topology.Nodes) > 0
}

func (store *ElectionStore) setNode(nodeID string, offline bool) error {
	if store == nil {
		return errors.New("hatriecache: election store is nil")
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return errors.New("hatriecache: election node id is required")
	}
	topology, ok := store.topologySnapshot()
	if !ok || !topologyHasNode(topology.Nodes, nodeID) {
		return errors.New("hatriecache: election node is not registered")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	store.nodes[nodeID] = electionNodeRecord{lastSeen: store.now(), offline: offline}
	return nil
}

func (store *ElectionStore) activeNodesLocked(topology ClusterTopology, now time.Time) map[string]bool {
	active := make(map[string]bool, len(topology.Nodes))
	for _, node := range topology.Nodes {
		active[node.ID] = store.nodeActiveLocked(topology.Nodes, node.ID, now)
	}
	return active
}

func (store *ElectionStore) electShardLeaderLocked(shard TopologyShard, nodes []TopologyNode, now time.Time) ElectionLeader {
	leader := ElectionLeader{Shard: shard.ID, Primary: shard.Primary, Candidates: Owners(shard)}
	for _, nodeID := range leader.Candidates {
		if store.nodeActiveLocked(nodes, nodeID, now) {
			leader.Leader, leader.Available = nodeID, true
			break
		}
	}
	return leader
}

func (store *ElectionStore) nodeActiveLocked(nodes []TopologyNode, nodeID string, now time.Time) bool {
	node, ok := FindNode(nodes, nodeID)
	if !ok || node.Maintenance {
		return false
	}
	record, tracked := store.nodes[nodeID]
	return !tracked || (!record.offline && (store.timeout <= 0 || record.lastSeen.IsZero() || now.Sub(record.lastSeen) <= store.timeout))
}

func (store *ElectionStore) nodeStatusLocked(node TopologyNode, now time.Time) ElectionNodeStatus {
	status := ElectionNodeStatus{ID: node.ID}
	record, tracked := store.nodes[node.ID]
	if !tracked {
		status.Online, status.Reason = !node.Maintenance, "assumed_online"
		if node.Maintenance {
			status.Reason = "maintenance"
		}
		return status
	}
	lastSeen := record.lastSeen
	status.LastSeen = &lastSeen
	switch {
	case node.Maintenance:
		status.Reason = "maintenance"
	case record.offline:
		status.Reason = "offline"
	case store.timeout > 0 && !record.lastSeen.IsZero() && now.Sub(record.lastSeen) > store.timeout:
		status.Reason = "timeout"
	default:
		status.Online, status.Reason = true, "healthy"
	}
	return status
}

func (store *ElectionStore) orphanNodeIDsLocked(topology ClusterTopology) []string {
	if len(store.nodes) == 0 {
		return nil
	}
	orphans := make([]string, 0)
	for nodeID := range store.nodes {
		if !topologyHasNode(topology.Nodes, nodeID) {
			orphans = append(orphans, nodeID)
		}
	}
	if len(orphans) == 0 {
		return nil
	}
	sort.Strings(orphans)
	return orphans
}

func topologyHasNode(nodes []TopologyNode, nodeID string) bool {
	_, ok := FindNode(nodes, nodeID)
	return ok
}
