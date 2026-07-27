package hatriecache

import (
	"errors"
	"strings"
	"sync"
	"time"
)

const DefaultElectionTimeout = 15 * time.Second

type ElectionOptions struct {
	Timeout time.Duration
	Now     func() time.Time
}

type ElectionStore struct {
	mu       sync.RWMutex
	topology *TopologyStore
	timeout  time.Duration
	now      func() time.Time
	nodes    map[string]electionNodeRecord
}

type electionNodeRecord struct {
	lastSeen time.Time
	offline  bool
}

type ElectionStatus struct {
	TimeoutMillis int64                `json:"timeout_ms"`
	Nodes         []ElectionNodeStatus `json:"nodes"`
	Leaders       []ElectionLeader     `json:"leaders"`
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

func NewElectionStore(topology *TopologyStore, options ElectionOptions) *ElectionStore {
	timeout := options.Timeout
	if timeout == 0 {
		timeout = DefaultElectionTimeout
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &ElectionStore{
		topology: topology,
		timeout:  timeout,
		now:      now,
		nodes:    map[string]electionNodeRecord{},
	}
}

func (store *ElectionStore) Heartbeat(nodeID string) error {
	return store.setNode(nodeID, false)
}

func (store *ElectionStore) MarkOffline(nodeID string) error {
	return store.setNode(nodeID, true)
}

func (store *ElectionStore) Status() ElectionStatus {
	if store == nil {
		return ElectionStatus{}
	}
	topology, hasMaintenance := store.topology.electionStatusSnapshot()
	store.mu.RLock()
	defer store.mu.RUnlock()

	nodes := make([]ElectionNodeStatus, 0, len(topology.Nodes))
	now := store.now()
	var active map[string]bool
	if hasMaintenance {
		active = store.activeNodesLockedAt(topology, now)
	}
	for _, node := range topology.Nodes {
		status := store.nodeStatusForTopologyNodeLocked(node, now)
		nodes = append(nodes, status)
	}
	lookup := newElectionStatusLeaderLookup(store.nodes, store.timeout, now)
	leaderCapacity := len(topology.Shards)
	if topology.Mode == TopologyModeFullReplica {
		leaderCapacity = 1
	}
	leaders := make([]ElectionLeader, 0, leaderCapacity)
	if hasMaintenance {
		leaders = electionStatusLeadersFromActiveMap(topology, leaders, active)
	} else {
		leaders = lookup.appendLeaders(topology, leaders)
	}
	return ElectionStatus{
		TimeoutMillis: store.timeout.Milliseconds(),
		Nodes:         nodes,
		Leaders:       leaders,
	}
}

func (store *ElectionStore) LeaderForKey(key string) (ElectionKeyRoute, bool) {
	if store == nil || store.topology == nil {
		return ElectionKeyRoute{}, false
	}
	route, nodes, ok := store.topology.electionRouteSnapshot(key)
	if !ok {
		return ElectionKeyRoute{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	leader := store.electShardLeaderLocked(route.Shard, nodes)
	return ElectionKeyRoute{Key: key, Route: route, Leader: leader}, true
}

func (store *ElectionStore) electShardLeaderLocked(shard TopologyShard, nodes []TopologyNode) ElectionLeader {
	candidates := routeOwners(shard)
	leader := ElectionLeader{
		Shard:      shard.ID,
		Primary:    shard.Primary,
		Candidates: candidates,
	}
	now := store.now()
	for _, nodeID := range candidates {
		if store.nodeActiveForElectionLocked(nodes, nodeID, now) {
			leader.Leader = nodeID
			leader.Available = true
			return leader
		}
	}
	return leader
}

func (store *ElectionStore) nodeActiveForElectionLocked(nodes []TopologyNode, nodeID string, now time.Time) bool {
	node, ok := normalizedTopologyNode(nodes, nodeID)
	if !ok || node.Maintenance {
		return false
	}
	record, tracked := store.nodes[nodeID]
	if !tracked {
		return true
	}
	if record.offline {
		return false
	}
	return store.timeout <= 0 || record.lastSeen.IsZero() || now.Sub(record.lastSeen) <= store.timeout
}

func (store *ElectionStore) setNode(nodeID string, offline bool) error {
	if store == nil {
		return errors.New("hatriecache: election store is nil")
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return errors.New("hatriecache: election node id is required")
	}
	if !store.topology.hasNode(nodeID) {
		return errors.New("hatriecache: election node is not registered")
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	store.nodes[nodeID] = electionNodeRecord{
		lastSeen: store.now(),
		offline:  offline,
	}
	return nil
}

func (store *ElectionStore) activeNodesLocked(topology ClusterTopology) map[string]bool {
	return store.activeNodesLockedAt(topology, store.now())
}

func (store *ElectionStore) activeNodesLockedAt(topology ClusterTopology, now time.Time) map[string]bool {
	active := make(map[string]bool, len(topology.Nodes))
	for _, node := range topology.Nodes {
		if node.Maintenance {
			active[node.ID] = false
			continue
		}
		record, tracked := store.nodes[node.ID]
		switch {
		case !tracked:
			active[node.ID] = true
		case record.offline:
			active[node.ID] = false
		case store.timeout > 0 && !record.lastSeen.IsZero() && now.Sub(record.lastSeen) > store.timeout:
			active[node.ID] = false
		default:
			active[node.ID] = true
		}
	}
	return active
}

func (store *ElectionStore) activeNodesSnapshot(topology ClusterTopology) map[string]bool {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.activeNodesLocked(topology)
}

func (store *ElectionStore) nodeStatusForTopologyNodeLocked(node TopologyNode, now time.Time) ElectionNodeStatus {
	status := ElectionNodeStatus{ID: node.ID}
	record, tracked := store.nodes[node.ID]
	if !tracked {
		status.Online = !node.Maintenance
		status.Reason = "assumed_online"
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
		status.Online = true
		status.Reason = "healthy"
	}
	return status
}

func electShardLeader(shard TopologyShard, active map[string]bool) ElectionLeader {
	candidates := routeOwners(shard)
	leader := ElectionLeader{
		Shard:      shard.ID,
		Primary:    shard.Primary,
		Candidates: candidates,
	}
	for _, nodeID := range candidates {
		if active[nodeID] {
			leader.Leader = nodeID
			leader.Available = true
			return leader
		}
	}
	return leader
}

type electionStatusLeaderLookup struct {
	records map[string]electionNodeRecord
	timeout time.Duration
	now     time.Time
}

func newElectionStatusLeaderLookup(
	records map[string]electionNodeRecord,
	timeout time.Duration,
	now time.Time,
) electionStatusLeaderLookup {
	return electionStatusLeaderLookup{
		records: records,
		timeout: timeout,
		now:     now,
	}
}

func (lookup *electionStatusLeaderLookup) appendLeaders(topology ClusterTopology, leaders []ElectionLeader) []ElectionLeader {
	if topology.Mode == TopologyModeFullReplica {
		if shard, ok := normalizedFullReplicaShard(topology); ok {
			return append(leaders, lookup.elect(shard))
		}
		return leaders
	}
	for _, shard := range topology.Shards {
		leaders = append(leaders, lookup.elect(shard))
	}
	return leaders
}

func electionStatusLeadersFromActiveMap(topology ClusterTopology, leaders []ElectionLeader, active map[string]bool) []ElectionLeader {
	if topology.Mode == TopologyModeFullReplica {
		if shard, ok := normalizedFullReplicaShard(topology); ok {
			return append(leaders, electShardLeader(shard, active))
		}
		return leaders
	}
	for _, shard := range topology.Shards {
		leaders = append(leaders, electShardLeader(shard, active))
	}
	return leaders
}

func (lookup *electionStatusLeaderLookup) elect(shard TopologyShard) ElectionLeader {
	candidates := routeOwners(shard)
	leader := ElectionLeader{
		Shard:      shard.ID,
		Primary:    shard.Primary,
		Candidates: candidates,
	}
	for _, nodeID := range candidates {
		if lookup.nodeOnline(nodeID) {
			leader.Leader = nodeID
			leader.Available = true
			return leader
		}
	}
	return leader
}

func (lookup *electionStatusLeaderLookup) nodeOnline(nodeID string) bool {
	record, tracked := lookup.records[nodeID]
	if !tracked {
		return true
	}
	return !record.offline && (lookup.timeout <= 0 || record.lastSeen.IsZero() || lookup.now.Sub(record.lastSeen) <= lookup.timeout)
}
