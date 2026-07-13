package hatriecache

import (
	"errors"
	"sort"
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
	topology := store.topologySnapshot()
	store.mu.RLock()
	defer store.mu.RUnlock()

	active := store.activeNodesLocked(topology)
	nodes := make([]ElectionNodeStatus, 0, len(topology.Nodes))
	for _, node := range topology.Nodes {
		online, reason, lastSeen := store.nodeStatusLocked(node.ID, active)
		nodes = append(nodes, ElectionNodeStatus{
			ID:       node.ID,
			Online:   online,
			Reason:   reason,
			LastSeen: lastSeen,
		})
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	shards := electionShards(topology)
	leaders := make([]ElectionLeader, 0, len(shards))
	for _, shard := range shards {
		leaders = append(leaders, electShardLeader(shard, active))
	}
	return ElectionStatus{
		TimeoutMillis: store.timeout.Milliseconds(),
		Nodes:         nodes,
		Leaders:       leaders,
	}
}

func (store *ElectionStore) LeaderForKey(key string) (ElectionKeyRoute, bool) {
	if store == nil {
		return ElectionKeyRoute{}, false
	}
	topology := store.topologySnapshot()
	route, ok := topology.RouteForKey(key)
	if !ok {
		return ElectionKeyRoute{}, false
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	active := store.activeNodesLocked(topology)
	leader := electShardLeader(route.Shard, active)
	return ElectionKeyRoute{Key: key, Route: route, Leader: leader}, true
}

func (store *ElectionStore) setNode(nodeID string, offline bool) error {
	if store == nil {
		return errors.New("hatriecache: election store is nil")
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return errors.New("hatriecache: election node id is required")
	}
	topology := store.topologySnapshot()
	if !topologyHasNode(topology, nodeID) {
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

func (store *ElectionStore) topologySnapshot() ClusterTopology {
	if store == nil || store.topology == nil {
		return ClusterTopology{}
	}
	return store.topology.Get()
}

func (store *ElectionStore) activeNodesLocked(topology ClusterTopology) map[string]bool {
	active := make(map[string]bool, len(topology.Nodes))
	now := store.now()
	for _, node := range topology.Nodes {
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

func (store *ElectionStore) nodeStatusLocked(nodeID string, active map[string]bool) (bool, string, *time.Time) {
	record, tracked := store.nodes[nodeID]
	if !tracked {
		return active[nodeID], "assumed_online", nil
	}
	lastSeen := record.lastSeen
	switch {
	case record.offline:
		return false, "offline", &lastSeen
	case !active[nodeID]:
		return false, "timeout", &lastSeen
	default:
		return true, "healthy", &lastSeen
	}
}

func electionShards(topology ClusterTopology) []TopologyShard {
	if topologyMode(topology.Mode) == TopologyModeFullReplica {
		shard, ok := topology.fullReplicaShard()
		if !ok {
			return nil
		}
		return []TopologyShard{shard}
	}
	shards := cloneShards(topology.Shards)
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].ID < shards[j].ID
	})
	return shards
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

func topologyHasNode(topology ClusterTopology, nodeID string) bool {
	for _, node := range topology.Nodes {
		if node.ID == nodeID {
			return true
		}
	}
	return false
}
