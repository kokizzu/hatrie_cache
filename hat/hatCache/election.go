package hatCache

import (
	"time"

	"hatrie_cache/hat/hatTopology"
)

// Election types remain available from the root package for compatibility.
// New integrations can depend on hatTopology directly.
const DefaultElectionTimeout = hatTopology.DefaultElectionTimeout

type ElectionOptions = hatTopology.ElectionOptions
type ElectionStatus = hatTopology.ElectionStatus
type ElectionNodeStatus = hatTopology.ElectionNodeStatus
type ElectionLeader = hatTopology.ElectionLeader
type ElectionKeyRoute = hatTopology.ElectionKeyRoute

// ElectionStore adapts the legacy root topology store to hatTopology's public
// election implementation.
type ElectionStore struct {
	topology *TopologyStore
	core     *hatTopology.ElectionStore
	timeout  time.Duration
}

func NewElectionStore(topology *TopologyStore, options ElectionOptions) *ElectionStore {
	timeout := options.Timeout
	if timeout == 0 {
		timeout = DefaultElectionTimeout
	}
	return &ElectionStore{
		topology: topology,
		core:     hatTopology.NewElectionStore(topology, options),
		timeout:  timeout,
	}
}

func (store *ElectionStore) Heartbeat(nodeID string) error {
	if store == nil || store.core == nil {
		return hatTopology.NewElectionStore(nil, ElectionOptions{}).Heartbeat(nodeID)
	}
	return store.core.Heartbeat(nodeID)
}

func (store *ElectionStore) MarkOffline(nodeID string) error {
	if store == nil || store.core == nil {
		return hatTopology.NewElectionStore(nil, ElectionOptions{}).MarkOffline(nodeID)
	}
	return store.core.MarkOffline(nodeID)
}

// IsHealthy reports whether nodeID is eligible to serve stale-sensitive reads.
func (store *ElectionStore) IsHealthy(nodeID string) bool {
	if store == nil || store.core == nil {
		return false
	}
	return store.core.IsHealthy(nodeID)
}

// OrphanNodes returns liveness records for node IDs no longer in the current
// topology. The returned IDs are sorted and independently owned.
func (store *ElectionStore) OrphanNodes() []string {
	if store == nil || store.core == nil {
		return nil
	}
	return store.core.OrphanNodes()
}

// PruneOrphanNodes removes stale liveness records without changing topology or
// cache data. The returned IDs are sorted and independently owned.
func (store *ElectionStore) PruneOrphanNodes() []string {
	if store == nil || store.core == nil {
		return nil
	}
	return store.core.PruneOrphanNodes()
}

func (store *ElectionStore) Status() ElectionStatus {
	if store == nil || store.core == nil {
		return ElectionStatus{}
	}
	return store.core.Status()
}

func (store *ElectionStore) LeaderForKey(key string) (ElectionKeyRoute, bool) {
	if store == nil || store.core == nil {
		return ElectionKeyRoute{}, false
	}
	return store.core.LeaderForKey(key)
}

func (store *ElectionStore) activeNodesSnapshot(topology ClusterTopology) map[string]bool {
	if store == nil || store.core == nil {
		return nil
	}
	return store.core.ActiveNodes(topology)
}

func (store *ElectionStore) inactiveNodesSnapshot(topology ClusterTopology) map[string]bool {
	if store == nil || store.core == nil {
		return nil
	}
	return store.core.InactiveNodes(topology)
}

func electShardLeader(shard TopologyShard, active map[string]bool) ElectionLeader {
	return hatTopology.ElectShardLeader(shard, active)
}
