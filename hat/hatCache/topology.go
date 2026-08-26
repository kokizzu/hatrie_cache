package hatCache

import (
	"errors"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strings"
	"sync"

	"hatrie_cache/hat/hatTopology"
)

const clusterTopologyVersion = hatTopology.Version

const (
	TopologyModeSharded     = hatTopology.TopologyModeSharded
	TopologyModeFullReplica = hatTopology.TopologyModeFullReplica
)

type ClusterTopology = hatTopology.ClusterTopology
type TopologyNode = hatTopology.TopologyNode
type TopologyShard = hatTopology.TopologyShard
type TopologyBucketRange = hatTopology.TopologyBucketRange
type TopologyRoute = hatTopology.TopologyRoute

// TopologyStore stores a validated topology and optionally persists updates.
type TopologyStore struct {
	mu                  sync.RWMutex
	path                string
	topology            ClusterTopology
	fingerprint         string
	verifiesFingerprint bool
	hasMaintenance      bool
}

// SingleNodeTopology returns a valid one-node topology with sharding disabled.
func SingleNodeTopology(nodeID string, address string) ClusterTopology {
	return hatTopology.SingleNodeTopology(nodeID, address)
}

// OpenTopologyStore loads a topology file, or uses fallback when the path is
// empty or the file does not exist.
func OpenTopologyStore(path string, fallback ClusterTopology) (*TopologyStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return NewTopologyStore(fallback)
	}

	topology, err := LoadTopology(path)
	if errors.Is(err, os.ErrNotExist) {
		topology = fallback
	} else if err != nil {
		return nil, err
	}
	normalized, err := normalizeTopology(topology)
	if err != nil {
		return nil, err
	}
	return newTopologyStore(path, normalized), nil
}

// NewTopologyStore validates and stores topology in memory.
func NewTopologyStore(topology ClusterTopology) (*TopologyStore, error) {
	normalized, err := normalizeTopology(topology)
	if err != nil {
		return nil, err
	}
	return newTopologyStore("", normalized), nil
}

func newTopologyStore(path string, topology ClusterTopology) *TopologyStore {
	return &TopologyStore{
		path:                path,
		topology:            topology,
		fingerprint:         topology.Fingerprint(),
		verifiesFingerprint: normalizedTopologyVerifiesReplicationFingerprint(topology),
		hasMaintenance:      topologyHasMaintenance(topology),
	}
}

// LoadTopology reads and validates topology JSON from disk.
func LoadTopology(path string) (ClusterTopology, error) {
	return hatTopology.LoadTopology(path)
}

// SaveTopology validates and writes topology JSON atomically.
func SaveTopology(path string, topology ClusterTopology) error {
	return hatTopology.SaveTopology(path, topology)
}

// Get returns a copy of the current topology.
func (store *TopologyStore) Get() ClusterTopology {
	if store == nil {
		return ClusterTopology{}
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return cloneTopology(store.topology)
}

// TopologySnapshot implements hatTopology.TopologySnapshotProvider. The
// returned topology is normalized and independent from later store updates.
func (store *TopologyStore) TopologySnapshot() hatTopology.ClusterTopology {
	return store.Get()
}

func (store *TopologyStore) replicationSnapshot() (ClusterTopology, string) {
	if store == nil {
		return ClusterTopology{}, ""
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return cloneTopology(store.topology), store.fingerprint
}

func (store *TopologyStore) replicationRoutingGeneration() (ClusterTopology, string) {
	if store == nil {
		return ClusterTopology{}, ""
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	// Set replaces the normalized generation instead of mutating its backing.
	return store.topology, store.fingerprint
}

// Fingerprint returns a stable content hash for the current topology. The local
// Self field is ignored so the same cluster file can be compared across nodes.
func (store *TopologyStore) Fingerprint() string {
	if store == nil {
		return ""
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.fingerprint
}

// VerifiesReplicationFingerprint reports whether the store has enough cluster
// routing metadata to reject replication from a different topology.
func (store *TopologyStore) VerifiesReplicationFingerprint() bool {
	if store == nil {
		return false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.verifiesFingerprint
}

// Set validates and stores topology, persisting it when the store has a path.
func (store *TopologyStore) Set(topology ClusterTopology) error {
	if store == nil {
		return errors.New("hatriecache: topology store is nil")
	}
	normalized, err := normalizeTopology(topology)
	if err != nil {
		return err
	}
	fingerprint := normalized.Fingerprint()
	verifiesFingerprint := normalizedTopologyVerifiesReplicationFingerprint(normalized)
	hasMaintenance := topologyHasMaintenance(normalized)
	if store.path != "" {
		if err := SaveTopology(store.path, normalized); err != nil {
			return err
		}
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	store.topology = normalized
	store.fingerprint = fingerprint
	store.verifiesFingerprint = verifiesFingerprint
	store.hasMaintenance = hasMaintenance
	return nil
}

// Route returns the shard selected for key by the current topology.
func (store *TopologyStore) Route(key string) (TopologyRoute, bool) {
	if store == nil {
		return TopologyRoute{}, false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return normalizedTopologyRouteForKey(store.topology, key)
}

func (store *TopologyStore) electionRouteSnapshot(key string) (TopologyRoute, []TopologyNode, bool) {
	if store == nil {
		return TopologyRoute{}, nil, false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	route, ok := normalizedTopologyRouteForKey(store.topology, key)
	if !ok {
		return TopologyRoute{}, nil, false
	}
	// Set replaces the normalized generation instead of mutating its backing.
	return route, store.topology.Nodes, true
}

func (store *TopologyStore) electionStatusSnapshot() (ClusterTopology, bool) {
	if store == nil {
		return ClusterTopology{}, false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	// Set replaces the normalized generation instead of mutating its backing.
	return store.topology, store.hasMaintenance
}

func topologyHasMaintenance(topology ClusterTopology) bool {
	for _, node := range topology.Nodes {
		if node.Maintenance {
			return true
		}
	}
	return false
}

func (store *TopologyStore) hasNode(nodeID string) bool {
	if store == nil {
		return false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	_, ok := normalizedTopologyNode(store.topology.Nodes, nodeID)
	return ok
}

func normalizedTopologyNode(nodes []TopologyNode, nodeID string) (TopologyNode, bool) {
	low, high := 0, len(nodes)
	for low < high {
		middle := int(uint(low+high) >> 1)
		if nodes[middle].ID < nodeID {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low >= len(nodes) || nodes[low].ID != nodeID {
		return TopologyNode{}, false
	}
	return nodes[low], true
}

func normalizedTopologyRouteForKey(topology ClusterTopology, key string) (TopologyRoute, bool) {
	mode := topology.Mode
	if mode == TopologyModeFullReplica {
		shard, ok := normalizedFullReplicaShard(topology)
		if !ok {
			return TopologyRoute{}, false
		}
		return TopologyRoute{Key: key, Mode: mode, Shard: shard, Owners: routeOwners(shard)}, true
	}

	shards := topology.Shards
	if len(shards) == 0 {
		return TopologyRoute{}, false
	}
	if topology.BucketCount > 0 {
		bucket := hashKeyToBucket(key, topology.BucketCount)
		shard, ok := shardForBucket(topology, bucket, shards)
		if !ok {
			return TopologyRoute{}, false
		}
		shard = cloneShard(shard)
		return TopologyRoute{
			Key:    key,
			Mode:   mode,
			Bucket: &bucket,
			Shard:  shard,
			Owners: routeOwners(shard),
		}, true
	}

	shard := cloneShard(shards[hashKeyToShardIndex(key, len(shards))])
	return TopologyRoute{Key: key, Mode: mode, Shard: shard, Owners: routeOwners(shard)}, true
}

func normalizedFullReplicaShard(topology ClusterTopology) (TopologyShard, bool) {
	nodes := topology.Nodes
	if len(nodes) == 0 {
		return TopologyShard{}, false
	}
	primary := topology.Self
	if primary == "" || !topologyNodeExists(nodes, primary) {
		primary = nodes[0].ID
	}
	replicas := make([]string, 0, len(nodes)-1)
	for _, node := range nodes {
		if node.ID != primary {
			replicas = append(replicas, node.ID)
		}
	}
	return TopologyShard{ID: 0, Primary: primary, Replicas: replicas}, true
}

func normalizedTopologyVerifiesReplicationFingerprint(topology ClusterTopology) bool {
	return len(topology.Nodes) > 1 || len(topology.Shards) > 1 || len(topology.BucketRanges) > 0
}

func decodeTopologyJSONReader(reader io.Reader) (ClusterTopology, error) {
	return hatTopology.DecodeJSON(reader)
}

func normalizeTopology(topology ClusterTopology) (ClusterTopology, error) {
	return hatTopology.Normalize(topology)
}

func topologyMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return TopologyModeFullReplica
	}
	return mode
}

func shardForBucket(topology ClusterTopology, bucket uint32, shards []TopologyShard) (TopologyShard, bool) {
	for _, bucketRange := range topology.BucketRanges {
		if bucket >= bucketRange.Start && bucket <= bucketRange.End {
			for _, shard := range shards {
				if shard.ID == bucketRange.Shard {
					return shard, true
				}
			}
			return TopologyShard{}, false
		}
	}
	if len(shards) == 0 {
		return TopologyShard{}, false
	}
	return shards[int(bucket%uint32(len(shards)))], true
}

func fullReplicaShard(topology ClusterTopology) (TopologyShard, bool) {
	nodes := cloneNodes(topology.Nodes)
	if len(nodes) == 0 {
		return TopologyShard{}, false
	}
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})
	primary := strings.TrimSpace(topology.Self)
	if primary == "" || !topologyNodeExists(nodes, primary) {
		primary = nodes[0].ID
	}
	replicas := make([]string, 0, len(nodes)-1)
	for _, node := range nodes {
		if node.ID != primary {
			replicas = append(replicas, node.ID)
		}
	}
	return TopologyShard{ID: 0, Primary: primary, Replicas: replicas}, true
}

func topologyNodeExists(nodes []TopologyNode, id string) bool {
	for _, node := range nodes {
		if node.ID == id {
			return true
		}
	}
	return false
}

func routeOwners(shard TopologyShard) []string {
	owners := make([]string, 0, 1+len(shard.Replicas))
	if shard.Primary != "" {
		owners = append(owners, shard.Primary)
	}
	owners = append(owners, shard.Replicas...)
	return owners
}

func hashKeyToBucket(key string, bucketCount uint32) uint32 {
	if bucketCount == 0 {
		return 0
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return hash.Sum32() % bucketCount
}

func hashKeyToShardIndex(key string, shardCount int) int {
	if shardCount <= 0 {
		return 0
	}
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(shardCount))
}

func cloneTopology(topology ClusterTopology) ClusterTopology {
	return ClusterTopology{
		Version:      topology.Version,
		Mode:         topology.Mode,
		BucketCount:  topology.BucketCount,
		BucketRanges: cloneBucketRanges(topology.BucketRanges),
		Self:         topology.Self,
		Nodes:        cloneNodes(topology.Nodes),
		Shards:       cloneShards(topology.Shards),
	}
}

func cloneNodes(nodes []TopologyNode) []TopologyNode {
	if nodes == nil {
		return nil
	}
	out := make([]TopologyNode, len(nodes))
	copy(out, nodes)
	return out
}

func cloneShards(shards []TopologyShard) []TopologyShard {
	if shards == nil {
		return nil
	}
	out := make([]TopologyShard, len(shards))
	for idx, shard := range shards {
		out[idx] = cloneShard(shard)
	}
	return out
}

func cloneShard(shard TopologyShard) TopologyShard {
	out := shard
	if shard.Replicas != nil {
		out.Replicas = append([]string(nil), shard.Replicas...)
	}
	return out
}

func cloneBucketRanges(ranges []TopologyBucketRange) []TopologyBucketRange {
	if ranges == nil {
		return nil
	}
	out := make([]TopologyBucketRange, len(ranges))
	copy(out, ranges)
	return out
}
