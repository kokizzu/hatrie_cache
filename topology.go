package hatriecache

import (
	"errors"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	json "github.com/goccy/go-json"
)

const clusterTopologyVersion = 1

const (
	TopologyModeSharded     = "sharded"
	TopologyModeFullReplica = "full_replica"
)

// ClusterTopology describes the cache cluster nodes and deterministic shard map.
type ClusterTopology struct {
	Version      uint64                `json:"version"`
	Mode         string                `json:"mode,omitempty"`
	BucketCount  uint32                `json:"bucket_count,omitempty"`
	BucketRanges []TopologyBucketRange `json:"bucket_ranges,omitempty"`
	Self         string                `json:"self,omitempty"`
	Nodes        []TopologyNode        `json:"nodes"`
	Shards       []TopologyShard       `json:"shards,omitempty"`
}

// TopologyNode describes one cache node address and optional role.
type TopologyNode struct {
	ID      string `json:"id"`
	Address string `json:"address"`
	Role    string `json:"role,omitempty"`
}

// TopologyShard describes one shard primary and optional replicas.
type TopologyShard struct {
	ID       uint32   `json:"id"`
	Primary  string   `json:"primary"`
	Replicas []string `json:"replicas,omitempty"`
}

// TopologyBucketRange maps an inclusive virtual-bucket range to one shard.
type TopologyBucketRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
	Shard uint32 `json:"shard"`
}

// TopologyRoute reports which shard owns a key.
type TopologyRoute struct {
	Key    string        `json:"key"`
	Mode   string        `json:"mode"`
	Bucket *uint32       `json:"bucket,omitempty"`
	Shard  TopologyShard `json:"shard"`
	Owners []string      `json:"owners,omitempty"`
}

// TopologyStore stores a validated topology and optionally persists updates.
type TopologyStore struct {
	mu       sync.RWMutex
	path     string
	topology ClusterTopology
}

// SingleNodeTopology returns a valid one-node, one-shard topology.
func SingleNodeTopology(nodeID string, address string) ClusterTopology {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		nodeID = "local"
	}
	return ClusterTopology{
		Version: clusterTopologyVersion,
		Mode:    TopologyModeSharded,
		Self:    nodeID,
		Nodes: []TopologyNode{
			{ID: nodeID, Address: strings.TrimSpace(address), Role: "primary"},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: nodeID},
		},
	}
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
	return &TopologyStore{path: path, topology: normalized}, nil
}

// NewTopologyStore validates and stores topology in memory.
func NewTopologyStore(topology ClusterTopology) (*TopologyStore, error) {
	normalized, err := normalizeTopology(topology)
	if err != nil {
		return nil, err
	}
	return &TopologyStore{topology: normalized}, nil
}

// LoadTopology reads and validates topology JSON from disk.
func LoadTopology(path string) (ClusterTopology, error) {
	file, err := os.Open(path)
	if err != nil {
		return ClusterTopology{}, err
	}
	defer file.Close()

	topology, err := decodeTopologyJSONReader(file)
	if err != nil {
		return ClusterTopology{}, err
	}
	return normalizeTopology(topology)
}

// SaveTopology validates and writes topology JSON atomically.
func SaveTopology(path string, topology ClusterTopology) error {
	normalized, err := normalizeTopology(topology)
	if err != nil {
		return err
	}
	return writeJSONFileAtomic(path, normalized)
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

// Fingerprint returns a stable content hash for the current topology. The local
// Self field is ignored so the same cluster file can be compared across nodes.
func (store *TopologyStore) Fingerprint() string {
	if store == nil {
		return ""
	}
	return store.Get().Fingerprint()
}

// VerifiesReplicationFingerprint reports whether the store has enough cluster
// routing metadata to reject replication from a different topology.
func (store *TopologyStore) VerifiesReplicationFingerprint() bool {
	if store == nil {
		return false
	}
	return store.Get().verifiesReplicationFingerprint()
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
	if store.path != "" {
		if err := SaveTopology(store.path, normalized); err != nil {
			return err
		}
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	store.topology = normalized
	return nil
}

// Route returns the shard selected for key by the current topology.
func (store *TopologyStore) Route(key string) (TopologyRoute, bool) {
	if store == nil {
		return TopologyRoute{}, false
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.topology.RouteForKey(key)
}

// Fingerprint returns a stable content hash for topology routing and ownership.
// The Self field is ignored because each node may set it to its own id.
func (topology ClusterTopology) Fingerprint() string {
	normalized, err := normalizeTopology(topology)
	if err != nil {
		return ""
	}
	normalized.Self = ""
	sort.Slice(normalized.Nodes, func(i, j int) bool {
		return normalized.Nodes[i].ID < normalized.Nodes[j].ID
	})
	sort.Slice(normalized.Shards, func(i, j int) bool {
		return normalized.Shards[i].ID < normalized.Shards[j].ID
	})
	sort.Slice(normalized.BucketRanges, func(i, j int) bool {
		if normalized.BucketRanges[i].Start != normalized.BucketRanges[j].Start {
			return normalized.BucketRanges[i].Start < normalized.BucketRanges[j].Start
		}
		if normalized.BucketRanges[i].End != normalized.BucketRanges[j].End {
			return normalized.BucketRanges[i].End < normalized.BucketRanges[j].End
		}
		return normalized.BucketRanges[i].Shard < normalized.BucketRanges[j].Shard
	})

	hash := fnv.New64a()
	writeTopologyFingerprintPart(hash, strconv.FormatUint(normalized.Version, 10))
	writeTopologyFingerprintPart(hash, normalized.Mode)
	writeTopologyFingerprintPart(hash, strconv.FormatUint(uint64(normalized.BucketCount), 10))
	for _, bucketRange := range normalized.BucketRanges {
		writeTopologyFingerprintPart(hash, strconv.FormatUint(uint64(bucketRange.Start), 10))
		writeTopologyFingerprintPart(hash, strconv.FormatUint(uint64(bucketRange.End), 10))
		writeTopologyFingerprintPart(hash, strconv.FormatUint(uint64(bucketRange.Shard), 10))
	}
	for _, node := range normalized.Nodes {
		writeTopologyFingerprintPart(hash, node.ID)
		writeTopologyFingerprintPart(hash, node.Address)
		writeTopologyFingerprintPart(hash, node.Role)
	}
	for _, shard := range normalized.Shards {
		writeTopologyFingerprintPart(hash, strconv.FormatUint(uint64(shard.ID), 10))
		writeTopologyFingerprintPart(hash, shard.Primary)
		replicas := append([]string(nil), shard.Replicas...)
		sort.Strings(replicas)
		for _, replica := range replicas {
			writeTopologyFingerprintPart(hash, replica)
		}
	}
	return strconv.FormatUint(hash.Sum64(), 16)
}

func writeTopologyFingerprintPart(writer io.Writer, value string) {
	_, _ = writer.Write([]byte(value))
	_, _ = writer.Write([]byte{0})
}

func (topology ClusterTopology) verifiesReplicationFingerprint() bool {
	normalized, err := normalizeTopology(topology)
	if err != nil {
		return false
	}
	return len(normalized.Nodes) > 1 || len(normalized.Shards) > 1 || len(normalized.BucketRanges) > 0
}

// ShardForKey returns the shard selected for key.
func (topology ClusterTopology) ShardForKey(key string) (TopologyShard, bool) {
	route, ok := topology.RouteForKey(key)
	return route.Shard, ok
}

// RouteForKey returns the deterministic route selected for key.
func (topology ClusterTopology) RouteForKey(key string) (TopologyRoute, bool) {
	mode := topologyMode(topology.Mode)
	if mode == TopologyModeFullReplica {
		shard, ok := topology.fullReplicaShard()
		if !ok {
			return TopologyRoute{}, false
		}
		return TopologyRoute{Key: key, Mode: mode, Shard: shard, Owners: routeOwners(shard)}, true
	}

	shards := cloneShards(topology.Shards)
	if len(shards) == 0 {
		return TopologyRoute{}, false
	}
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].ID < shards[j].ID
	})

	if topology.BucketCount > 0 {
		bucket := hashKeyToBucket(key, topology.BucketCount)
		shard, ok := topology.shardForBucket(bucket, shards)
		if !ok {
			return TopologyRoute{}, false
		}
		return TopologyRoute{
			Key:    key,
			Mode:   mode,
			Bucket: &bucket,
			Shard:  shard,
			Owners: routeOwners(shard),
		}, true
	}

	idx := hashKeyToShardIndex(key, len(shards))
	shard := shards[idx]
	return TopologyRoute{Key: key, Mode: mode, Shard: shard, Owners: routeOwners(shard)}, true
}

func decodeTopologyJSONReader(reader io.Reader) (ClusterTopology, error) {
	var topology ClusterTopology
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&topology); err != nil {
		return ClusterTopology{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return ClusterTopology{}, errors.New("hatriecache: invalid topology JSON")
		}
		return ClusterTopology{}, err
	}
	return topology, nil
}

func normalizeTopology(topology ClusterTopology) (ClusterTopology, error) {
	if topology.Version == 0 {
		topology.Version = clusterTopologyVersion
	}
	if topology.Version != clusterTopologyVersion {
		return ClusterTopology{}, errors.New("hatriecache: unsupported topology version")
	}
	topology.Mode = topologyMode(topology.Mode)
	if topology.Mode != TopologyModeSharded && topology.Mode != TopologyModeFullReplica {
		return ClusterTopology{}, errors.New("hatriecache: topology mode must be sharded or full_replica")
	}
	if len(topology.Nodes) == 0 {
		return ClusterTopology{}, errors.New("hatriecache: topology requires at least one node")
	}
	if topology.Mode == TopologyModeSharded && len(topology.Shards) == 0 {
		return ClusterTopology{}, errors.New("hatriecache: topology requires at least one shard")
	}
	if topology.Mode == TopologyModeFullReplica && len(topology.BucketRanges) > 0 {
		return ClusterTopology{}, errors.New("hatriecache: full replica topology cannot define bucket ranges")
	}
	if topology.Mode == TopologyModeFullReplica && topology.BucketCount != 0 {
		return ClusterTopology{}, errors.New("hatriecache: full replica topology cannot define bucket_count")
	}

	out := ClusterTopology{
		Version:      topology.Version,
		Mode:         topology.Mode,
		BucketCount:  topology.BucketCount,
		BucketRanges: cloneBucketRanges(topology.BucketRanges),
		Self:         strings.TrimSpace(topology.Self),
		Nodes:        cloneNodes(topology.Nodes),
		Shards:       cloneShards(topology.Shards),
	}

	nodeIDs := map[string]bool{}
	for idx := range out.Nodes {
		out.Nodes[idx].ID = strings.TrimSpace(out.Nodes[idx].ID)
		out.Nodes[idx].Address = strings.TrimSpace(out.Nodes[idx].Address)
		out.Nodes[idx].Role = strings.TrimSpace(out.Nodes[idx].Role)
		if out.Nodes[idx].ID == "" {
			return ClusterTopology{}, errors.New("hatriecache: topology node id is required")
		}
		if nodeIDs[out.Nodes[idx].ID] {
			return ClusterTopology{}, errors.New("hatriecache: duplicate topology node")
		}
		if out.Nodes[idx].Role != "" && out.Nodes[idx].Role != "primary" && out.Nodes[idx].Role != "replica" {
			return ClusterTopology{}, errors.New("hatriecache: topology node role must be primary or replica")
		}
		nodeIDs[out.Nodes[idx].ID] = true
	}
	if out.Self != "" && !nodeIDs[out.Self] {
		return ClusterTopology{}, errors.New("hatriecache: topology self node is not registered")
	}

	shardIDs := map[uint32]bool{}
	for idx := range out.Shards {
		shard := &out.Shards[idx]
		shard.Primary = strings.TrimSpace(shard.Primary)
		if shardIDs[shard.ID] {
			return ClusterTopology{}, errors.New("hatriecache: duplicate topology shard")
		}
		if !nodeIDs[shard.Primary] {
			return ClusterTopology{}, errors.New("hatriecache: topology shard primary is not registered")
		}
		shardIDs[shard.ID] = true

		replicas := make([]string, 0, len(shard.Replicas))
		seenReplicas := map[string]bool{shard.Primary: true}
		for _, replica := range shard.Replicas {
			replica = strings.TrimSpace(replica)
			if replica == "" {
				continue
			}
			if !nodeIDs[replica] {
				return ClusterTopology{}, errors.New("hatriecache: topology shard replica is not registered")
			}
			if seenReplicas[replica] {
				return ClusterTopology{}, errors.New("hatriecache: duplicate topology shard replica")
			}
			seenReplicas[replica] = true
			replicas = append(replicas, replica)
		}
		sort.Strings(replicas)
		if len(replicas) == 0 {
			shard.Replicas = nil
		} else {
			shard.Replicas = replicas
		}
	}

	if err := normalizeBucketRanges(&out, shardIDs); err != nil {
		return ClusterTopology{}, err
	}

	sort.Slice(out.Nodes, func(i, j int) bool {
		return out.Nodes[i].ID < out.Nodes[j].ID
	})
	sort.Slice(out.Shards, func(i, j int) bool {
		return out.Shards[i].ID < out.Shards[j].ID
	})
	return out, nil
}

func topologyMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return TopologyModeSharded
	}
	return mode
}

func normalizeBucketRanges(topology *ClusterTopology, shardIDs map[uint32]bool) error {
	if len(topology.BucketRanges) == 0 {
		return nil
	}
	if topology.Mode != TopologyModeSharded {
		return errors.New("hatriecache: bucket ranges require sharded topology")
	}
	if topology.BucketCount == 0 {
		return errors.New("hatriecache: bucket_count is required for bucket ranges")
	}
	for idx := range topology.BucketRanges {
		bucketRange := &topology.BucketRanges[idx]
		if bucketRange.Start > bucketRange.End {
			return errors.New("hatriecache: topology bucket range start exceeds end")
		}
		if bucketRange.End >= topology.BucketCount {
			return errors.New("hatriecache: topology bucket range exceeds bucket_count")
		}
		if !shardIDs[bucketRange.Shard] {
			return errors.New("hatriecache: topology bucket range shard is not registered")
		}
	}
	sort.Slice(topology.BucketRanges, func(i, j int) bool {
		if topology.BucketRanges[i].Start == topology.BucketRanges[j].Start {
			return topology.BucketRanges[i].End < topology.BucketRanges[j].End
		}
		return topology.BucketRanges[i].Start < topology.BucketRanges[j].Start
	})
	if topology.BucketRanges[0].Start != 0 {
		return errors.New("hatriecache: topology bucket ranges must start at zero")
	}
	for idx := 1; idx < len(topology.BucketRanges); idx++ {
		if topology.BucketRanges[idx].Start != topology.BucketRanges[idx-1].End+1 {
			return errors.New("hatriecache: topology bucket ranges must not overlap or leave gaps")
		}
	}
	if topology.BucketRanges[len(topology.BucketRanges)-1].End != topology.BucketCount-1 {
		return errors.New("hatriecache: topology bucket ranges must cover every bucket")
	}
	return nil
}

func (topology ClusterTopology) shardForBucket(bucket uint32, shards []TopologyShard) (TopologyShard, bool) {
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

func (topology ClusterTopology) fullReplicaShard() (TopologyShard, bool) {
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
		out[idx] = shard
		if shard.Replicas != nil {
			out[idx].Replicas = append([]string(nil), shard.Replicas...)
		}
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
