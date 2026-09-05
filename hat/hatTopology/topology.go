// Package hatTopology provides immutable cluster topology validation, routing,
// fingerprints, and persistence for hatrie_cache deployments.
package hatTopology

import (
	"bufio"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	json "github.com/goccy/go-json"
)

// Version is the supported cluster topology file format version.
const Version uint64 = 1

const (
	TopologyModeSharded     = "sharded"
	TopologyModeFullReplica = "full_replica"
)

// ClusterTopology describes cache cluster nodes and a deterministic shard map.
type ClusterTopology struct {
	Version      uint64                `json:"version"`
	Mode         string                `json:"mode,omitempty"`
	BucketCount  uint32                `json:"bucket_count,omitempty"`
	BucketRanges []TopologyBucketRange `json:"bucket_ranges,omitempty"`
	Self         string                `json:"self,omitempty"`
	Nodes        []TopologyNode        `json:"nodes"`
	Shards       []TopologyShard       `json:"shards,omitempty"`
}

type TopologyNode struct {
	ID                string `json:"id"`
	Address           string `json:"address"`
	GRPCAddress       string `json:"grpc_address,omitempty"`
	Role              string `json:"role,omitempty"`
	FailureDomain     string `json:"failure_domain,omitempty"`
	Maintenance       bool   `json:"maintenance,omitempty"`
	MaintenanceReason string `json:"maintenance_reason,omitempty"`
	MaintenanceSince  string `json:"maintenance_since,omitempty"`
}

type TopologyShard struct {
	ID       uint32   `json:"id"`
	Primary  string   `json:"primary"`
	Replicas []string `json:"replicas,omitempty"`
}

type TopologyBucketRange struct {
	Start uint32 `json:"start"`
	End   uint32 `json:"end"`
	Shard uint32 `json:"shard"`
}

type TopologyRoute struct {
	Key    string        `json:"key"`
	Mode   string        `json:"mode"`
	Bucket *uint32       `json:"bucket,omitempty"`
	Shard  TopologyShard `json:"shard"`
	Owners []string      `json:"owners,omitempty"`
}

func SingleNodeTopology(nodeID, address string) ClusterTopology {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		nodeID = "local"
	}
	return ClusterTopology{Version: Version, Mode: TopologyModeFullReplica, Self: nodeID,
		Nodes: []TopologyNode{{ID: nodeID, Address: strings.TrimSpace(address), Role: "primary"}}}
}

// LoadTopology reads and validates one topology JSON file.
func LoadTopology(path string) (ClusterTopology, error) {
	file, err := os.Open(path)
	if err != nil {
		return ClusterTopology{}, err
	}
	defer file.Close()
	topology, err := DecodeJSON(file)
	if err != nil {
		return ClusterTopology{}, err
	}
	return Normalize(topology)
}

// SaveTopology validates and atomically writes one topology JSON file.
func SaveTopology(path string, topology ClusterTopology) error {
	normalized, err := Normalize(topology)
	if err != nil {
		return err
	}
	return writeJSONFileAtomic(path, normalized)
}

// DecodeJSON decodes exactly one topology JSON document.
func DecodeJSON(reader io.Reader) (ClusterTopology, error) {
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

// Normalize validates topology and returns a deterministic, independent copy.
func Normalize(topology ClusterTopology) (ClusterTopology, error) {
	if topology.Version == 0 {
		topology.Version = Version
	}
	if topology.Version != Version {
		return ClusterTopology{}, errors.New("hatriecache: unsupported topology version")
	}
	topology.Mode = ModeFor(topology)
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
	out := Clone(topology)
	out.Self = strings.TrimSpace(out.Self)
	nodeIDs := map[string]bool{}
	for idx := range out.Nodes {
		node := &out.Nodes[idx]
		node.ID, node.Address, node.GRPCAddress, node.Role = strings.TrimSpace(node.ID), strings.TrimSpace(node.Address), strings.TrimSpace(node.GRPCAddress), strings.TrimSpace(node.Role)
		node.FailureDomain = strings.TrimSpace(node.FailureDomain)
		node.MaintenanceReason, node.MaintenanceSince = strings.TrimSpace(node.MaintenanceReason), strings.TrimSpace(node.MaintenanceSince)
		if !node.Maintenance {
			node.MaintenanceReason, node.MaintenanceSince = "", ""
		}
		if node.ID == "" {
			return ClusterTopology{}, errors.New("hatriecache: topology node id is required")
		}
		if nodeIDs[node.ID] {
			return ClusterTopology{}, errors.New("hatriecache: duplicate topology node")
		}
		if node.Role != "" && node.Role != "primary" && node.Role != "replica" {
			return ClusterTopology{}, errors.New("hatriecache: topology node role must be primary or replica")
		}
		nodeIDs[node.ID] = true
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
		replicas, seen := make([]string, 0, len(shard.Replicas)), map[string]bool{shard.Primary: true}
		for _, replica := range shard.Replicas {
			replica = strings.TrimSpace(replica)
			if replica == "" {
				continue
			}
			if !nodeIDs[replica] {
				return ClusterTopology{}, errors.New("hatriecache: topology shard replica is not registered")
			}
			if seen[replica] {
				return ClusterTopology{}, errors.New("hatriecache: duplicate topology shard replica")
			}
			seen[replica] = true
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
	sort.Slice(out.Nodes, func(i, j int) bool { return out.Nodes[i].ID < out.Nodes[j].ID })
	sort.Slice(out.Shards, func(i, j int) bool { return out.Shards[i].ID < out.Shards[j].ID })
	return out, nil
}

// ValidateFailureDomainPlacement checks that every shard has enough distinct,
// explicitly known failure domains. A zero or negative minimum disables the
// policy without inspecting the topology, preserving legacy behavior.
func ValidateFailureDomainPlacement(topology ClusterTopology, minimumDistinctDomains int) error {
	if minimumDistinctDomains <= 0 {
		return nil
	}
	normalized, err := Normalize(topology)
	if err != nil {
		return err
	}
	domains := make(map[string]string, len(normalized.Nodes))
	for _, node := range normalized.Nodes {
		domains[node.ID] = node.FailureDomain
	}
	validateOwners := func(label string, owners []string) error {
		seen := make(map[string]struct{}, len(owners))
		for _, owner := range owners {
			domain := strings.TrimSpace(domains[owner])
			if domain == "" {
				return fmt.Errorf("hatriecache: %s node %q failure domain is required", label, owner)
			}
			seen[domain] = struct{}{}
		}
		if len(seen) < minimumDistinctDomains {
			return fmt.Errorf("hatriecache: %s requires at least %d distinct failure domains, got %d", label, minimumDistinctDomains, len(seen))
		}
		return nil
	}
	if normalized.Mode == TopologyModeFullReplica {
		owners := make([]string, 0, len(normalized.Nodes))
		for _, node := range normalized.Nodes {
			owners = append(owners, node.ID)
		}
		return validateOwners("full replica topology", owners)
	}
	for _, shard := range normalized.Shards {
		owners := make([]string, 1, 1+len(shard.Replicas))
		owners[0] = shard.Primary
		owners = append(owners, shard.Replicas...)
		if err := validateOwners(fmt.Sprintf("shard %d", shard.ID), owners); err != nil {
			return err
		}
	}
	return nil
}

func TopologyMode(mode string) string {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		return TopologyModeFullReplica
	}
	return mode
}

func ModeFor(topology ClusterTopology) string {
	if strings.TrimSpace(topology.Mode) == "" && (len(topology.Shards) > 0 || topology.BucketCount > 0 || len(topology.BucketRanges) > 0) {
		return TopologyModeSharded
	}
	return TopologyMode(topology.Mode)
}

func (topology ClusterTopology) Fingerprint() string {
	normalized, err := Normalize(topology)
	if err != nil {
		return ""
	}
	normalized.Self = ""
	hash := fnv.New64a()
	part := func(value string) { _, _ = io.WriteString(hash, value); _, _ = hash.Write([]byte{0}) }
	part(strconv.FormatUint(normalized.Version, 10))
	part(normalized.Mode)
	part(strconv.FormatUint(uint64(normalized.BucketCount), 10))
	for _, bucketRange := range normalized.BucketRanges {
		part(strconv.FormatUint(uint64(bucketRange.Start), 10))
		part(strconv.FormatUint(uint64(bucketRange.End), 10))
		part(strconv.FormatUint(uint64(bucketRange.Shard), 10))
	}
	for _, node := range normalized.Nodes {
		part(node.ID)
		part(node.Address)
		part(node.GRPCAddress)
		part(node.Role)
	}
	for _, shard := range normalized.Shards {
		part(strconv.FormatUint(uint64(shard.ID), 10))
		part(shard.Primary)
		for _, replica := range shard.Replicas {
			part(replica)
		}
	}
	return strconv.FormatUint(hash.Sum64(), 16)
}

func (topology ClusterTopology) ShardForKey(key string) (TopologyShard, bool) {
	route, ok := topology.RouteForKey(key)
	return route.Shard, ok
}

func (topology ClusterTopology) RouteForKey(key string) (TopologyRoute, bool) {
	mode := ModeFor(topology)
	if mode == TopologyModeFullReplica {
		shard, ok := topology.FullReplicaShard()
		if !ok {
			return TopologyRoute{}, false
		}
		return TopologyRoute{Key: key, Mode: mode, Shard: shard, Owners: Owners(shard)}, true
	}
	shards := cloneShards(topology.Shards)
	if len(shards) == 0 {
		return TopologyRoute{}, false
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i].ID < shards[j].ID })
	if topology.BucketCount > 0 {
		bucket := HashKeyToBucket(key, topology.BucketCount)
		shard, ok := topology.shardForBucket(bucket, shards)
		if !ok {
			return TopologyRoute{}, false
		}
		return TopologyRoute{Key: key, Mode: mode, Bucket: &bucket, Shard: shard, Owners: Owners(shard)}, true
	}
	shard := shards[HashKeyToShardIndex(key, len(shards))]
	return TopologyRoute{Key: key, Mode: mode, Shard: shard, Owners: Owners(shard)}, true
}

func (topology ClusterTopology) FullReplicaShard() (TopologyShard, bool) {
	nodes := cloneNodes(topology.Nodes)
	if len(nodes) == 0 {
		return TopologyShard{}, false
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	primary := strings.TrimSpace(topology.Self)
	if primary == "" || !nodeExists(nodes, primary) {
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

func FindNode(nodes []TopologyNode, nodeID string) (TopologyNode, bool) {
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

func Owners(shard TopologyShard) []string {
	owners := make([]string, 0, 1+len(shard.Replicas))
	if shard.Primary != "" {
		owners = append(owners, shard.Primary)
	}
	return append(owners, shard.Replicas...)
}
func HashKeyToBucket(key string, bucketCount uint32) uint32 {
	if bucketCount == 0 {
		return 0
	}
	hash := fnv.New32a()
	_, _ = io.WriteString(hash, key)
	return hash.Sum32() % bucketCount
}
func HashKeyToShardIndex(key string, shardCount int) int {
	if shardCount <= 0 {
		return 0
	}
	hash := fnv.New32a()
	_, _ = io.WriteString(hash, key)
	return int(hash.Sum32() % uint32(shardCount))
}
func Clone(topology ClusterTopology) ClusterTopology {
	return ClusterTopology{Version: topology.Version, Mode: topology.Mode, BucketCount: topology.BucketCount, BucketRanges: cloneBucketRanges(topology.BucketRanges), Self: topology.Self, Nodes: cloneNodes(topology.Nodes), Shards: cloneShards(topology.Shards)}
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
		item := &topology.BucketRanges[idx]
		if item.Start > item.End {
			return errors.New("hatriecache: topology bucket range start exceeds end")
		}
		if item.End >= topology.BucketCount {
			return errors.New("hatriecache: topology bucket range exceeds bucket_count")
		}
		if !shardIDs[item.Shard] {
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

func nodeExists(nodes []TopologyNode, id string) bool {
	for _, node := range nodes {
		if node.ID == id {
			return true
		}
	}
	return false
}
func cloneNodes(nodes []TopologyNode) []TopologyNode {
	if nodes == nil {
		return nil
	}
	return append([]TopologyNode(nil), nodes...)
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
	out.Replicas = append([]string(nil), shard.Replicas...)
	return out
}
func cloneBucketRanges(ranges []TopologyBucketRange) []TopologyBucketRange {
	if ranges == nil {
		return nil
	}
	return append([]TopologyBucketRange(nil), ranges...)
}

func writeJSONFileAtomic(path string, value any) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	file, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temporary := file.Name()
	cleanup := func() {
		_ = file.Close()
		_ = os.Remove(temporary)
	}
	writer := bufio.NewWriter(file)
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		cleanup()
		return err
	}
	if err := writer.Flush(); err != nil {
		cleanup()
		return err
	}
	if err := file.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return err
	}
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := directory.Sync(); err != nil {
		_ = directory.Close()
		return err
	}
	return directory.Close()
}
