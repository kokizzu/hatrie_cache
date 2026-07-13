package hatriecache

import (
	"bytes"
	"encoding/json"
	"errors"
	"hash/fnv"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
)

const clusterTopologyVersion = 1

// ClusterTopology describes the cache cluster nodes and deterministic shard map.
type ClusterTopology struct {
	Version uint64          `json:"version"`
	Self    string          `json:"self,omitempty"`
	Nodes   []TopologyNode  `json:"nodes"`
	Shards  []TopologyShard `json:"shards"`
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

// TopologyRoute reports which shard owns a key.
type TopologyRoute struct {
	Key   string        `json:"key"`
	Shard TopologyShard `json:"shard"`
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
	data, err := os.ReadFile(path)
	if err != nil {
		return ClusterTopology{}, err
	}
	topology, err := decodeTopologyJSON(data)
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
	data, err := json.MarshalIndent(normalized, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFileAtomic(path, data)
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
	shard, ok := store.topology.ShardForKey(key)
	if !ok {
		return TopologyRoute{}, false
	}
	return TopologyRoute{Key: key, Shard: shard}, true
}

// ShardForKey returns the shard selected for key.
func (topology ClusterTopology) ShardForKey(key string) (TopologyShard, bool) {
	if len(topology.Shards) == 0 {
		return TopologyShard{}, false
	}
	shards := cloneShards(topology.Shards)
	sort.Slice(shards, func(i, j int) bool {
		return shards[i].ID < shards[j].ID
	})
	idx := hashKeyToShardIndex(key, len(shards))
	return shards[idx], true
}

func decodeTopologyJSON(data []byte) (ClusterTopology, error) {
	var topology ClusterTopology
	decoder := json.NewDecoder(bytes.NewReader(data))
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
	if len(topology.Nodes) == 0 {
		return ClusterTopology{}, errors.New("hatriecache: topology requires at least one node")
	}
	if len(topology.Shards) == 0 {
		return ClusterTopology{}, errors.New("hatriecache: topology requires at least one shard")
	}

	out := ClusterTopology{
		Version: topology.Version,
		Self:    strings.TrimSpace(topology.Self),
		Nodes:   cloneNodes(topology.Nodes),
		Shards:  cloneShards(topology.Shards),
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

	sort.Slice(out.Nodes, func(i, j int) bool {
		return out.Nodes[i].ID < out.Nodes[j].ID
	})
	sort.Slice(out.Shards, func(i, j int) bool {
		return out.Shards[i].ID < out.Shards[j].ID
	})
	return out, nil
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
		Version: topology.Version,
		Self:    topology.Self,
		Nodes:   cloneNodes(topology.Nodes),
		Shards:  cloneShards(topology.Shards),
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
