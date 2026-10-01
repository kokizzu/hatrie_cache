package hatTopology

import (
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"

	json "github.com/goccy/go-json"
)

// MembershipVersion is the durable membership file format version.
const MembershipVersion uint64 = 1

// MembershipChangeKind identifies a durable membership transition.
type MembershipChangeKind string

const (
	MembershipJoin  MembershipChangeKind = "join"
	MembershipLeave MembershipChangeKind = "leave"
)

var (
	ErrMembershipInvalid            = errors.New("hatriecache: invalid membership change")
	ErrMembershipCorrupt            = errors.New("hatriecache: corrupt durable membership state")
	ErrMembershipInitialMismatch    = errors.New("hatriecache: durable membership initial topology mismatch")
	ErrMembershipGenerationConflict = errors.New("hatriecache: membership generation conflict")
	ErrMembershipFencingStale       = errors.New("hatriecache: membership fencing token is stale")
	ErrMembershipChangeConflict     = errors.New("hatriecache: membership change id conflicts with prior change")
	ErrMembershipNodeExists         = errors.New("hatriecache: membership node already exists")
	ErrMembershipNodeMissing        = errors.New("hatriecache: membership node is missing")
	ErrMembershipNodeReferenced     = errors.New("hatriecache: membership node is referenced by a shard")
	ErrMembershipSelfLeave          = errors.New("hatriecache: membership self node cannot leave")
	ErrMembershipDurability         = errors.New("hatriecache: durable membership write failed")
)

// MembershipChange is a caller-proposed join or leave. ID is the durable
// idempotency key. ExpectedGeneration must match the current state unless the
// same ID and exact change are being retried.
type MembershipChange struct {
	ID                 string               `json:"id"`
	Kind               MembershipChangeKind `json:"kind"`
	ExpectedGeneration uint64               `json:"expected_generation"`
	FencingToken       uint64               `json:"fencing_token"`
	Node               TopologyNode         `json:"node,omitempty"`
	NodeID             string               `json:"node_id,omitempty"`
}

// MembershipRecord is one durable, generation-bound membership transition.
// The previous and resulting topology fingerprints make replay and audit
// checks independent of the caller's mutable topology value.
type MembershipRecord struct {
	ChangeID            string               `json:"change_id"`
	Kind                MembershipChangeKind `json:"kind"`
	ExpectedGeneration  uint64               `json:"expected_generation"`
	Generation          uint64               `json:"generation"`
	FencingToken        uint64               `json:"fencing_token"`
	NodeID              string               `json:"node_id"`
	Node                TopologyNode         `json:"node"`
	PreviousFingerprint string               `json:"previous_fingerprint"`
	TopologyFingerprint string               `json:"topology_fingerprint"`
}

// MembershipSnapshot is an immutable copy of the current durable state.
type MembershipSnapshot struct {
	Version      uint64
	Generation   uint64
	FencingToken uint64
	Topology     ClusterTopology
	LastChange   *MembershipRecord
}

// MembershipApplyResult reports one applied or idempotently replayed change.
type MembershipApplyResult struct {
	Snapshot MembershipSnapshot
	Record   MembershipRecord
	Replayed bool
}

// DurableMembershipStore owns a generation-checked membership state machine.
// It is process-safe and durable, but it is not a distributed consensus
// authority; callers must collect and validate votes before calling Apply.
type DurableMembershipStore struct {
	mu           sync.RWMutex
	path         string
	initial      ClusterTopology
	current      ClusterTopology
	generation   uint64
	fencingToken uint64
	records      []MembershipRecord
}

type durableMembershipFile struct {
	Version uint64             `json:"version"`
	Initial ClusterTopology    `json:"initial"`
	Records []MembershipRecord `json:"records,omitempty"`
}

// OpenDurableMembershipStore opens or creates a membership state file. When
// the file already exists, initial may be zero; a non-zero initial topology is
// checked against the durable bootstrap topology before opening.
func OpenDurableMembershipStore(path string, initial ClusterTopology) (*DurableMembershipStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: path is required", ErrMembershipInvalid)
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		normalized, normalizeErr := Normalize(initial)
		if normalizeErr != nil {
			return nil, fmt.Errorf("%w: initial topology: %v", ErrMembershipInvalid, normalizeErr)
		}
		store := &DurableMembershipStore{path: path, initial: normalized, current: Clone(normalized), fencingToken: normalized.FencingToken}
		if err := store.persistLocked(nil); err != nil {
			return nil, err
		}
		return store, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: open: %v", ErrMembershipDurability, err)
	}
	defer file.Close()
	durable, err := decodeDurableMembershipFile(file)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMembershipCorrupt, err)
	}
	if durable.Version != MembershipVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrMembershipCorrupt, durable.Version)
	}
	normalizedInitial, err := Normalize(durable.Initial)
	if err != nil {
		return nil, fmt.Errorf("%w: initial topology: %v", ErrMembershipCorrupt, err)
	}
	if hasTopologyValue(initial) {
		provided, normalizeErr := Normalize(initial)
		if normalizeErr != nil {
			return nil, fmt.Errorf("%w: provided topology: %v", ErrMembershipInitialMismatch, normalizeErr)
		}
		if !reflect.DeepEqual(provided, normalizedInitial) {
			return nil, ErrMembershipInitialMismatch
		}
	}
	current, generation, fencingToken, records, err := replayMembershipRecords(normalizedInitial, durable.Records)
	if err != nil {
		return nil, fmt.Errorf("%w: replay: %v", ErrMembershipCorrupt, err)
	}
	return &DurableMembershipStore{
		path:         path,
		initial:      normalizedInitial,
		current:      current,
		generation:   generation,
		fencingToken: fencingToken,
		records:      records,
	}, nil
}

// Snapshot returns an independently owned current membership snapshot.
func (store *DurableMembershipStore) Snapshot() MembershipSnapshot {
	if store == nil {
		return MembershipSnapshot{}
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return store.snapshotLocked()
}

// History returns an independently owned copy of the durable transition
// history in generation order.
func (store *DurableMembershipStore) History() []MembershipRecord {
	if store == nil {
		return nil
	}
	store.mu.RLock()
	defer store.mu.RUnlock()
	return cloneMembershipRecords(store.records)
}

// Apply validates, durably records, and publishes one membership change. The
// in-memory state is updated only after the fsync-plus-rename succeeds.
func (store *DurableMembershipStore) Apply(change MembershipChange) (MembershipApplyResult, error) {
	if store == nil {
		return MembershipApplyResult{}, fmt.Errorf("%w: store is nil", ErrMembershipInvalid)
	}
	normalizedChange, err := normalizeMembershipChange(change)
	if err != nil {
		return MembershipApplyResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	for index := len(store.records) - 1; index >= 0; index-- {
		record := store.records[index]
		if record.ChangeID != normalizedChange.ID {
			continue
		}
		if !membershipChangeMatches(record, normalizedChange) {
			return MembershipApplyResult{}, ErrMembershipChangeConflict
		}
		return MembershipApplyResult{Snapshot: store.snapshotLocked(), Record: record, Replayed: true}, nil
	}
	if normalizedChange.ExpectedGeneration != store.generation {
		return MembershipApplyResult{}, fmt.Errorf("%w: expected %d, current %d", ErrMembershipGenerationConflict, normalizedChange.ExpectedGeneration, store.generation)
	}
	if normalizedChange.FencingToken == 0 || normalizedChange.FencingToken <= store.fencingToken {
		return MembershipApplyResult{}, fmt.Errorf("%w: proposed %d, current %d", ErrMembershipFencingStale, normalizedChange.FencingToken, store.fencingToken)
	}
	if store.generation == ^uint64(0) {
		return MembershipApplyResult{}, fmt.Errorf("%w: generation exhausted", ErrMembershipInvalid)
	}
	next, nodeID, node, err := applyMembershipTopology(store.current, normalizedChange)
	if err != nil {
		return MembershipApplyResult{}, err
	}
	record := MembershipRecord{
		ChangeID:            normalizedChange.ID,
		Kind:                normalizedChange.Kind,
		ExpectedGeneration:  normalizedChange.ExpectedGeneration,
		Generation:          store.generation + 1,
		FencingToken:        normalizedChange.FencingToken,
		NodeID:              nodeID,
		Node:                node,
		PreviousFingerprint: fingerprintNormalized(store.current),
		TopologyFingerprint: fingerprintNormalized(next),
	}
	records := append(cloneMembershipRecords(store.records), record)
	if err := store.persistLocked(records); err != nil {
		return MembershipApplyResult{}, err
	}
	store.current = next
	store.generation = record.Generation
	store.fencingToken = record.FencingToken
	store.records = records
	return MembershipApplyResult{Snapshot: store.snapshotLocked(), Record: record}, nil
}

func normalizeMembershipChange(change MembershipChange) (MembershipChange, error) {
	change.ID = strings.TrimSpace(change.ID)
	change.Kind = MembershipChangeKind(strings.ToLower(strings.TrimSpace(string(change.Kind))))
	change.NodeID = strings.TrimSpace(change.NodeID)
	change.Node = normalizeMembershipNode(change.Node)
	if change.ID == "" {
		return MembershipChange{}, fmt.Errorf("%w: change id is required", ErrMembershipInvalid)
	}
	if change.Kind != MembershipJoin && change.Kind != MembershipLeave {
		return MembershipChange{}, fmt.Errorf("%w: unknown change kind %q", ErrMembershipInvalid, change.Kind)
	}
	if change.Kind == MembershipJoin && change.Node.ID == "" {
		return MembershipChange{}, fmt.Errorf("%w: join node id is required", ErrMembershipInvalid)
	}
	if change.Kind == MembershipJoin {
		change.NodeID = change.Node.ID
	}
	if change.Kind == MembershipLeave {
		if change.NodeID == "" {
			change.NodeID = change.Node.ID
		}
		if change.NodeID == "" {
			return MembershipChange{}, fmt.Errorf("%w: leave node id is required", ErrMembershipInvalid)
		}
		if change.Node.ID != "" && change.Node.ID != change.NodeID {
			return MembershipChange{}, fmt.Errorf("%w: leave node id disagrees with node", ErrMembershipInvalid)
		}
	}
	return change, nil
}

func normalizeMembershipNode(node TopologyNode) TopologyNode {
	node.ID = strings.TrimSpace(node.ID)
	node.Address = strings.TrimSpace(node.Address)
	node.GRPCAddress = strings.TrimSpace(node.GRPCAddress)
	node.Role = strings.TrimSpace(node.Role)
	node.FailureDomain = strings.TrimSpace(node.FailureDomain)
	node.Region = strings.TrimSpace(node.Region)
	node.MaintenanceReason = strings.TrimSpace(node.MaintenanceReason)
	node.MaintenanceSince = strings.TrimSpace(node.MaintenanceSince)
	if !node.Maintenance {
		node.MaintenanceReason, node.MaintenanceSince = "", ""
	}
	return node
}

func membershipChangeMatches(record MembershipRecord, change MembershipChange) bool {
	if record.ChangeID != change.ID || record.Kind != change.Kind || record.ExpectedGeneration != change.ExpectedGeneration || record.FencingToken != change.FencingToken || record.NodeID != change.NodeID {
		return false
	}
	if change.Kind == MembershipJoin {
		return reflect.DeepEqual(record.Node, change.Node)
	}
	nodeDetails := change.Node
	nodeDetails.ID = ""
	return nodeDetails == (TopologyNode{}) || reflect.DeepEqual(record.Node, change.Node)
}

func applyMembershipTopology(current ClusterTopology, change MembershipChange) (ClusterTopology, string, TopologyNode, error) {
	next := Clone(current)
	next.FencingToken = change.FencingToken
	switch change.Kind {
	case MembershipJoin:
		for _, existing := range current.Nodes {
			if existing.ID == change.Node.ID {
				return ClusterTopology{}, "", TopologyNode{}, fmt.Errorf("%w: %s", ErrMembershipNodeExists, change.Node.ID)
			}
		}
		next.Nodes = append(next.Nodes, change.Node)
		normalized, err := Normalize(next)
		if err != nil {
			return ClusterTopology{}, "", TopologyNode{}, fmt.Errorf("%w: join topology: %v", ErrMembershipInvalid, err)
		}
		for _, node := range normalized.Nodes {
			if node.ID == change.Node.ID {
				return normalized, node.ID, node, nil
			}
		}
	case MembershipLeave:
		if current.Self == change.NodeID {
			return ClusterTopology{}, "", TopologyNode{}, ErrMembershipSelfLeave
		}
		index := -1
		var removed TopologyNode
		for nodeIndex, node := range current.Nodes {
			if node.ID == change.NodeID {
				index, removed = nodeIndex, node
				break
			}
		}
		if index < 0 {
			return ClusterTopology{}, "", TopologyNode{}, fmt.Errorf("%w: %s", ErrMembershipNodeMissing, change.NodeID)
		}
		for _, shard := range current.Shards {
			if shard.Primary == change.NodeID {
				return ClusterTopology{}, "", TopologyNode{}, fmt.Errorf("%w: shard %d primary", ErrMembershipNodeReferenced, shard.ID)
			}
			for _, replica := range shard.Replicas {
				if replica == change.NodeID {
					return ClusterTopology{}, "", TopologyNode{}, fmt.Errorf("%w: shard %d replica", ErrMembershipNodeReferenced, shard.ID)
				}
			}
		}
		next.Nodes = append(next.Nodes[:index], next.Nodes[index+1:]...)
		normalized, err := Normalize(next)
		if err != nil {
			return ClusterTopology{}, "", TopologyNode{}, fmt.Errorf("%w: leave topology: %v", ErrMembershipInvalid, err)
		}
		return normalized, change.NodeID, removed, nil
	}
	return ClusterTopology{}, "", TopologyNode{}, fmt.Errorf("%w: membership operation produced no topology", ErrMembershipInvalid)
}

func (store *DurableMembershipStore) snapshotLocked() MembershipSnapshot {
	snapshot := MembershipSnapshot{
		Version:      MembershipVersion,
		Generation:   store.generation,
		FencingToken: store.fencingToken,
		Topology:     Clone(store.current),
	}
	if len(store.records) > 0 {
		last := store.records[len(store.records)-1]
		snapshot.LastChange = &last
	}
	return snapshot
}

func (store *DurableMembershipStore) persistLocked(records []MembershipRecord) error {
	if err := writeJSONFileAtomic(store.path, durableMembershipFile{Version: MembershipVersion, Initial: Clone(store.initial), Records: records}); err != nil {
		return fmt.Errorf("%w: %v", ErrMembershipDurability, err)
	}
	return nil
}

func decodeDurableMembershipFile(reader io.Reader) (durableMembershipFile, error) {
	var file durableMembershipFile
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&file); err != nil {
		return durableMembershipFile{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return durableMembershipFile{}, errors.New("trailing JSON document")
		}
		return durableMembershipFile{}, err
	}
	return file, nil
}

func replayMembershipRecords(initial ClusterTopology, records []MembershipRecord) (ClusterTopology, uint64, uint64, []MembershipRecord, error) {
	current := Clone(initial)
	var generation uint64
	fencingToken := initial.FencingToken
	seenIDs := make(map[string]struct{}, len(records))
	owned := cloneMembershipRecords(records)
	for index, record := range owned {
		if record.ChangeID == "" || (record.Kind != MembershipJoin && record.Kind != MembershipLeave) {
			return ClusterTopology{}, 0, 0, nil, fmt.Errorf("record %d has invalid identity", index)
		}
		if _, exists := seenIDs[record.ChangeID]; exists {
			return ClusterTopology{}, 0, 0, nil, fmt.Errorf("record %d repeats change id %q", index, record.ChangeID)
		}
		seenIDs[record.ChangeID] = struct{}{}
		if record.ExpectedGeneration != generation || record.Generation != generation+1 || record.FencingToken == 0 || record.FencingToken <= fencingToken {
			return ClusterTopology{}, 0, 0, nil, fmt.Errorf("record %d has invalid generation or fencing sequence", index)
		}
		if record.PreviousFingerprint != fingerprintNormalized(current) {
			return ClusterTopology{}, 0, 0, nil, fmt.Errorf("record %d previous fingerprint does not match", index)
		}
		change := MembershipChange{ID: record.ChangeID, Kind: record.Kind, ExpectedGeneration: record.ExpectedGeneration, FencingToken: record.FencingToken, Node: record.Node, NodeID: record.NodeID}
		next, nodeID, node, err := applyMembershipTopology(current, change)
		if err != nil || nodeID != record.NodeID || !reflect.DeepEqual(node, record.Node) {
			return ClusterTopology{}, 0, 0, nil, fmt.Errorf("record %d cannot be applied: %v", index, err)
		}
		if record.TopologyFingerprint != fingerprintNormalized(next) {
			return ClusterTopology{}, 0, 0, nil, fmt.Errorf("record %d topology fingerprint does not match", index)
		}
		current, generation, fencingToken = next, record.Generation, record.FencingToken
	}
	return current, generation, fencingToken, owned, nil
}

func cloneMembershipRecords(records []MembershipRecord) []MembershipRecord {
	if len(records) == 0 {
		return nil
	}
	return append([]MembershipRecord(nil), records...)
}

func hasTopologyValue(topology ClusterTopology) bool {
	return topology.Version != 0 || topology.Mode != "" || topology.BucketCount != 0 || len(topology.BucketRanges) > 0 || topology.Self != "" || topology.FencingToken != 0 || len(topology.Nodes) > 0 || len(topology.Shards) > 0
}
