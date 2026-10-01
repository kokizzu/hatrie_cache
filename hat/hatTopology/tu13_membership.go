package hatTopology

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

const (
	// MembershipSnapshotVersion is the durable membership snapshot format.
	MembershipSnapshotVersion uint64 = 1
	// DefaultMembershipLogMaxRecords bounds retained membership audit records.
	DefaultMembershipLogMaxRecords = 1024
	// MaxMembershipLogRecords prevents an accidental unbounded in-memory log.
	MaxMembershipLogRecords = 1 << 20
	// MaxMembershipSnapshotBytes bounds JSON before it is decoded.
	MaxMembershipSnapshotBytes = 4 << 20
)

var (
	ErrMembershipLogInvalid          = errors.New("hatTopology: invalid membership log")
	ErrMembershipSnapshotInvalid     = errors.New("hatTopology: invalid membership snapshot")
	ErrMembershipNodeExists          = errors.New("hatTopology: membership node already exists")
	ErrMembershipNodeMissing         = errors.New("hatTopology: membership node does not exist")
	ErrMembershipNodeOwnsShard       = errors.New("hatTopology: membership node still owns a shard")
	ErrMembershipGenerationExhausted = errors.New("hatTopology: membership generation exhausted")
	ErrMembershipPersistence         = errors.New("hatTopology: membership persistence failed")
	ErrMembershipCommitStale         = errors.New("hatTopology: membership commit is already superseded")
)

// MembershipOperationKind identifies a durable membership transition.
type MembershipOperationKind string

const (
	MembershipJoin  MembershipOperationKind = "join"
	MembershipLeave MembershipOperationKind = "leave"
)

// MembershipOperation is the validated intent recorded for one membership
// transition. Shard ownership is deliberately not changed by a join or leave.
type MembershipOperation struct {
	Kind   MembershipOperationKind `json:"kind"`
	Node   TopologyNode            `json:"node,omitempty"`
	NodeID string                  `json:"node_id,omitempty"`
}

// MembershipProposal binds one join or leave to the topology fingerprint from
// which it was prepared. The caller must obtain a quorum decision for Commit.
type MembershipProposal struct {
	Generation uint64              `json:"generation"`
	Operation  MembershipOperation `json:"operation"`
	Commit     TopologyCommit      `json:"commit"`
}

// MembershipRecord is the durable audit record for one quorum-approved
// transition. The complete consensus decision is retained for replay checks.
type MembershipRecord struct {
	Generation           uint64                    `json:"generation"`
	Operation            MembershipOperation       `json:"operation"`
	ExpectedFingerprint  string                    `json:"expected_fingerprint"`
	CandidateFingerprint string                    `json:"candidate_fingerprint"`
	Consensus            TopologyConsensusDecision `json:"consensus"`
}

// MembershipSnapshot is an atomic, replayable view of membership state.
type MembershipSnapshot struct {
	Version    uint64             `json:"version"`
	Generation uint64             `json:"generation"`
	Topology   ClusterTopology    `json:"topology"`
	Records    []MembershipRecord `json:"records,omitempty"`
}

// MembershipLogOptions configures the bounded durable membership log. Path is
// optional; when set, successful commits atomically replace that file.
type MembershipLogOptions struct {
	Path       string
	MaxRecords int
}

// MembershipLog provides an opt-in, concurrency-safe membership state machine.
// Consensus transport remains caller-owned; Commit accepts only a validated
// TopologyConsensusDecision and persists the resulting state atomically.
type MembershipLog struct {
	mu         sync.RWMutex
	topology   ClusterTopology
	generation uint64
	records    []MembershipRecord
	path       string
	maxRecords int
}

// NewMembershipLog creates a log from an initial normalized topology. If Path
// already exists, the durable snapshot is loaded instead of using initial.
func NewMembershipLog(initial ClusterTopology, options MembershipLogOptions) (*MembershipLog, error) {
	normalizedOptions, err := normalizeMembershipLogOptions(options)
	if err != nil {
		return nil, err
	}
	if normalizedOptions.Path != "" {
		if _, statErr := os.Stat(normalizedOptions.Path); statErr == nil {
			return LoadMembershipLog(normalizedOptions.Path, normalizedOptions)
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: stat %q: %v", ErrMembershipPersistence, normalizedOptions.Path, statErr)
		}
	}
	normalized, err := Normalize(initial)
	if err != nil {
		return nil, fmt.Errorf("%w: initial topology: %v", ErrMembershipLogInvalid, err)
	}
	return &MembershipLog{
		topology:   normalized,
		generation: normalized.FencingToken,
		path:       normalizedOptions.Path,
		maxRecords: normalizedOptions.MaxRecords,
	}, nil
}

// LoadMembershipLog loads and validates one durable membership snapshot.
func LoadMembershipLog(path string, options MembershipLogOptions) (*MembershipLog, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: path is required", ErrMembershipLogInvalid)
	}
	options.Path = path
	normalizedOptions, err := normalizeMembershipLogOptions(options)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open %q: %v", ErrMembershipPersistence, path, err)
	}
	defer file.Close()
	encoded, err := io.ReadAll(io.LimitReader(file, MaxMembershipSnapshotBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read %q: %v", ErrMembershipPersistence, path, err)
	}
	if err := validateMembershipSnapshotSize(encoded); err != nil {
		return nil, err
	}
	snapshot, err := decodeMembershipSnapshot(encoded)
	if err != nil {
		return nil, err
	}
	return newMembershipLogFromSnapshot(snapshot, normalizedOptions)
}

func normalizeMembershipLogOptions(options MembershipLogOptions) (MembershipLogOptions, error) {
	options.Path = strings.TrimSpace(options.Path)
	if options.MaxRecords == 0 {
		options.MaxRecords = DefaultMembershipLogMaxRecords
	}
	if options.MaxRecords < 1 || options.MaxRecords > MaxMembershipLogRecords {
		return MembershipLogOptions{}, fmt.Errorf("%w: max records %d is outside 1..%d", ErrMembershipLogInvalid, options.MaxRecords, MaxMembershipLogRecords)
	}
	return options, nil
}

func newMembershipLogFromSnapshot(snapshot MembershipSnapshot, options MembershipLogOptions) (*MembershipLog, error) {
	validated, err := validateMembershipSnapshot(snapshot, options.MaxRecords)
	if err != nil {
		return nil, err
	}
	return &MembershipLog{
		topology:   validated.Topology,
		generation: validated.Generation,
		records:    validated.Records,
		path:       options.Path,
		maxRecords: options.MaxRecords,
	}, nil
}

// Current returns an independent normalized topology snapshot.
func (log *MembershipLog) Current() ClusterTopology {
	if log == nil {
		return ClusterTopology{}
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return Clone(log.topology)
}

// Generation returns the fencing generation of the current topology.
func (log *MembershipLog) Generation() uint64 {
	if log == nil {
		return 0
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.generation
}

// Records returns an independent, oldest-first audit snapshot.
func (log *MembershipLog) Records() []MembershipRecord {
	if log == nil {
		return nil
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return cloneMembershipRecords(log.records)
}

// Snapshot returns an independent durable state snapshot.
func (log *MembershipLog) Snapshot() MembershipSnapshot {
	if log == nil {
		return MembershipSnapshot{}
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.snapshotLocked()
}

// MarshalSnapshot encodes a bounded membership snapshot as strict JSON.
func (log *MembershipLog) MarshalSnapshot() ([]byte, error) {
	if log == nil {
		return nil, fmt.Errorf("%w: log is nil", ErrMembershipLogInvalid)
	}
	snapshot := log.Snapshot()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: encode snapshot: %v", ErrMembershipSnapshotInvalid, err)
	}
	if len(encoded) > MaxMembershipSnapshotBytes {
		return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrMembershipSnapshotInvalid, MaxMembershipSnapshotBytes)
	}
	return encoded, nil
}

// Save atomically writes the current snapshot to path. It does not change the
// log's configured automatic persistence path.
func (log *MembershipLog) Save(path string) error {
	if log == nil {
		return fmt.Errorf("%w: log is nil", ErrMembershipLogInvalid)
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return fmt.Errorf("%w: path is required", ErrMembershipLogInvalid)
	}
	snapshot := log.Snapshot()
	encoded, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("%w: encode snapshot: %v", ErrMembershipSnapshotInvalid, err)
	}
	if err := validateMembershipSnapshotSize(encoded); err != nil {
		return err
	}
	if err := writeJSONFileAtomic(path, snapshot); err != nil {
		return fmt.Errorf("%w: write %q: %v", ErrMembershipPersistence, path, err)
	}
	return nil
}

// ProposeJoin creates a fenced join proposal. It does not mutate the log.
func (log *MembershipLog) ProposeJoin(node TopologyNode) (MembershipProposal, error) {
	return log.propose(MembershipOperation{Kind: MembershipJoin, Node: node})
}

// ProposeLeave creates a fenced leave proposal. A node referenced by explicit
// shard ownership must be moved by a separate topology change first.
func (log *MembershipLog) ProposeLeave(nodeID string) (MembershipProposal, error) {
	return log.propose(MembershipOperation{Kind: MembershipLeave, NodeID: nodeID})
}

func (log *MembershipLog) propose(operation MembershipOperation) (MembershipProposal, error) {
	if log == nil {
		return MembershipProposal{}, fmt.Errorf("%w: log is nil", ErrMembershipLogInvalid)
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	candidate, err := membershipCandidate(log.topology, operation)
	if err != nil {
		return MembershipProposal{}, err
	}
	commit, err := NewTopologyCommit(log.topology.Fingerprint(), candidate)
	if err != nil {
		return MembershipProposal{}, fmt.Errorf("%w: proposal: %v", ErrMembershipLogInvalid, err)
	}
	return MembershipProposal{Generation: candidate.FencingToken, Operation: operation, Commit: commit}, nil
}

// Commit validates quorum evidence, applies the proposal, and atomically
// persists the new state when the log has a configured path.
func (log *MembershipLog) Commit(proposal MembershipProposal, decision TopologyConsensusDecision) error {
	if log == nil {
		return fmt.Errorf("%w: log is nil", ErrMembershipLogInvalid)
	}
	candidateFingerprint := proposal.Commit.CandidateFingerprint()
	if candidateFingerprint == "" {
		return fmt.Errorf("%w: candidate topology is invalid", ErrMembershipLogInvalid)
	}
	if err := ValidateTopologyConsensusDecision(decision, proposal.Commit.ExpectedFingerprint, candidateFingerprint); err != nil {
		return err
	}

	log.mu.Lock()
	defer log.mu.Unlock()
	currentFingerprint := log.topology.Fingerprint()
	if candidateFingerprint == currentFingerprint {
		if membershipRecordExists(log.records, candidateFingerprint) {
			return nil
		}
		return ErrMembershipCommitStale
	}
	if proposal.Commit.ExpectedFingerprint != currentFingerprint {
		return fmt.Errorf("%w: expected %q, current %q", ErrTopologyCommitConflict, proposal.Commit.ExpectedFingerprint, currentFingerprint)
	}
	candidate, err := membershipCandidate(log.topology, proposal.Operation)
	if err != nil {
		return err
	}
	if candidate.FencingToken != proposal.Generation || fingerprintNormalized(candidate) != candidateFingerprint {
		return fmt.Errorf("%w: proposal operation does not match candidate topology", ErrMembershipLogInvalid)
	}
	if err := ValidateTopologyCommit(log.topology, proposal.Commit); err != nil {
		return err
	}

	previousTopology := log.topology
	previousGeneration := log.generation
	previousRecords := log.records
	log.topology = candidate
	log.generation = candidate.FencingToken
	log.records = appendMembershipRecord(log.records, MembershipRecord{
		Generation:           candidate.FencingToken,
		Operation:            proposal.Operation,
		ExpectedFingerprint:  currentFingerprint,
		CandidateFingerprint: candidateFingerprint,
		Consensus:            cloneConsensusDecision(decision),
	}, log.maxRecords)
	if log.path != "" {
		snapshot := log.snapshotLocked()
		encoded, encodeErr := json.Marshal(snapshot)
		if encodeErr != nil {
			log.topology = previousTopology
			log.generation = previousGeneration
			log.records = previousRecords
			return fmt.Errorf("%w: encode snapshot: %v", ErrMembershipSnapshotInvalid, encodeErr)
		}
		if sizeErr := validateMembershipSnapshotSize(encoded); sizeErr != nil {
			log.topology = previousTopology
			log.generation = previousGeneration
			log.records = previousRecords
			return sizeErr
		}
		if err := writeJSONFileAtomic(log.path, snapshot); err != nil {
			log.topology = previousTopology
			log.generation = previousGeneration
			log.records = previousRecords
			return fmt.Errorf("%w: write %q: %v", ErrMembershipPersistence, log.path, err)
		}
	}
	return nil
}

func validateMembershipSnapshotSize(encoded []byte) error {
	if len(encoded) > MaxMembershipSnapshotBytes {
		return fmt.Errorf("%w: snapshot exceeds %d bytes", ErrMembershipSnapshotInvalid, MaxMembershipSnapshotBytes)
	}
	return nil
}

func membershipCandidate(current ClusterTopology, operation MembershipOperation) (ClusterTopology, error) {
	normalized, err := Normalize(current)
	if err != nil {
		return ClusterTopology{}, fmt.Errorf("%w: current topology: %v", ErrMembershipLogInvalid, err)
	}
	switch operation.Kind {
	case MembershipJoin:
		node := operation.Node
		node.ID = strings.TrimSpace(node.ID)
		if node.ID == "" {
			return ClusterTopology{}, fmt.Errorf("%w: join node id is required", ErrMembershipLogInvalid)
		}
		if topologyNodeIndex(normalized.Nodes, node.ID) >= 0 {
			return ClusterTopology{}, fmt.Errorf("%w: %q", ErrMembershipNodeExists, node.ID)
		}
		if strings.TrimSpace(node.Role) == "" {
			node.Role = "replica"
		}
		normalized.Nodes = append(normalized.Nodes, node)
	case MembershipLeave:
		nodeID := strings.TrimSpace(operation.NodeID)
		if nodeID == "" {
			return ClusterTopology{}, fmt.Errorf("%w: leave node id is required", ErrMembershipLogInvalid)
		}
		index := topologyNodeIndex(normalized.Nodes, nodeID)
		if index < 0 {
			return ClusterTopology{}, fmt.Errorf("%w: %q", ErrMembershipNodeMissing, nodeID)
		}
		for _, shard := range normalized.Shards {
			if shard.Primary == nodeID {
				return ClusterTopology{}, fmt.Errorf("%w: %q is primary for shard %d", ErrMembershipNodeOwnsShard, nodeID, shard.ID)
			}
			for _, replica := range shard.Replicas {
				if replica == nodeID {
					return ClusterTopology{}, fmt.Errorf("%w: %q is replica for shard %d", ErrMembershipNodeOwnsShard, nodeID, shard.ID)
				}
			}
		}
		normalized.Nodes = append(normalized.Nodes[:index], normalized.Nodes[index+1:]...)
		if normalized.Self == nodeID {
			normalized.Self = ""
		}
	default:
		return ClusterTopology{}, fmt.Errorf("%w: unsupported operation %q", ErrMembershipLogInvalid, operation.Kind)
	}
	if normalized.FencingToken == ^uint64(0) {
		return ClusterTopology{}, ErrMembershipGenerationExhausted
	}
	normalized.FencingToken++
	return Normalize(normalized)
}

func topologyNodeIndex(nodes []TopologyNode, nodeID string) int {
	for index, node := range nodes {
		if node.ID == nodeID {
			return index
		}
	}
	return -1
}

func (log *MembershipLog) snapshotLocked() MembershipSnapshot {
	return MembershipSnapshot{
		Version:    MembershipSnapshotVersion,
		Generation: log.generation,
		Topology:   Clone(log.topology),
		Records:    cloneMembershipRecords(log.records),
	}
}

func appendMembershipRecord(records []MembershipRecord, record MembershipRecord, maxRecords int) []MembershipRecord {
	if len(records) < maxRecords {
		return append(records, record)
	}
	rotated := make([]MembershipRecord, len(records))
	copy(rotated, records[1:])
	rotated[len(rotated)-1] = record
	return rotated
}

func membershipRecordExists(records []MembershipRecord, candidateFingerprint string) bool {
	for _, record := range records {
		if record.CandidateFingerprint == candidateFingerprint {
			return true
		}
	}
	return false
}

func cloneMembershipRecords(records []MembershipRecord) []MembershipRecord {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]MembershipRecord, len(records))
	for index, record := range records {
		cloned[index] = record
		cloned[index].Consensus = cloneConsensusDecision(record.Consensus)
	}
	return cloned
}

func cloneConsensusDecision(decision TopologyConsensusDecision) TopologyConsensusDecision {
	decision.Voters = append([]string(nil), decision.Voters...)
	decision.Acknowledged = append([]string(nil), decision.Acknowledged...)
	decision.Rejected = append([]string(nil), decision.Rejected...)
	return decision
}

func decodeMembershipSnapshot(encoded []byte) (MembershipSnapshot, error) {
	if len(encoded) == 0 || len(encoded) > MaxMembershipSnapshotBytes {
		return MembershipSnapshot{}, fmt.Errorf("%w: encoded length is invalid", ErrMembershipSnapshotInvalid)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var snapshot MembershipSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return MembershipSnapshot{}, fmt.Errorf("%w: decode: %v", ErrMembershipSnapshotInvalid, err)
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return MembershipSnapshot{}, fmt.Errorf("%w: trailing JSON value", ErrMembershipSnapshotInvalid)
		}
		return MembershipSnapshot{}, fmt.Errorf("%w: trailing data: %v", ErrMembershipSnapshotInvalid, err)
	}
	return snapshot, nil
}

func validateMembershipSnapshot(snapshot MembershipSnapshot, maxRecords int) (MembershipSnapshot, error) {
	if snapshot.Version != MembershipSnapshotVersion {
		return MembershipSnapshot{}, fmt.Errorf("%w: unsupported version %d", ErrMembershipSnapshotInvalid, snapshot.Version)
	}
	if len(snapshot.Records) > maxRecords {
		return MembershipSnapshot{}, fmt.Errorf("%w: record count %d exceeds %d", ErrMembershipSnapshotInvalid, len(snapshot.Records), maxRecords)
	}
	normalized, err := Normalize(snapshot.Topology)
	if err != nil {
		return MembershipSnapshot{}, fmt.Errorf("%w: topology: %v", ErrMembershipSnapshotInvalid, err)
	}
	if normalized.FencingToken != snapshot.Generation {
		return MembershipSnapshot{}, fmt.Errorf("%w: generation %d does not match fencing token %d", ErrMembershipSnapshotInvalid, snapshot.Generation, normalized.FencingToken)
	}
	previousGeneration := uint64(0)
	for index, record := range snapshot.Records {
		if record.Generation == 0 || (index > 0 && record.Generation <= previousGeneration) || record.Generation > snapshot.Generation {
			return MembershipSnapshot{}, fmt.Errorf("%w: invalid record generation at %d", ErrMembershipSnapshotInvalid, index)
		}
		if record.ExpectedFingerprint == "" || record.CandidateFingerprint == "" {
			return MembershipSnapshot{}, fmt.Errorf("%w: record fingerprints are required at %d", ErrMembershipSnapshotInvalid, index)
		}
		if err := ValidateTopologyConsensusDecision(record.Consensus, record.ExpectedFingerprint, record.CandidateFingerprint); err != nil {
			return MembershipSnapshot{}, fmt.Errorf("%w: record consensus at %d: %v", ErrMembershipSnapshotInvalid, index, err)
		}
		if _, err := membershipOperationShape(record.Operation); err != nil {
			return MembershipSnapshot{}, fmt.Errorf("%w: record operation at %d: %v", ErrMembershipSnapshotInvalid, index, err)
		}
		previousGeneration = record.Generation
	}
	if len(snapshot.Records) > 0 && snapshot.Records[len(snapshot.Records)-1].CandidateFingerprint != normalized.Fingerprint() {
		return MembershipSnapshot{}, fmt.Errorf("%w: final record does not describe the current topology", ErrMembershipSnapshotInvalid)
	}
	snapshot.Topology = normalized
	snapshot.Records = cloneMembershipRecords(snapshot.Records)
	return snapshot, nil
}

func membershipOperationShape(operation MembershipOperation) (MembershipOperation, error) {
	switch operation.Kind {
	case MembershipJoin:
		operation.Node.ID = strings.TrimSpace(operation.Node.ID)
		if operation.Node.ID == "" {
			return MembershipOperation{}, fmt.Errorf("join node id is required")
		}
	case MembershipLeave:
		operation.NodeID = strings.TrimSpace(operation.NodeID)
		if operation.NodeID == "" {
			return MembershipOperation{}, fmt.Errorf("leave node id is required")
		}
	default:
		return MembershipOperation{}, fmt.Errorf("unsupported operation %q", operation.Kind)
	}
	return operation, nil
}
