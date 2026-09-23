package hatTopology

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	json "github.com/goccy/go-json"
)

const (
	MembershipOperationJoin  = "join"
	MembershipOperationLeave = "leave"

	MembershipJournalVersion        uint64 = 1
	DefaultMembershipJournalHistory        = 256
	MaxMembershipJournalHistory            = 65536
	DefaultMembershipJournalBytes          = 8 << 20
	MaxMembershipJournalBytes              = 64 << 20
	DefaultMembershipJournalReplay         = 256
	MaxMembershipJournalReplay             = 65536
	MaxMembershipOperationIDBytes          = 128
	MaxMembershipJournalNodes              = 65536

	membershipJournalMagic      = "HMM1"
	membershipJournalHeaderSize = 13
)

var (
	ErrMembershipJournalInvalidOptions     = errors.New("hatriecache: membership journal options are invalid")
	ErrMembershipJournalInvalidChange      = errors.New("hatriecache: membership change is invalid")
	ErrMembershipJournalInvalidProposal    = errors.New("hatriecache: membership proposal is invalid")
	ErrMembershipJournalGenerationConflict = errors.New("hatriecache: membership generation conflicts")
	ErrMembershipJournalFenceStale         = errors.New("hatriecache: membership fencing token is stale")
	ErrMembershipJournalOperationConflict  = errors.New("hatriecache: membership operation conflicts")
	ErrMembershipJournalNodeExists         = errors.New("hatriecache: membership node already exists")
	ErrMembershipJournalNodeMissing        = errors.New("hatriecache: membership node does not exist")
	ErrMembershipJournalLastNode           = errors.New("hatriecache: membership cannot remove the last node")
	ErrMembershipJournalHistoryGap         = errors.New("hatriecache: membership journal history gap")
	ErrMembershipJournalLimit              = errors.New("hatriecache: membership journal limit is invalid")
	ErrMembershipJournalFormat             = errors.New("hatriecache: membership journal format is invalid")
	ErrMembershipJournalChecksum           = errors.New("hatriecache: membership journal checksum mismatch")
	ErrMembershipJournalVersion            = errors.New("hatriecache: unsupported membership journal version")
	ErrMembershipJournalPersist            = errors.New("hatriecache: membership journal persistence failed")
)

var membershipJournalCRCTable = crc32.MakeTable(crc32.Castagnoli)

// MembershipJournalOptions configures a bounded membership journal. Path is
// optional; when set, every successful Apply atomically persists the complete
// bounded state before publishing it in memory.
type MembershipJournalOptions struct {
	Path       string
	MaxHistory int
	MaxBytes   int
}

// MembershipChange is a generation-fenced join or leave proposal. A change is
// idempotent by OperationID when the complete proposal is retried unchanged.
type MembershipChange struct {
	Operation          string       `json:"operation"`
	OperationID        string       `json:"operation_id"`
	ExpectedGeneration uint64       `json:"expected_generation"`
	FencingToken       uint64       `json:"fencing_token"`
	Node               TopologyNode `json:"node"`
}

// MembershipRecord is the durable result of one accepted membership change.
type MembershipRecord struct {
	Sequence           uint64       `json:"sequence"`
	Generation         uint64       `json:"generation"`
	ExpectedGeneration uint64       `json:"expected_generation"`
	FencingToken       uint64       `json:"fencing_token"`
	Operation          string       `json:"operation"`
	OperationID        string       `json:"operation_id"`
	Node               TopologyNode `json:"node"`
}

// MembershipJournalSnapshot is an immutable detached view of the current
// membership and retained bounded history.
type MembershipJournalSnapshot struct {
	Version          uint64             `json:"version"`
	Generation       uint64             `json:"generation"`
	FencingToken     uint64             `json:"fencing_token"`
	LastSequence     uint64             `json:"last_sequence"`
	CompactedThrough uint64             `json:"compacted_through,omitempty"`
	Nodes            []TopologyNode     `json:"nodes"`
	Records          []MembershipRecord `json:"records,omitempty"`
}

// MembershipJournal stores generation-fenced membership changes. It has no
// network or consensus side effects; callers use the records with their
// existing consensus/fencing mechanism before accepting a proposal.
type MembershipJournal struct {
	mu               sync.RWMutex
	path             string
	maxHistory       int
	maxBytes         int
	generation       uint64
	fencingToken     uint64
	lastSequence     uint64
	compactedThrough uint64
	nodes            []TopologyNode
	records          []MembershipRecord
}

// NewMembershipJournal creates an empty bounded membership journal.
func NewMembershipJournal(options MembershipJournalOptions) (*MembershipJournal, error) {
	maxHistory := options.MaxHistory
	if maxHistory == 0 {
		maxHistory = DefaultMembershipJournalHistory
	}
	if maxHistory < 1 || maxHistory > MaxMembershipJournalHistory {
		return nil, ErrMembershipJournalInvalidOptions
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultMembershipJournalBytes
	}
	if maxBytes < membershipJournalHeaderSize || maxBytes > MaxMembershipJournalBytes {
		return nil, ErrMembershipJournalInvalidOptions
	}
	return &MembershipJournal{
		path:       strings.TrimSpace(options.Path),
		maxHistory: maxHistory,
		maxBytes:   maxBytes,
	}, nil
}

// OpenMembershipJournal opens an existing persisted journal or creates an
// empty one when path does not exist. The path is retained for future Apply
// calls, which persist successful changes atomically.
func OpenMembershipJournal(path string, options MembershipJournalOptions) (*MembershipJournal, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, ErrMembershipJournalInvalidOptions
	}
	options.Path = path
	journal, err := NewMembershipJournal(options)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return journal, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: read: %v", ErrMembershipJournalPersist, err)
	}
	if err := journal.UnmarshalBinary(data); err != nil {
		return nil, err
	}
	return journal, nil
}

// Apply validates and commits one membership change. When the journal has a
// configured path, persistence completes before the in-memory state changes.
func (journal *MembershipJournal) Apply(change MembershipChange) (MembershipRecord, error) {
	if journal == nil {
		return MembershipRecord{}, ErrMembershipJournalInvalidOptions
	}
	normalized, err := normalizeMembershipChange(change)
	if err != nil {
		return MembershipRecord{}, err
	}

	journal.mu.Lock()
	defer journal.mu.Unlock()
	for _, record := range journal.records {
		if record.OperationID != normalized.OperationID {
			continue
		}
		if membershipRecordMatchesChange(record, normalized) {
			return record, nil
		}
		return MembershipRecord{}, fmt.Errorf("%w: operation_id=%q", ErrMembershipJournalOperationConflict, normalized.OperationID)
	}
	if normalized.ExpectedGeneration != journal.generation {
		return MembershipRecord{}, fmt.Errorf("%w: expected=%d current=%d", ErrMembershipJournalGenerationConflict, normalized.ExpectedGeneration, journal.generation)
	}
	if normalized.FencingToken <= journal.fencingToken {
		return MembershipRecord{}, fmt.Errorf("%w: proposed=%d current=%d", ErrMembershipJournalFenceStale, normalized.FencingToken, journal.fencingToken)
	}

	nodes := cloneMembershipNodes(journal.nodes)
	switch normalized.Operation {
	case MembershipOperationJoin:
		if membershipNodeIndex(nodes, normalized.Node.ID) >= 0 {
			return MembershipRecord{}, fmt.Errorf("%w: node=%q", ErrMembershipJournalNodeExists, normalized.Node.ID)
		}
		nodes = append(nodes, normalized.Node)
	case MembershipOperationLeave:
		index := membershipNodeIndex(nodes, normalized.Node.ID)
		if index < 0 {
			return MembershipRecord{}, fmt.Errorf("%w: node=%q", ErrMembershipJournalNodeMissing, normalized.Node.ID)
		}
		if len(nodes) == 1 {
			return MembershipRecord{}, ErrMembershipJournalLastNode
		}
		nodes = append(nodes[:index], nodes[index+1:]...)
	default:
		return MembershipRecord{}, ErrMembershipJournalInvalidChange
	}
	sort.Slice(nodes, func(left, right int) bool { return nodes[left].ID < nodes[right].ID })
	if journal.lastSequence == ^uint64(0) || journal.generation == ^uint64(0) {
		return MembershipRecord{}, ErrMembershipJournalInvalidProposal
	}
	record := MembershipRecord{
		Sequence:           journal.lastSequence + 1,
		Generation:         journal.generation + 1,
		ExpectedGeneration: normalized.ExpectedGeneration,
		FencingToken:       normalized.FencingToken,
		Operation:          normalized.Operation,
		OperationID:        normalized.OperationID,
		Node:               normalized.Node,
	}
	records := append(cloneMembershipRecords(journal.records), record)
	compactedThrough := journal.compactedThrough
	if len(records) > journal.maxHistory {
		compactedThrough = records[0].Sequence
		records = records[1:]
	}
	candidate := MembershipJournalSnapshot{
		Version:          MembershipJournalVersion,
		Generation:       record.Generation,
		FencingToken:     record.FencingToken,
		LastSequence:     record.Sequence,
		CompactedThrough: compactedThrough,
		Nodes:            nodes,
		Records:          records,
	}
	if err := validateMembershipJournalSnapshot(candidate, journal.maxHistory); err != nil {
		return MembershipRecord{}, err
	}
	if journal.path != "" {
		data, marshalErr := marshalMembershipJournalSnapshot(candidate, journal.maxBytes)
		if marshalErr != nil {
			return MembershipRecord{}, marshalErr
		}
		if persistErr := persistMembershipJournalFile(journal.path, data); persistErr != nil {
			return MembershipRecord{}, persistErr
		}
	}
	journal.generation = candidate.Generation
	journal.fencingToken = candidate.FencingToken
	journal.lastSequence = candidate.LastSequence
	journal.compactedThrough = candidate.CompactedThrough
	journal.nodes = cloneMembershipNodes(candidate.Nodes)
	journal.records = cloneMembershipRecords(candidate.Records)
	return record, nil
}

// Snapshot returns a detached view that callers may mutate safely.
func (journal *MembershipJournal) Snapshot() MembershipJournalSnapshot {
	if journal == nil {
		return MembershipJournalSnapshot{}
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	return journal.snapshotLocked()
}

// Replay returns retained records strictly after afterSequence. A history gap
// is returned rather than silently replaying an incomplete membership stream.
func (journal *MembershipJournal) Replay(afterSequence uint64, limit int) ([]MembershipRecord, error) {
	if journal == nil {
		return nil, ErrMembershipJournalInvalidOptions
	}
	if limit == 0 {
		limit = DefaultMembershipJournalReplay
	}
	if limit < 1 || limit > MaxMembershipJournalReplay {
		return nil, ErrMembershipJournalLimit
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	if afterSequence < journal.compactedThrough {
		return nil, ErrMembershipJournalHistoryGap
	}
	records := make([]MembershipRecord, 0, minMembershipJournalInt(limit, len(journal.records)))
	for _, record := range journal.records {
		if record.Sequence <= afterSequence {
			continue
		}
		records = append(records, record)
		if len(records) == limit {
			break
		}
	}
	return cloneMembershipRecords(records), nil
}

// MarshalBinary encodes the bounded state with a versioned CRC-protected
// envelope. The path is never serialized.
func (journal *MembershipJournal) MarshalBinary() ([]byte, error) {
	if journal == nil {
		return nil, ErrMembershipJournalInvalidOptions
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	return marshalMembershipJournalSnapshot(journal.snapshotLocked(), journal.maxBytes)
}

// UnmarshalBinary validates and replaces the in-memory state. On failure the
// previous state remains unchanged.
func (journal *MembershipJournal) UnmarshalBinary(data []byte) error {
	if journal == nil {
		return ErrMembershipJournalInvalidOptions
	}
	snapshot, err := decodeMembershipJournalSnapshot(data, journal.maxHistory, journal.maxBytes)
	if err != nil {
		return err
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	journal.generation = snapshot.Generation
	journal.fencingToken = snapshot.FencingToken
	journal.lastSequence = snapshot.LastSequence
	journal.compactedThrough = snapshot.CompactedThrough
	journal.nodes = cloneMembershipNodes(snapshot.Nodes)
	journal.records = cloneMembershipRecords(snapshot.Records)
	return nil
}

// Save writes the current state to path, or to the configured path when path
// is empty. It is useful for journals created without a persistence path.
func (journal *MembershipJournal) Save(path string) error {
	if journal == nil {
		return ErrMembershipJournalInvalidOptions
	}
	path = strings.TrimSpace(path)
	journal.mu.RLock()
	if path == "" {
		path = journal.path
	}
	snapshot := journal.snapshotLocked()
	maxBytes := journal.maxBytes
	journal.mu.RUnlock()
	if path == "" {
		return ErrMembershipJournalInvalidOptions
	}
	data, err := marshalMembershipJournalSnapshot(snapshot, maxBytes)
	if err != nil {
		return err
	}
	return persistMembershipJournalFile(path, data)
}

func (journal *MembershipJournal) snapshotLocked() MembershipJournalSnapshot {
	return MembershipJournalSnapshot{
		Version:          MembershipJournalVersion,
		Generation:       journal.generation,
		FencingToken:     journal.fencingToken,
		LastSequence:     journal.lastSequence,
		CompactedThrough: journal.compactedThrough,
		Nodes:            cloneMembershipNodes(journal.nodes),
		Records:          cloneMembershipRecords(journal.records),
	}
}

func normalizeMembershipChange(change MembershipChange) (MembershipChange, error) {
	change.Operation = strings.ToLower(strings.TrimSpace(change.Operation))
	change.OperationID = strings.TrimSpace(change.OperationID)
	if (change.Operation != MembershipOperationJoin && change.Operation != MembershipOperationLeave) || change.OperationID == "" || len(change.OperationID) > MaxMembershipOperationIDBytes {
		return MembershipChange{}, ErrMembershipJournalInvalidChange
	}
	node, err := normalizeMembershipNode(change.Node)
	if err != nil {
		return MembershipChange{}, err
	}
	if change.Operation == MembershipOperationLeave {
		node = TopologyNode{ID: node.ID}
	}
	change.Node = node
	return change, nil
}

func normalizeMembershipNode(node TopologyNode) (TopologyNode, error) {
	node.ID = strings.TrimSpace(node.ID)
	node.Address = strings.TrimSpace(node.Address)
	node.GRPCAddress = strings.TrimSpace(node.GRPCAddress)
	node.Role = strings.TrimSpace(node.Role)
	node.FailureDomain = strings.TrimSpace(node.FailureDomain)
	node.Region = strings.TrimSpace(node.Region)
	node.MaintenanceReason = strings.TrimSpace(node.MaintenanceReason)
	node.MaintenanceSince = strings.TrimSpace(node.MaintenanceSince)
	if node.ID == "" || !isValidTopologyRole(node.Role) {
		return TopologyNode{}, ErrMembershipJournalInvalidChange
	}
	if !node.Maintenance {
		node.MaintenanceReason = ""
		node.MaintenanceSince = ""
	}
	return node, nil
}

func membershipRecordMatchesChange(record MembershipRecord, change MembershipChange) bool {
	return record.ExpectedGeneration == change.ExpectedGeneration &&
		record.FencingToken == change.FencingToken &&
		record.Operation == change.Operation &&
		record.OperationID == change.OperationID &&
		record.Node == change.Node
}

func validateMembershipJournalSnapshot(snapshot MembershipJournalSnapshot, maxHistory int) error {
	if snapshot.Version != MembershipJournalVersion || maxHistory < 1 || len(snapshot.Records) > maxHistory || len(snapshot.Nodes) > MaxMembershipJournalNodes {
		return ErrMembershipJournalVersion
	}
	if snapshot.CompactedThrough > snapshot.LastSequence {
		return ErrMembershipJournalFormat
	}
	nodes := make(map[string]struct{}, len(snapshot.Nodes))
	for index := range snapshot.Nodes {
		node, err := normalizeMembershipNode(snapshot.Nodes[index])
		if err != nil {
			return err
		}
		if node != snapshot.Nodes[index] {
			return ErrMembershipJournalFormat
		}
		if _, exists := nodes[node.ID]; exists {
			return ErrMembershipJournalFormat
		}
		nodes[node.ID] = struct{}{}
		if index > 0 && snapshot.Nodes[index-1].ID >= node.ID {
			return ErrMembershipJournalFormat
		}
	}
	operationIDs := make(map[string]struct{}, len(snapshot.Records))
	var previousSequence, previousGeneration, previousFence uint64
	for index, record := range snapshot.Records {
		if record.Sequence == 0 || (index > 0 && record.Sequence <= previousSequence) || record.Generation == 0 || (index > 0 && record.Generation <= previousGeneration) || record.FencingToken == 0 || (index > 0 && record.FencingToken <= previousFence) {
			return ErrMembershipJournalFormat
		}
		if record.ExpectedGeneration+1 != record.Generation {
			return ErrMembershipJournalFormat
		}
		if record.OperationID == "" || len(record.OperationID) > MaxMembershipOperationIDBytes {
			return ErrMembershipJournalFormat
		}
		if _, exists := operationIDs[record.OperationID]; exists {
			return ErrMembershipJournalFormat
		}
		operationIDs[record.OperationID] = struct{}{}
		if record.Operation != MembershipOperationJoin && record.Operation != MembershipOperationLeave {
			return ErrMembershipJournalFormat
		}
		node, err := normalizeMembershipNode(record.Node)
		if err != nil {
			return err
		}
		if node != record.Node {
			return ErrMembershipJournalFormat
		}
		previousSequence, previousGeneration, previousFence = record.Sequence, record.Generation, record.FencingToken
	}
	if len(snapshot.Records) == 0 {
		if snapshot.LastSequence != snapshot.CompactedThrough || snapshot.Generation != 0 || snapshot.FencingToken != 0 || len(snapshot.Nodes) != 0 {
			return ErrMembershipJournalFormat
		}
		return nil
	}
	if snapshot.LastSequence != previousSequence || snapshot.Generation != previousGeneration || snapshot.FencingToken != previousFence || snapshot.CompactedThrough >= snapshot.Records[0].Sequence {
		return ErrMembershipJournalFormat
	}
	return nil
}

func marshalMembershipJournalSnapshot(snapshot MembershipJournalSnapshot, maxBytes int) ([]byte, error) {
	if err := validateMembershipJournalSnapshot(snapshot, MaxMembershipJournalHistory); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: encode: %v", ErrMembershipJournalFormat, err)
	}
	if len(payload) > maxBytes-membershipJournalHeaderSize {
		return nil, ErrMembershipJournalLimit
	}
	data := make([]byte, membershipJournalHeaderSize+len(payload))
	copy(data[:4], membershipJournalMagic)
	data[4] = byte(MembershipJournalVersion)
	binary.BigEndian.PutUint32(data[5:9], uint32(len(payload)))
	binary.BigEndian.PutUint32(data[9:13], crc32.Checksum(payload, membershipJournalCRCTable))
	copy(data[membershipJournalHeaderSize:], payload)
	return data, nil
}

func decodeMembershipJournalSnapshot(data []byte, maxHistory, maxBytes int) (MembershipJournalSnapshot, error) {
	if len(data) < membershipJournalHeaderSize || len(data) > maxBytes || string(data[:4]) != membershipJournalMagic {
		return MembershipJournalSnapshot{}, ErrMembershipJournalFormat
	}
	if data[4] != byte(MembershipJournalVersion) {
		return MembershipJournalSnapshot{}, ErrMembershipJournalVersion
	}
	payloadLength := int(binary.BigEndian.Uint32(data[5:9]))
	if payloadLength < 1 || membershipJournalHeaderSize+payloadLength != len(data) {
		return MembershipJournalSnapshot{}, ErrMembershipJournalFormat
	}
	payload := data[membershipJournalHeaderSize:]
	if binary.BigEndian.Uint32(data[9:13]) != crc32.Checksum(payload, membershipJournalCRCTable) {
		return MembershipJournalSnapshot{}, ErrMembershipJournalChecksum
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var snapshot MembershipJournalSnapshot
	if err := decoder.Decode(&snapshot); err != nil {
		return MembershipJournalSnapshot{}, fmt.Errorf("%w: decode: %v", ErrMembershipJournalFormat, err)
	}
	if err := validateMembershipJournalSnapshot(snapshot, maxHistory); err != nil {
		return MembershipJournalSnapshot{}, err
	}
	snapshot.Nodes = cloneMembershipNodes(snapshot.Nodes)
	snapshot.Records = cloneMembershipRecords(snapshot.Records)
	return snapshot, nil
}

func persistMembershipJournalFile(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("%w: mkdir: %v", ErrMembershipJournalPersist, err)
	}
	temporary, err := os.CreateTemp(directory, ".hatrie-membership-*")
	if err != nil {
		return fmt.Errorf("%w: create: %v", ErrMembershipJournalPersist, err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: chmod: %v", ErrMembershipJournalPersist, err)
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: write: %v", ErrMembershipJournalPersist, err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("%w: sync: %v", ErrMembershipJournalPersist, err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("%w: close: %v", ErrMembershipJournalPersist, err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("%w: rename: %v", ErrMembershipJournalPersist, err)
	}
	removeTemporary = false
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("%w: open directory: %v", ErrMembershipJournalPersist, err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil {
		return fmt.Errorf("%w: sync directory: %v", ErrMembershipJournalPersist, syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("%w: close directory: %v", ErrMembershipJournalPersist, closeErr)
	}
	return nil
}

func membershipNodeIndex(nodes []TopologyNode, id string) int {
	for index, node := range nodes {
		if node.ID == id {
			return index
		}
	}
	return -1
}

func cloneMembershipNodes(nodes []TopologyNode) []TopologyNode {
	if len(nodes) == 0 {
		return nil
	}
	cloned := make([]TopologyNode, len(nodes))
	copy(cloned, nodes)
	return cloned
}

func cloneMembershipRecords(records []MembershipRecord) []MembershipRecord {
	if len(records) == 0 {
		return nil
	}
	cloned := make([]MembershipRecord, len(records))
	copy(cloned, records)
	return cloned
}

func minMembershipJournalInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
