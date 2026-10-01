package hatTopology

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
)

const (
	defaultDurableMembershipMaxRecordBytes = 1 << 20
	minDurableMembershipMaxRecordBytes     = 256
	maxDurableMembershipMaxRecordBytes     = 16 << 20
	maxDurableMembershipNodeIDBytes        = 256
	maxDurableMembershipAddressBytes       = 2048
	maxDurableMembershipRoleBytes          = 128
)

var (
	ErrDurableMembershipCorrupt            = errors.New("durable membership log is corrupt")
	ErrDurableMembershipGenerationConflict = errors.New("durable membership generation conflict")
	ErrDurableMembershipNodeExists         = errors.New("durable membership node already exists")
	ErrDurableMembershipNodeMissing        = errors.New("durable membership node does not exist")
	ErrDurableMembershipInvalid            = errors.New("invalid durable membership record")
	ErrDurableMembershipClosed             = errors.New("durable membership log is closed")
)

// DurableMembershipOperation identifies one membership journal record.
type DurableMembershipOperation string

const (
	DurableMembershipJoin  DurableMembershipOperation = "join"
	DurableMembershipLeave DurableMembershipOperation = "leave"
)

// DurableMembershipLogOptions controls an on-disk membership journal.
// Writes are synchronized before Join or Leave returns unless UnsafeNoSync is
// explicitly enabled. The unsafe option is intended only for a caller that
// supplies durability through another ordered storage layer.
type DurableMembershipLogOptions struct {
	UnsafeNoSync   bool
	MaxRecordBytes int
}

// DurableMembershipRecord is one generation-fenced join or leave operation.
// Leave records retain the removed node metadata for audit and replay.
type DurableMembershipRecord struct {
	Generation uint64                     `json:"generation"`
	Operation  DurableMembershipOperation `json:"operation"`
	Node       TopologyNode               `json:"node"`
}

// DurableMembershipSnapshot is a stable, sorted view of the active members.
type DurableMembershipSnapshot struct {
	Generation uint64
	Nodes      []TopologyNode
}

// DurableMembershipLog is a concurrency-safe, append-only membership journal.
// It is transport-neutral: a consensus or operator policy can decide which
// generation is authorized, while this type guarantees local replay and
// stale-writer rejection.
type DurableMembershipLog struct {
	mu             sync.RWMutex
	file           *os.File
	path           string
	syncWrites     bool
	maxRecordBytes int
	generation     uint64
	members        map[string]TopologyNode
	closed         bool
}

// OpenDurableMembershipLog opens or creates a journal and replays every
// complete record. A malformed or non-contiguous log is rejected without
// exposing a partially reconstructed membership set.
func OpenDurableMembershipLog(path string, options DurableMembershipLogOptions) (*DurableMembershipLog, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("%w: path is required", ErrDurableMembershipInvalid)
	}
	maxRecordBytes := options.MaxRecordBytes
	if maxRecordBytes == 0 {
		maxRecordBytes = defaultDurableMembershipMaxRecordBytes
	}
	if maxRecordBytes < minDurableMembershipMaxRecordBytes || maxRecordBytes > maxDurableMembershipMaxRecordBytes {
		return nil, fmt.Errorf("%w: max record bytes %d outside %d..%d", ErrDurableMembershipInvalid, maxRecordBytes, minDurableMembershipMaxRecordBytes, maxDurableMembershipMaxRecordBytes)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	log := &DurableMembershipLog{
		file:           file,
		path:           path,
		syncWrites:     !options.UnsafeNoSync,
		maxRecordBytes: maxRecordBytes,
		members:        make(map[string]TopologyNode),
	}
	if err := log.replay(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return log, nil
}

func (log *DurableMembershipLog) replay() error {
	if _, err := log.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("%w: seek: %v", ErrDurableMembershipCorrupt, err)
	}
	scanner := bufio.NewScanner(log.file)
	scanner.Buffer(make([]byte, 4096), log.maxRecordBytes+1)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		line := scanner.Bytes()
		if len(line) == 0 {
			return fmt.Errorf("%w: empty line %d", ErrDurableMembershipCorrupt, lineNumber)
		}
		if len(line)+1 > log.maxRecordBytes {
			return fmt.Errorf("%w: line %d exceeds %d bytes", ErrDurableMembershipCorrupt, lineNumber, log.maxRecordBytes)
		}
		var record DurableMembershipRecord
		if err := json.Unmarshal(line, &record); err != nil {
			return fmt.Errorf("%w: line %d: %v", ErrDurableMembershipCorrupt, lineNumber, err)
		}
		if err := log.applyRecord(record); err != nil {
			return fmt.Errorf("%w: line %d: %v", ErrDurableMembershipCorrupt, lineNumber, err)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("%w: scan: %v", ErrDurableMembershipCorrupt, err)
	}
	if _, err := log.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("%w: seek end: %v", ErrDurableMembershipCorrupt, err)
	}
	return nil
}

// Join appends a member after verifying the caller's exact current
// generation. The returned record is the durable fencing token for the change.
func (log *DurableMembershipLog) Join(expectedGeneration uint64, node TopologyNode) (DurableMembershipRecord, error) {
	node, err := normalizeDurableMembershipNode(node)
	if err != nil {
		return DurableMembershipRecord{}, err
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if err := log.ensureOpenLocked(); err != nil {
		return DurableMembershipRecord{}, err
	}
	if err := log.checkGenerationLocked(expectedGeneration); err != nil {
		return DurableMembershipRecord{}, err
	}
	if _, exists := log.members[node.ID]; exists {
		return DurableMembershipRecord{}, fmt.Errorf("%w: %q", ErrDurableMembershipNodeExists, node.ID)
	}
	record := DurableMembershipRecord{Generation: log.generation + 1, Operation: DurableMembershipJoin, Node: node}
	if err := log.appendRecordLocked(record); err != nil {
		return DurableMembershipRecord{}, err
	}
	log.members[node.ID] = node
	log.generation = record.Generation
	return record, nil
}

// Leave appends a member removal after verifying the caller's exact current
// generation and retaining the removed node in the audit record.
func (log *DurableMembershipLog) Leave(expectedGeneration uint64, nodeID string) (DurableMembershipRecord, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" || len(nodeID) > maxDurableMembershipNodeIDBytes {
		return DurableMembershipRecord{}, fmt.Errorf("%w: node ID", ErrDurableMembershipInvalid)
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if err := log.ensureOpenLocked(); err != nil {
		return DurableMembershipRecord{}, err
	}
	if err := log.checkGenerationLocked(expectedGeneration); err != nil {
		return DurableMembershipRecord{}, err
	}
	node, exists := log.members[nodeID]
	if !exists {
		return DurableMembershipRecord{}, fmt.Errorf("%w: %q", ErrDurableMembershipNodeMissing, nodeID)
	}
	record := DurableMembershipRecord{Generation: log.generation + 1, Operation: DurableMembershipLeave, Node: node}
	if err := log.appendRecordLocked(record); err != nil {
		return DurableMembershipRecord{}, err
	}
	delete(log.members, nodeID)
	log.generation = record.Generation
	return record, nil
}

// Snapshot returns a sorted copy that can be retained and modified by the
// caller without affecting the journal.
func (log *DurableMembershipLog) Snapshot() (DurableMembershipSnapshot, error) {
	log.mu.RLock()
	defer log.mu.RUnlock()
	if log.closed {
		return DurableMembershipSnapshot{}, ErrDurableMembershipClosed
	}
	nodes := make([]TopologyNode, 0, len(log.members))
	for _, node := range log.members {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(left, right int) bool { return nodes[left].ID < nodes[right].ID })
	return DurableMembershipSnapshot{Generation: log.generation, Nodes: nodes}, nil
}

// Generation returns the current journal generation, or zero for a nil log.
// Use Snapshot when the member set is also required.
func (log *DurableMembershipLog) Generation() uint64 {
	if log == nil {
		return 0
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return log.generation
}

// Close closes the journal. It is safe to call repeatedly.
func (log *DurableMembershipLog) Close() error {
	if log == nil {
		return nil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return nil
	}
	log.closed = true
	return log.file.Close()
}

func (log *DurableMembershipLog) ensureOpenLocked() error {
	if log == nil || log.closed || log.file == nil {
		return ErrDurableMembershipClosed
	}
	return nil
}

func (log *DurableMembershipLog) checkGenerationLocked(expected uint64) error {
	if expected != log.generation {
		return fmt.Errorf("%w: expected %d, current %d", ErrDurableMembershipGenerationConflict, expected, log.generation)
	}
	return nil
}

func (log *DurableMembershipLog) applyRecord(record DurableMembershipRecord) error {
	if record.Generation != log.generation+1 {
		return fmt.Errorf("generation %d follows %d", record.Generation, log.generation)
	}
	node, err := normalizeDurableMembershipNode(record.Node)
	if err != nil {
		return err
	}
	switch record.Operation {
	case DurableMembershipJoin:
		if _, exists := log.members[node.ID]; exists {
			return fmt.Errorf("duplicate join for %q", node.ID)
		}
		log.members[node.ID] = node
	case DurableMembershipLeave:
		if _, exists := log.members[node.ID]; !exists {
			return fmt.Errorf("leave for missing node %q", node.ID)
		}
		delete(log.members, node.ID)
	default:
		return fmt.Errorf("unsupported operation %q", record.Operation)
	}
	log.generation = record.Generation
	return nil
}

func (log *DurableMembershipLog) appendRecordLocked(record DurableMembershipRecord) error {
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("%w: encode: %v", ErrDurableMembershipInvalid, err)
	}
	if len(encoded)+1 > log.maxRecordBytes {
		return fmt.Errorf("%w: record exceeds %d bytes", ErrDurableMembershipInvalid, log.maxRecordBytes)
	}
	encoded = append(encoded, '\n')
	offset, err := log.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("membership append seek: %w", err)
	}
	if err := writeDurableMembershipBytes(log.file, encoded); err != nil {
		_ = log.file.Truncate(offset)
		_, _ = log.file.Seek(0, io.SeekEnd)
		return fmt.Errorf("membership append: %w", err)
	}
	if log.syncWrites {
		if err := log.file.Sync(); err != nil {
			_ = log.file.Truncate(offset)
			_, _ = log.file.Seek(0, io.SeekEnd)
			return fmt.Errorf("membership sync: %w", err)
		}
	}
	return nil
}

func writeDurableMembershipBytes(file *os.File, encoded []byte) error {
	for len(encoded) > 0 {
		written, err := file.Write(encoded)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		encoded = encoded[written:]
	}
	return nil
}

func normalizeDurableMembershipNode(node TopologyNode) (TopologyNode, error) {
	node.ID = strings.TrimSpace(node.ID)
	node.Address = strings.TrimSpace(node.Address)
	node.Role = strings.TrimSpace(node.Role)
	if node.ID == "" || len(node.ID) > maxDurableMembershipNodeIDBytes {
		return TopologyNode{}, fmt.Errorf("%w: node ID", ErrDurableMembershipInvalid)
	}
	if node.Address == "" || len(node.Address) > maxDurableMembershipAddressBytes {
		return TopologyNode{}, fmt.Errorf("%w: node address", ErrDurableMembershipInvalid)
	}
	if node.Role == "" || len(node.Role) > maxDurableMembershipRoleBytes {
		return TopologyNode{}, fmt.Errorf("%w: node role", ErrDurableMembershipInvalid)
	}
	return node, nil
}
