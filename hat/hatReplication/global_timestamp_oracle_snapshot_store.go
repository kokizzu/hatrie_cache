package hatReplication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const (
	globalTimestampOracleSnapshotMagic        = "GTO1"
	globalTimestampOracleSnapshotVersion byte = 1
	// MaxGlobalTimestampOracleSnapshotBytes bounds both memory use and file
	// reads for this operator-owned state.
	MaxGlobalTimestampOracleSnapshotBytes = 1 << 20
	maxGlobalTimestampOracleSnapshotNodes = 65535
	maxGlobalTimestampOracleSnapshotText  = 4096
)

var (
	// ErrGlobalTimestampOracleSnapshotInvalid reports malformed, corrupt, or
	// semantically invalid binary oracle state.
	ErrGlobalTimestampOracleSnapshotInvalid = errors.New("hatriecache: invalid global timestamp oracle snapshot")
	globalTimestampOracleSnapshotCRCTable   = crc32.MakeTable(crc32.Castagnoli)
)

// GlobalTimestampOracleSnapshotFileStore atomically persists one validated
// global timestamp oracle snapshot. It is opt-in; callers still own when a
// consensus/coordinator state transition is made durable.
type GlobalTimestampOracleSnapshotFileStore struct {
	path string
}

// NewGlobalTimestampOracleSnapshotFileStore creates a file-backed snapshot
// store. The parent directory is created by Save, not by this constructor.
func NewGlobalTimestampOracleSnapshotFileStore(path string) (*GlobalTimestampOracleSnapshotFileStore, error) {
	if path == "" {
		return nil, ErrGlobalTimestampOracleSnapshotInvalid
	}
	return &GlobalTimestampOracleSnapshotFileStore{path: filepath.Clean(path)}, nil
}

// MarshalGlobalTimestampOracleSnapshotBinary encodes a deterministic,
// CRC32C-protected snapshot. The binary form is smaller than the JSON form
// used for diagnostics and is suitable for local storage or wire transfer.
func MarshalGlobalTimestampOracleSnapshotBinary(snapshot GlobalTimestampOracleSnapshot) ([]byte, error) {
	if err := validateGlobalTimestampOracleSnapshotForBinary(snapshot); err != nil {
		return nil, err
	}
	nodes := snapshot.Nodes
	for index := 1; index < len(nodes); index++ {
		if nodes[index-1].NodeID > nodes[index].NodeID {
			nodes = append([]GlobalTimestampNodeSnapshot(nil), snapshot.Nodes...)
			sort.Slice(nodes, func(left, right int) bool {
				return nodes[left].NodeID < nodes[right].NodeID
			})
			break
		}
	}
	for index := 1; index < len(nodes); index++ {
		if nodes[index-1].NodeID == nodes[index].NodeID {
			return nil, ErrGlobalTimestampOracleSnapshotInvalid
		}
	}

	encoded := make([]byte, 0, globalTimestampOracleSnapshotEncodedSize(snapshot, nodes))
	encoded = append(encoded, globalTimestampOracleSnapshotMagic...)
	encoded = append(encoded, globalTimestampOracleSnapshotVersion)
	encoded = appendGlobalTimestampUvarint(encoded, uint64(snapshot.Term))
	encoded = appendGlobalTimestampUvarint(encoded, uint64(snapshot.Current))
	encoded = appendGlobalTimestampUvarint(encoded, uint64(len(nodes)))
	for _, node := range nodes {
		var err error
		encoded, err = appendGlobalTimestampText(encoded, node.NodeID)
		if err != nil {
			return nil, err
		}
		encoded = appendGlobalTimestampUvarint(encoded, node.NodeEpoch)
		encoded = appendGlobalTimestampUvarint(encoded, node.Sequence)
		encoded = appendGlobalTimestampUvarint(encoded, uint64(node.Observed))
		encoded = appendGlobalTimestampUvarint(encoded, uint64(node.Grant.Term))
		encoded, err = appendGlobalTimestampText(encoded, node.Grant.NodeID)
		if err != nil {
			return nil, err
		}
		encoded = appendGlobalTimestampUvarint(encoded, node.Grant.NodeEpoch)
		encoded = appendGlobalTimestampUvarint(encoded, node.Grant.Sequence)
		encoded = appendGlobalTimestampUvarint(encoded, uint64(node.Grant.Start))
		encoded = appendGlobalTimestampUvarint(encoded, uint64(node.Grant.End))
		encoded = appendGlobalTimestampUvarint(encoded, node.Grant.Count)
	}
	if len(encoded)+4 > MaxGlobalTimestampOracleSnapshotBytes {
		return nil, ErrGlobalTimestampOracleSnapshotInvalid
	}
	checksum := crc32.Checksum(encoded, globalTimestampOracleSnapshotCRCTable)
	var checksumBytes [4]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	return encoded, nil
}

// UnmarshalGlobalTimestampOracleSnapshotBinary validates and decodes one
// binary snapshot, including its checksum and oracle state invariants.
func UnmarshalGlobalTimestampOracleSnapshotBinary(encoded []byte) (GlobalTimestampOracleSnapshot, error) {
	if len(encoded) < len(globalTimestampOracleSnapshotMagic)+1+4 || len(encoded) > MaxGlobalTimestampOracleSnapshotBytes {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	payloadEnd := len(encoded) - 4
	if string(encoded[:len(globalTimestampOracleSnapshotMagic)]) != globalTimestampOracleSnapshotMagic || encoded[len(globalTimestampOracleSnapshotMagic)] != globalTimestampOracleSnapshotVersion {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	actualChecksum := crc32.Checksum(encoded[:payloadEnd], globalTimestampOracleSnapshotCRCTable)
	wantChecksum := binary.LittleEndian.Uint32(encoded[payloadEnd:])
	if actualChecksum != wantChecksum {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	position := len(globalTimestampOracleSnapshotMagic) + 1
	term, ok := readGlobalTimestampUvarint(encoded[:payloadEnd], &position)
	if !ok || term == 0 {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	current, ok := readGlobalTimestampUvarint(encoded[:payloadEnd], &position)
	if !ok || current > uint64(^uint64(0)>>1) {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	nodeCount, ok := readGlobalTimestampUvarint(encoded[:payloadEnd], &position)
	if !ok || nodeCount > maxGlobalTimestampOracleSnapshotNodes {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	snapshot := GlobalTimestampOracleSnapshot{
		Term:    term,
		Current: int64(current),
		Nodes:   make([]GlobalTimestampNodeSnapshot, 0, int(nodeCount)),
	}
	for index := uint64(0); index < nodeCount; index++ {
		nodeID, ok := readGlobalTimestampText(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		nodeEpoch, ok := readGlobalTimestampUvarint(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		sequence, ok := readGlobalTimestampUvarint(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		observed, ok := readGlobalTimestampInt64(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		grantTerm, ok := readGlobalTimestampUvarint(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		grantNodeID, ok := readGlobalTimestampText(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		grantNodeEpoch, ok := readGlobalTimestampUvarint(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		grantSequence, ok := readGlobalTimestampUvarint(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		grantStart, ok := readGlobalTimestampInt64(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		grantEnd, ok := readGlobalTimestampInt64(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		grantCount, ok := readGlobalTimestampUvarint(encoded[:payloadEnd], &position)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
		snapshot.Nodes = append(snapshot.Nodes, GlobalTimestampNodeSnapshot{
			NodeID: nodeID, NodeEpoch: nodeEpoch, Sequence: sequence, Observed: observed,
			Grant: GlobalTimestampGrant{
				Term: grantTerm, NodeID: grantNodeID, NodeEpoch: grantNodeEpoch,
				Sequence: grantSequence, Start: grantStart, End: grantEnd, Count: grantCount,
			},
		})
	}
	if position != payloadEnd {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	if err := validateGlobalTimestampOracleSnapshotForBinary(snapshot); err != nil {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	for index := 1; index < len(snapshot.Nodes); index++ {
		if snapshot.Nodes[index-1].NodeID >= snapshot.Nodes[index].NodeID {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
		}
	}
	return snapshot, nil
}

// Save validates and atomically replaces the snapshot file with private
// permissions. A failed encode or write leaves the previous file untouched.
func (store *GlobalTimestampOracleSnapshotFileStore) Save(snapshot GlobalTimestampOracleSnapshot) error {
	if store == nil || store.path == "" {
		return ErrGlobalTimestampOracleSnapshotInvalid
	}
	encoded, err := MarshalGlobalTimestampOracleSnapshotBinary(snapshot)
	if err != nil {
		return err
	}
	directory := filepath.Dir(store.path)
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	temporary, err := os.CreateTemp(directory, ".global-timestamp-oracle-*.tmp")
	if err != nil {
		return fmt.Errorf("create snapshot temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set snapshot permissions: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write snapshot: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync snapshot: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close snapshot: %w", err)
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return fmt.Errorf("publish snapshot: %w", err)
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open snapshot directory: %w", err)
	}
	syncErr := directoryFile.Sync()
	closeErr := directoryFile.Close()
	if syncErr != nil {
		return fmt.Errorf("sync snapshot directory: %w", syncErr)
	}
	if closeErr != nil {
		return fmt.Errorf("close snapshot directory: %w", closeErr)
	}
	return nil
}

// Load reads and validates one snapshot file without exposing partial data.
func (store *GlobalTimestampOracleSnapshotFileStore) Load() (GlobalTimestampOracleSnapshot, error) {
	if store == nil || store.path == "" {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	file, err := os.Open(store.path)
	if err != nil {
		return GlobalTimestampOracleSnapshot{}, err
	}
	defer file.Close()
	encoded, err := io.ReadAll(io.LimitReader(file, MaxGlobalTimestampOracleSnapshotBytes+1))
	if err != nil {
		return GlobalTimestampOracleSnapshot{}, fmt.Errorf("read snapshot: %w", err)
	}
	if len(encoded) > MaxGlobalTimestampOracleSnapshotBytes {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleSnapshotInvalid
	}
	return UnmarshalGlobalTimestampOracleSnapshotBinary(encoded)
}

func validateGlobalTimestampOracleSnapshotForBinary(snapshot GlobalTimestampOracleSnapshot) error {
	if snapshot.Term == 0 || snapshot.Current < 0 || len(snapshot.Nodes) > maxGlobalTimestampOracleSnapshotNodes {
		return ErrGlobalTimestampOracleSnapshotInvalid
	}
	for _, node := range snapshot.Nodes {
		if node.NodeID == "" || strings.TrimSpace(node.NodeID) != node.NodeID || len(node.NodeID) > maxGlobalTimestampOracleSnapshotText || node.NodeEpoch == 0 || node.Sequence == 0 || node.Observed < 0 || node.Observed > snapshot.Current {
			return ErrGlobalTimestampOracleSnapshotInvalid
		}
		grant := node.Grant
		if grant.Term != snapshot.Term || grant.NodeID != node.NodeID || strings.TrimSpace(grant.NodeID) != grant.NodeID || len(grant.NodeID) > maxGlobalTimestampOracleSnapshotText || grant.NodeEpoch == 0 || grant.Sequence == 0 || grant.Start < 0 || grant.End < grant.Start || grant.Count == 0 || grant.Count != uint64(grant.End-grant.Start)+1 || grant.End > snapshot.Current || grant.NodeEpoch != node.NodeEpoch || grant.Sequence != node.Sequence {
			return ErrGlobalTimestampOracleSnapshotInvalid
		}
	}
	return nil
}

func globalTimestampOracleSnapshotEncodedSize(snapshot GlobalTimestampOracleSnapshot, nodes []GlobalTimestampNodeSnapshot) int {
	size := len(globalTimestampOracleSnapshotMagic) + 1 + globalTimestampUvarintSize(uint64(snapshot.Term)) + globalTimestampUvarintSize(uint64(snapshot.Current)) + globalTimestampUvarintSize(uint64(len(nodes))) + 4
	for _, node := range nodes {
		size += globalTimestampTextSize(node.NodeID)
		size += globalTimestampUvarintSize(node.NodeEpoch)
		size += globalTimestampUvarintSize(node.Sequence)
		size += globalTimestampUvarintSize(uint64(node.Observed))
		size += globalTimestampUvarintSize(uint64(node.Grant.Term))
		size += globalTimestampTextSize(node.Grant.NodeID)
		size += globalTimestampUvarintSize(node.Grant.NodeEpoch)
		size += globalTimestampUvarintSize(node.Grant.Sequence)
		size += globalTimestampUvarintSize(uint64(node.Grant.Start))
		size += globalTimestampUvarintSize(uint64(node.Grant.End))
		size += globalTimestampUvarintSize(node.Grant.Count)
	}
	return size
}

func globalTimestampTextSize(value string) int {
	return globalTimestampUvarintSize(uint64(len(value))) + len(value)
}

func globalTimestampUvarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func appendGlobalTimestampUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:length]...)
}

func appendGlobalTimestampText(destination []byte, value string) ([]byte, error) {
	if len(value) == 0 || len(value) > maxGlobalTimestampOracleSnapshotText {
		return nil, ErrGlobalTimestampOracleSnapshotInvalid
	}
	destination = appendGlobalTimestampUvarint(destination, uint64(len(value)))
	return append(destination, value...), nil
}

func readGlobalTimestampUvarint(source []byte, position *int) (uint64, bool) {
	if position == nil || *position < 0 || *position >= len(source) {
		return 0, false
	}
	value, length := binary.Uvarint(source[*position:])
	if length <= 0 {
		return 0, false
	}
	*position += length
	return value, true
}

func readGlobalTimestampInt64(source []byte, position *int) (int64, bool) {
	value, ok := readGlobalTimestampUvarint(source, position)
	if !ok || value > uint64(^uint64(0)>>1) {
		return 0, false
	}
	return int64(value), true
}

func readGlobalTimestampText(source []byte, position *int) (string, bool) {
	length, ok := readGlobalTimestampUvarint(source, position)
	if !ok || length == 0 || length > maxGlobalTimestampOracleSnapshotText || length > uint64(len(source)-*position) {
		return "", false
	}
	start := *position
	*position += int(length)
	return string(source[start:*position]), true
}
