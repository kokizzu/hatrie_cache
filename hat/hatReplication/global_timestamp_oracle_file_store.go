package hatReplication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultGlobalTimestampOracleFileStoreMaxBytes bounds one persisted
	// snapshot when callers do not provide a limit.
	DefaultGlobalTimestampOracleFileStoreMaxBytes = 8 << 20
	// MaxGlobalTimestampOracleFileStoreBytes is the largest file accepted by
	// the bounded codec and file store.
	MaxGlobalTimestampOracleFileStoreBytes  = 64 << 20
	maxGlobalTimestampOracleFileStoreNodes  = 1 << 20
	maxGlobalTimestampOracleFileStoreNodeID = 1 << 20
	globalTimestampOracleSnapshotMagic      = "GTO1"
)

var (
	// ErrGlobalTimestampOracleFileStoreInvalid reports invalid store options,
	// paths, or snapshot input.
	ErrGlobalTimestampOracleFileStoreInvalid = errors.New("hatriecache: invalid global timestamp oracle file store input")
	// ErrGlobalTimestampOracleFileStoreCorrupt reports a malformed or
	// checksum-invalid persisted snapshot.
	ErrGlobalTimestampOracleFileStoreCorrupt = errors.New("hatriecache: corrupt global timestamp oracle snapshot")
	// ErrGlobalTimestampOracleFileStoreTooLarge reports a snapshot or file
	// above the configured bound.
	ErrGlobalTimestampOracleFileStoreTooLarge = errors.New("hatriecache: global timestamp oracle snapshot is too large")
	// ErrGlobalTimestampOracleFileStoreSymlink reports a symlink at the state
	// path. The store never follows one for reads or overwrites one on save.
	ErrGlobalTimestampOracleFileStoreSymlink = errors.New("hatriecache: global timestamp oracle snapshot path is a symlink")
)

// GlobalTimestampOracleFileStoreOptions configures an explicit durable
// snapshot store. SaveSnapshot is caller-controlled; Reserve never performs
// file I/O or fsync work implicitly.
type GlobalTimestampOracleFileStoreOptions struct {
	Path     string
	MaxBytes int
}

// GlobalTimestampOracleFileStore atomically persists validated oracle
// snapshots. The store is safe for concurrent SaveSnapshot and LoadSnapshot
// calls on the same instance.
type GlobalTimestampOracleFileStore struct {
	mu       sync.Mutex
	path     string
	maxBytes int
}

// NewGlobalTimestampOracleFileStore creates a bounded file store. The parent
// directory is created on the first save with private permissions.
func NewGlobalTimestampOracleFileStore(options GlobalTimestampOracleFileStoreOptions) (*GlobalTimestampOracleFileStore, error) {
	path := strings.TrimSpace(options.Path)
	if path == "" {
		return nil, ErrGlobalTimestampOracleFileStoreInvalid
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultGlobalTimestampOracleFileStoreMaxBytes
	}
	if maxBytes < 1 || maxBytes > MaxGlobalTimestampOracleFileStoreBytes {
		return nil, ErrGlobalTimestampOracleFileStoreInvalid
	}
	return &GlobalTimestampOracleFileStore{path: filepath.Clean(path), maxBytes: maxBytes}, nil
}

// SaveOracle persists the current state of oracle as one atomic checkpoint.
func (store *GlobalTimestampOracleFileStore) SaveOracle(oracle *GlobalTimestampOracle) error {
	if oracle == nil {
		return ErrGlobalTimestampOracleFileStoreInvalid
	}
	return store.SaveSnapshot(oracle.Snapshot())
}

// SaveSnapshot validates and persists one oracle snapshot. Existing regular
// files are replaced only after the complete new payload is synced.
func (store *GlobalTimestampOracleFileStore) SaveSnapshot(snapshot GlobalTimestampOracleSnapshot) error {
	if store == nil {
		return ErrGlobalTimestampOracleFileStoreInvalid
	}
	payload, err := MarshalGlobalTimestampOracleSnapshot(snapshot)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(payload) > store.maxBytes {
		return ErrGlobalTimestampOracleFileStoreTooLarge
	}
	return store.writeLocked(payload)
}

// LoadSnapshot reads and validates the latest complete checkpoint. Missing
// files preserve os.IsNotExist behavior for callers that need bootstrap logic.
func (store *GlobalTimestampOracleFileStore) LoadSnapshot() (GlobalTimestampOracleSnapshot, error) {
	if store == nil {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreInvalid
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	info, err := lstatGlobalTimestampOracleStorePath(store.path)
	if err != nil {
		return GlobalTimestampOracleSnapshot{}, err
	}
	if info.Size() > int64(store.maxBytes) {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreTooLarge
	}
	file, err := os.Open(store.path)
	if err != nil {
		return GlobalTimestampOracleSnapshot{}, err
	}
	defer file.Close()
	openedInfo, err := file.Stat()
	if err != nil {
		return GlobalTimestampOracleSnapshot{}, err
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreSymlink
	}
	if openedInfo.Size() > int64(store.maxBytes) {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreTooLarge
	}
	payload, err := io.ReadAll(io.LimitReader(file, int64(store.maxBytes)+1))
	if err != nil {
		return GlobalTimestampOracleSnapshot{}, err
	}
	if len(payload) > store.maxBytes {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreTooLarge
	}
	return UnmarshalGlobalTimestampOracleSnapshot(payload)
}

// LoadOracle restores a new independent oracle from the latest checkpoint.
func (store *GlobalTimestampOracleFileStore) LoadOracle() (*GlobalTimestampOracle, error) {
	snapshot, err := store.LoadSnapshot()
	if err != nil {
		return nil, err
	}
	oracle, err := NewGlobalTimestampOracleFromSnapshot(snapshot)
	if err != nil {
		return nil, ErrGlobalTimestampOracleFileStoreCorrupt
	}
	return oracle, nil
}

// MarshalGlobalTimestampOracleSnapshot encodes a deterministic bounded binary
// snapshot with a CRC32C trailer. JSON remains available for callers that
// explicitly need the existing human-readable representation.
func MarshalGlobalTimestampOracleSnapshot(snapshot GlobalTimestampOracleSnapshot) ([]byte, error) {
	canonical, err := canonicalGlobalTimestampOracleSnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	snapshot = canonical
	if len(snapshot.Nodes) > maxGlobalTimestampOracleFileStoreNodes {
		return nil, ErrGlobalTimestampOracleFileStoreTooLarge
	}
	payload := make([]byte, 0, len(globalTimestampOracleSnapshotMagic)+len(snapshot.Nodes)*64+4)
	payload = append(payload, globalTimestampOracleSnapshotMagic...)
	payload = appendGlobalTimestampOracleUvarint(payload, snapshot.Term)
	payload = appendGlobalTimestampOracleUvarint(payload, uint64(snapshot.Current))
	payload = appendGlobalTimestampOracleUvarint(payload, uint64(len(snapshot.Nodes)))
	for _, node := range snapshot.Nodes {
		if len(node.NodeID) == 0 || len(node.NodeID) > maxGlobalTimestampOracleFileStoreNodeID {
			return nil, ErrGlobalTimestampOracleFileStoreInvalid
		}
		payload = appendGlobalTimestampOracleString(payload, node.NodeID)
		payload = appendGlobalTimestampOracleUvarint(payload, node.NodeEpoch)
		payload = appendGlobalTimestampOracleUvarint(payload, node.Sequence)
		payload = appendGlobalTimestampOracleUvarint(payload, uint64(node.Observed))
		payload = appendGlobalTimestampOracleUvarint(payload, node.Grant.Term)
		payload = appendGlobalTimestampOracleUvarint(payload, uint64(node.Grant.Start))
		payload = appendGlobalTimestampOracleUvarint(payload, uint64(node.Grant.End))
		payload = appendGlobalTimestampOracleUvarint(payload, node.Grant.Count)
		if len(payload) > MaxGlobalTimestampOracleFileStoreBytes-4 {
			return nil, ErrGlobalTimestampOracleFileStoreTooLarge
		}
	}
	checksum := crc32.Checksum(payload, globalTimestampOracleCRC32CTable)
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], checksum)
	return append(payload, trailer[:]...), nil
}

// UnmarshalGlobalTimestampOracleSnapshot validates and decodes one complete
// deterministic binary snapshot.
func UnmarshalGlobalTimestampOracleSnapshot(payload []byte) (GlobalTimestampOracleSnapshot, error) {
	if len(payload) > MaxGlobalTimestampOracleFileStoreBytes {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreTooLarge
	}
	if len(payload) < len(globalTimestampOracleSnapshotMagic)+4 || string(payload[:len(globalTimestampOracleSnapshotMagic)]) != globalTimestampOracleSnapshotMagic {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
	}
	data := payload[:len(payload)-4]
	gotChecksum := binary.LittleEndian.Uint32(payload[len(payload)-4:])
	if crc32.Checksum(data, globalTimestampOracleCRC32CTable) != gotChecksum {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
	}
	offset := len(globalTimestampOracleSnapshotMagic)
	term, ok := readGlobalTimestampOracleUvarint(data, &offset)
	if !ok {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
	}
	current, ok := readGlobalTimestampOracleUvarint(data, &offset)
	if !ok || current > uint64(^uint64(0)>>1) {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
	}
	count, ok := readGlobalTimestampOracleUvarint(data, &offset)
	if !ok || count > maxGlobalTimestampOracleFileStoreNodes {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
	}
	nodes := make([]GlobalTimestampNodeSnapshot, 0, int(count))
	var previousNodeID string
	for index := uint64(0); index < count; index++ {
		nodeID, ok := readGlobalTimestampOracleString(data, &offset)
		if !ok || (index > 0 && nodeID <= previousNodeID) {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
		}
		previousNodeID = nodeID
		nodeEpoch, ok := readGlobalTimestampOracleUvarint(data, &offset)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
		}
		sequence, ok := readGlobalTimestampOracleUvarint(data, &offset)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
		}
		observed, ok := readGlobalTimestampOracleUvarint(data, &offset)
		if !ok || observed > uint64(^uint64(0)>>1) {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
		}
		grantTerm, ok := readGlobalTimestampOracleUvarint(data, &offset)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
		}
		start, ok := readGlobalTimestampOracleUvarint(data, &offset)
		if !ok || start > uint64(^uint64(0)>>1) {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
		}
		end, ok := readGlobalTimestampOracleUvarint(data, &offset)
		if !ok || end > uint64(^uint64(0)>>1) {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
		}
		grantCount, ok := readGlobalTimestampOracleUvarint(data, &offset)
		if !ok {
			return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
		}
		nodes = append(nodes, GlobalTimestampNodeSnapshot{
			NodeID:    nodeID,
			NodeEpoch: nodeEpoch,
			Sequence:  sequence,
			Observed:  int64(observed),
			Grant: GlobalTimestampGrant{
				Term:      grantTerm,
				NodeID:    nodeID,
				NodeEpoch: nodeEpoch,
				Sequence:  sequence,
				Start:     int64(start),
				End:       int64(end),
				Count:     grantCount,
			},
		})
	}
	if offset != len(data) {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
	}
	snapshot := GlobalTimestampOracleSnapshot{Term: term, Current: int64(current), Nodes: nodes}
	if err := validateCanonicalGlobalTimestampOracleSnapshot(snapshot); err != nil {
		return GlobalTimestampOracleSnapshot{}, ErrGlobalTimestampOracleFileStoreCorrupt
	}
	return snapshot, nil
}

func canonicalGlobalTimestampOracleSnapshot(snapshot GlobalTimestampOracleSnapshot) (GlobalTimestampOracleSnapshot, error) {
	if globalTimestampOracleSnapshotHasCanonicalNodeOrder(snapshot) {
		if err := validateCanonicalGlobalTimestampOracleSnapshot(snapshot); err != nil {
			return GlobalTimestampOracleSnapshot{}, err
		}
		return snapshot, nil
	}
	oracle, err := NewGlobalTimestampOracleFromSnapshot(snapshot)
	if err != nil {
		return GlobalTimestampOracleSnapshot{}, err
	}
	return oracle.Snapshot(), nil
}

func globalTimestampOracleSnapshotHasCanonicalNodeOrder(snapshot GlobalTimestampOracleSnapshot) bool {
	var previous string
	for index, node := range snapshot.Nodes {
		if node.NodeID == "" || strings.TrimSpace(node.NodeID) != node.NodeID || (index > 0 && node.NodeID <= previous) {
			return false
		}
		previous = node.NodeID
	}
	return true
}

func validateCanonicalGlobalTimestampOracleSnapshot(snapshot GlobalTimestampOracleSnapshot) error {
	if snapshot.Term == 0 || snapshot.Current < 0 || len(snapshot.Nodes) > maxGlobalTimestampOracleFileStoreNodes {
		return ErrGlobalTimestampOracleFileStoreInvalid
	}
	ranges := make([]GlobalTimestampGrant, len(snapshot.Nodes))
	var previousNodeID string
	for index, node := range snapshot.Nodes {
		if node.NodeID == "" || len(node.NodeID) > maxGlobalTimestampOracleFileStoreNodeID || node.NodeID <= previousNodeID {
			return ErrGlobalTimestampOracleFileStoreInvalid
		}
		previousNodeID = node.NodeID
		grant := node.Grant
		if grant.NodeID != node.NodeID || grant.NodeEpoch != node.NodeEpoch || grant.Sequence != node.Sequence || grant.Term > snapshot.Term || grant.End > snapshot.Current || node.Observed < 0 || node.Observed >= grant.Start || node.Observed > snapshot.Current {
			return ErrGlobalTimestampOracleFileStoreInvalid
		}
		if err := validateGlobalTimestampGrant(grant); err != nil {
			return err
		}
		ranges[index] = grant
	}
	sort.Slice(ranges, func(left, right int) bool { return ranges[left].Start < ranges[right].Start })
	for index := 1; index < len(ranges); index++ {
		if ranges[index].Start <= ranges[index-1].End {
			return ErrGlobalTimestampOracleFileStoreInvalid
		}
	}
	return nil
}

func (store *GlobalTimestampOracleFileStore) writeLocked(payload []byte) error {
	dir := filepath.Dir(store.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if _, err := lstatGlobalTimestampOracleStorePath(store.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	temporary, err := os.CreateTemp(dir, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	keepTemporary := false
	defer func() {
		if !keepTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(payload); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return err
	}
	keepTemporary = true
	directory, err := os.Open(dir)
	if err != nil {
		return err
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func lstatGlobalTimestampOracleStorePath(path string) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, ErrGlobalTimestampOracleFileStoreSymlink
	}
	if !info.Mode().IsRegular() {
		return nil, ErrGlobalTimestampOracleFileStoreInvalid
	}
	return info, nil
}

var globalTimestampOracleCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

func appendGlobalTimestampOracleUvarint(payload []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(buffer[:], value)
	return append(payload, buffer[:size]...)
}

func appendGlobalTimestampOracleString(payload []byte, value string) []byte {
	payload = appendGlobalTimestampOracleUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func readGlobalTimestampOracleUvarint(payload []byte, offset *int) (uint64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	start := *offset
	value, size := binary.Uvarint(payload[start:])
	if size <= 0 || start+size > len(payload) {
		return 0, false
	}
	var canonical [binary.MaxVarintLen64]byte
	canonicalSize := binary.PutUvarint(canonical[:], value)
	if canonicalSize != size || !bytes.Equal(canonical[:canonicalSize], payload[start:start+size]) {
		return 0, false
	}
	*offset += size
	return value, true
}

func readGlobalTimestampOracleString(payload []byte, offset *int) (string, bool) {
	length, ok := readGlobalTimestampOracleUvarint(payload, offset)
	if !ok || length == 0 || length > maxGlobalTimestampOracleFileStoreNodeID || length > uint64(len(payload)-*offset) {
		return "", false
	}
	start := *offset
	*offset += int(length)
	return string(payload[start:*offset]), true
}
