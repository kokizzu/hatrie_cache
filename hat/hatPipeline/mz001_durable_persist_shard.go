package hatPipeline

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"unicode/utf8"
)

const (
	// DurablePersistShardSnapshotVersion is the version of the shard envelope.
	DurablePersistShardSnapshotVersion uint8 = 1
	// DefaultDurablePersistShardMaxBytes bounds one encoded shard snapshot.
	DefaultDurablePersistShardMaxBytes = 16 << 20
	// MaxDurablePersistShardMaxBytes prevents accidental unbounded allocations.
	MaxDurablePersistShardMaxBytes = 256 << 20

	durablePersistShardMagic          = "HPS1"
	durablePersistShardChecksumBytes  = 4
	durablePersistShardMinimumMaxSize = 128
	maxDurablePersistShardIDBytes     = 256
)

var (
	// ErrDurablePersistShardInvalid indicates a nil or malformed shard.
	ErrDurablePersistShardInvalid = errors.New("hatPipeline: durable persist shard is invalid")
	// ErrDurablePersistShardOptionsInvalid indicates an invalid shard bound.
	ErrDurablePersistShardOptionsInvalid = errors.New("hatPipeline: durable persist shard options are invalid")
	// ErrDurablePersistShardSnapshotInvalid indicates a malformed or corrupt
	// encoded snapshot.
	ErrDurablePersistShardSnapshotInvalid = errors.New("hatPipeline: durable persist shard snapshot is invalid")
	// ErrDurablePersistShardStale indicates a generation or frontier regression.
	ErrDurablePersistShardStale = errors.New("hatPipeline: durable persist shard snapshot is stale")
	// ErrDurablePersistShardEmpty indicates that no state has been published.
	ErrDurablePersistShardEmpty = errors.New("hatPipeline: durable persist shard has no state")
	// ErrDurablePersistShardStoreRequired indicates a missing durable store.
	ErrDurablePersistShardStoreRequired = errors.New("hatPipeline: durable persist shard store is required")
	// ErrDurablePersistShardPayloadTooLarge indicates a snapshot exceeds its
	// configured bound.
	ErrDurablePersistShardPayloadTooLarge = errors.New("hatPipeline: durable persist shard payload is too large")
)

// DurablePersistShardOptions configures one independently hydratable shard.
type DurablePersistShardOptions struct {
	ShardID  string
	MaxBytes int
}

// DurablePersistShardSnapshot is a detached materialized shard checkpoint.
// Payload is opaque to this package and may be decoded directly by the caller
// without rereading the source collection.
type DurablePersistShardSnapshot struct {
	Version    uint8
	ShardID    string
	Generation uint64
	Upper      uint64
	Payload    []byte
}

// DurablePersistShard stores one monotone, durable collection shard state.
// It does not retain a source reader or run background work. The embedding
// dataflow owns payload interpretation and chooses when to Save or Hydrate.
type DurablePersistShard struct {
	mu         sync.RWMutex
	shardID    string
	maxBytes   int
	generation uint64
	upper      uint64
	payload    []byte
}

// NewDurablePersistShard creates an empty bounded shard.
func NewDurablePersistShard(options DurablePersistShardOptions) (*DurablePersistShard, error) {
	shardID, err := normalizeDurablePersistShardID(options.ShardID)
	if err != nil {
		return nil, err
	}
	maxBytes := options.MaxBytes
	if maxBytes == 0 {
		maxBytes = DefaultDurablePersistShardMaxBytes
	}
	if maxBytes < durablePersistShardMinimumMaxSize || maxBytes > MaxDurablePersistShardMaxBytes {
		return nil, fmt.Errorf("%w: max bytes must be from %d through %d", ErrDurablePersistShardOptionsInvalid, durablePersistShardMinimumMaxSize, MaxDurablePersistShardMaxBytes)
	}
	return &DurablePersistShard{shardID: shardID, maxBytes: maxBytes}, nil
}

// ShardID returns the immutable shard identity.
func (shard *DurablePersistShard) ShardID() string {
	if shard == nil {
		return ""
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.shardID
}

// Publish atomically replaces the shard payload at a newer generation. A
// repeated identical generation is idempotent, which lets an upstream retry a
// successful publication without rereading the source.
func (shard *DurablePersistShard) Publish(generation, upper uint64, payload []byte) error {
	if shard == nil {
		return ErrDurablePersistShardInvalid
	}
	if generation == 0 {
		return fmt.Errorf("%w: generation must be positive", ErrDurablePersistShardInvalid)
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if err := shard.validatePayloadLocked(payload); err != nil {
		return err
	}
	if shard.generation != 0 {
		if generation < shard.generation || upper < shard.upper {
			return ErrDurablePersistShardStale
		}
		if generation == shard.generation {
			if upper == shard.upper && bytes.Equal(payload, shard.payload) {
				return nil
			}
			return ErrDurablePersistShardStale
		}
	}
	shard.generation = generation
	shard.upper = upper
	shard.payload = append(shard.payload[:0], payload...)
	return nil
}

// Snapshot returns a detached checkpoint. The boolean is false before the
// first successful Publish or Hydrate.
func (shard *DurablePersistShard) Snapshot() (DurablePersistShardSnapshot, bool) {
	if shard == nil {
		return DurablePersistShardSnapshot{}, false
	}
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	if shard.generation == 0 {
		return DurablePersistShardSnapshot{}, false
	}
	return DurablePersistShardSnapshot{
		Version:    DurablePersistShardSnapshotVersion,
		ShardID:    shard.shardID,
		Generation: shard.generation,
		Upper:      shard.upper,
		Payload:    append([]byte(nil), shard.payload...),
	}, true
}

// MarshalSnapshot encodes the current checkpoint with a CRC32 integrity guard.
func (shard *DurablePersistShard) MarshalSnapshot() ([]byte, error) {
	if shard == nil {
		return nil, ErrDurablePersistShardInvalid
	}
	snapshot, ok := shard.Snapshot()
	if !ok {
		return nil, ErrDurablePersistShardEmpty
	}
	return marshalDurablePersistShardSnapshot(snapshot, shard.maxBytes)
}

// MarshalBinary encodes a detached checkpoint using the maximum supported
// envelope size. Prefer DurablePersistShard.MarshalSnapshot for configured
// size enforcement.
func (snapshot DurablePersistShardSnapshot) MarshalBinary() ([]byte, error) {
	return marshalDurablePersistShardSnapshot(snapshot, MaxDurablePersistShardMaxBytes)
}

// UnmarshalDurablePersistShardSnapshot verifies and decodes one checkpoint.
func UnmarshalDurablePersistShardSnapshot(encoded []byte) (DurablePersistShardSnapshot, error) {
	return unmarshalDurablePersistShardSnapshot(encoded, MaxDurablePersistShardMaxBytes)
}

// RestoreSnapshot validates and atomically applies one encoded checkpoint.
// Invalid or stale input never changes the current state.
func (shard *DurablePersistShard) RestoreSnapshot(encoded []byte) error {
	if shard == nil {
		return ErrDurablePersistShardInvalid
	}
	snapshot, err := unmarshalDurablePersistShardSnapshot(encoded, shard.maxBytes)
	if err != nil {
		return err
	}
	if snapshot.ShardID != shard.shardID {
		return fmt.Errorf("%w: shard ID %q does not match %q", ErrDurablePersistShardSnapshotInvalid, snapshot.ShardID, shard.shardID)
	}
	shard.mu.Lock()
	defer shard.mu.Unlock()
	if shard.generation != 0 {
		if snapshot.Generation < shard.generation || snapshot.Upper < shard.upper {
			return ErrDurablePersistShardStale
		}
		if snapshot.Generation == shard.generation {
			if snapshot.Upper == shard.upper && bytes.Equal(snapshot.Payload, shard.payload) {
				return nil
			}
			return ErrDurablePersistShardStale
		}
	}
	shard.generation = snapshot.Generation
	shard.upper = snapshot.Upper
	shard.payload = append(shard.payload[:0], snapshot.Payload...)
	return nil
}

// Save writes the current checkpoint through a caller-supplied durable store.
// FrontierSnapshotStore is intentionally reused so existing atomic file stores
// can persist shard state without a second storage implementation.
func (shard *DurablePersistShard) Save(ctx context.Context, store FrontierSnapshotStore) error {
	if shard == nil {
		return ErrDurablePersistShardInvalid
	}
	if store == nil {
		return ErrDurablePersistShardStoreRequired
	}
	if err := durablePersistShardContextErr(ctx); err != nil {
		return err
	}
	payload, err := shard.MarshalSnapshot()
	if err != nil {
		return err
	}
	if err := durablePersistShardContextErr(ctx); err != nil {
		return err
	}
	return store.Save(ctx, payload)
}

// Hydrate loads one checkpoint from durable storage. A missing store payload
// returns found=false. The source collection is never consulted.
func (shard *DurablePersistShard) Hydrate(ctx context.Context, store FrontierSnapshotStore) (found bool, err error) {
	if shard == nil {
		return false, ErrDurablePersistShardInvalid
	}
	if store == nil {
		return false, ErrDurablePersistShardStoreRequired
	}
	if err := durablePersistShardContextErr(ctx); err != nil {
		return false, err
	}
	payload, err := store.Load(ctx)
	if err != nil {
		return false, err
	}
	if payload == nil {
		return false, nil
	}
	if err := durablePersistShardContextErr(ctx); err != nil {
		return false, err
	}
	if err := shard.RestoreSnapshot(payload); err != nil {
		return false, err
	}
	return true, nil
}

func (shard *DurablePersistShard) validatePayloadLocked(payload []byte) error {
	if len(payload) > shard.maxBytes {
		return fmt.Errorf("%w: payload bytes=%d max=%d", ErrDurablePersistShardPayloadTooLarge, len(payload), shard.maxBytes)
	}
	return nil
}

func normalizeDurablePersistShardID(shardID string) (string, error) {
	if shardID == "" || !utf8.ValidString(shardID) || len(shardID) > maxDurablePersistShardIDBytes {
		return "", fmt.Errorf("%w: shard ID must be non-empty valid UTF-8 and at most %d bytes", ErrDurablePersistShardOptionsInvalid, maxDurablePersistShardIDBytes)
	}
	return shardID, nil
}

func marshalDurablePersistShardSnapshot(snapshot DurablePersistShardSnapshot, maxBytes int) ([]byte, error) {
	if snapshot.Version != DurablePersistShardSnapshotVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", ErrDurablePersistShardSnapshotInvalid, snapshot.Version)
	}
	shardID, err := normalizeDurablePersistShardID(snapshot.ShardID)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDurablePersistShardSnapshotInvalid, err)
	}
	if snapshot.Generation == 0 {
		return nil, fmt.Errorf("%w: generation must be positive", ErrDurablePersistShardSnapshotInvalid)
	}
	if len(snapshot.Payload) > maxBytes {
		return nil, fmt.Errorf("%w: payload bytes=%d max=%d", ErrDurablePersistShardPayloadTooLarge, len(snapshot.Payload), maxBytes)
	}
	encoded := make([]byte, 0, durablePersistShardMinimumMaxSize+len(shardID)+len(snapshot.Payload))
	encoded = append(encoded, durablePersistShardMagic...)
	encoded = append(encoded, snapshot.Version)
	encoded = appendDurablePersistShardUvarint(encoded, uint64(len(shardID)))
	encoded = append(encoded, shardID...)
	encoded = appendDurablePersistShardUvarint(encoded, snapshot.Generation)
	encoded = appendDurablePersistShardUvarint(encoded, snapshot.Upper)
	encoded = appendDurablePersistShardUvarint(encoded, uint64(len(snapshot.Payload)))
	encoded = append(encoded, snapshot.Payload...)
	checksum := crc32.ChecksumIEEE(encoded)
	var checksumBytes [durablePersistShardChecksumBytes]byte
	binary.LittleEndian.PutUint32(checksumBytes[:], checksum)
	encoded = append(encoded, checksumBytes[:]...)
	if len(encoded) > maxBytes {
		return nil, fmt.Errorf("%w: encoded bytes=%d max=%d", ErrDurablePersistShardPayloadTooLarge, len(encoded), maxBytes)
	}
	return encoded, nil
}

func unmarshalDurablePersistShardSnapshot(encoded []byte, maxBytes int) (DurablePersistShardSnapshot, error) {
	invalid := func(format string, args ...interface{}) (DurablePersistShardSnapshot, error) {
		return DurablePersistShardSnapshot{}, fmt.Errorf("%w: %s", ErrDurablePersistShardSnapshotInvalid, fmt.Sprintf(format, args...))
	}
	if len(encoded) < len(durablePersistShardMagic)+1+durablePersistShardChecksumBytes {
		return invalid("snapshot is truncated")
	}
	if len(encoded) > maxBytes {
		return DurablePersistShardSnapshot{}, fmt.Errorf("%w: encoded bytes=%d max=%d", ErrDurablePersistShardPayloadTooLarge, len(encoded), maxBytes)
	}
	body := encoded[:len(encoded)-durablePersistShardChecksumBytes]
	if string(body[:len(durablePersistShardMagic)]) != durablePersistShardMagic {
		return invalid("magic does not match")
	}
	version := body[len(durablePersistShardMagic)]
	if version != DurablePersistShardSnapshotVersion {
		return invalid("unsupported version %d", version)
	}
	expected := binary.LittleEndian.Uint32(encoded[len(body):])
	if actual := crc32.ChecksumIEEE(body); actual != expected {
		return invalid("checksum does not match")
	}
	offset := len(durablePersistShardMagic) + 1
	idBytes, err := readDurablePersistShardBytes(body, &offset, maxDurablePersistShardIDBytes)
	if err != nil {
		return invalid("shard ID: %v", err)
	}
	shardID, err := normalizeDurablePersistShardID(string(idBytes))
	if err != nil {
		return invalid("shard ID: %v", err)
	}
	generation, err := readDurablePersistShardUvarint(body, &offset)
	if err != nil || generation == 0 {
		return invalid("generation is invalid")
	}
	upper, err := readDurablePersistShardUvarint(body, &offset)
	if err != nil {
		return invalid("upper frontier is invalid")
	}
	payload, err := readDurablePersistShardBytes(body, &offset, maxBytes)
	if err != nil {
		return invalid("payload: %v", err)
	}
	if offset != len(body) {
		return invalid("trailing bytes")
	}
	return DurablePersistShardSnapshot{
		Version:    version,
		ShardID:    shardID,
		Generation: generation,
		Upper:      upper,
		Payload:    payload,
	}, nil
}

func appendDurablePersistShardUvarint(encoded []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buffer[:], value)
	return append(encoded, buffer[:n]...)
}

func readDurablePersistShardUvarint(encoded []byte, offset *int) (uint64, error) {
	if *offset >= len(encoded) {
		return 0, errors.New("missing integer")
	}
	value, width := binary.Uvarint(encoded[*offset:])
	if width <= 0 {
		return 0, errors.New("invalid integer")
	}
	*offset += width
	return value, nil
}

func readDurablePersistShardBytes(encoded []byte, offset *int, maxBytes int) ([]byte, error) {
	length, err := readDurablePersistShardUvarint(encoded, offset)
	if err != nil {
		return nil, err
	}
	if length > uint64(maxBytes) || length > uint64(len(encoded)-*offset) {
		return nil, errors.New("length exceeds snapshot bounds")
	}
	end := *offset + int(length)
	value := append([]byte(nil), encoded[*offset:end]...)
	*offset = end
	return value, nil
}

func durablePersistShardContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
