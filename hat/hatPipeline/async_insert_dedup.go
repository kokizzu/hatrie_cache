package hatPipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	// DefaultAsyncInsertDedupCapacity bounds the number of live insert IDs
	// retained when a caller does not choose a capacity.
	DefaultAsyncInsertDedupCapacity = 100_000
	// MaxAsyncInsertDedupCapacity prevents accidental unbounded memory use.
	MaxAsyncInsertDedupCapacity = 1 << 20
	// DefaultAsyncInsertDedupTTL bounds the normal retry window.
	DefaultAsyncInsertDedupTTL = 24 * time.Hour
	// MaxAsyncInsertDedupSourceBytes bounds one source name.
	MaxAsyncInsertDedupSourceBytes = 256
	// MaxAsyncInsertDedupIDBytes bounds one client-provided insert ID.
	MaxAsyncInsertDedupIDBytes = 512
	// DefaultAsyncInsertDedupFileMaxBytes bounds one append ledger file.
	DefaultAsyncInsertDedupFileMaxBytes = 64 << 20
	// MaxAsyncInsertDedupFileMaxBytes prevents an accidental oversized ledger.
	MaxAsyncInsertDedupFileMaxBytes = 1 << 30
)

var (
	// ErrAsyncInsertDedupInvalid indicates malformed options, source, or ID.
	ErrAsyncInsertDedupInvalid = errors.New("hatPipeline: async insert deduplication input is invalid")
	// ErrAsyncInsertConflict indicates reuse of an ID with different payload.
	ErrAsyncInsertConflict = errors.New("hatPipeline: async insert ID payload conflicts")
	// ErrAsyncInsertCapacity indicates that all ledger slots are retained.
	ErrAsyncInsertCapacity = errors.New("hatPipeline: async insert deduplication capacity exceeded")
	// ErrAsyncInsertDedupCorrupt indicates a malformed durable ledger.
	ErrAsyncInsertDedupCorrupt = errors.New("hatPipeline: async insert deduplication ledger is corrupt")
	// ErrAsyncInsertDedupCompactionUnsupported indicates a store without
	// optional compaction support.
	ErrAsyncInsertDedupCompactionUnsupported = errors.New("hatPipeline: async insert deduplication store cannot compact")
	// ErrAsyncInsertDedupFilePathEmpty indicates that a file store has no path.
	ErrAsyncInsertDedupFilePathEmpty = errors.New("hatPipeline: async insert deduplication ledger path is empty")
	// ErrAsyncInsertDedupFileOptionsInvalid indicates an invalid file bound.
	ErrAsyncInsertDedupFileOptionsInvalid = errors.New("hatPipeline: async insert deduplication file options are invalid")
	// ErrAsyncInsertDedupFileTooLarge indicates a file or append would exceed
	// the configured bound.
	ErrAsyncInsertDedupFileTooLarge = errors.New("hatPipeline: async insert deduplication ledger is too large")
	// ErrAsyncInsertDedupSymlink indicates that a ledger path is a symlink.
	ErrAsyncInsertDedupSymlink = errors.New("hatPipeline: async insert deduplication ledger path is a symlink")
)

var asyncInsertDedupCRCTable = crc32.MakeTable(crc32.Castagnoli)

// AsyncInsertDedupDecision describes the result of an insert-ID admission.
type AsyncInsertDedupDecision uint8

const (
	AsyncInsertAccepted AsyncInsertDedupDecision = iota + 1
	AsyncInsertDuplicate
)

// String returns the stable decision name.
func (decision AsyncInsertDedupDecision) String() string {
	switch decision {
	case AsyncInsertAccepted:
		return "accepted"
	case AsyncInsertDuplicate:
		return "duplicate"
	default:
		return "invalid"
	}
}

// AsyncInsertDedupRecord is one durable ID and payload digest.
// ExpiresAt is stored in UTC and is exclusive.
type AsyncInsertDedupRecord struct {
	Source    string
	ID        string
	Digest    [sha256.Size]byte
	ExpiresAt time.Time
}

// AsyncInsertDedupStore persists accepted IDs. Append must return only after
// the record is durable according to the store's contract.
type AsyncInsertDedupStore interface {
	Load(context.Context) ([]AsyncInsertDedupRecord, error)
	Append(context.Context, AsyncInsertDedupRecord) error
}

// AsyncInsertDedupCompactor is an optional store extension used by Compact.
type AsyncInsertDedupCompactor interface {
	Compact(context.Context, []AsyncInsertDedupRecord) error
}

// AsyncInsertDeduplicatorOptions configures an opt-in insert-ID ledger. A nil
// Store keeps the ledger in memory only; the default AsyncBatcher is unaffected.
type AsyncInsertDeduplicatorOptions struct {
	Capacity int
	TTL      time.Duration
	Now      func() time.Time
	Store    AsyncInsertDedupStore
}

// AsyncInsertDedupStats is a point-in-time ledger report.
type AsyncInsertDedupStats struct {
	Capacity   int
	Entries    int
	Accepted   uint64
	Duplicates uint64
	Conflicts  uint64
	Expired    uint64
}

type asyncInsertDedupKey struct {
	source string
	id     string
}

// AsyncInsertDeduplicator admits each source/ID pair once for the configured
// retry window. The digest detects accidental ID reuse with changed payload.
// Accept serializes store append and in-memory admission so a durable caller
// never observes an accepted ID that was not appended first.
type AsyncInsertDeduplicator struct {
	mu       sync.Mutex
	capacity int
	ttl      time.Duration
	now      func() time.Time
	store    AsyncInsertDedupStore
	entries  map[asyncInsertDedupKey]AsyncInsertDedupRecord
	stats    AsyncInsertDedupStats
}

// NewAsyncInsertDeduplicator creates a bounded insert-ID ledger and restores
// live records from Store when one is supplied.
func NewAsyncInsertDeduplicator(options AsyncInsertDeduplicatorOptions) (*AsyncInsertDeduplicator, error) {
	if options.Capacity == 0 {
		options.Capacity = DefaultAsyncInsertDedupCapacity
	}
	if options.Capacity < 1 || options.Capacity > MaxAsyncInsertDedupCapacity {
		return nil, ErrAsyncInsertDedupInvalid
	}
	if options.TTL == 0 {
		options.TTL = DefaultAsyncInsertDedupTTL
	}
	if options.TTL <= 0 {
		return nil, ErrAsyncInsertDedupInvalid
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	deduplicator := &AsyncInsertDeduplicator{
		capacity: options.Capacity,
		ttl:      options.TTL,
		now:      options.Now,
		store:    options.Store,
		entries:  make(map[asyncInsertDedupKey]AsyncInsertDedupRecord),
		stats:    AsyncInsertDedupStats{Capacity: options.Capacity},
	}
	if options.Store == nil {
		return deduplicator, nil
	}
	records, err := options.Store.Load(context.Background())
	if err != nil {
		return nil, err
	}
	now := deduplicator.now()
	for _, record := range records {
		record, err = normalizeAsyncInsertDedupRecord(record)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrAsyncInsertDedupCorrupt, err)
		}
		if !record.ExpiresAt.After(now) {
			continue
		}
		key := asyncInsertDedupKey{source: record.Source, id: record.ID}
		if previous, exists := deduplicator.entries[key]; exists {
			if previous.Digest != record.Digest {
				return nil, fmt.Errorf("%w: source=%q id=%q", ErrAsyncInsertDedupCorrupt, record.Source, record.ID)
			}
			if record.ExpiresAt.After(previous.ExpiresAt) {
				deduplicator.entries[key] = record
			}
			continue
		}
		if len(deduplicator.entries) >= options.Capacity {
			return nil, ErrAsyncInsertCapacity
		}
		deduplicator.entries[key] = record
	}
	deduplicator.stats.Entries = len(deduplicator.entries)
	return deduplicator, nil
}

// Accept durably admits one insert ID, returns Duplicate for the same digest,
// and rejects a conflicting reuse. A durable Store is appended before memory
// is changed; a failed append therefore leaves the ID retryable.
func (deduplicator *AsyncInsertDeduplicator) Accept(ctx context.Context, source, id string, payload []byte) (AsyncInsertDedupDecision, error) {
	if deduplicator == nil {
		return 0, ErrAsyncInsertDedupInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	source, id, err := normalizeAsyncInsertDedupIdentity(source, id)
	if err != nil {
		return 0, err
	}
	digest := sha256.Sum256(payload)
	now := deduplicator.now()
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	key := asyncInsertDedupKey{source: source, id: id}
	if record, exists := deduplicator.entries[key]; exists {
		if !record.ExpiresAt.After(now) {
			delete(deduplicator.entries, key)
			deduplicator.stats.Expired++
			deduplicator.stats.Entries = len(deduplicator.entries)
		} else {
			if record.Digest == digest {
				deduplicator.stats.Duplicates++
				return AsyncInsertDuplicate, nil
			}
			deduplicator.stats.Conflicts++
			return 0, fmt.Errorf("%w: source=%q id=%q", ErrAsyncInsertConflict, source, id)
		}
	}
	if len(deduplicator.entries) >= deduplicator.capacity {
		deduplicator.pruneExpiredLocked(now)
	}
	if len(deduplicator.entries) >= deduplicator.capacity {
		return 0, ErrAsyncInsertCapacity
	}
	record := AsyncInsertDedupRecord{Source: source, ID: id, Digest: digest, ExpiresAt: now.Add(deduplicator.ttl).UTC()}
	if deduplicator.store != nil {
		if err := deduplicator.store.Append(ctx, record); err != nil {
			return 0, err
		}
	}
	deduplicator.entries[key] = record
	deduplicator.stats.Accepted++
	deduplicator.stats.Entries = len(deduplicator.entries)
	return AsyncInsertAccepted, nil
}

// Compact removes expired records from a durable store when it implements
// AsyncInsertDedupCompactor. It is explicit so normal Accept calls do not pay
// an O(n) rewrite cost on the hot path.
func (deduplicator *AsyncInsertDeduplicator) Compact(ctx context.Context) error {
	if deduplicator == nil {
		return ErrAsyncInsertDedupInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	compactor, ok := deduplicator.store.(AsyncInsertDedupCompactor)
	if !ok {
		return ErrAsyncInsertDedupCompactionUnsupported
	}
	deduplicator.pruneExpiredLocked(deduplicator.now())
	records := deduplicator.snapshotLocked()
	return compactor.Compact(ctx, records)
}

// Snapshot returns sorted live records for diagnostics or a custom compactor.
func (deduplicator *AsyncInsertDeduplicator) Snapshot() []AsyncInsertDedupRecord {
	if deduplicator == nil {
		return nil
	}
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	deduplicator.pruneExpiredLocked(deduplicator.now())
	return deduplicator.snapshotLocked()
}

// Stats returns current bounded counters without flushing or compacting.
func (deduplicator *AsyncInsertDeduplicator) Stats() AsyncInsertDedupStats {
	if deduplicator == nil {
		return AsyncInsertDedupStats{}
	}
	deduplicator.mu.Lock()
	defer deduplicator.mu.Unlock()
	deduplicator.pruneExpiredLocked(deduplicator.now())
	stats := deduplicator.stats
	stats.Entries = len(deduplicator.entries)
	return stats
}

func (deduplicator *AsyncInsertDeduplicator) pruneExpiredLocked(now time.Time) {
	for key, record := range deduplicator.entries {
		if !record.ExpiresAt.After(now) {
			delete(deduplicator.entries, key)
			deduplicator.stats.Expired++
		}
	}
	deduplicator.stats.Entries = len(deduplicator.entries)
}

func (deduplicator *AsyncInsertDeduplicator) snapshotLocked() []AsyncInsertDedupRecord {
	records := make([]AsyncInsertDedupRecord, 0, len(deduplicator.entries))
	for _, record := range deduplicator.entries {
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool {
		if records[i].Source != records[j].Source {
			return records[i].Source < records[j].Source
		}
		return records[i].ID < records[j].ID
	})
	return records
}

// AsyncInsertDedupFileStoreOptions configures the private append ledger.
type AsyncInsertDedupFileStoreOptions struct {
	Path     string
	MaxBytes int
}

// AsyncInsertDedupFileStore is a private CRC-protected append-only ledger.
// Compact atomically rewrites live records and bounds retained disk usage.
type AsyncInsertDedupFileStore struct {
	mu       sync.Mutex
	path     string
	maxBytes int
}

// NewAsyncInsertDedupFileStore creates a durable file store. Its parent
// directory must already exist and its path must not be a symlink.
func NewAsyncInsertDedupFileStore(options AsyncInsertDedupFileStoreOptions) (*AsyncInsertDedupFileStore, error) {
	if options.Path == "" {
		return nil, ErrAsyncInsertDedupFilePathEmpty
	}
	if options.MaxBytes == 0 {
		options.MaxBytes = DefaultAsyncInsertDedupFileMaxBytes
	}
	if options.MaxBytes < 1 || options.MaxBytes > MaxAsyncInsertDedupFileMaxBytes {
		return nil, ErrAsyncInsertDedupFileOptionsInvalid
	}
	return &AsyncInsertDedupFileStore{path: filepath.Clean(options.Path), maxBytes: options.MaxBytes}, nil
}

// Load replays all valid append records. A missing file is an empty ledger.
func (store *AsyncInsertDedupFileStore) Load(ctx context.Context) ([]AsyncInsertDedupRecord, error) {
	if store == nil {
		return nil, ErrAsyncInsertDedupInvalid
	}
	if err := asyncInsertDedupContextErr(ctx); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := rejectAsyncInsertDedupSymlink(store.path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	file, err := os.Open(store.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < 0 || info.Size() > int64(store.maxBytes) {
		return nil, ErrAsyncInsertDedupFileTooLarge
	}
	data := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(file, data); err != nil {
		return nil, err
	}
	records, err := decodeAsyncInsertDedupFrames(data)
	if err != nil {
		return nil, err
	}
	if err := asyncInsertDedupContextErr(ctx); err != nil {
		return nil, err
	}
	return records, nil
}

// Append writes one CRC-protected record and fsyncs it before returning.
func (store *AsyncInsertDedupFileStore) Append(ctx context.Context, record AsyncInsertDedupRecord) error {
	if store == nil {
		return ErrAsyncInsertDedupInvalid
	}
	if err := asyncInsertDedupContextErr(ctx); err != nil {
		return err
	}
	frame, err := encodeAsyncInsertDedupFrame(record)
	if err != nil {
		return err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := rejectAsyncInsertDedupSymlink(store.path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	file, err := os.OpenFile(store.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := file.Chmod(0o600); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() < 0 || info.Size() > int64(store.maxBytes)-int64(len(frame)) {
		return ErrAsyncInsertDedupFileTooLarge
	}
	if err := asyncInsertDedupContextErr(ctx); err != nil {
		return err
	}
	if _, err := file.Write(frame); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	return asyncInsertDedupContextErr(ctx)
}

// Compact atomically replaces the ledger with the supplied live records.
func (store *AsyncInsertDedupFileStore) Compact(ctx context.Context, records []AsyncInsertDedupRecord) error {
	if store == nil {
		return ErrAsyncInsertDedupInvalid
	}
	if err := asyncInsertDedupContextErr(ctx); err != nil {
		return err
	}
	data := make([]byte, 0)
	for _, record := range records {
		frame, err := encodeAsyncInsertDedupFrame(record)
		if err != nil {
			return err
		}
		if len(data) > store.maxBytes-len(frame) {
			return ErrAsyncInsertDedupFileTooLarge
		}
		data = append(data, frame...)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := rejectAsyncInsertDedupSymlink(store.path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	directory := filepath.Dir(store.path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(store.path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := asyncInsertDedupContextErr(ctx); err != nil {
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
	if err := asyncInsertDedupContextErr(ctx); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, store.path); err != nil {
		return err
	}
	return syncAsyncInsertDedupDirectory(directory)
}

func normalizeAsyncInsertDedupIdentity(source, id string) (string, string, error) {
	source = strings.TrimSpace(source)
	id = strings.TrimSpace(id)
	if source == "" || id == "" || len(source) > MaxAsyncInsertDedupSourceBytes || len(id) > MaxAsyncInsertDedupIDBytes || !utf8.ValidString(source) || !utf8.ValidString(id) {
		return "", "", ErrAsyncInsertDedupInvalid
	}
	return source, id, nil
}

func normalizeAsyncInsertDedupRecord(record AsyncInsertDedupRecord) (AsyncInsertDedupRecord, error) {
	source, id, err := normalizeAsyncInsertDedupIdentity(record.Source, record.ID)
	if err != nil {
		return AsyncInsertDedupRecord{}, err
	}
	if record.ExpiresAt.IsZero() {
		return AsyncInsertDedupRecord{}, ErrAsyncInsertDedupInvalid
	}
	record.Source = source
	record.ID = id
	record.ExpiresAt = record.ExpiresAt.UTC()
	return record, nil
}

func encodeAsyncInsertDedupFrame(record AsyncInsertDedupRecord) ([]byte, error) {
	record, err := normalizeAsyncInsertDedupRecord(record)
	if err != nil {
		return nil, err
	}
	payload := make([]byte, 0, len(record.Source)+len(record.ID)+64)
	payload = appendAsyncInsertDedupUvarint(payload, uint64(len(record.Source)))
	payload = append(payload, record.Source...)
	payload = appendAsyncInsertDedupUvarint(payload, uint64(len(record.ID)))
	payload = append(payload, record.ID...)
	var expiration [8]byte
	binary.LittleEndian.PutUint64(expiration[:], uint64(record.ExpiresAt.UnixNano()))
	payload = append(payload, expiration[:]...)
	payload = append(payload, record.Digest[:]...)
	if len(payload) > int(^uint32(0)) {
		return nil, ErrAsyncInsertDedupFileTooLarge
	}
	frame := make([]byte, 0, 12+len(payload))
	frame = append(frame, 'H', 'A', 'D', '1')
	var length [4]byte
	binary.LittleEndian.PutUint32(length[:], uint32(len(payload)))
	frame = append(frame, length[:]...)
	frame = append(frame, payload...)
	var checksum [4]byte
	binary.LittleEndian.PutUint32(checksum[:], crc32.Checksum(payload, asyncInsertDedupCRCTable))
	frame = append(frame, checksum[:]...)
	return frame, nil
}

func decodeAsyncInsertDedupFrames(data []byte) ([]AsyncInsertDedupRecord, error) {
	records := make([]AsyncInsertDedupRecord, 0)
	for len(data) > 0 {
		if len(data) < 12 || !bytes.Equal(data[:4], []byte{'H', 'A', 'D', '1'}) {
			return nil, ErrAsyncInsertDedupCorrupt
		}
		payloadLength := int(binary.LittleEndian.Uint32(data[4:8]))
		if payloadLength < 2+8+sha256.Size || payloadLength > len(data)-12 {
			return nil, ErrAsyncInsertDedupCorrupt
		}
		payload := data[8 : 8+payloadLength]
		wantChecksum := binary.LittleEndian.Uint32(data[8+payloadLength : 12+payloadLength])
		if crc32.Checksum(payload, asyncInsertDedupCRCTable) != wantChecksum {
			return nil, ErrAsyncInsertDedupCorrupt
		}
		record, err := decodeAsyncInsertDedupRecord(payload)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrAsyncInsertDedupCorrupt, err)
		}
		records = append(records, record)
		data = data[12+payloadLength:]
	}
	return records, nil
}

func decodeAsyncInsertDedupRecord(payload []byte) (AsyncInsertDedupRecord, error) {
	sourceLength, sourceBytes, ok := readAsyncInsertDedupUvarint(payload)
	if !ok || sourceLength > MaxAsyncInsertDedupSourceBytes || len(sourceBytes) < int(sourceLength) {
		return AsyncInsertDedupRecord{}, ErrAsyncInsertDedupInvalid
	}
	source := string(sourceBytes[:sourceLength])
	payload = sourceBytes[sourceLength:]
	idLength, idBytes, ok := readAsyncInsertDedupUvarint(payload)
	if !ok || idLength > MaxAsyncInsertDedupIDBytes || len(idBytes) < int(idLength) {
		return AsyncInsertDedupRecord{}, ErrAsyncInsertDedupInvalid
	}
	id := string(idBytes[:idLength])
	payload = idBytes[idLength:]
	if len(payload) != 8+sha256.Size {
		return AsyncInsertDedupRecord{}, ErrAsyncInsertDedupInvalid
	}
	nanoseconds := int64(binary.LittleEndian.Uint64(payload[:8]))
	var digest [sha256.Size]byte
	copy(digest[:], payload[8:])
	return normalizeAsyncInsertDedupRecord(AsyncInsertDedupRecord{
		Source:    source,
		ID:        id,
		Digest:    digest,
		ExpiresAt: time.Unix(0, nanoseconds),
	})
}

func appendAsyncInsertDedupUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:length]...)
}

func readAsyncInsertDedupUvarint(payload []byte) (uint64, []byte, bool) {
	value, length := binary.Uvarint(payload)
	if length <= 0 || length > len(payload) {
		return 0, nil, false
	}
	return value, payload[length:], true
}

func rejectAsyncInsertDedupSymlink(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ErrAsyncInsertDedupSymlink
	}
	return nil
}

func syncAsyncInsertDedupDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func asyncInsertDedupContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

var _ AsyncInsertDedupCompactor = (*AsyncInsertDedupFileStore)(nil)
