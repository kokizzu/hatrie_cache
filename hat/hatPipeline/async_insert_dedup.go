package hatPipeline

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

var (
	// ErrAsyncInsertLedgerNil indicates that a ledger-dependent operation was
	// called without a ledger.
	ErrAsyncInsertLedgerNil = errors.New("hatPipeline: async insert ledger is nil")
	// ErrAsyncInsertLedgerPathRequired indicates that no durable ledger path was
	// supplied.
	ErrAsyncInsertLedgerPathRequired = errors.New("hatPipeline: async insert ledger path is required")
	// ErrAsyncInsertLedgerMaxEntriesInvalid indicates an unsupported bound.
	ErrAsyncInsertLedgerMaxEntriesInvalid = errors.New("hatPipeline: async insert ledger max entries is invalid")
	// ErrAsyncInsertLedgerTTLInvalid indicates a non-positive expiry interval.
	ErrAsyncInsertLedgerTTLInvalid = errors.New("hatPipeline: async insert ledger ttl is invalid")
	// ErrAsyncInsertLedgerDurabilityInvalid indicates an unknown durability mode.
	ErrAsyncInsertLedgerDurabilityInvalid = errors.New("hatPipeline: async insert ledger durability is invalid")
	// ErrAsyncInsertLedgerClosed indicates that the ledger has been closed.
	ErrAsyncInsertLedgerClosed = errors.New("hatPipeline: async insert ledger is closed")
	// ErrAsyncInsertIDRequired indicates that an insert ID is empty.
	ErrAsyncInsertIDRequired = errors.New("hatPipeline: async insert ID is required")
	// ErrAsyncInsertIDTooLarge indicates that an insert ID exceeds the bounded
	// record size.
	ErrAsyncInsertIDTooLarge = errors.New("hatPipeline: async insert ID is too large")
	// ErrAsyncInsertLedgerFull indicates that the configured entry bound is full.
	ErrAsyncInsertLedgerFull = errors.New("hatPipeline: async insert ledger is full")
	// ErrAsyncInsertNotPending indicates that a commit was attempted without a
	// matching successful Acquire.
	ErrAsyncInsertNotPending = errors.New("hatPipeline: async insert ID is not pending")
	// ErrAsyncInsertLedgerCorrupt indicates invalid durable ledger bytes.
	ErrAsyncInsertLedgerCorrupt = errors.New("hatPipeline: async insert ledger is corrupt")
	// ErrDurableAsyncBatcherHandlerRequired indicates that no sink handler was
	// supplied.
	ErrDurableAsyncBatcherHandlerRequired = errors.New("hatPipeline: durable async batch handler is required")
)

const (
	// DefaultAsyncInsertLedgerMaxEntries bounds retained committed IDs.
	DefaultAsyncInsertLedgerMaxEntries = 100_000
	// MaxAsyncInsertLedgerMaxEntries prevents an accidental large map.
	MaxAsyncInsertLedgerMaxEntries = 1 << 20
	// DefaultAsyncInsertLedgerTTL bounds how long a committed ID suppresses a
	// retried asynchronous insert.
	DefaultAsyncInsertLedgerTTL = 24 * time.Hour
	maxAsyncInsertIDBytes       = 4 << 10
	asyncInsertLedgerMagic      = "HID1"
	asyncInsertLedgerCommit     = byte(1)
	asyncInsertLedgerHeaderSize = 4 + 1 + 8 + 4
	asyncInsertLedgerCRCSize    = 4
)

var asyncInsertLedgerCRCTable = crc32.MakeTable(crc32.Castagnoli)

// AsyncInsertLedgerDurability controls when a committed ID is acknowledged.
// The zero value is synchronous and is the durable default.
type AsyncInsertLedgerDurability uint8

const (
	AsyncInsertLedgerDurabilitySync AsyncInsertLedgerDurability = iota
	AsyncInsertLedgerDurabilityBuffered
)

// AsyncInsertLedgerOptions configures a bounded durable insert-ID ledger.
// Path must be on a filesystem owned by the embedding service. The ledger is
// single-writer; callers must not open the same path from multiple processes.
type AsyncInsertLedgerOptions struct {
	Path       string
	MaxEntries int
	TTL        time.Duration
	Durability AsyncInsertLedgerDurability
}

// AsyncInsertLedgerStats reports bounded ledger state.
type AsyncInsertLedgerStats struct {
	Entries   int
	Pending   int
	Records   int
	FileBytes int64
}

// AsyncInsertLedger is an append-only, CRC-protected committed-ID journal.
// Pending IDs are process-local reservations and are intentionally not written
// until the sink handler succeeds, so an interrupted handler can be replayed
// after restart. This provides durable suppression of completed retries; an
// exactly-once boundary still requires the sink to coordinate its transaction
// with the ledger.
type AsyncInsertLedger struct {
	mu         sync.Mutex
	file       *os.File
	path       string
	maxEntries int
	ttl        time.Duration
	durability AsyncInsertLedgerDurability
	entries    map[string]time.Time
	pending    map[string]struct{}
	records    int
}

// NewAsyncInsertLedger opens or creates a durable insert-ID ledger.
func NewAsyncInsertLedger(options AsyncInsertLedgerOptions) (*AsyncInsertLedger, error) {
	if options.Path == "" {
		return nil, ErrAsyncInsertLedgerPathRequired
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultAsyncInsertLedgerMaxEntries
	}
	if options.MaxEntries < 1 || options.MaxEntries > MaxAsyncInsertLedgerMaxEntries {
		return nil, ErrAsyncInsertLedgerMaxEntriesInvalid
	}
	if options.TTL == 0 {
		options.TTL = DefaultAsyncInsertLedgerTTL
	}
	if options.TTL <= 0 {
		return nil, ErrAsyncInsertLedgerTTLInvalid
	}
	if options.Durability > AsyncInsertLedgerDurabilityBuffered {
		return nil, ErrAsyncInsertLedgerDurabilityInvalid
	}
	file, err := os.OpenFile(options.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open async insert ledger: %w", err)
	}
	ledger := &AsyncInsertLedger{
		file:       file,
		path:       options.Path,
		maxEntries: options.MaxEntries,
		ttl:        options.TTL,
		durability: options.Durability,
		entries:    make(map[string]time.Time),
		pending:    make(map[string]struct{}),
	}
	if err := ledger.loadLocked(); err != nil {
		_ = file.Close()
		return nil, err
	}
	return ledger, nil
}

// Acquire reserves id for one in-flight insert. false, nil means that the ID
// is already committed or currently being processed and should be ignored.
func (ledger *AsyncInsertLedger) Acquire(id string) (bool, error) {
	if ledger == nil {
		return false, ErrAsyncInsertLedgerNil
	}
	if err := validateAsyncInsertID(id); err != nil {
		return false, err
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.file == nil {
		return false, ErrAsyncInsertLedgerClosed
	}
	now := time.Now()
	if _, err := ledger.removeExpiredLocked(now); err != nil {
		return false, err
	}
	if _, ok := ledger.pending[id]; ok {
		return false, nil
	}
	if expiry, ok := ledger.entries[id]; ok && expiry.After(now) {
		return false, nil
	}
	if len(ledger.entries)+len(ledger.pending) >= ledger.maxEntries {
		return false, ErrAsyncInsertLedgerFull
	}
	ledger.pending[id] = struct{}{}
	return true, nil
}

// Commit durably records one previously acquired ID.
func (ledger *AsyncInsertLedger) Commit(id string) error {
	return ledger.CommitAt(time.Now(), []string{id})
}

// CommitAt durably records IDs using now as their expiry reference. It is
// useful to callers that already have one batch timestamp and to deterministic
// tests.
func (ledger *AsyncInsertLedger) CommitAt(now time.Time, ids []string) error {
	if ledger == nil {
		return ErrAsyncInsertLedgerNil
	}
	if len(ids) == 0 {
		return nil
	}
	for _, id := range ids {
		if err := validateAsyncInsertID(id); err != nil {
			return err
		}
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.file == nil {
		return ErrAsyncInsertLedgerClosed
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if _, duplicate := seen[id]; duplicate {
			return fmt.Errorf("%w: duplicate ID %q", ErrAsyncInsertNotPending, id)
		}
		seen[id] = struct{}{}
		if _, ok := ledger.pending[id]; !ok {
			return fmt.Errorf("%w: %q", ErrAsyncInsertNotPending, id)
		}
	}
	expiry := now.Add(ledger.ttl)
	encoded := make([]byte, 0, len(ids)*(asyncInsertLedgerHeaderSize+asyncInsertLedgerCRCSize+32))
	for _, id := range ids {
		encoded = appendAsyncInsertLedgerRecord(encoded, id, expiry)
	}
	if err := writeAsyncInsertLedger(ledger.file, encoded); err != nil {
		return err
	}
	if ledger.durability == AsyncInsertLedgerDurabilitySync {
		if err := ledger.file.Sync(); err != nil {
			return fmt.Errorf("sync async insert ledger: %w", err)
		}
	}
	for _, id := range ids {
		delete(ledger.pending, id)
		ledger.entries[id] = expiry
	}
	ledger.records += len(ids)
	return nil
}

// Release allows a failed sink batch to be retried.
func (ledger *AsyncInsertLedger) Release(id string) error {
	return ledger.ReleaseBatch([]string{id})
}

// ReleaseBatch releases process-local reservations without writing a durable
// record.
func (ledger *AsyncInsertLedger) ReleaseBatch(ids []string) error {
	if ledger == nil {
		return ErrAsyncInsertLedgerNil
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.file == nil {
		return ErrAsyncInsertLedgerClosed
	}
	for _, id := range ids {
		delete(ledger.pending, id)
	}
	return nil
}

// PurgeExpired removes expired committed IDs and rewrites the compact ledger.
func (ledger *AsyncInsertLedger) PurgeExpired() (int, error) {
	if ledger == nil {
		return 0, ErrAsyncInsertLedgerNil
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.file == nil {
		return 0, ErrAsyncInsertLedgerClosed
	}
	removed, err := ledger.removeExpiredLocked(time.Now())
	return removed, err
}

// Stats returns a point-in-time view of retained IDs and journal size.
func (ledger *AsyncInsertLedger) Stats() AsyncInsertLedgerStats {
	if ledger == nil {
		return AsyncInsertLedgerStats{}
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	stats := AsyncInsertLedgerStats{
		Entries: len(ledger.entries),
		Pending: len(ledger.pending),
		Records: ledger.records,
	}
	if ledger.file != nil {
		if info, err := ledger.file.Stat(); err == nil {
			stats.FileBytes = info.Size()
		}
	}
	return stats
}

// Close flushes and closes the ledger. The caller owns the ledger lifetime;
// closing a DurableAsyncBatcher does not close its ledger.
func (ledger *AsyncInsertLedger) Close() error {
	if ledger == nil {
		return nil
	}
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	if ledger.file == nil {
		return nil
	}
	if err := ledger.file.Sync(); err != nil {
		return fmt.Errorf("sync async insert ledger: %w", err)
	}
	err := ledger.file.Close()
	ledger.file = nil
	return err
}

func (ledger *AsyncInsertLedger) loadLocked() error {
	if _, err := ledger.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek async insert ledger: %w", err)
	}
	stat, err := ledger.file.Stat()
	if err != nil {
		return fmt.Errorf("stat async insert ledger: %w", err)
	}
	fileSize := stat.Size()
	var offset int64
	now := time.Now()
	for offset < fileSize {
		start := offset
		header := make([]byte, asyncInsertLedgerHeaderSize)
		if _, err := io.ReadFull(ledger.file, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return ledger.truncateTailLocked(start)
			}
			return fmt.Errorf("read async insert ledger header: %w", err)
		}
		offset += int64(len(header))
		if string(header[:len(asyncInsertLedgerMagic)]) != asyncInsertLedgerMagic || header[4] != asyncInsertLedgerCommit {
			return fmt.Errorf("%w: invalid record header at byte %d", ErrAsyncInsertLedgerCorrupt, start)
		}
		idLength := binary.LittleEndian.Uint32(header[13:17])
		if idLength == 0 || idLength > maxAsyncInsertIDBytes {
			return fmt.Errorf("%w: invalid ID length %d", ErrAsyncInsertLedgerCorrupt, idLength)
		}
		idBytes := make([]byte, int(idLength))
		if _, err := io.ReadFull(ledger.file, idBytes); err != nil {
			return ledger.truncateTailLocked(start)
		}
		offset += int64(len(idBytes))
		crcBytes := make([]byte, asyncInsertLedgerCRCSize)
		if _, err := io.ReadFull(ledger.file, crcBytes); err != nil {
			return ledger.truncateTailLocked(start)
		}
		offset += int64(len(crcBytes))
		wantCRC := binary.LittleEndian.Uint32(crcBytes)
		gotCRC := crc32.Checksum(idBytes, asyncInsertLedgerCRCTable)
		if gotCRC != wantCRC {
			if offset == fileSize {
				return ledger.truncateTailLocked(start)
			}
			return fmt.Errorf("%w: checksum at byte %d", ErrAsyncInsertLedgerCorrupt, start)
		}
		ledger.records++
		expiry := time.Unix(0, int64(binary.LittleEndian.Uint64(header[5:13])))
		ledger.entries[string(idBytes)] = expiry
	}
	_, err = ledger.file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("seek async insert ledger end: %w", err)
	}
	if _, err := ledger.removeExpiredLocked(now); err != nil {
		return err
	}
	return nil
}

func (ledger *AsyncInsertLedger) truncateTailLocked(offset int64) error {
	if err := ledger.file.Truncate(offset); err != nil {
		return fmt.Errorf("truncate async insert ledger tail: %w", err)
	}
	if _, err := ledger.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek async insert ledger after truncate: %w", err)
	}
	return nil
}

func (ledger *AsyncInsertLedger) removeExpiredLocked(now time.Time) (int, error) {
	removed := 0
	for id, expiry := range ledger.entries {
		if !expiry.After(now) {
			delete(ledger.entries, id)
			removed++
		}
	}
	if removed == 0 || ledger.records <= len(ledger.entries)+16 {
		return removed, nil
	}
	if err := ledger.compactLocked(); err != nil {
		return removed, err
	}
	return removed, nil
}

func (ledger *AsyncInsertLedger) compactLocked() error {
	directory := filepath.Dir(ledger.path)
	temporary, err := os.CreateTemp(directory, filepath.Base(ledger.path)+".tmp-")
	if err != nil {
		return fmt.Errorf("create async insert ledger compaction file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer os.Remove(temporaryPath)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("chmod async insert ledger compaction file: %w", err)
	}
	ids := make([]string, 0, len(ledger.entries))
	for id := range ledger.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		encoded := appendAsyncInsertLedgerRecord(nil, id, ledger.entries[id])
		if err := writeAsyncInsertLedger(temporary, encoded); err != nil {
			_ = temporary.Close()
			return err
		}
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync async insert ledger compaction file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close async insert ledger compaction file: %w", err)
	}
	if err := ledger.file.Close(); err != nil {
		return fmt.Errorf("close async insert ledger before compaction: %w", err)
	}
	if err := os.Rename(temporaryPath, ledger.path); err != nil {
		ledger.file, _ = os.OpenFile(ledger.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
		return fmt.Errorf("replace async insert ledger: %w", err)
	}
	file, err := os.OpenFile(ledger.path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		ledger.file = nil
		return fmt.Errorf("reopen async insert ledger: %w", err)
	}
	ledger.file = file
	ledger.records = len(ids)
	directoryFile, err := os.Open(directory)
	if err == nil {
		_ = directoryFile.Sync()
		_ = directoryFile.Close()
	}
	return nil
}

func appendAsyncInsertLedgerRecord(destination []byte, id string, expiry time.Time) []byte {
	start := len(destination)
	destination = append(destination, asyncInsertLedgerMagic...)
	destination = append(destination, asyncInsertLedgerCommit)
	var encodedExpiry [8]byte
	binary.LittleEndian.PutUint64(encodedExpiry[:], uint64(expiry.UnixNano()))
	destination = append(destination, encodedExpiry[:]...)
	var encodedLength [4]byte
	binary.LittleEndian.PutUint32(encodedLength[:], uint32(len(id)))
	destination = append(destination, encodedLength[:]...)
	destination = append(destination, id...)
	checksum := crc32.Checksum([]byte(id), asyncInsertLedgerCRCTable)
	var encodedChecksum [4]byte
	binary.LittleEndian.PutUint32(encodedChecksum[:], checksum)
	destination = append(destination, encodedChecksum[:]...)
	if len(destination)-start != asyncInsertLedgerHeaderSize+len(id)+asyncInsertLedgerCRCSize {
		panic("hatrie: invalid async insert ledger record size")
	}
	return destination
}

func writeAsyncInsertLedger(file *os.File, encoded []byte) error {
	for len(encoded) > 0 {
		written, err := file.Write(encoded)
		if err != nil {
			return fmt.Errorf("write async insert ledger: %w", err)
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		encoded = encoded[written:]
	}
	return nil
}

func validateAsyncInsertID(id string) error {
	if id == "" {
		return ErrAsyncInsertIDRequired
	}
	if len(id) > maxAsyncInsertIDBytes {
		return ErrAsyncInsertIDTooLarge
	}
	return nil
}

// AsyncInsert couples an idempotency ID with one value delivered to the sink.
type AsyncInsert[T any] struct {
	ID    string
	Value T
}

// DurableAsyncBatcherOptions configures an opt-in asynchronous insert batcher
// that commits IDs only after the sink handler accepts the complete batch.
type DurableAsyncBatcherOptions[T any] struct {
	Ledger  *AsyncInsertLedger
	Batcher AsyncBatcherOptions[AsyncInsert[T]]
	Handler func(context.Context, []AsyncInsert[T]) error
}

// DurableAsyncBatcher adds durable completed-insert deduplication to
// AsyncBatcher. The ledger remains caller-owned and must be closed separately.
type DurableAsyncBatcher[T any] struct {
	batcher *AsyncBatcher[AsyncInsert[T]]
	ledger  *AsyncInsertLedger
}

// NewDurableAsyncBatcher creates a deduplicating asynchronous insert batcher.
func NewDurableAsyncBatcher[T any](options DurableAsyncBatcherOptions[T]) (*DurableAsyncBatcher[T], error) {
	if options.Ledger == nil {
		return nil, ErrAsyncInsertLedgerNil
	}
	if options.Handler == nil {
		return nil, ErrDurableAsyncBatcherHandlerRequired
	}
	options.Batcher.Handler = func(ctx context.Context, batch []AsyncInsert[T]) error {
		ids := asyncInsertIDs(batch)
		if err := options.Handler(ctx, batch); err != nil {
			_ = options.Ledger.ReleaseBatch(ids)
			return err
		}
		if err := options.Ledger.CommitAt(time.Now(), ids); err != nil {
			_ = options.Ledger.ReleaseBatch(ids)
			return err
		}
		return nil
	}
	batcher, err := NewAsyncBatcher(options.Batcher)
	if err != nil {
		return nil, err
	}
	return &DurableAsyncBatcher[T]{batcher: batcher, ledger: options.Ledger}, nil
}

// Submit accepts one insert. false, nil means the ID was already committed or
// is currently in flight and no handler work was queued.
func (batcher *DurableAsyncBatcher[T]) Submit(ctx context.Context, id string, value T) (bool, error) {
	if batcher == nil || batcher.ledger == nil || batcher.batcher == nil {
		return false, ErrAsyncInsertLedgerNil
	}
	accepted, err := batcher.ledger.Acquire(id)
	if err != nil || !accepted {
		return accepted, err
	}
	if err := batcher.batcher.Submit(ctx, AsyncInsert[T]{ID: id, Value: value}); err != nil {
		_ = batcher.ledger.Release(id)
		return false, err
	}
	return true, nil
}

// Flush waits for all accepted inserts before the marker and returns sink or
// ledger errors.
func (batcher *DurableAsyncBatcher[T]) Flush(ctx context.Context) error {
	if batcher == nil || batcher.batcher == nil {
		return ErrAsyncInsertLedgerNil
	}
	return batcher.batcher.Flush(ctx)
}

// Close drains the batcher. The ledger must be closed separately.
func (batcher *DurableAsyncBatcher[T]) Close(ctx context.Context) error {
	if batcher == nil || batcher.batcher == nil {
		return ErrAsyncInsertLedgerNil
	}
	return batcher.batcher.Close(ctx)
}

// Stats returns asynchronous batcher counters.
func (batcher *DurableAsyncBatcher[T]) Stats() AsyncBatcherStats {
	if batcher == nil || batcher.batcher == nil {
		return AsyncBatcherStats{}
	}
	return batcher.batcher.Stats()
}

func asyncInsertIDs[T any](batch []AsyncInsert[T]) []string {
	ids := make([]string, len(batch))
	for index, insert := range batch {
		ids[index] = insert.ID
	}
	return ids
}
