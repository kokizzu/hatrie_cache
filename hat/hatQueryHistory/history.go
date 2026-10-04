package hatQueryHistory

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"
)

const (
	formatVersion = "hatrie-cache-query-history/v1"

	// DefaultMaxEntries bounds the in-memory and recoverable history by default.
	DefaultMaxEntries = 1024
	// DefaultMaxEntryBytes bounds one redacted JSON record by default.
	DefaultMaxEntryBytes = 4 << 10
	// DefaultMaxFileBytes rotates the active append-only file by default.
	DefaultMaxFileBytes int64 = 16 << 20

	maxQueryIDBytes         = 256
	maxErrorCodeBytes       = 128
	maxEntries              = 1 << 20
	maxEntryBytes           = 1 << 20
	maxFileBytes      int64 = 1 << 30
)

var (
	// ErrInvalidOptions indicates unsafe or incomplete history configuration.
	ErrInvalidOptions = errors.New("hatQueryHistory: invalid options")
	// ErrPathRequired indicates that durable history needs an explicit path.
	ErrPathRequired = errors.New("hatQueryHistory: path is required")
	// ErrInvalidRecord indicates a record that cannot be safely persisted.
	ErrInvalidRecord = errors.New("hatQueryHistory: invalid record")
	// ErrEntryTooLarge indicates that the encoded record exceeds its bound.
	ErrEntryTooLarge = errors.New("hatQueryHistory: entry is too large")
	// ErrCorrupt indicates an unreadable or incompatible history line.
	ErrCorrupt = errors.New("hatQueryHistory: corrupt history")
	// ErrClosed indicates an operation on a closed history.
	ErrClosed = errors.New("hatQueryHistory: history is closed")
	// ErrHistoryIO wraps an active file operation failure.
	ErrHistoryIO = errors.New("hatQueryHistory: history I/O failure")
)

// State describes one query lifecycle state. Durable history is redacted to
// status fields and never stores SQL text, parameters, reasons, or rows.
type State string

const (
	StateRunning         State = "running"
	StateCancelRequested State = "cancel_requested"
	StateSucceeded       State = "succeeded"
	StateFailed          State = "failed"
	StateCanceled        State = "canceled"
)

// QueryRecord is the intentionally small, redacted record persisted by this
// package. Query text and arbitrary cancellation reasons have no field here.
type QueryRecord struct {
	QueryID    string    `json:"query_id"`
	State      State     `json:"state"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`
	ErrorCode  string    `json:"error_code,omitempty"`
}

// Options configures a durable history. Zero limits use sane defaults. Sync
// requests an fsync after every successful append and is slower by design.
type Options struct {
	Path          string
	MaxEntries    int
	MaxEntryBytes int
	MaxFileBytes  int64
	Sync          bool
}

type persistedRecord struct {
	Format string `json:"format"`
	QueryRecord
}

// History is a bounded append-only query-history log with one rotated file.
// It is safe for concurrent Append, Snapshot, Len, and Close calls.
type History struct {
	mu sync.Mutex

	options Options
	path    string
	file    *os.File
	bytes   int64
	closed  bool

	records []QueryRecord
	start   int
	size    int
}

// Open opens or creates a history log with mode 0600. Existing current and
// rotated records are recovered in order; only the most recent MaxEntries are
// retained in memory.
func Open(options Options) (*History, error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	history := &History{
		options: options,
		path:    options.Path,
		records: make([]QueryRecord, options.MaxEntries),
	}
	if err := history.loadFile(options.Path + ".1"); err != nil {
		return nil, err
	}
	if err := history.loadFile(options.Path); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(options.Path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("%w: open %s: %v", ErrHistoryIO, options.Path, err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("%w: chmod %s: %v", ErrHistoryIO, options.Path, err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("%w: stat %s: %v", ErrHistoryIO, options.Path, err)
	}
	history.file = file
	history.bytes = info.Size()
	return history, nil
}

// Append validates and persists one redacted record.
func (history *History) Append(record QueryRecord) error {
	if history == nil {
		return ErrClosed
	}
	record, err := normalizeRecord(record)
	if err != nil {
		return err
	}
	encoded, err := json.Marshal(persistedRecord{Format: formatVersion, QueryRecord: record})
	if err != nil {
		return fmt.Errorf("%w: encode record: %v", ErrHistoryIO, err)
	}
	if int64(len(encoded)+1) > int64(history.options.MaxEntryBytes) {
		return ErrEntryTooLarge
	}

	history.mu.Lock()
	defer history.mu.Unlock()
	if history.closed || history.file == nil {
		return ErrClosed
	}
	if history.bytes > 0 && history.bytes+int64(len(encoded)+1) > history.options.MaxFileBytes {
		if err := history.rotateLocked(); err != nil {
			return err
		}
	}
	encoded = append(encoded, '\n')
	written, err := history.file.Write(encoded)
	if err != nil || written != len(encoded) {
		return fmt.Errorf("%w: append record: %v", ErrHistoryIO, err)
	}
	history.bytes += int64(written)
	if history.options.Sync {
		if err := history.file.Sync(); err != nil {
			return fmt.Errorf("%w: sync record: %v", ErrHistoryIO, err)
		}
	}
	history.retainLocked(record)
	return nil
}

// Snapshot returns an oldest-first copy of the retained records.
func (history *History) Snapshot() []QueryRecord {
	if history == nil {
		return nil
	}
	history.mu.Lock()
	defer history.mu.Unlock()
	if history.size == 0 {
		return nil
	}
	result := make([]QueryRecord, history.size)
	for index := range result {
		result[index] = history.records[(history.start+index)%len(history.records)]
	}
	return result
}

// Len returns the number of retained records.
func (history *History) Len() int {
	if history == nil {
		return 0
	}
	history.mu.Lock()
	defer history.mu.Unlock()
	return history.size
}

// Close flushes and closes the active file. It is safe to call more than once.
func (history *History) Close() error {
	if history == nil {
		return nil
	}
	history.mu.Lock()
	defer history.mu.Unlock()
	if history.closed {
		return nil
	}
	history.closed = true
	if history.file == nil {
		return nil
	}
	if history.options.Sync {
		if err := history.file.Sync(); err != nil {
			_ = history.file.Close()
			history.file = nil
			return fmt.Errorf("%w: close sync: %v", ErrHistoryIO, err)
		}
	}
	err := history.file.Close()
	history.file = nil
	if err != nil {
		return fmt.Errorf("%w: close history: %v", ErrHistoryIO, err)
	}
	return nil
}

func (history *History) loadFile(path string) error {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("%w: open existing %s: %v", ErrHistoryIO, path, err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	bufferSize := history.options.MaxEntryBytes + 1
	if bufferSize < 1024 {
		bufferSize = 1024
	}
	scanner.Buffer(make([]byte, minInt(bufferSize, 64<<10)), bufferSize)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line)+1 > history.options.MaxEntryBytes {
			return fmt.Errorf("%w: entry in %s exceeds limit", ErrEntryTooLarge, path)
		}
		var persisted persistedRecord
		if err := json.Unmarshal(line, &persisted); err != nil || persisted.Format != formatVersion {
			return fmt.Errorf("%w: invalid line in %s", ErrCorrupt, path)
		}
		record, err := normalizeRecord(persisted.QueryRecord)
		if err != nil {
			return fmt.Errorf("%w: invalid record in %s", ErrCorrupt, path)
		}
		history.retainLocked(record)
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("%w: read %s: %v", ErrHistoryIO, path, err)
	}
	return nil
}

func (history *History) retainLocked(record QueryRecord) {
	index := (history.start + history.size) % len(history.records)
	if history.size < len(history.records) {
		history.size++
	} else {
		history.start = (history.start + 1) % len(history.records)
	}
	history.records[index] = record
}

func (history *History) rotateLocked() error {
	if history.file == nil {
		return ErrClosed
	}
	if history.options.Sync {
		if err := history.file.Sync(); err != nil {
			return fmt.Errorf("%w: sync before rotation: %v", ErrHistoryIO, err)
		}
	}
	if err := history.file.Close(); err != nil {
		history.file = nil
		return fmt.Errorf("%w: close before rotation: %v", ErrHistoryIO, err)
	}
	history.file = nil
	rotated := history.path + ".1"
	if err := os.Remove(rotated); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%w: remove rotated history: %v", ErrHistoryIO, err)
	}
	if err := os.Rename(history.path, rotated); err != nil {
		return fmt.Errorf("%w: rotate history: %v", ErrHistoryIO, err)
	}
	file, err := os.OpenFile(history.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("%w: create rotated history: %v", ErrHistoryIO, err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return fmt.Errorf("%w: chmod rotated history: %v", ErrHistoryIO, err)
	}
	history.file = file
	history.bytes = 0
	return nil
}

func normalizeOptions(options Options) (Options, error) {
	options.Path = strings.TrimSpace(options.Path)
	if options.Path == "" {
		return Options{}, ErrPathRequired
	}
	if options.MaxEntries == 0 {
		options.MaxEntries = DefaultMaxEntries
	}
	if options.MaxEntryBytes == 0 {
		options.MaxEntryBytes = DefaultMaxEntryBytes
	}
	if options.MaxFileBytes == 0 {
		options.MaxFileBytes = DefaultMaxFileBytes
	}
	if options.MaxEntries < 1 || options.MaxEntries > maxEntries ||
		options.MaxEntryBytes < 128 || options.MaxEntryBytes > maxEntryBytes ||
		options.MaxFileBytes < int64(options.MaxEntryBytes)+1 || options.MaxFileBytes > maxFileBytes {
		return Options{}, ErrInvalidOptions
	}
	return options, nil
}

func normalizeRecord(record QueryRecord) (QueryRecord, error) {
	record.QueryID = strings.TrimSpace(record.QueryID)
	record.ErrorCode = strings.TrimSpace(record.ErrorCode)
	if record.QueryID == "" || len(record.QueryID) > maxQueryIDBytes || hasControl(record.QueryID) || !validState(record.State) {
		return QueryRecord{}, ErrInvalidRecord
	}
	if len(record.ErrorCode) > maxErrorCodeBytes || hasControl(record.ErrorCode) {
		return QueryRecord{}, ErrInvalidRecord
	}
	if !record.StartedAt.IsZero() {
		record.StartedAt = record.StartedAt.UTC()
	}
	if !record.FinishedAt.IsZero() {
		record.FinishedAt = record.FinishedAt.UTC()
	}
	return record, nil
}

func validState(state State) bool {
	switch state {
	case StateRunning, StateCancelRequested, StateSucceeded, StateFailed, StateCanceled:
		return true
	default:
		return false
	}
}

func hasControl(value string) bool {
	for _, character := range value {
		if unicode.IsControl(character) {
			return true
		}
	}
	return false
}

func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
