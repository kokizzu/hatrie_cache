package hatSql

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultSQLQueryLogMaxRecordBytes bounds one durable record and prevents
	// malformed input from forcing unbounded allocation during recovery.
	DefaultSQLQueryLogMaxRecordBytes = 64 << 10
	maxSQLQueryLogRecordBytes        = 1 << 20
)

var (
	ErrSQLQueryLogClosed        = errors.New("hatSql: SQL query log is closed")
	ErrSQLQueryLogRecordInvalid = errors.New("hatSql: SQL query log record is invalid")
)

// SQLQueryLogOptions controls durable query-log writes. SyncOnAppend provides
// crash durability at the cost of one filesystem sync per completed query;
// the default batches durability in the operating system and keeps the query
// completion path lightweight. Call Sync explicitly at operational checkpoints
// when the default is selected.
type SQLQueryLogOptions struct {
	SyncOnAppend   bool
	MaxRecordBytes int
}

// SQLQueryLogEntry is the privacy-safe durable form of SQLQueryStatus. Query
// text, source names, parameters, result rows, and cancellation reasons are
// deliberately excluded.
type SQLQueryLogEntry struct {
	QueryID       string        `json:"query_id"`
	State         SQLQueryState `json:"state"`
	StartedAt     time.Time     `json:"started_at"`
	FinishedAt    time.Time     `json:"finished_at"`
	DurationNanos int64         `json:"duration_ns"`
	ErrorCode     ErrorCode     `json:"error_code,omitempty"`
}

// SQLQueryLog appends completed query entries to a local, operator-readable
// newline-delimited JSON file. It is safe for concurrent Append, Sync, Read,
// and Close calls.
type SQLQueryLog struct {
	mu            sync.Mutex
	path          string
	file          *os.File
	maxRecordSize int
	syncOnAppend  bool
}

// OpenSQLQueryLog opens or creates a privacy-safe query log with restrictive
// file permissions. Parent directories are created with mode 0700 when absent.
func OpenSQLQueryLog(path string) (*SQLQueryLog, error) {
	return OpenSQLQueryLogWithOptions(path, SQLQueryLogOptions{})
}

// OpenSQLQueryLogWithOptions opens a query log with explicit durability and
// record-size settings.
func OpenSQLQueryLogWithOptions(path string, options SQLQueryLogOptions) (*SQLQueryLog, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, errors.New("hatSql: SQL query log path is required")
	}
	maxRecordSize := options.MaxRecordBytes
	if maxRecordSize <= 0 {
		maxRecordSize = DefaultSQLQueryLogMaxRecordBytes
	}
	if maxRecordSize > maxSQLQueryLogRecordBytes {
		return nil, fmt.Errorf("hatSql: SQL query log record limit exceeds %d bytes", maxSQLQueryLogRecordBytes)
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("hatSql: SQL query log path: %w", err)
	}
	if info, err := os.Lstat(absolutePath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("hatSql: SQL query log path must not be a symlink")
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("hatSql: SQL query log path is not a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("hatSql: inspect SQL query log path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o700); err != nil {
		return nil, fmt.Errorf("hatSql: create SQL query log directory: %w", err)
	}
	file, err := os.OpenFile(absolutePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("hatSql: open SQL query log: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("hatSql: restrict SQL query log permissions: %w", err)
	}
	return &SQLQueryLog{
		path:          absolutePath,
		file:          file,
		maxRecordSize: maxRecordSize,
		syncOnAppend:  options.SyncOnAppend,
	}, nil
}

// Path returns the absolute path associated with the log.
func (log *SQLQueryLog) Path() string {
	if log == nil {
		return ""
	}
	return log.path
}

// Append persists the terminal portion of one query status. Cancellation
// reasons are intentionally discarded before encoding.
func (log *SQLQueryLog) Append(status SQLQueryStatus) error {
	entry, err := newSQLQueryLogEntry(status)
	if err != nil {
		return err
	}
	return log.AppendEntry(entry)
}

// AppendEntry persists one already-sanitized terminal query entry.
func (log *SQLQueryLog) AppendEntry(entry SQLQueryLogEntry) error {
	if log == nil {
		return ErrSQLQueryLogClosed
	}
	if err := validateSQLQueryLogEntry(entry); err != nil {
		return err
	}
	encoded, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("hatSql: encode SQL query log entry: %w", err)
	}
	if len(encoded)+1 > log.maxRecordSize {
		return fmt.Errorf("hatSql: SQL query log entry exceeds %d bytes", log.maxRecordSize)
	}
	encoded = append(encoded, '\n')
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.file == nil {
		return ErrSQLQueryLogClosed
	}
	written, err := log.file.Write(encoded)
	if err != nil {
		return fmt.Errorf("hatSql: append SQL query log entry: %w", err)
	}
	if written != len(encoded) {
		return fmt.Errorf("hatSql: append SQL query log entry: %w", io.ErrShortWrite)
	}
	if log.syncOnAppend {
		if err := log.file.Sync(); err != nil {
			return fmt.Errorf("hatSql: sync SQL query log entry: %w", err)
		}
	}
	return nil
}

// Sync flushes the log's file contents to the filesystem.
func (log *SQLQueryLog) Sync() error {
	if log == nil {
		return ErrSQLQueryLogClosed
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.file == nil {
		return ErrSQLQueryLogClosed
	}
	return log.file.Sync()
}

// Read validates and returns all durable entries in oldest-first order. A
// malformed or truncated record is rejected instead of silently discarding
// potentially missing audit data.
func (log *SQLQueryLog) Read() ([]SQLQueryLogEntry, error) {
	if log == nil {
		return nil, ErrSQLQueryLogClosed
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	file, err := os.Open(log.path)
	if err != nil {
		return nil, fmt.Errorf("hatSql: read SQL query log: %w", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), log.maxRecordSize)
	entries := make([]SQLQueryLogEntry, 0)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			return nil, ErrSQLQueryLogRecordInvalid
		}
		var entry SQLQueryLogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("hatSql: decode SQL query log record: %w", err)
		}
		if err := validateSQLQueryLogEntry(entry); err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("hatSql: scan SQL query log: %w", err)
	}
	return entries, nil
}

// Close syncs and closes the log. It is idempotent.
func (log *SQLQueryLog) Close() error {
	if log == nil {
		return nil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.file == nil {
		return nil
	}
	syncErr := log.file.Sync()
	closeErr := log.file.Close()
	log.file = nil
	return errors.Join(syncErr, closeErr)
}

func newSQLQueryLogEntry(status SQLQueryStatus) (SQLQueryLogEntry, error) {
	entry := SQLQueryLogEntry{
		QueryID:    status.QueryID,
		State:      status.State,
		StartedAt:  status.StartedAt,
		FinishedAt: status.FinishedAt,
		ErrorCode:  status.ErrorCode,
	}
	if !status.StartedAt.IsZero() && !status.FinishedAt.IsZero() {
		entry.DurationNanos = status.FinishedAt.Sub(status.StartedAt).Nanoseconds()
	}
	if err := validateSQLQueryLogEntry(entry); err != nil {
		return SQLQueryLogEntry{}, err
	}
	return entry, nil
}

func validateSQLQueryLogEntry(entry SQLQueryLogEntry) error {
	if entry.QueryID == "" || len(entry.QueryID) > maxSQLQueryManagerIDBytes {
		return fmt.Errorf("%w: query ID", ErrSQLQueryLogRecordInvalid)
	}
	switch entry.State {
	case SQLQueryStateSucceeded, SQLQueryStateFailed, SQLQueryStateCanceled:
	default:
		return fmt.Errorf("%w: terminal state", ErrSQLQueryLogRecordInvalid)
	}
	if entry.StartedAt.IsZero() || entry.FinishedAt.IsZero() || entry.FinishedAt.Before(entry.StartedAt) || entry.DurationNanos < 0 {
		return fmt.Errorf("%w: timestamps", ErrSQLQueryLogRecordInvalid)
	}
	return nil
}
