package hatSql

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
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
	// DefaultSQLQueryLogRetainedFiles is used when rotation is enabled without
	// an explicit archive count. It retains the active file plus seven archives.
	DefaultSQLQueryLogRetainedFiles = 7
	maxSQLQueryLogRecordBytes       = 1 << 20
	maxSQLQueryLogRetainedFiles     = 1024
	defaultSQLQueryLogSampleSeed    = uint64(0x9e3779b97f4a7c15)
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
	SyncOnAppend bool
	// MaxRecordBytes bounds one encoded record. A nonpositive value selects the
	// default.
	MaxRecordBytes int
	// MaxFileBytes rotates before an append would exceed the active file limit;
	// zero disables size rotation.
	MaxFileBytes int64
	// MaxFileAge rotates a non-empty active file before the next append once its
	// age reaches this duration; zero disables age rotation.
	MaxFileAge time.Duration
	// MaxRetainedFiles controls numbered archives beside Path. Zero selects
	// DefaultSQLQueryLogRetainedFiles when rotation is enabled.
	MaxRetainedFiles int
	// SampleRate is the fraction of valid entries retained by the log. Zero
	// preserves the existing retain-all behavior; values between zero and one
	// enable Bernoulli sampling, and one retains every entry.
	SampleRate float64
	// SampleSeed makes probabilistic sampling reproducible. Zero selects a
	// stable nonzero seed.
	SampleSeed uint64
}

// SQLQueryLogSamplingStats reports sampling counters since the log was opened.
// Accepted is the number selected for writing; a later filesystem error can
// still prevent an accepted entry from being persisted. Dropped entries are
// valid entries rejected before rotation and file writes.
type SQLQueryLogSamplingStats struct {
	SampleRate float64 `json:"sample_rate"`
	Observed   uint64  `json:"observed"`
	Accepted   uint64  `json:"accepted"`
	Dropped    uint64  `json:"dropped"`
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
// newline-delimited JSON file with optional numbered archives. It is safe for
// concurrent Append, Sync, Read, and Close calls.
type SQLQueryLog struct {
	mu               sync.Mutex
	path             string
	file             *os.File
	maxRecordSize    int
	syncOnAppend     bool
	maxFileBytes     int64
	maxFileAge       time.Duration
	maxRetainedFiles int
	rotationEnabled  bool
	fileBytes        int64
	segmentStartedAt time.Time
	sampleRate       float64
	sampleThreshold  uint64
	sampleState      uint64
	samplingObserved uint64
	samplingAccepted uint64
	samplingDropped  uint64
}

// OpenSQLQueryLog opens or creates a privacy-safe query log with restrictive
// file permissions. Parent directories are created with mode 0700 when absent.
func OpenSQLQueryLog(path string) (*SQLQueryLog, error) {
	return OpenSQLQueryLogWithOptions(path, SQLQueryLogOptions{})
}

// OpenSQLQueryLogWithOptions opens a query log with explicit durability,
// record-size, and optional rotation settings.
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
	if options.MaxFileBytes < 0 {
		return nil, errors.New("hatSql: SQL query log max file bytes must not be negative")
	}
	if options.MaxFileAge < 0 {
		return nil, errors.New("hatSql: SQL query log max file age must not be negative")
	}
	if options.MaxRetainedFiles < 0 {
		return nil, errors.New("hatSql: SQL query log retained files must not be negative")
	}
	if math.IsNaN(options.SampleRate) || math.IsInf(options.SampleRate, 0) || options.SampleRate < 0 || options.SampleRate > 1 {
		return nil, errors.New("hatSql: SQL query log sample rate must be between 0 and 1")
	}
	sampleRate := options.SampleRate
	if sampleRate == 0 {
		sampleRate = 1
	}
	sampleSeed := options.SampleSeed
	if sampleSeed == 0 {
		sampleSeed = defaultSQLQueryLogSampleSeed
	}
	maxRetainedFiles := options.MaxRetainedFiles
	if options.MaxFileBytes > 0 || options.MaxFileAge > 0 {
		if maxRetainedFiles == 0 {
			maxRetainedFiles = DefaultSQLQueryLogRetainedFiles
		}
	}
	if maxRetainedFiles > maxSQLQueryLogRetainedFiles {
		return nil, fmt.Errorf("hatSql: SQL query log retained files exceed %d", maxSQLQueryLogRetainedFiles)
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
	file, info, err := openSQLQueryLogFile(absolutePath)
	if err != nil {
		return nil, err
	}
	return &SQLQueryLog{
		path:             absolutePath,
		file:             file,
		maxRecordSize:    maxRecordSize,
		syncOnAppend:     options.SyncOnAppend,
		maxFileBytes:     options.MaxFileBytes,
		maxFileAge:       options.MaxFileAge,
		maxRetainedFiles: maxRetainedFiles,
		rotationEnabled:  options.MaxFileBytes > 0 || options.MaxFileAge > 0,
		fileBytes:        info.Size(),
		segmentStartedAt: info.ModTime(),
		sampleRate:       sampleRate,
		sampleThreshold:  sqlQueryLogSampleThreshold(sampleRate),
		sampleState:      sampleSeed,
	}, nil
}

func openSQLQueryLogFile(path string) (*os.File, os.FileInfo, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, nil, fmt.Errorf("hatSql: SQL query log path must not be a symlink")
		}
		if !info.Mode().IsRegular() {
			return nil, nil, fmt.Errorf("hatSql: SQL query log path is not a regular file")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, nil, fmt.Errorf("hatSql: inspect SQL query log path: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("hatSql: open SQL query log: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("hatSql: restrict SQL query log permissions: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("hatSql: stat SQL query log: %w", err)
	}
	if !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, nil, fmt.Errorf("hatSql: SQL query log path is not a regular file")
	}
	return file, info, nil
}

func createSQLQueryLogFile(path string) (*os.File, os.FileInfo, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, fmt.Errorf("hatSql: create SQL query log segment: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("hatSql: restrict SQL query log segment permissions: %w", err)
	}
	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, nil, fmt.Errorf("hatSql: stat SQL query log segment: %w", err)
	}
	return file, info, nil
}

func (log *SQLQueryLog) shouldRotateLocked(nextBytes int64) bool {
	if log.maxFileBytes <= 0 && log.maxFileAge <= 0 {
		return false
	}
	if log.fileBytes <= 0 {
		return false
	}
	if log.maxFileBytes > 0 && log.fileBytes > log.maxFileBytes-nextBytes {
		return true
	}
	return log.maxFileAge > 0 && !log.segmentStartedAt.IsZero() && time.Since(log.segmentStartedAt) >= log.maxFileAge
}

func (log *SQLQueryLog) rotateLocked() error {
	if log.maxRetainedFiles <= 0 {
		return errors.New("hatSql: SQL query log rotation requires retained files")
	}
	if err := log.file.Sync(); err != nil {
		return fmt.Errorf("hatSql: sync SQL query log before rotation: %w", err)
	}
	closeErr := log.file.Close()
	log.file = nil
	if closeErr != nil {
		reopenErr := log.reopenAfterRotationFailure()
		return errors.Join(fmt.Errorf("hatSql: close SQL query log before rotation: %w", closeErr), reopenErr)
	}
	if err := rotateSQLQueryLogFiles(log.path, log.maxRetainedFiles); err != nil {
		reopenErr := log.reopenAfterRotationFailure()
		return errors.Join(fmt.Errorf("hatSql: rotate SQL query log: %w", err), reopenErr)
	}
	file, info, err := createSQLQueryLogFile(log.path)
	if err != nil {
		reopenErr := log.reopenAfterRotationFailure()
		return errors.Join(err, reopenErr)
	}
	log.file = file
	log.fileBytes = info.Size()
	log.segmentStartedAt = info.ModTime()
	return nil
}

func (log *SQLQueryLog) reopenAfterRotationFailure() error {
	file, info, err := openSQLQueryLogFile(log.path)
	if err != nil {
		return fmt.Errorf("hatSql: reopen SQL query log after rotation: %w", err)
	}
	log.file = file
	log.fileBytes = info.Size()
	log.segmentStartedAt = info.ModTime()
	return nil
}

func rotateSQLQueryLogFiles(path string, retainedFiles int) error {
	if err := removeSQLQueryLogPath(sqlQueryLogRotatedPath(path, retainedFiles)); err != nil {
		return err
	}
	for index := retainedFiles - 1; index >= 1; index-- {
		if err := renameSQLQueryLogPath(sqlQueryLogRotatedPath(path, index), sqlQueryLogRotatedPath(path, index+1)); err != nil {
			return err
		}
	}
	if err := os.Rename(path, sqlQueryLogRotatedPath(path, 1)); err != nil {
		return fmt.Errorf("rename active segment: %w", err)
	}
	return nil
}

func sqlQueryLogRotatedPath(path string, index int) string {
	return fmt.Sprintf("%s.%d", path, index)
}

func removeSQLQueryLogPath(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func renameSQLQueryLogPath(source, destination string) error {
	err := os.Rename(source, destination)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
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
	if log.sampleRate < 1 && !log.recordSamplingLocked() {
		return nil
	}
	if !log.rotationEnabled {
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
	if log.shouldRotateLocked(int64(len(encoded))) {
		if err := log.rotateLocked(); err != nil {
			return err
		}
	}
	written, err := log.file.Write(encoded)
	if written > 0 {
		log.fileBytes += int64(written)
	}
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

// SamplingStats returns a consistent snapshot of sampling activity since the
// log was opened. It is safe to call concurrently with Append.
func (log *SQLQueryLog) SamplingStats() SQLQueryLogSamplingStats {
	if log == nil {
		return SQLQueryLogSamplingStats{}
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	return SQLQueryLogSamplingStats{
		SampleRate: log.sampleRate,
		Observed:   log.samplingObserved,
		Accepted:   log.samplingAccepted,
		Dropped:    log.samplingDropped,
	}
}

func (log *SQLQueryLog) recordSamplingLocked() bool {
	log.samplingObserved++
	if sqlQueryLogNextSample(&log.sampleState) < log.sampleThreshold {
		log.samplingAccepted++
		return true
	}
	log.samplingDropped++
	return false
}

func sqlQueryLogSampleThreshold(rate float64) uint64 {
	if rate >= 1 {
		return ^uint64(0)
	}
	scaled := math.Ldexp(rate, 64)
	if scaled <= 0 {
		return 0
	}
	if scaled >= math.Ldexp(1, 64) {
		return ^uint64(0)
	}
	return uint64(scaled)
}

func sqlQueryLogNextSample(state *uint64) uint64 {
	value := *state
	value ^= value >> 12
	value ^= value << 25
	value ^= value >> 27
	*state = value
	return value * 2685821657736338717
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
	if log.file == nil {
		return nil, ErrSQLQueryLogClosed
	}
	paths, err := log.readSegmentPathsLocked()
	if err != nil {
		return nil, err
	}
	entries := make([]SQLQueryLogEntry, 0)
	for _, path := range paths {
		segmentEntries, err := readSQLQueryLogSegment(path, log.maxRecordSize)
		if err != nil {
			return nil, fmt.Errorf("hatSql: read SQL query log segment %q: %w", path, err)
		}
		entries = append(entries, segmentEntries...)
	}
	return entries, nil
}

func (log *SQLQueryLog) readSegmentPathsLocked() ([]string, error) {
	paths := make([]string, 0, log.maxRetainedFiles+1)
	for index := log.maxRetainedFiles; index >= 1; index-- {
		path := sqlQueryLogRotatedPath(log.path, index)
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("hatSql: inspect SQL query log segment %q: %w", path, err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("hatSql: SQL query log segment %q must not be a symlink", path)
		}
		if !info.Mode().IsRegular() {
			return nil, fmt.Errorf("hatSql: SQL query log segment %q is not a regular file", path)
		}
		paths = append(paths, path)
	}
	paths = append(paths, log.path)
	return paths, nil
}

func readSQLQueryLogSegment(path string, maxRecordSize int) ([]SQLQueryLogEntry, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("SQL query log segment must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("SQL query log segment is not a regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1024), maxRecordSize)
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
