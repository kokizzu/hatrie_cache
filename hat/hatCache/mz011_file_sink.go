package hatCache

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

const (
	// DefaultFileCommandJournalExactlyOnceSinkMaxBatchBytes bounds one
	// committed batch when the caller does not provide a limit.
	DefaultFileCommandJournalExactlyOnceSinkMaxBatchBytes int64 = 8 << 20
	// MaxFileCommandJournalExactlyOnceSinkMaxBatchBytes prevents oversized
	// allocations from a single connector batch.
	MaxFileCommandJournalExactlyOnceSinkMaxBatchBytes int64 = 64 << 20

	fileCommandJournalSinkMagic        = "HJS1"
	fileCommandJournalSinkHeaderBytes  = 4 + 8 + 8 + 4 + 4
	fileCommandJournalSinkTrailerBytes = 4
	fileCommandJournalSinkFileSuffix   = ".hjs"
)

var (
	// ErrFileCommandJournalSinkOptions reports invalid connector bounds.
	ErrFileCommandJournalSinkOptions = errors.New("hatriecache: invalid file command journal sink options")
	// ErrFileCommandJournalSinkNil reports a nil filesystem connector.
	ErrFileCommandJournalSinkNil = errors.New("hatriecache: file command journal sink is nil")
	// ErrFileCommandJournalSinkCorrupt reports a malformed committed batch.
	ErrFileCommandJournalSinkCorrupt = errors.New("hatriecache: corrupt file command journal batch")
	// ErrFileCommandJournalSinkSequence reports a non-monotone batch or
	// watermark.
	ErrFileCommandJournalSinkSequence = errors.New("hatriecache: invalid file command journal sequence")
	// ErrFileCommandJournalSinkBatchTooLarge reports a batch over the configured
	// encoded-byte bound.
	ErrFileCommandJournalSinkBatchTooLarge = errors.New("hatriecache: file command journal batch is too large")
	// ErrFileCommandJournalSinkConflict reports an existing sequence with
	// different bytes.
	ErrFileCommandJournalSinkConflict = errors.New("hatriecache: file command journal batch conflicts")
	// ErrFileCommandJournalSinkTransaction reports an invalid transaction state.
	ErrFileCommandJournalSinkTransaction = errors.New("hatriecache: file command journal transaction is invalid")
)

var fileCommandJournalSinkChecksumTable = crc32.MakeTable(crc32.Castagnoli)

// FileCommandJournalExactlyOnceSinkOptions configures one local filesystem
// connector. The directory is created on the first successful commit.
type FileCommandJournalExactlyOnceSinkOptions struct {
	MaxBatchBytes int64
}

// FileCommandJournalBatch is one validated immutable batch read from the
// filesystem connector.
type FileCommandJournalBatch struct {
	FirstSequence uint64
	LastSequence  uint64
	Records       []CommandJournalRecord
}

// FileCommandJournalExactlyOnceSink is a concrete local connector for the
// CommandJournalExactlyOnceSink contract. Each committed batch is one
// CRC-protected immutable HJS1 file; the highest committed sequence is the
// durable watermark.
type FileCommandJournalExactlyOnceSink struct {
	mu            sync.Mutex
	directory     string
	maxBatchBytes int64
}

// NewFileCommandJournalExactlyOnceSink creates a filesystem connector with
// the default batch bound.
func NewFileCommandJournalExactlyOnceSink(directory string) (*FileCommandJournalExactlyOnceSink, error) {
	return NewFileCommandJournalExactlyOnceSinkWithOptions(directory, FileCommandJournalExactlyOnceSinkOptions{})
}

// NewFileCommandJournalExactlyOnceSinkWithOptions creates a bounded
// filesystem connector.
func NewFileCommandJournalExactlyOnceSinkWithOptions(directory string, options FileCommandJournalExactlyOnceSinkOptions) (*FileCommandJournalExactlyOnceSink, error) {
	directory = strings.TrimSpace(directory)
	if directory == "" {
		return nil, fmt.Errorf("%w: directory is required", ErrFileCommandJournalSinkOptions)
	}
	maxBatchBytes := options.MaxBatchBytes
	if maxBatchBytes == 0 {
		maxBatchBytes = DefaultFileCommandJournalExactlyOnceSinkMaxBatchBytes
	}
	minimumBytes := int64(fileCommandJournalSinkHeaderBytes + fileCommandJournalSinkTrailerBytes + 1)
	if maxBatchBytes < minimumBytes || maxBatchBytes > MaxFileCommandJournalExactlyOnceSinkMaxBatchBytes {
		return nil, fmt.Errorf("%w: MaxBatchBytes=%d", ErrFileCommandJournalSinkOptions, maxBatchBytes)
	}
	return &FileCommandJournalExactlyOnceSink{
		directory:     filepath.Clean(directory),
		maxBatchBytes: maxBatchBytes,
	}, nil
}

// LoadSequence returns the highest validated committed sequence. A missing
// directory is an empty sink.
func (sink *FileCommandJournalExactlyOnceSink) LoadSequence(ctx context.Context) (uint64, error) {
	if sink == nil {
		return 0, ErrFileCommandJournalSinkNil
	}
	if err := fileCommandJournalSinkContextError(ctx); err != nil {
		return 0, err
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.loadSequenceLocked(ctx)
}

// Begin starts a serialized transaction at the caller's current watermark.
// The sink holds its connector lock until Commit or Rollback, preventing two
// writers from publishing the same next sequence concurrently.
func (sink *FileCommandJournalExactlyOnceSink) Begin(ctx context.Context, sequence uint64) (CommandJournalExactlyOnceTransaction, error) {
	if sink == nil {
		return nil, ErrFileCommandJournalSinkNil
	}
	if err := fileCommandJournalSinkContextError(ctx); err != nil {
		return nil, err
	}
	sink.mu.Lock()
	current, err := sink.loadSequenceLocked(ctx)
	if err != nil {
		sink.mu.Unlock()
		return nil, err
	}
	if sequence != current {
		sink.mu.Unlock()
		return nil, fmt.Errorf("%w: begin sequence %d, current sequence %d", ErrFileCommandJournalSinkSequence, sequence, current)
	}
	return &fileCommandJournalTransaction{
		sink:     sink,
		expected: sequence,
	}, nil
}

// ReadBatches returns all committed batches in sequence order. It validates
// every file, not only the latest watermark, so corruption cannot be hidden
// behind a later successful commit.
func (sink *FileCommandJournalExactlyOnceSink) ReadBatches(ctx context.Context) ([]FileCommandJournalBatch, error) {
	if sink == nil {
		return nil, ErrFileCommandJournalSinkNil
	}
	if err := fileCommandJournalSinkContextError(ctx); err != nil {
		return nil, err
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.readBatchesLocked(ctx)
}

type fileCommandJournalTransaction struct {
	sink     *FileCommandJournalExactlyOnceSink
	expected uint64
	first    uint64
	last     uint64
	payload  []byte
	written  bool
	closed   bool
}

func (transaction *fileCommandJournalTransaction) Write(ctx context.Context, records []CommandJournalRecord) error {
	if transaction == nil || transaction.sink == nil || transaction.closed || transaction.written {
		return ErrFileCommandJournalSinkTransaction
	}
	if err := fileCommandJournalSinkContextError(ctx); err != nil {
		return err
	}
	if len(records) == 0 {
		return fmt.Errorf("%w: empty batch", ErrFileCommandJournalSinkSequence)
	}
	first := records[0].Sequence
	if transaction.expected == ^uint64(0) || first != transaction.expected+1 {
		return fmt.Errorf("%w: first sequence %d does not follow %d", ErrFileCommandJournalSinkSequence, first, transaction.expected)
	}
	for index := 1; index < len(records); index++ {
		if records[index].Sequence <= records[index-1].Sequence {
			return fmt.Errorf("%w: records are not strictly increasing", ErrFileCommandJournalSinkSequence)
		}
	}
	last := records[len(records)-1].Sequence
	encoded, err := encodeFileCommandJournalBatch(ctx, first, last, records, transaction.sink.maxBatchBytes)
	if err != nil {
		return err
	}
	transaction.first = first
	transaction.last = last
	transaction.payload = encoded
	transaction.written = true
	return nil
}

func (transaction *fileCommandJournalTransaction) Commit(ctx context.Context, lastSequence uint64) error {
	if transaction == nil || transaction.sink == nil || transaction.closed || !transaction.written {
		return ErrFileCommandJournalSinkTransaction
	}
	if err := fileCommandJournalSinkContextError(ctx); err != nil {
		return err
	}
	if lastSequence != transaction.last {
		return fmt.Errorf("%w: commit watermark %d, batch watermark %d", ErrFileCommandJournalSinkSequence, lastSequence, transaction.last)
	}
	defer transaction.finish()
	if err := os.MkdirAll(transaction.sink.directory, 0o750); err != nil {
		return fmt.Errorf("create file command journal sink directory: %w", err)
	}
	target := filepath.Join(transaction.sink.directory, fileCommandJournalBatchName(transaction.last))
	if existing, err := os.ReadFile(target); err == nil {
		if bytes.Equal(existing, transaction.payload) {
			return nil
		}
		return ErrFileCommandJournalSinkConflict
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read existing file command journal batch: %w", err)
	}
	temporary, err := os.CreateTemp(transaction.sink.directory, ".command-journal-*.tmp")
	if err != nil {
		return fmt.Errorf("create file command journal temporary batch: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		_ = temporary.Close()
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return fmt.Errorf("set file command journal batch permissions: %w", err)
	}
	if _, err := temporary.Write(transaction.payload); err != nil {
		return fmt.Errorf("write file command journal batch: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync file command journal batch: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close file command journal batch: %w", err)
	}
	if err := os.Rename(temporaryPath, target); err != nil {
		return fmt.Errorf("publish file command journal batch: %w", err)
	}
	removeTemporary = false
	return nil
}

func (transaction *fileCommandJournalTransaction) Rollback(context.Context) error {
	if transaction == nil || transaction.sink == nil {
		return ErrFileCommandJournalSinkTransaction
	}
	if transaction.closed {
		return nil
	}
	transaction.finish()
	return nil
}

func (transaction *fileCommandJournalTransaction) finish() {
	if transaction.closed {
		return
	}
	transaction.closed = true
	transaction.sink.mu.Unlock()
}

func (sink *FileCommandJournalExactlyOnceSink) loadSequenceLocked(ctx context.Context) (uint64, error) {
	batches, err := sink.readBatchesLocked(ctx)
	if err != nil || len(batches) == 0 {
		return 0, err
	}
	return batches[len(batches)-1].LastSequence, nil
}

func (sink *FileCommandJournalExactlyOnceSink) readBatchesLocked(ctx context.Context) ([]FileCommandJournalBatch, error) {
	entries, err := os.ReadDir(sink.directory)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read file command journal sink directory: %w", err)
	}
	batches := make([]FileCommandJournalBatch, 0, len(entries))
	for _, entry := range entries {
		if err := fileCommandJournalSinkContextError(ctx); err != nil {
			return nil, err
		}
		lastSequence, ok := parseFileCommandJournalBatchName(entry.Name())
		if !ok {
			continue
		}
		if entry.IsDir() {
			return nil, fmt.Errorf("%w: batch path is a directory", ErrFileCommandJournalSinkCorrupt)
		}
		batch, err := readFileCommandJournalBatch(filepath.Join(sink.directory, entry.Name()), lastSequence, sink.maxBatchBytes)
		if err != nil {
			return nil, err
		}
		batches = append(batches, batch)
	}
	sort.Slice(batches, func(left, right int) bool {
		return batches[left].LastSequence < batches[right].LastSequence
	})
	var previous uint64
	for index, batch := range batches {
		if index > 0 && batch.FirstSequence <= previous {
			return nil, fmt.Errorf("%w: overlapping committed batches", ErrFileCommandJournalSinkCorrupt)
		}
		previous = batch.LastSequence
	}
	return batches, nil
}

func encodeFileCommandJournalBatch(ctx context.Context, first, last uint64, records []CommandJournalRecord, maxBytes int64) ([]byte, error) {
	if err := fileCommandJournalSinkContextError(ctx); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("encode file command journal batch: %w", err)
	}
	total := fileCommandJournalSinkHeaderBytes + len(payload) + fileCommandJournalSinkTrailerBytes
	if int64(total) > maxBytes {
		return nil, fmt.Errorf("%w: %d bytes exceeds %d", ErrFileCommandJournalSinkBatchTooLarge, total, maxBytes)
	}
	encoded := make([]byte, total)
	copy(encoded, fileCommandJournalSinkMagic)
	binary.BigEndian.PutUint64(encoded[4:12], first)
	binary.BigEndian.PutUint64(encoded[12:20], last)
	binary.BigEndian.PutUint32(encoded[20:24], uint32(len(records)))
	binary.BigEndian.PutUint32(encoded[24:28], uint32(len(payload)))
	copy(encoded[fileCommandJournalSinkHeaderBytes:], payload)
	binary.BigEndian.PutUint32(encoded[total-fileCommandJournalSinkTrailerBytes:], crc32.Checksum(encoded[:total-fileCommandJournalSinkTrailerBytes], fileCommandJournalSinkChecksumTable))
	return encoded, nil
}

func readFileCommandJournalBatch(path string, expectedLast uint64, maxBytes int64) (FileCommandJournalBatch, error) {
	file, err := os.Open(path)
	if err != nil {
		return FileCommandJournalBatch{}, fmt.Errorf("open file command journal batch: %w", err)
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil {
		return FileCommandJournalBatch{}, fmt.Errorf("read file command journal batch: %w", err)
	}
	if int64(len(data)) > maxBytes {
		return FileCommandJournalBatch{}, fmt.Errorf("%w: batch exceeds %d bytes", ErrFileCommandJournalSinkCorrupt, maxBytes)
	}
	minimum := fileCommandJournalSinkHeaderBytes + fileCommandJournalSinkTrailerBytes
	if len(data) < minimum || string(data[:len(fileCommandJournalSinkMagic)]) != fileCommandJournalSinkMagic {
		return FileCommandJournalBatch{}, fmt.Errorf("%w: invalid header", ErrFileCommandJournalSinkCorrupt)
	}
	first := binary.BigEndian.Uint64(data[4:12])
	last := binary.BigEndian.Uint64(data[12:20])
	count := binary.BigEndian.Uint32(data[20:24])
	payloadLength := int(binary.BigEndian.Uint32(data[24:28]))
	if payloadLength != len(data)-minimum || first > last || last != expectedLast || count == 0 {
		return FileCommandJournalBatch{}, fmt.Errorf("%w: invalid batch bounds", ErrFileCommandJournalSinkCorrupt)
	}
	checksumOffset := len(data) - fileCommandJournalSinkTrailerBytes
	if want, got := binary.BigEndian.Uint32(data[checksumOffset:]), crc32.Checksum(data[:checksumOffset], fileCommandJournalSinkChecksumTable); want != got {
		return FileCommandJournalBatch{}, fmt.Errorf("%w: checksum mismatch", ErrFileCommandJournalSinkCorrupt)
	}
	var records []CommandJournalRecord
	if err := json.Unmarshal(data[fileCommandJournalSinkHeaderBytes:checksumOffset], &records); err != nil {
		return FileCommandJournalBatch{}, fmt.Errorf("%w: invalid records", ErrFileCommandJournalSinkCorrupt)
	}
	if uint32(len(records)) != count || len(records) == 0 || records[0].Sequence != first || records[len(records)-1].Sequence != last {
		return FileCommandJournalBatch{}, fmt.Errorf("%w: sequence metadata mismatch", ErrFileCommandJournalSinkCorrupt)
	}
	for index := 1; index < len(records); index++ {
		if records[index].Sequence <= records[index-1].Sequence {
			return FileCommandJournalBatch{}, fmt.Errorf("%w: records are not strictly increasing", ErrFileCommandJournalSinkCorrupt)
		}
	}
	return FileCommandJournalBatch{FirstSequence: first, LastSequence: last, Records: records}, nil
}

func fileCommandJournalBatchName(lastSequence uint64) string {
	return "batch-" + fmt.Sprintf("%020d", lastSequence) + fileCommandJournalSinkFileSuffix
}

func parseFileCommandJournalBatchName(name string) (uint64, bool) {
	if !strings.HasPrefix(name, "batch-") || !strings.HasSuffix(name, fileCommandJournalSinkFileSuffix) {
		return 0, false
	}
	value := strings.TrimSuffix(strings.TrimPrefix(name, "batch-"), fileCommandJournalSinkFileSuffix)
	if value == "" {
		return 0, false
	}
	sequence, err := strconv.ParseUint(value, 10, 64)
	return sequence, err == nil
}

func fileCommandJournalSinkContextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
