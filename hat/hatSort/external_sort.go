package hatSort

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
)

const (
	// DefaultMaxMemoryBytes is the bounded run-buffer budget when callers do
	// not provide one.
	DefaultMaxMemoryBytes int64 = 4 << 20
	// DefaultMaxSpillBytes bounds cumulative temporary-run bytes.
	DefaultMaxSpillBytes int64 = 1 << 30
	// DefaultMaxMergeRuns bounds the number of open runs in one merge pass.
	DefaultMaxMergeRuns = 32
	maxSortRecordBytes  = uint64(^uint32(0))
)

var (
	ErrInvalidOptions      = errors.New("hatSort: invalid external sort options")
	ErrRecordTooLarge      = errors.New("hatSort: record exceeds external sort memory budget")
	ErrSpillBudgetExceeded = errors.New("hatSort: external sort spill budget exceeded")
	ErrCorruptRun          = errors.New("hatSort: external sort run is corrupt")
	ErrContextRequired     = errors.New("hatSort: external sort context is required")
	ErrSpillDirectory      = errors.New("hatSort: external sort spill directory is invalid")
	ErrEmitterRequired     = errors.New("hatSort: external sort emitter is required")
)

// Record is an opaque sortable item. Key is used by the default comparator;
// Value is carried unchanged to the sorted output.
//
// ExternalSort copies both byte slices before retaining them and returns
// independent byte slices, so callers may reuse their input buffers.
type Record struct {
	Key   []byte
	Value []byte
}

// Options bounds temporary state. Compare returns a negative value, zero, or
// a positive value just like bytes.Compare. Equal records retain their input
// order, including across spill runs.
type Options struct {
	MaxMemoryBytes int64
	MaxSpillBytes  int64
	MaxMergeRuns   int
	SpillDirectory string
	Compare        func(left, right Record) int
}

// Stats reports the work and temporary storage used by one sort.
type Stats struct {
	InputRecords  int
	OutputRecords int
	Runs          int
	MergePasses   int
	SpilledBytes  int64
	PeakMemory    int64
}

// ExternalSort returns records in stable comparator order. It is a convenience
// wrapper over ExternalSortInto and therefore retains the complete result in
// memory; use ExternalSortInto when the output should be streamed.
func ExternalSort(ctx context.Context, records []Record, options Options) ([]Record, Stats, error) {
	result := make([]Record, 0, len(records))
	stats, err := externalSort(ctx, records, options, func(record Record) error {
		result = append(result, record)
		return nil
	})
	if err != nil {
		return nil, stats, err
	}
	return result, stats, nil
}

// ExternalSortInto returns statistics and emits records in stable comparator
// order. The emitted byte slices are borrowed until the callback returns and
// must be copied if the caller retains them. This form bounds the sorter’s
// retained output memory.
func ExternalSortInto(ctx context.Context, records []Record, options Options, emit func(Record) error) (Stats, error) {
	return externalSort(ctx, records, options, emit)
}

func externalSort(ctx context.Context, records []Record, options Options, emit func(Record) error) (Stats, error) {
	var stats Stats
	if emit == nil {
		return stats, ErrEmitterRequired
	}
	if ctx == nil {
		return stats, ErrContextRequired
	}
	if err := ctx.Err(); err != nil {
		return stats, err
	}
	normalized, err := normalizeOptions(options)
	if err != nil {
		return stats, err
	}
	compare := normalized.Compare
	if compare == nil {
		compare = compareDefault
	}

	buffer := make([]sortRecord, 0)
	var bufferBytes int64
	runs := make([]string, 0)
	allPaths := make([]string, 0)
	cleanup := func() {
		for _, path := range allPaths {
			_ = os.Remove(path)
		}
	}
	defer cleanup()

	var spillDir string
	ownedSpillDir := false
	ensureSpillDir := func() error {
		if spillDir != "" {
			return nil
		}
		if normalized.SpillDirectory == "" {
			spillDir, err = os.MkdirTemp("", "hatrie-sort-")
			ownedSpillDir = true
		} else {
			spillDir = normalized.SpillDirectory
			err = os.MkdirAll(spillDir, 0o700)
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrSpillDirectory, err)
		}
		return nil
	}
	defer func() {
		if ownedSpillDir && spillDir != "" {
			_ = os.Remove(spillDir)
		}
	}()

	flush := func() error {
		if len(buffer) == 0 {
			return nil
		}
		if err := ensureSpillDir(); err != nil {
			return err
		}
		sort.SliceStable(buffer, func(left, right int) bool {
			return compare(buffer[left].record, buffer[right].record) < 0
		})
		path, _, err := writeRun(spillDir, buffer, normalized.MaxSpillBytes, &stats.SpilledBytes)
		if err != nil {
			return err
		}
		runs = append(runs, path)
		allPaths = append(allPaths, path)
		stats.Runs++
		for index := range buffer {
			buffer[index] = sortRecord{}
		}
		buffer = buffer[:0]
		bufferBytes = 0
		return nil
	}

	for _, input := range records {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		record := sortRecord{record: cloneRecord(input)}
		record.bytes = recordWireBytes(record.record)
		if record.bytes > normalized.MaxMemoryBytes {
			return stats, ErrRecordTooLarge
		}
		if len(buffer) > 0 && bufferBytes+record.bytes > normalized.MaxMemoryBytes {
			if err := flush(); err != nil {
				return stats, err
			}
		}
		buffer = append(buffer, record)
		bufferBytes += record.bytes
		if bufferBytes > stats.PeakMemory {
			stats.PeakMemory = bufferBytes
		}
	}

	if len(runs) == 0 {
		sort.SliceStable(buffer, func(left, right int) bool {
			return compare(buffer[left].record, buffer[right].record) < 0
		})
		for index := range buffer {
			if err := emit(buffer[index].record); err != nil {
				return stats, err
			}
		}
		stats.InputRecords = len(records)
		stats.OutputRecords = len(buffer)
		if len(buffer) != 0 {
			stats.Runs = 1
		}
		return stats, nil
	}
	if err := flush(); err != nil {
		return stats, err
	}

	for len(runs) > normalized.MaxMergeRuns {
		next := make([]string, 0, (len(runs)+normalized.MaxMergeRuns-1)/normalized.MaxMergeRuns)
		for start := 0; start < len(runs); start += normalized.MaxMergeRuns {
			end := start + normalized.MaxMergeRuns
			if end > len(runs) {
				end = len(runs)
			}
			path, err := mergeGroup(ctx, runs[start:end], spillDir, compare, normalized.MaxSpillBytes, &stats.SpilledBytes, normalized.MaxMemoryBytes)
			if err != nil {
				return stats, err
			}
			next = append(next, path)
			allPaths = append(allPaths, path)
		}
		for _, path := range runs {
			_ = os.Remove(path)
		}
		runs = next
		stats.MergePasses++
	}

	err = mergeRuns(ctx, runs, compare, normalized.MaxMemoryBytes, func(record Record) error {
		if err := emit(record); err != nil {
			return err
		}
		stats.OutputRecords++
		return nil
	})
	if err != nil {
		return stats, err
	}
	stats.InputRecords = len(records)
	return stats, nil
}

type normalizedOptions struct {
	MaxMemoryBytes int64
	MaxSpillBytes  int64
	MaxMergeRuns   int
	SpillDirectory string
	Compare        func(left, right Record) int
}

func normalizeOptions(options Options) (normalizedOptions, error) {
	if options.MaxMemoryBytes < 0 || options.MaxSpillBytes < 0 || options.MaxMergeRuns < 0 {
		return normalizedOptions{}, ErrInvalidOptions
	}
	if options.MaxMemoryBytes == 0 {
		options.MaxMemoryBytes = DefaultMaxMemoryBytes
	}
	if options.MaxSpillBytes == 0 {
		options.MaxSpillBytes = DefaultMaxSpillBytes
	}
	if options.MaxMergeRuns == 0 {
		options.MaxMergeRuns = DefaultMaxMergeRuns
	}
	if options.MaxMemoryBytes < 16 || options.MaxMergeRuns < 2 {
		return normalizedOptions{}, ErrInvalidOptions
	}
	return normalizedOptions{
		MaxMemoryBytes: options.MaxMemoryBytes,
		MaxSpillBytes:  options.MaxSpillBytes,
		MaxMergeRuns:   options.MaxMergeRuns,
		SpillDirectory: options.SpillDirectory,
		Compare:        options.Compare,
	}, nil
}

type sortRecord struct {
	record Record
	bytes  int64
}

func writeRun(directory string, records []sortRecord, maxSpill int64, spilled *int64) (string, int64, error) {
	path, err := os.CreateTemp(directory, ".hatrie-sort-run-")
	if err != nil {
		return "", 0, fmt.Errorf("create sort run: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(path.Name())
		}
	}()
	writer := bufio.NewWriterSize(path, 32<<10)
	var written int64
	for _, item := range records {
		size := recordWireBytes(item.record)
		if *spilled > maxSpill || size > maxSpill-*spilled {
			_ = path.Close()
			return "", written, ErrSpillBudgetExceeded
		}
		if err := writeRecord(writer, item.record); err != nil {
			_ = path.Close()
			return "", written, fmt.Errorf("write sort run: %w", err)
		}
		written += size
		*spilled += size
	}
	if err := writer.Flush(); err != nil {
		_ = path.Close()
		return "", written, fmt.Errorf("flush sort run: %w", err)
	}
	if err := path.Close(); err != nil {
		return "", written, fmt.Errorf("close sort run: %w", err)
	}
	keep = true
	return path.Name(), written, nil
}

func mergeGroup(ctx context.Context, paths []string, directory string, compare func(left, right Record) int, maxSpill int64, spilled *int64, maxMemory int64) (string, error) {
	path, err := os.CreateTemp(directory, ".hatrie-sort-merge-")
	if err != nil {
		return "", fmt.Errorf("create merged sort run: %w", err)
	}
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(path.Name())
		}
	}()
	writer := bufio.NewWriterSize(path, 32<<10)
	var written int64
	err = mergeRuns(ctx, paths, compare, maxMemory, func(record Record) error {
		size := recordWireBytes(record)
		if *spilled > maxSpill || size > maxSpill-*spilled {
			return ErrSpillBudgetExceeded
		}
		if err := writeRecord(writer, record); err != nil {
			return fmt.Errorf("write merged sort run: %w", err)
		}
		written += size
		*spilled += size
		return nil
	})
	if err != nil {
		_ = path.Close()
		return "", err
	}
	if err := writer.Flush(); err != nil {
		_ = path.Close()
		return "", fmt.Errorf("flush merged sort run: %w", err)
	}
	if err := path.Close(); err != nil {
		return "", fmt.Errorf("close merged sort run: %w", err)
	}
	_ = written
	keep = true
	return path.Name(), nil
}

func mergeRuns(ctx context.Context, paths []string, compare func(left, right Record) int, maxMemory int64, emit func(Record) error) error {
	readers := make([]*runReader, len(paths))
	defer func() {
		for _, reader := range readers {
			if reader != nil {
				_ = reader.close()
			}
		}
	}()
	items := make([]mergeItem, 0, len(paths))
	for index, path := range paths {
		reader, err := openRun(path)
		if err != nil {
			return err
		}
		readers[index] = reader
		item, ok, err := reader.next(maxMemory)
		if err != nil {
			return err
		}
		if ok {
			items = append(items, mergeItem{run: index, record: item})
		}
	}

	for len(items) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		best := 0
		for index := 1; index < len(items); index++ {
			comparison := compare(items[index].record, items[best].record)
			if comparison < 0 || (comparison == 0 && items[index].run < items[best].run) {
				best = index
			}
		}
		selected := items[best]
		if err := emit(selected.record); err != nil {
			return err
		}
		next, ok, err := readers[selected.run].next(maxMemory)
		if err != nil {
			return err
		}
		if ok {
			items[best].record = next
		} else {
			items[best] = items[len(items)-1]
			items = items[:len(items)-1]
		}
	}
	return nil
}

type mergeItem struct {
	run    int
	record Record
}

type runReader struct {
	file   *os.File
	reader *bufio.Reader
}

func openRun(path string) (*runReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open sort run: %w", err)
	}
	return &runReader{file: file, reader: bufio.NewReaderSize(file, 32<<10)}, nil
}

func (reader *runReader) close() error {
	return reader.file.Close()
}

func (reader *runReader) next(maxMemory int64) (Record, bool, error) {
	var header [8]byte
	if _, err := io.ReadFull(reader.reader, header[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return Record{}, false, nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Record{}, false, ErrCorruptRun
		}
		return Record{}, false, fmt.Errorf("read sort run: %w", err)
	}
	keyBytes := binary.BigEndian.Uint32(header[:4])
	valueBytes := binary.BigEndian.Uint32(header[4:])
	size := int64(keyBytes) + int64(valueBytes) + 8
	if size > maxMemory || uint64(size) > maxSortRecordBytes {
		return Record{}, false, ErrRecordTooLarge
	}
	record := Record{Key: make([]byte, int(keyBytes)), Value: make([]byte, int(valueBytes))}
	if _, err := io.ReadFull(reader.reader, record.Key); err != nil {
		return Record{}, false, ErrCorruptRun
	}
	if _, err := io.ReadFull(reader.reader, record.Value); err != nil {
		return Record{}, false, ErrCorruptRun
	}
	return record, true, nil
}

func writeRecord(writer io.Writer, record Record) error {
	if uint64(len(record.Key))+uint64(len(record.Value))+8 > maxSortRecordBytes {
		return ErrRecordTooLarge
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(record.Key)))
	binary.BigEndian.PutUint32(header[4:], uint32(len(record.Value)))
	if _, err := writer.Write(header[:]); err != nil {
		return err
	}
	if _, err := writer.Write(record.Key); err != nil {
		return err
	}
	_, err := writer.Write(record.Value)
	return err
}

func recordWireBytes(record Record) int64 {
	return int64(len(record.Key) + len(record.Value) + 8)
}

func compareDefault(left, right Record) int {
	return bytes.Compare(left.Key, right.Key)
}

func cloneRecord(record Record) Record {
	return Record{Key: bytes.Clone(record.Key), Value: bytes.Clone(record.Value)}
}
