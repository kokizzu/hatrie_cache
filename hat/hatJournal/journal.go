// Package hatJournal exposes portable command-journal configuration and
// read-only metadata inspection. It deliberately does not decode cache command
// values; cache-specific semantic validation remains in the parent package.
package hatJournal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	maxRecordBytes = 1 << 30
	segmentSuffix  = ".journal"
)

var binaryMagic = []byte{'h', 'c', 'j', 'n', 1}

// Format identifies an on-disk command-journal encoding.
type Format string

const (
	FormatJSON    Format = "json"
	FormatBinary  Format = "binary"
	FormatUnknown Format = "unknown"
	FormatMixed   Format = "mixed"
)

const DefaultFormat = FormatBinary

// ParseFormat returns the canonical command-journal format. Empty input uses
// the binary default and "bin" is accepted as a binary alias.
func ParseFormat(value string) (Format, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(FormatBinary), "bin":
		return FormatBinary, nil
	case string(FormatJSON):
		return FormatJSON, nil
	default:
		return "", fmt.Errorf("hatJournal: unsupported command journal format %q", value)
	}
}

const (
	DefaultGroupCommitWindow   time.Duration = 0
	DefaultGroupCommitMaxBatch               = 64
	MaxGroupCommitBatch                      = 4096
	DefaultSegmentMaxBytes     int64         = 64 << 20
	DefaultRetainedSegments                  = 16
	MaxRetainedSegments                      = 1024
	DefaultIdempotencyCapacity              = 0
	MaxIdempotencyCapacity                   = 1 << 20
)

// Options configures journal encoding, durable group commit, and optional
// bounded segment rotation. SegmentMaxBytes zero keeps one active file.
type Options struct {
	Format              Format
	GroupCommitWindow   time.Duration
	GroupCommitMaxBatch int
	SegmentMaxBytes     int64
	RetainedSegments    int
	IdempotencyCapacity int
}

// ValidateOptions verifies journal options and returns a copy with a
// canonical format. A batch size of one uses immediate fsync behavior.
func ValidateOptions(options Options) (Options, error) {
	format, err := ParseFormat(string(options.Format))
	if err != nil {
		return Options{}, err
	}
	if options.GroupCommitWindow < 0 {
		return Options{}, errors.New("hatJournal: group commit window must be non-negative")
	}
	if options.GroupCommitMaxBatch < 1 {
		return Options{}, errors.New("hatJournal: group commit max batch must be positive")
	}
	if options.GroupCommitMaxBatch > MaxGroupCommitBatch {
		return Options{}, fmt.Errorf("hatJournal: group commit max batch must be <= %d", MaxGroupCommitBatch)
	}
	if options.SegmentMaxBytes < 0 {
		return Options{}, errors.New("hatJournal: segment max bytes must be non-negative")
	}
	if options.RetainedSegments < 0 {
		return Options{}, errors.New("hatJournal: retained segments must be non-negative")
	}
	if options.RetainedSegments > MaxRetainedSegments {
		return Options{}, fmt.Errorf("hatJournal: retained segments must be <= %d", MaxRetainedSegments)
	}
	if options.SegmentMaxBytes > 0 && options.RetainedSegments == 0 {
		return Options{}, errors.New("hatJournal: retained segments must be positive when segmentation is enabled")
	}
	if options.IdempotencyCapacity < 0 {
		return Options{}, errors.New("hatJournal: idempotency capacity must be non-negative")
	}
	if options.IdempotencyCapacity > MaxIdempotencyCapacity {
		return Options{}, fmt.Errorf("hatJournal: idempotency capacity must be <= %d", MaxIdempotencyCapacity)
	}
	options.Format = format
	return options, nil
}

// InspectOptions controls a read-only journal inspection. MaxRecordBytes
// limits one record before allocation; zero uses the on-disk format maximum.
type InspectOptions struct {
	Segmented      bool
	MaxRecordBytes int64
}

// File describes one active or archived journal file.
type File struct {
	Path             string `json:"path"`
	Format           Format `json:"format"`
	Size             int64  `json:"size"`
	ValidBytes       int64  `json:"valid_bytes"`
	RecordCount      int    `json:"record_count"`
	FirstSequence    uint64 `json:"first_sequence,omitempty"`
	LastSequence     uint64 `json:"last_sequence,omitempty"`
	CompactedThrough uint64 `json:"compacted_through,omitempty"`
	TruncatedTail    bool   `json:"truncated_tail,omitempty"`
}

// Inspection summarizes the durable records visible from a journal path.
// Archived segment files must be complete. An incomplete active-file suffix is
// reported as TruncatedTail and excluded from counts, matching recovery.
type Inspection struct {
	Path             string `json:"path"`
	Segmented        bool   `json:"segmented"`
	Segments         []File `json:"segments"`
	Active           File   `json:"active"`
	RecordCount      int    `json:"record_count"`
	FirstSequence    uint64 `json:"first_sequence,omitempty"`
	LastSequence     uint64 `json:"last_sequence,omitempty"`
	CompactedThrough uint64 `json:"compacted_through,omitempty"`
	ValidBytes       int64  `json:"valid_bytes"`
	TruncatedTail    bool   `json:"truncated_tail,omitempty"`
}

type record struct {
	sequence   uint64
	checkpoint bool
	format     Format
}

type segment struct {
	path  string
	start uint64
	end   uint64
}

// Segment identifies one immutable archived range in a segmented journal.
type Segment struct {
	Path  string
	Start uint64
	End   uint64
}

// SegmentDirectory returns the directory used to retain archived journal
// segments for path.
func SegmentDirectory(path string) string {
	return path + ".segments"
}

// SegmentPath returns the canonical file path for an archived sequence range.
func SegmentPath(path string, start uint64, end uint64) string {
	name := fmt.Sprintf("%020d-%020d%s", start, end, segmentSuffix)
	return filepath.Join(SegmentDirectory(path), name)
}

// ListSegments returns all archived journal segments in sequence order. It
// rejects malformed, overlapping, symbolic-link, and directory entries.
func ListSegments(path string) ([]Segment, error) {
	segments, err := listSegments(path)
	if err != nil {
		return nil, err
	}
	out := make([]Segment, len(segments))
	for index, current := range segments {
		out[index] = Segment{Path: current.path, Start: current.start, End: current.end}
	}
	return out, nil
}

// Inspect scans journal framing and sequence continuity without changing any
// file. It validates the portable record header; callers that need command
// semantic validation should additionally use the cache package inspector.
func Inspect(path string, options InspectOptions) (Inspection, error) {
	if strings.TrimSpace(path) == "" {
		return Inspection{}, errors.New("hatJournal: journal path is required")
	}
	limit := options.MaxRecordBytes
	if limit == 0 {
		limit = maxRecordBytes
	}
	if limit < 1 || limit > maxRecordBytes {
		return Inspection{}, fmt.Errorf("hatJournal: max record bytes must be between 1 and %d", maxRecordBytes)
	}
	inspection := Inspection{Path: path, Segmented: options.Segmented, Segments: []File{}}
	segments := []segment(nil)
	if options.Segmented {
		var err error
		segments, err = listSegments(path)
		if err != nil {
			return Inspection{}, err
		}
	}

	var previous uint64
	var hasPrevious bool
	for index, current := range segments {
		file, err := inspectFile(current.path, limit)
		if err != nil {
			return Inspection{}, err
		}
		if file.TruncatedTail || file.ValidBytes != file.Size {
			return Inspection{}, fmt.Errorf("hatJournal: archived journal segment %q is truncated", filepath.Base(current.path))
		}
		if err := mergeFile(&inspection, &previous, &hasPrevious, file, index > 0, current); err != nil {
			return Inspection{}, err
		}
		inspection.Segments = append(inspection.Segments, file)
	}
	active, err := inspectFile(path, limit)
	if err != nil {
		return Inspection{}, err
	}
	if err := mergeFile(&inspection, &previous, &hasPrevious, active, len(segments) > 0, segment{}); err != nil {
		return Inspection{}, err
	}
	inspection.Active = active
	inspection.TruncatedTail = active.TruncatedTail
	return inspection, nil
}

func listSegments(path string) ([]segment, error) {
	dir := SegmentDirectory(path)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	segments := make([]segment, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return nil, fmt.Errorf("hatJournal: unexpected journal segment entry %q", entry.Name())
		}
		name := entry.Name()
		if !strings.HasSuffix(name, segmentSuffix) {
			return nil, fmt.Errorf("hatJournal: unexpected journal segment file %q", name)
		}
		bounds := strings.Split(strings.TrimSuffix(name, segmentSuffix), "-")
		if len(bounds) != 2 {
			return nil, fmt.Errorf("hatJournal: invalid journal segment file %q", name)
		}
		start, startErr := strconv.ParseUint(bounds[0], 10, 64)
		end, endErr := strconv.ParseUint(bounds[1], 10, 64)
		if startErr != nil || endErr != nil || start == 0 || end < start {
			return nil, fmt.Errorf("hatJournal: invalid journal segment file %q", name)
		}
		segments = append(segments, segment{path: filepath.Join(dir, name), start: start, end: end})
	}
	sort.Slice(segments, func(left, right int) bool { return segments[left].start < segments[right].start })
	for index := 1; index < len(segments); index++ {
		if segments[index].start <= segments[index-1].end {
			return nil, fmt.Errorf("hatJournal: overlapping journal segments %q and %q", filepath.Base(segments[index-1].path), filepath.Base(segments[index].path))
		}
	}
	return segments, nil
}

func inspectFile(path string, limit int64) (File, error) {
	file := File{Path: path, Format: FormatUnknown}
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return file, nil
	}
	if err != nil {
		return File{}, err
	}
	if !info.Mode().IsRegular() {
		return File{}, fmt.Errorf("hatJournal: journal file %q is not regular", path)
	}
	file.Size = info.Size()
	handle, err := os.Open(path)
	if err != nil {
		return File{}, err
	}
	defer handle.Close()

	reader := bufio.NewReader(handle)
	for {
		record, bytesRead, complete, err := readRecord(reader, limit)
		if err != nil {
			return File{}, fmt.Errorf("hatJournal: inspect %q: %w", path, err)
		}
		if !complete {
			file.TruncatedTail = bytesRead > 0
			return file, nil
		}
		file.ValidBytes += int64(bytesRead)
		file.RecordCount++
		if file.RecordCount == 1 {
			file.FirstSequence = record.sequence
			file.Format = record.format
		} else if file.Format != record.format {
			file.Format = FormatMixed
		}
		file.LastSequence = record.sequence
		if record.checkpoint && record.sequence > file.CompactedThrough {
			file.CompactedThrough = record.sequence
		}
	}
}

func mergeFile(inspection *Inspection, previous *uint64, hasPrevious *bool, file File, handoff bool, bounds segment) error {
	inspection.ValidBytes += file.ValidBytes
	inspection.RecordCount += file.RecordCount
	if file.CompactedThrough > inspection.CompactedThrough {
		inspection.CompactedThrough = file.CompactedThrough
	}
	if file.TruncatedTail {
		inspection.TruncatedTail = true
	}
	if file.RecordCount == 0 {
		return nil
	}

	handle, err := os.Open(file.Path)
	if err != nil {
		return err
	}
	defer handle.Close()
	reader := bufio.NewReader(handle)
	first := true
	var firstMutation uint64
	var lastMutation uint64
	for {
		record, _, complete, err := readRecord(reader, maxRecordBytes)
		if err != nil {
			return err
		}
		if !complete {
			break
		}
		if first && handoff && record.checkpoint {
			if !*hasPrevious || record.sequence != *previous {
				return fmt.Errorf("hatJournal: journal checkpoint %d does not continue after %d", record.sequence, *previous)
			}
			first = false
			continue
		}
		first = false
		if !record.checkpoint {
			if firstMutation == 0 {
				firstMutation = record.sequence
			}
			lastMutation = record.sequence
		}
		if err := validateSequence(*previous, *hasPrevious, record); err != nil {
			return err
		}
		if !*hasPrevious {
			inspection.FirstSequence = record.sequence
		}
		if record.sequence > inspection.LastSequence {
			inspection.LastSequence = record.sequence
		}
		*previous = record.sequence
		*hasPrevious = true
	}
	if bounds.path != "" && (firstMutation != bounds.start || lastMutation != bounds.end) {
		return fmt.Errorf("hatJournal: journal segment %q bounds do not match records %d-%d", filepath.Base(bounds.path), firstMutation, lastMutation)
	}
	return nil
}

func validateSequence(previous uint64, hasPrevious bool, current record) error {
	if !hasPrevious {
		if current.checkpoint || current.sequence == 1 {
			return nil
		}
		return fmt.Errorf("hatJournal: journal starts at sequence %d without checkpoint", current.sequence)
	}
	if previous == ^uint64(0) {
		return fmt.Errorf("hatJournal: journal sequence %d follows exhausted sequence", current.sequence)
	}
	if current.sequence != previous+1 {
		return fmt.Errorf("hatJournal: journal sequence %d does not continue after %d", current.sequence, previous)
	}
	return nil
}

func readRecord(reader *bufio.Reader, limit int64) (record, int, bool, error) {
	header, err := reader.Peek(len(binaryMagic))
	if err == nil && bytes.Equal(header, binaryMagic) {
		return readBinaryRecord(reader, limit)
	}
	if len(header) > 0 && bytes.HasPrefix(binaryMagic, header) {
		_, _ = reader.Discard(len(header))
		return record{}, len(header), false, nil
	}
	return readJSONRecord(reader, limit)
}

func readJSONRecord(reader *bufio.Reader, limit int64) (record, int, bool, error) {
	line, bytesRead, complete, err := readLine(reader, limit)
	if err != nil || !complete {
		return record{}, bytesRead, complete, err
	}
	var header struct {
		Version    int    `json:"version"`
		Sequence   uint64 `json:"sequence"`
		Checkpoint bool   `json:"checkpoint"`
		Request    *struct {
			Command string `json:"command"`
		} `json:"request"`
	}
	decoder := json.NewDecoder(bytes.NewReader(line))
	if err := decoder.Decode(&header); err != nil {
		return record{}, 0, false, err
	}
	if header.Version != 1 {
		return record{}, 0, false, errors.New("unsupported journal version")
	}
	if !header.Checkpoint && (header.Request == nil || strings.TrimSpace(header.Request.Command) == "") {
		return record{}, 0, false, errors.New("journal request is not journalable")
	}
	return record{sequence: header.Sequence, checkpoint: header.Checkpoint, format: FormatJSON}, bytesRead, true, nil
}

func readLine(reader *bufio.Reader, limit int64) ([]byte, int, bool, error) {
	var line []byte
	for {
		part, err := reader.ReadSlice('\n')
		if int64(len(line)+len(part)) > limit {
			return nil, len(line) + len(part), false, fmt.Errorf("journal record exceeds %d bytes", limit)
		}
		line = append(line, part...)
		if err == nil {
			return line, len(line), true, nil
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			continue
		}
		if errors.Is(err, io.EOF) {
			return nil, len(line), false, nil
		}
		return nil, len(line), false, err
	}
}

func readBinaryRecord(reader *bufio.Reader, limit int64) (record, int, bool, error) {
	consumed, err := reader.Discard(len(binaryMagic))
	if err != nil {
		return record{}, consumed, false, err
	}
	size, sizeBytes, complete, err := readUvarint(reader)
	if err != nil || !complete {
		return record{}, consumed + len(sizeBytes), complete, err
	}
	consumed += len(sizeBytes)
	if size > uint64(limit) || size > uint64(int(^uint(0)>>1)) {
		return record{}, consumed, false, fmt.Errorf("binary journal record exceeds %d bytes", limit)
	}
	payload := make([]byte, int(size))
	if _, err := io.ReadFull(reader, payload); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return record{}, consumed, false, nil
		}
		return record{}, consumed, false, err
	}
	entry, err := parseBinaryHeader(payload)
	if err != nil {
		return record{}, consumed, false, err
	}
	return entry, consumed + len(payload), true, nil
}

func readUvarint(reader *bufio.Reader) (uint64, []byte, bool, error) {
	var raw [10]byte
	for index := range raw {
		value, err := reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, raw[:index], false, nil
			}
			return 0, raw[:index], false, err
		}
		raw[index] = value
		if value < 0x80 {
			parsed, count := binary.Uvarint(raw[:index+1])
			if count <= 0 {
				return 0, raw[:index+1], false, errors.New("invalid binary journal record size")
			}
			return parsed, raw[:index+1], true, nil
		}
	}
	return 0, raw[:], false, errors.New("invalid binary journal record size")
}

func parseBinaryHeader(payload []byte) (record, error) {
	reader := payloadReader{data: payload}
	version, err := reader.uvarint()
	if err != nil {
		return record{}, err
	}
	if version < 1 || version > 3 {
		return record{}, errors.New("unsupported binary journal version")
	}
	sequence, err := reader.uvarint()
	if err != nil {
		return record{}, err
	}
	checkpoint, err := reader.bool()
	if err != nil {
		return record{}, err
	}
	command, err := reader.bytes()
	if err != nil {
		return record{}, err
	}
	for range 3 { // key, value, subkey
		if _, err := reader.bytes(); err != nil {
			return record{}, err
		}
	}
	for range 3 { // priority, TTL, and absolute expiry
		if err := reader.optionalVarint(); err != nil {
			return record{}, err
		}
	}
	for range 2 { // values and pairs payloads
		if _, err := reader.bytes(); err != nil {
			return record{}, err
		}
	}
	if version >= 3 {
		if _, err := reader.bytes(); err != nil {
			return record{}, err
		}
	}
	if !reader.done() {
		return record{}, errors.New("invalid trailing binary journal payload data")
	}
	if !checkpoint && len(bytes.TrimSpace(command)) == 0 {
		return record{}, errors.New("journal request is not journalable")
	}
	return record{sequence: sequence, checkpoint: checkpoint, format: FormatBinary}, nil
}

type payloadReader struct {
	data []byte
	off  int
}

func (reader *payloadReader) done() bool { return reader.off == len(reader.data) }

func (reader *payloadReader) uvarint() (uint64, error) {
	value, count := binary.Uvarint(reader.data[reader.off:])
	if count == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if count < 0 {
		return 0, errors.New("invalid binary unsigned integer")
	}
	reader.off += count
	return value, nil
}

func (reader *payloadReader) bool() (bool, error) {
	if reader.off >= len(reader.data) {
		return false, io.ErrUnexpectedEOF
	}
	value := reader.data[reader.off]
	reader.off++
	if value == 0 || value == 1 {
		return value == 1, nil
	}
	return false, errors.New("invalid binary boolean")
}

func (reader *payloadReader) bytes() ([]byte, error) {
	size, err := reader.uvarint()
	if err != nil {
		return nil, err
	}
	if size > uint64(len(reader.data)-reader.off) {
		return nil, io.ErrUnexpectedEOF
	}
	start := reader.off
	reader.off += int(size)
	return reader.data[start:reader.off], nil
}

func (reader *payloadReader) optionalVarint() error {
	present, err := reader.bool()
	if err != nil || !present {
		return err
	}
	_, count := binary.Varint(reader.data[reader.off:])
	if count == 0 {
		return io.ErrUnexpectedEOF
	}
	if count < 0 {
		return errors.New("invalid binary signed integer")
	}
	reader.off += count
	return nil
}
