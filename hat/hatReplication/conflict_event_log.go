package hatReplication

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var (
	ErrConflictEventLogNil          = errors.New("hatriecache: conflict event log is nil")
	ErrConflictEventLogClosed       = errors.New("hatriecache: conflict event log is closed")
	ErrConflictEventOptionsInvalid  = errors.New("hatriecache: conflict event log options are invalid")
	ErrConflictEventSecretInvalid   = errors.New("hatriecache: conflict event log secret is invalid")
	ErrConflictEventSpaceRequired   = errors.New("hatriecache: conflict event space is required")
	ErrConflictEventInvalid         = errors.New("hatriecache: conflict event is invalid")
	ErrConflictEventLimitInvalid    = errors.New("hatriecache: conflict event replay limit is invalid")
	ErrConflictEventHistoryGap      = errors.New("hatriecache: conflict event history gap")
	ErrConflictEventFileInvalid     = errors.New("hatriecache: conflict event file is invalid")
	ErrConflictEventContextRequired = errors.New("hatriecache: conflict event wait context is required")
)

const (
	DefaultConflictEventLogCapacity = 1024
	MaxConflictEventLogCapacity     = 65536
	MinConflictEventSecretBytes     = 16
	MaxConflictEventSecretBytes     = 256
	MaxConflictEventSpaceBytes      = 256
	MaxConflictEventNodeBytes       = 256
	MaxConflictEventKeyBytes        = 1 << 20
	MaxConflictEventReplayBatch     = 1024
	maxConflictEventPayloadBytes    = 4096
	conflictEventFileHeaderBytes    = 4 + 2 + 4
	conflictEventFrameOverheadBytes = 4 + 4
)

var conflictEventFileMagic = [4]byte{'h', 'c', 'e', '1'}

const conflictEventFileVersion uint16 = 1

var conflictEventCRC32 = crc32.MakeTable(crc32.Castagnoli)

// ConflictEventDecision identifies which side of a conflict was retained.
type ConflictEventDecision string

const (
	ConflictEventDecisionLeftWins  ConflictEventDecision = "left_wins"
	ConflictEventDecisionRightWins ConflictEventDecision = "right_wins"
	ConflictEventDecisionEqual     ConflictEventDecision = "equal"
	ConflictEventDecisionRejected  ConflictEventDecision = "rejected"
)

// ConflictEventLogOptions bounds an in-memory conflict history. Secret is an
// HMAC key and is never written to the durable event file. Reuse the same key
// after restart when fingerprints must remain joinable across files.
type ConflictEventLogOptions struct {
	Capacity int
	Secret   []byte
}

// ConflictEventInput is the non-redacted input to Record. Key is hashed and
// never retained. Winner must be one of Left or Right except for Rejected.
type ConflictEventInput struct {
	Space    string
	Key      []byte
	Left     ConflictVersion
	Right    ConflictVersion
	Decision ConflictEventDecision
	Winner   *ConflictVersion
}

// ConflictEvent is a privacy-safe conflict record. KeyFingerprint is an
// HMAC-SHA256 digest, so the original key cannot be recovered from the event.
type ConflictEvent struct {
	Sequence       uint64                `json:"sequence"`
	Space          string                `json:"space"`
	KeyFingerprint string                `json:"key_fingerprint"`
	Left           ConflictVersion       `json:"left"`
	Right          ConflictVersion       `json:"right"`
	Decision       ConflictEventDecision `json:"decision"`
	Winner         *ConflictVersion      `json:"winner,omitempty"`
}

// ConflictEventPage is a bounded replay result. NextSequence is the cursor
// to pass to the next ReadAfter or WaitAfter call.
type ConflictEventPage struct {
	Events         []ConflictEvent `json:"events"`
	NextSequence   uint64          `json:"next_sequence"`
	OldestSequence uint64          `json:"oldest_sequence"`
	NewestSequence uint64          `json:"newest_sequence"`
}

type conflictEventRecord struct {
	sequence       uint64
	space          string
	keyFingerprint [sha256.Size]byte
	left           ConflictVersion
	right          ConflictVersion
	decision       ConflictEventDecision
	winner         ConflictVersion
	hasWinner      bool
}

// ConflictEventLog is an opt-in bounded conflict history. It does not attach
// to conflict resolution automatically: callers record only decisions they
// have actually applied. A non-empty path enables a compact CRC-protected
// binary file with fsync-before-publish semantics.
type ConflictEventLog struct {
	mu              sync.Mutex
	capacity        int
	secret          []byte
	path            string
	events          []conflictEventRecord
	start           int
	count           int
	nextSequence    uint64
	notify          chan struct{}
	fileInitialized bool
	closed          bool
}

// NewConflictEventLog creates an in-memory conflict event log.
func NewConflictEventLog(options ConflictEventLogOptions) (*ConflictEventLog, error) {
	return OpenConflictEventLog("", options)
}

// OpenConflictEventLog opens or creates a durable conflict event log. The
// file is created lazily by the first Record call.
func OpenConflictEventLog(path string, options ConflictEventLogOptions) (*ConflictEventLog, error) {
	secret, err := normalizeConflictEventSecret(options.Secret)
	if err != nil {
		return nil, err
	}
	requestedCapacity, err := normalizeConflictEventCapacity(options.Capacity)
	if err != nil {
		return nil, err
	}
	path = strings.TrimSpace(path)
	log := &ConflictEventLog{
		capacity:     requestedCapacity,
		secret:       secret,
		path:         path,
		events:       make([]conflictEventRecord, requestedCapacity),
		nextSequence: 1,
		notify:       make(chan struct{}),
	}
	if path == "" {
		return log, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return log, nil
		}
		return nil, err
	}
	fileCapacity, records, err := decodeConflictEventFile(data)
	if err != nil {
		return nil, err
	}
	if options.Capacity != 0 && fileCapacity != requestedCapacity {
		return nil, fmt.Errorf("%w: file capacity=%d requested=%d", ErrConflictEventOptionsInvalid, fileCapacity, requestedCapacity)
	}
	log.capacity = fileCapacity
	log.events = make([]conflictEventRecord, fileCapacity)
	for _, record := range records {
		log.insertLocked(record)
		log.nextSequence = record.sequence + 1
	}
	log.fileInitialized = true
	return log, nil
}

func normalizeConflictEventCapacity(capacity int) (int, error) {
	if capacity == 0 {
		return DefaultConflictEventLogCapacity, nil
	}
	if capacity < 1 || capacity > MaxConflictEventLogCapacity {
		return 0, fmt.Errorf("%w: capacity must be 1..%d", ErrConflictEventOptionsInvalid, MaxConflictEventLogCapacity)
	}
	return capacity, nil
}

func normalizeConflictEventSecret(secret []byte) ([]byte, error) {
	if len(secret) < MinConflictEventSecretBytes || len(secret) > MaxConflictEventSecretBytes {
		return nil, fmt.Errorf("%w: secret must be %d..%d bytes", ErrConflictEventSecretInvalid, MinConflictEventSecretBytes, MaxConflictEventSecretBytes)
	}
	return append([]byte(nil), secret...), nil
}

// Record validates and appends one conflict decision. The durable file is
// synced before the event becomes visible to readers and waiters.
func (log *ConflictEventLog) Record(input ConflictEventInput) (ConflictEvent, error) {
	if log == nil {
		return ConflictEvent{}, ErrConflictEventLogNil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return ConflictEvent{}, ErrConflictEventLogClosed
	}
	if log.nextSequence == 0 {
		return ConflictEvent{}, fmt.Errorf("%w: sequence overflow", ErrConflictEventInvalid)
	}
	record, err := log.recordFromInputLocked(input)
	if err != nil {
		return ConflictEvent{}, err
	}
	record.sequence = log.nextSequence
	if log.path != "" {
		if log.count >= log.capacity || !log.fileInitialized {
			records := log.recordsLocked()
			records = append(records, record)
			if len(records) > log.capacity {
				records = records[len(records)-log.capacity:]
			}
			if err := rewriteConflictEventFile(log.path, log.capacity, records); err != nil {
				return ConflictEvent{}, err
			}
			log.fileInitialized = true
		} else if err := appendConflictEventFile(log.path, record); err != nil {
			return ConflictEvent{}, err
		}
	}
	log.insertLocked(record)
	log.nextSequence++
	oldNotify := log.notify
	log.notify = make(chan struct{})
	close(oldNotify)
	return record.public(), nil
}

func (log *ConflictEventLog) recordFromInputLocked(input ConflictEventInput) (conflictEventRecord, error) {
	space := strings.TrimSpace(input.Space)
	if space == "" {
		return conflictEventRecord{}, ErrConflictEventSpaceRequired
	}
	if len(space) > MaxConflictEventSpaceBytes || len(input.Key) > MaxConflictEventKeyBytes {
		return conflictEventRecord{}, ErrConflictEventInvalid
	}
	record := conflictEventRecord{
		space:     space,
		left:      input.Left,
		right:     input.Right,
		decision:  input.Decision,
		hasWinner: input.Winner != nil,
	}
	if input.Winner != nil {
		record.winner = *input.Winner
	}
	if err := validateConflictEventRecord(record); err != nil {
		return conflictEventRecord{}, err
	}
	mac := hmac.New(sha256.New, log.secret)
	_, _ = mac.Write(input.Key)
	copy(record.keyFingerprint[:], mac.Sum(nil))
	return record, nil
}

func validateConflictEventRecord(record conflictEventRecord) error {
	if strings.TrimSpace(record.space) == "" || len(record.space) > MaxConflictEventSpaceBytes {
		return fmt.Errorf("%w: space is invalid", ErrConflictEventInvalid)
	}
	if err := validateConflictEventVersion(record.left); err != nil {
		return err
	}
	if err := validateConflictEventVersion(record.right); err != nil {
		return err
	}
	comparison, err := CompareConflictVersions(record.left, record.right)
	if err != nil {
		return fmt.Errorf("%w: versions: %v", ErrConflictEventInvalid, err)
	}
	switch record.decision {
	case ConflictEventDecisionLeftWins:
		if !record.hasWinner || !sameConflictVersion(record.winner, record.left) {
			return fmt.Errorf("%w: left-wins winner mismatch", ErrConflictEventInvalid)
		}
	case ConflictEventDecisionRightWins:
		if !record.hasWinner || !sameConflictVersion(record.winner, record.right) {
			return fmt.Errorf("%w: right-wins winner mismatch", ErrConflictEventInvalid)
		}
	case ConflictEventDecisionEqual:
		if comparison != 0 || !record.hasWinner || !sameConflictVersion(record.winner, record.left) {
			return fmt.Errorf("%w: equal decision mismatch", ErrConflictEventInvalid)
		}
	case ConflictEventDecisionRejected:
		if comparison == 0 || record.hasWinner {
			return fmt.Errorf("%w: rejected decision mismatch", ErrConflictEventInvalid)
		}
	default:
		return fmt.Errorf("%w: unsupported decision %q", ErrConflictEventInvalid, record.decision)
	}
	return nil
}

func validateConflictEventVersion(version ConflictVersion) error {
	if version.NodeID == "" || len(version.NodeID) > MaxConflictEventNodeBytes {
		return fmt.Errorf("%w: node id is invalid", ErrConflictEventInvalid)
	}
	return nil
}

func sameConflictVersion(left, right ConflictVersion) bool {
	comparison, err := CompareConflictVersions(left, right)
	return err == nil && comparison == 0
}

// ReadAfter returns at most limit events with sequence greater than after.
// A history gap is reported when retention already evicted a required event.
func (log *ConflictEventLog) ReadAfter(after uint64, limit int) (ConflictEventPage, error) {
	if log == nil {
		return ConflictEventPage{}, ErrConflictEventLogNil
	}
	if err := validateConflictEventLimit(limit); err != nil {
		return ConflictEventPage{}, err
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return ConflictEventPage{}, ErrConflictEventLogClosed
	}
	return log.readAfterLocked(after, limit)
}

func validateConflictEventLimit(limit int) error {
	if limit < 1 || limit > MaxConflictEventReplayBatch {
		return fmt.Errorf("%w: limit must be 1..%d", ErrConflictEventLimitInvalid, MaxConflictEventReplayBatch)
	}
	return nil
}

func (log *ConflictEventLog) readAfterLocked(after uint64, limit int) (ConflictEventPage, error) {
	page := ConflictEventPage{NextSequence: after}
	if log.count == 0 {
		return page, nil
	}
	oldest := log.events[log.start].sequence
	newest := log.nextSequence - 1
	page.OldestSequence = oldest
	page.NewestSequence = newest
	if after < oldest-1 {
		return ConflictEventPage{}, fmt.Errorf("%w: after=%d oldest=%d", ErrConflictEventHistoryGap, after, oldest)
	}
	if after >= newest {
		return page, nil
	}
	first := after + 1
	available := newest - after
	if available > uint64(limit) {
		available = uint64(limit)
	}
	page.Events = make([]ConflictEvent, 0, int(available))
	for sequence := first; sequence < first+available; sequence++ {
		offset := int(sequence - oldest)
		index := (log.start + offset) % log.capacity
		page.Events = append(page.Events, log.events[index].public())
	}
	page.NextSequence = page.Events[len(page.Events)-1].Sequence
	return page, nil
}

// WaitAfter blocks without allocating a per-waiter goroutine until a retained
// event is available, the context ends, or the log closes.
func (log *ConflictEventLog) WaitAfter(ctx context.Context, after uint64, limit int) (ConflictEventPage, error) {
	if log == nil {
		return ConflictEventPage{}, ErrConflictEventLogNil
	}
	if ctx == nil {
		return ConflictEventPage{}, ErrConflictEventContextRequired
	}
	if err := validateConflictEventLimit(limit); err != nil {
		return ConflictEventPage{}, err
	}
	for {
		log.mu.Lock()
		if log.closed {
			log.mu.Unlock()
			return ConflictEventPage{}, ErrConflictEventLogClosed
		}
		page, err := log.readAfterLocked(after, limit)
		if err != nil || len(page.Events) != 0 {
			log.mu.Unlock()
			return page, err
		}
		notify := log.notify
		log.mu.Unlock()
		select {
		case <-ctx.Done():
			return ConflictEventPage{}, ctx.Err()
		case <-notify:
		}
	}
}

// Snapshot returns the retained events in sequence order.
func (log *ConflictEventLog) Snapshot() ([]ConflictEvent, error) {
	if log == nil {
		return nil, ErrConflictEventLogNil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return nil, ErrConflictEventLogClosed
	}
	page := make([]ConflictEvent, 0, log.count)
	for index := 0; index < log.count; index++ {
		page = append(page, log.events[(log.start+index)%log.capacity].public())
	}
	return page, nil
}

// Close wakes waiters and rejects future writes. All successful durable
// Record calls are already synced, so Close has no pending flush work.
func (log *ConflictEventLog) Close() error {
	if log == nil {
		return ErrConflictEventLogNil
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.closed {
		return nil
	}
	log.closed = true
	close(log.notify)
	return nil
}

func (log *ConflictEventLog) insertLocked(record conflictEventRecord) {
	if log.count < log.capacity {
		index := (log.start + log.count) % log.capacity
		log.events[index] = record
		log.count++
		return
	}
	log.events[log.start] = record
	log.start = (log.start + 1) % log.capacity
}

func (log *ConflictEventLog) recordsLocked() []conflictEventRecord {
	records := make([]conflictEventRecord, 0, log.count)
	for index := 0; index < log.count; index++ {
		records = append(records, log.events[(log.start+index)%log.capacity])
	}
	return records
}

func (record conflictEventRecord) public() ConflictEvent {
	event := ConflictEvent{
		Sequence:       record.sequence,
		Space:          record.space,
		KeyFingerprint: hex.EncodeToString(record.keyFingerprint[:]),
		Left:           record.left,
		Right:          record.right,
		Decision:       record.decision,
	}
	if record.hasWinner {
		winner := record.winner
		event.Winner = &winner
	}
	return event
}

func encodeConflictEventRecord(record conflictEventRecord) ([]byte, error) {
	if record.sequence == 0 {
		return nil, fmt.Errorf("%w: sequence is required", ErrConflictEventInvalid)
	}
	if err := validateConflictEventRecord(record); err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, 1024)
	encoded = appendUint64(encoded, record.sequence)
	var err error
	encoded, err = appendBoundedString(encoded, record.space, MaxConflictEventSpaceBytes)
	if err != nil {
		return nil, err
	}
	encoded = append(encoded, record.keyFingerprint[:]...)
	encoded, err = appendConflictEventVersion(encoded, record.left)
	if err != nil {
		return nil, err
	}
	encoded, err = appendConflictEventVersion(encoded, record.right)
	if err != nil {
		return nil, err
	}
	encoded = append(encoded, conflictEventDecisionCode(record.decision))
	if record.hasWinner {
		encoded = append(encoded, 1)
		encoded, err = appendConflictEventVersion(encoded, record.winner)
		if err != nil {
			return nil, err
		}
	} else {
		encoded = append(encoded, 0)
	}
	if len(encoded) > maxConflictEventPayloadBytes {
		return nil, fmt.Errorf("%w: encoded payload exceeds %d bytes", ErrConflictEventInvalid, maxConflictEventPayloadBytes)
	}
	return encoded, nil
}

func appendConflictEventVersion(encoded []byte, version ConflictVersion) ([]byte, error) {
	encoded = appendInt64(encoded, version.Timestamp)
	encoded = appendUint64(encoded, version.Sequence)
	return appendBoundedString(encoded, version.NodeID, MaxConflictEventNodeBytes)
}

func appendBoundedString(encoded []byte, value string, maxBytes int) ([]byte, error) {
	if len(value) > maxBytes || len(value) > int(^uint16(0)) {
		return nil, fmt.Errorf("%w: string exceeds %d bytes", ErrConflictEventInvalid, maxBytes)
	}
	var size [2]byte
	binary.BigEndian.PutUint16(size[:], uint16(len(value)))
	encoded = append(encoded, size[:]...)
	return append(encoded, value...), nil
}

func appendUint64(encoded []byte, value uint64) []byte {
	var bytes [8]byte
	binary.BigEndian.PutUint64(bytes[:], value)
	return append(encoded, bytes[:]...)
}

func appendInt64(encoded []byte, value int64) []byte {
	return appendUint64(encoded, uint64(value))
}

func conflictEventDecisionCode(decision ConflictEventDecision) byte {
	switch decision {
	case ConflictEventDecisionLeftWins:
		return 1
	case ConflictEventDecisionRightWins:
		return 2
	case ConflictEventDecisionEqual:
		return 3
	case ConflictEventDecisionRejected:
		return 4
	default:
		return 0
	}
}

func conflictEventDecisionFromCode(code byte) (ConflictEventDecision, error) {
	switch code {
	case 1:
		return ConflictEventDecisionLeftWins, nil
	case 2:
		return ConflictEventDecisionRightWins, nil
	case 3:
		return ConflictEventDecisionEqual, nil
	case 4:
		return ConflictEventDecisionRejected, nil
	default:
		return "", fmt.Errorf("%w: decision code %d", ErrConflictEventFileInvalid, code)
	}
}

func decodeConflictEventFile(data []byte) (int, []conflictEventRecord, error) {
	if len(data) < conflictEventFileHeaderBytes || !bytes.Equal(data[:4], conflictEventFileMagic[:]) {
		return 0, nil, ErrConflictEventFileInvalid
	}
	if binary.BigEndian.Uint16(data[4:6]) != conflictEventFileVersion {
		return 0, nil, ErrConflictEventFileInvalid
	}
	capacity := int(binary.BigEndian.Uint32(data[6:10]))
	if capacity < 1 || capacity > MaxConflictEventLogCapacity {
		return 0, nil, ErrConflictEventFileInvalid
	}
	records := make([]conflictEventRecord, 0, capacity)
	offset := conflictEventFileHeaderBytes
	for offset < len(data) {
		if len(data)-offset < conflictEventFrameOverheadBytes {
			return 0, nil, ErrConflictEventFileInvalid
		}
		payloadBytes := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		if payloadBytes < 1 || payloadBytes > maxConflictEventPayloadBytes || len(data)-offset < payloadBytes+4 {
			return 0, nil, ErrConflictEventFileInvalid
		}
		payload := data[offset : offset+payloadBytes]
		offset += payloadBytes
		checksum := binary.BigEndian.Uint32(data[offset : offset+4])
		offset += 4
		if crc32.Checksum(payload, conflictEventCRC32) != checksum {
			return 0, nil, ErrConflictEventFileInvalid
		}
		record, err := decodeConflictEventRecord(payload)
		if err != nil {
			return 0, nil, err
		}
		if len(records) != 0 && record.sequence != records[len(records)-1].sequence+1 {
			return 0, nil, ErrConflictEventFileInvalid
		}
		records = append(records, record)
		if len(records) > capacity {
			return 0, nil, ErrConflictEventFileInvalid
		}
	}
	return capacity, records, nil
}

type conflictEventReader struct {
	data []byte
	off  int
}

func decodeConflictEventRecord(payload []byte) (conflictEventRecord, error) {
	if len(payload) == 0 || len(payload) > maxConflictEventPayloadBytes {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	reader := conflictEventReader{data: payload}
	sequence, ok := reader.uint64()
	if !ok || sequence == 0 {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	space, ok := reader.string(MaxConflictEventSpaceBytes)
	if !ok {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	keyFingerprint, ok := reader.bytes(sha256.Size)
	if !ok {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	left, ok := reader.version(MaxConflictEventNodeBytes)
	if !ok {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	right, ok := reader.version(MaxConflictEventNodeBytes)
	if !ok {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	decisionCode, ok := reader.byte()
	if !ok {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	decision, err := conflictEventDecisionFromCode(decisionCode)
	if err != nil {
		return conflictEventRecord{}, err
	}
	hasWinnerByte, ok := reader.byte()
	if !ok || hasWinnerByte > 1 {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	record := conflictEventRecord{
		sequence:  sequence,
		space:     space,
		left:      left,
		right:     right,
		decision:  decision,
		hasWinner: hasWinnerByte == 1,
	}
	if record.hasWinner {
		record.winner, ok = reader.version(MaxConflictEventNodeBytes)
		if !ok {
			return conflictEventRecord{}, ErrConflictEventFileInvalid
		}
	}
	copy(record.keyFingerprint[:], keyFingerprint)
	if reader.off != len(reader.data) {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	if err := validateConflictEventRecord(record); err != nil {
		return conflictEventRecord{}, ErrConflictEventFileInvalid
	}
	return record, nil
}

func (reader *conflictEventReader) bytes(length int) ([]byte, bool) {
	if length < 0 || reader.off > len(reader.data)-length {
		return nil, false
	}
	value := reader.data[reader.off : reader.off+length]
	reader.off += length
	return value, true
}

func (reader *conflictEventReader) byte() (byte, bool) {
	value, ok := reader.bytes(1)
	if !ok {
		return 0, false
	}
	return value[0], true
}

func (reader *conflictEventReader) uint64() (uint64, bool) {
	value, ok := reader.bytes(8)
	if !ok {
		return 0, false
	}
	return binary.BigEndian.Uint64(value), true
}

func (reader *conflictEventReader) int64() (int64, bool) {
	value, ok := reader.uint64()
	return int64(value), ok
}

func (reader *conflictEventReader) string(maxBytes int) (string, bool) {
	length, ok := reader.bytes(2)
	if !ok {
		return "", false
	}
	size := int(binary.BigEndian.Uint16(length))
	if size > maxBytes {
		return "", false
	}
	value, ok := reader.bytes(size)
	return string(value), ok
}

func (reader *conflictEventReader) version(maxNodeBytes int) (ConflictVersion, bool) {
	timestamp, ok := reader.int64()
	if !ok {
		return ConflictVersion{}, false
	}
	sequence, ok := reader.uint64()
	if !ok {
		return ConflictVersion{}, false
	}
	nodeID, ok := reader.string(maxNodeBytes)
	if !ok || nodeID == "" {
		return ConflictVersion{}, false
	}
	return ConflictVersion{Timestamp: timestamp, NodeID: nodeID, Sequence: sequence}, true
}

func rewriteConflictEventFile(path string, capacity int, records []conflictEventRecord) error {
	if path == "" || capacity < 1 || len(records) > capacity {
		return ErrConflictEventFileInvalid
	}
	encoded := make([]byte, conflictEventFileHeaderBytes, conflictEventFileHeaderBytes+len(records)*256)
	copy(encoded[:4], conflictEventFileMagic[:])
	binary.BigEndian.PutUint16(encoded[4:6], conflictEventFileVersion)
	binary.BigEndian.PutUint32(encoded[6:10], uint32(capacity))
	for _, record := range records {
		frame, err := encodeConflictEventFrame(record)
		if err != nil {
			return err
		}
		encoded = append(encoded, frame...)
	}
	directory := filepath.Dir(path)
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	defer func() {
		_ = os.Remove(temporaryPath)
	}()
	if err := writeConflictEventBytes(temporary, encoded); err != nil {
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
	if err := os.Rename(temporaryPath, path); err != nil {
		return err
	}
	return nil
}

func encodeConflictEventFrame(record conflictEventRecord) ([]byte, error) {
	payload, err := encodeConflictEventRecord(record)
	if err != nil {
		return nil, err
	}
	frame := make([]byte, 4+len(payload)+4)
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	binary.BigEndian.PutUint32(frame[4+len(payload):], crc32.Checksum(payload, conflictEventCRC32))
	return frame, nil
}

func appendConflictEventFile(path string, record conflictEventRecord) error {
	frame, err := encodeConflictEventFrame(record)
	if err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	if err := writeConflictEventBytes(file, frame); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func writeConflictEventBytes(file *os.File, data []byte) error {
	for len(data) != 0 {
		written, err := file.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
