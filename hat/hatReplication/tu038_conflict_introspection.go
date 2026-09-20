package hatReplication

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"strings"
	"sync"
)

var (
	ErrConflictIntrospectionNil            = errors.New("hatriecache: conflict introspection log is nil")
	ErrConflictIntrospectionOptionsInvalid = errors.New("hatriecache: conflict introspection options are invalid")
	ErrConflictIntrospectionSecretRequired = errors.New("hatriecache: conflict introspection key hash secret is required")
	ErrConflictIntrospectionReadLimit      = errors.New("hatriecache: conflict introspection read limit is invalid")
	ErrConflictIntrospectionSpaceRequired  = errors.New("hatriecache: conflict introspection space is required")
	ErrConflictIntrospectionKeyRequired    = errors.New("hatriecache: conflict introspection key is required")
	ErrConflictIntrospectionKeyTooLarge    = errors.New("hatriecache: conflict introspection key is too large")
	ErrConflictIntrospectionSnapshot       = errors.New("hatriecache: conflict introspection snapshot is invalid")
	ErrConflictIntrospectionChecksum       = errors.New("hatriecache: conflict introspection snapshot checksum mismatch")
	ErrConflictIntrospectionSequence       = errors.New("hatriecache: conflict introspection sequence exhausted")
)

const (
	DefaultConflictIntrospectionMaxEvents = 1024
	MaxConflictIntrospectionMaxEvents     = 65536
	DefaultConflictIntrospectionReadLimit = 256
	MaxConflictIntrospectionReadLimit     = 4096
	MinConflictIntrospectionSecretBytes   = 16
	MaxConflictIntrospectionSecretBytes   = 4096
	MaxConflictIntrospectionSpaceBytes    = 256
	MaxConflictIntrospectionNodeBytes     = 256
	MaxConflictIntrospectionKeyBytes      = 1 << 20
	MaxConflictIntrospectionSnapshotBytes = 16 << 20

	conflictIntrospectionMagic = "HCIS1"
)

var conflictIntrospectionChecksumTable = crc32.MakeTable(crc32.Castagnoli)

// ConflictIntrospectionOptions bounds the optional conflict event stream.
// KeyHashSecret is required so retained key identifiers are not predictable
// hashes of application keys.
type ConflictIntrospectionOptions struct {
	MaxEvents     int
	KeyHashSecret []byte
}

// ConflictIntrospectionDecision describes how a conflict policy resolved two
// versions. The event contains versions and source IDs, but never the raw key.
type ConflictIntrospectionDecision uint8

const (
	ConflictIntrospectionDecisionUnknown ConflictIntrospectionDecision = iota
	ConflictIntrospectionDecisionLeft
	ConflictIntrospectionDecisionRight
	ConflictIntrospectionDecisionEqual
	ConflictIntrospectionDecisionRejected
	ConflictIntrospectionDecisionError
)

// ConflictIntrospectionEvent is a detached redacted conflict record.
type ConflictIntrospectionEvent struct {
	Sequence  uint64
	Space     string
	KeyDigest [16]byte
	Left      ConflictVersion
	Right     ConflictVersion
	Winner    ConflictVersion
	Policy    ConflictPolicyMode
	Decision  ConflictIntrospectionDecision
}

// KeyHashHex returns the fixed-size HMAC digest in a log-friendly form.
func (event ConflictIntrospectionEvent) KeyHashHex() string {
	return hex.EncodeToString(event.KeyDigest[:])
}

// ConflictIntrospectionPage is a bounded cursor read. Call ReadSince again
// with NextSequence to continue. Gap is true when the requested cursor was
// evicted by the bounded ring.
type ConflictIntrospectionPage struct {
	Events         []ConflictIntrospectionEvent
	NextSequence   uint64
	OldestSequence uint64
	LatestSequence uint64
	Gap            bool
}

// ConflictIntrospectionStats is a point-in-time stream health view.
type ConflictIntrospectionStats struct {
	RetainedEvents int
	MaxEvents      int
	DroppedEvents  uint64
	NextSequence   uint64
	OldestSequence uint64
}

// ConflictIntrospectionLog is a concurrency-safe bounded redacted event
// stream. It allocates only when explicitly constructed and never stores raw
// conflict keys.
type ConflictIntrospectionLog struct {
	mu           sync.RWMutex
	secret       []byte
	maxEvents    int
	events       []ConflictIntrospectionEvent
	head         int
	count        int
	nextSequence uint64
	dropped      uint64
	hasherPool   sync.Pool
}

// NewConflictIntrospectionLog creates an opt-in bounded conflict stream.
func NewConflictIntrospectionLog(options ConflictIntrospectionOptions) (*ConflictIntrospectionLog, error) {
	maxEvents := options.MaxEvents
	if maxEvents == 0 {
		maxEvents = DefaultConflictIntrospectionMaxEvents
	}
	if maxEvents < 0 || maxEvents > MaxConflictIntrospectionMaxEvents {
		return nil, fmt.Errorf("%w: MaxEvents must be 0..%d", ErrConflictIntrospectionOptionsInvalid, MaxConflictIntrospectionMaxEvents)
	}
	if len(options.KeyHashSecret) < MinConflictIntrospectionSecretBytes {
		return nil, ErrConflictIntrospectionSecretRequired
	}
	if len(options.KeyHashSecret) > MaxConflictIntrospectionSecretBytes {
		return nil, fmt.Errorf("%w: key hash secret exceeds %d bytes", ErrConflictIntrospectionOptionsInvalid, MaxConflictIntrospectionSecretBytes)
	}
	log := &ConflictIntrospectionLog{
		secret:       append([]byte(nil), options.KeyHashSecret...),
		maxEvents:    maxEvents,
		events:       make([]ConflictIntrospectionEvent, maxEvents),
		nextSequence: 1,
	}
	log.hasherPool.New = func() any {
		return hmac.New(sha256.New, log.secret)
	}
	return log, nil
}

// Record appends one already-resolved conflict. It is safe to call from a
// replication callback; any raw key is reduced to a keyed 128-bit digest.
func (log *ConflictIntrospectionLog) Record(space, key string, left, right ConflictVersion, policy ConflictPolicyMode, winner ConflictVersion, decision ConflictIntrospectionDecision) error {
	if log == nil {
		return ErrConflictIntrospectionNil
	}
	if log.maxEvents <= 0 {
		return ErrConflictIntrospectionOptionsInvalid
	}
	space = strings.TrimSpace(space)
	if space == "" {
		return ErrConflictIntrospectionSpaceRequired
	}
	if len(space) > MaxConflictIntrospectionSpaceBytes {
		return fmt.Errorf("%w: space exceeds %d bytes", ErrConflictIntrospectionOptionsInvalid, MaxConflictIntrospectionSpaceBytes)
	}
	if len(key) == 0 {
		return ErrConflictIntrospectionKeyRequired
	}
	if len(key) > MaxConflictIntrospectionKeyBytes {
		return ErrConflictIntrospectionKeyTooLarge
	}
	if !validConflictPolicyMode(policy) || !validConflictIntrospectionDecision(decision) {
		return fmt.Errorf("%w: invalid policy or decision", ErrConflictIntrospectionSnapshot)
	}
	if err := validateIntrospectionVersion(left); err != nil {
		return err
	}
	if err := validateIntrospectionVersion(right); err != nil {
		return err
	}
	if decision == ConflictIntrospectionDecisionLeft || decision == ConflictIntrospectionDecisionRight || decision == ConflictIntrospectionDecisionEqual {
		if err := validateIntrospectionVersion(winner); err != nil {
			return err
		}
	} else if winner != (ConflictVersion{}) {
		return fmt.Errorf("%w: rejected or failed event must not contain a winner", ErrConflictIntrospectionSnapshot)
	}

	event := ConflictIntrospectionEvent{
		Space:     space,
		KeyDigest: log.digestKey(key),
		Left:      left,
		Right:     right,
		Winner:    winner,
		Policy:    policy,
		Decision:  decision,
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if len(log.events) != log.maxEvents {
		return ErrConflictIntrospectionOptionsInvalid
	}
	if log.nextSequence == ^uint64(0) {
		return ErrConflictIntrospectionSequence
	}
	event.Sequence = log.nextSequence
	log.nextSequence++
	if log.count < log.maxEvents {
		index := (log.head + log.count) % log.maxEvents
		log.events[index] = event
		log.count++
		return nil
	}
	log.events[log.head] = event
	log.head = (log.head + 1) % log.maxEvents
	log.dropped++
	return nil
}

// ReadSince returns retained events after sequence. A zero limit uses the
// default page size; a gap tells the consumer that older events were evicted.
func (log *ConflictIntrospectionLog) ReadSince(sequence uint64, limit int) (ConflictIntrospectionPage, error) {
	if log == nil {
		return ConflictIntrospectionPage{}, ErrConflictIntrospectionNil
	}
	if limit == 0 {
		limit = DefaultConflictIntrospectionReadLimit
	}
	if limit < 0 || limit > MaxConflictIntrospectionReadLimit {
		return ConflictIntrospectionPage{}, ErrConflictIntrospectionReadLimit
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	page := ConflictIntrospectionPage{
		NextSequence:   log.nextSequence,
		OldestSequence: log.nextSequence - uint64(log.count),
	}
	if log.count == 0 {
		return page, nil
	}
	page.LatestSequence = log.nextSequence - 1
	if sequence >= page.LatestSequence {
		return page, nil
	}
	start := sequence + 1
	if start < page.OldestSequence {
		page.Gap = true
		start = page.OldestSequence
	}
	available := page.LatestSequence - start + 1
	if available > uint64(limit) {
		available = uint64(limit)
	}
	page.Events = make([]ConflictIntrospectionEvent, 0, int(available))
	for current := start; current < start+available; current++ {
		offset := current - page.OldestSequence
		index := (uint64(log.head) + offset) % uint64(log.maxEvents)
		page.Events = append(page.Events, cloneIntrospectionEvent(log.events[index]))
	}
	if len(page.Events) > 0 {
		page.NextSequence = start + uint64(len(page.Events))
	}
	return page, nil
}

// Stats returns a detached bounded-stream health view.
func (log *ConflictIntrospectionLog) Stats() ConflictIntrospectionStats {
	if log == nil {
		return ConflictIntrospectionStats{}
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	return ConflictIntrospectionStats{
		RetainedEvents: log.count,
		MaxEvents:      log.maxEvents,
		DroppedEvents:  log.dropped,
		NextSequence:   log.nextSequence,
		OldestSequence: log.nextSequence - uint64(log.count),
	}
}

// MarshalBinary returns a deterministic CRC-protected snapshot. The secret
// and all raw keys are intentionally excluded; a restored log only needs a
// configured secret to accept future records.
func (log *ConflictIntrospectionLog) MarshalBinary() ([]byte, error) {
	if log == nil {
		return nil, ErrConflictIntrospectionNil
	}
	log.mu.RLock()
	defer log.mu.RUnlock()
	data := make([]byte, 0, len(conflictIntrospectionMagic)+64+log.count*128)
	data = append(data, conflictIntrospectionMagic...)
	data = appendUint64(data, log.nextSequence)
	data = appendUint64(data, log.dropped)
	data = appendUvarint(data, uint64(log.count))
	for offset := 0; offset < log.count; offset++ {
		index := (log.head + offset) % log.maxEvents
		data = appendIntrospectionEvent(data, log.events[index])
		if len(data)+4 > MaxConflictIntrospectionSnapshotBytes {
			return nil, fmt.Errorf("%w: snapshot exceeds %d bytes", ErrConflictIntrospectionSnapshot, MaxConflictIntrospectionSnapshotBytes)
		}
	}
	checksum := crc32.Checksum(data, conflictIntrospectionChecksumTable)
	data = appendUint32(data, checksum)
	return data, nil
}

// UnmarshalBinary replaces retained events with a validated snapshot.
func (log *ConflictIntrospectionLog) UnmarshalBinary(data []byte) error {
	if log == nil {
		return ErrConflictIntrospectionNil
	}
	if len(data) < len(conflictIntrospectionMagic)+8+8+1+4 || len(data) > MaxConflictIntrospectionSnapshotBytes {
		return ErrConflictIntrospectionSnapshot
	}
	if !bytes.Equal(data[:len(conflictIntrospectionMagic)], []byte(conflictIntrospectionMagic)) {
		return ErrConflictIntrospectionSnapshot
	}
	wantChecksum := binary.LittleEndian.Uint32(data[len(data)-4:])
	gotChecksum := crc32.Checksum(data[:len(data)-4], conflictIntrospectionChecksumTable)
	if wantChecksum != gotChecksum {
		return ErrConflictIntrospectionChecksum
	}
	index := len(conflictIntrospectionMagic)
	nextSequence, ok := readUint64(data[:len(data)-4], &index)
	if !ok || nextSequence == 0 {
		return ErrConflictIntrospectionSnapshot
	}
	dropped, ok := readUint64(data[:len(data)-4], &index)
	if !ok {
		return ErrConflictIntrospectionSnapshot
	}
	count, ok := readUvarint(data[:len(data)-4], &index)
	if !ok {
		return ErrConflictIntrospectionSnapshot
	}
	log.mu.RLock()
	maxEvents := log.maxEvents
	log.mu.RUnlock()
	if count > uint64(maxEvents) {
		return fmt.Errorf("%w: snapshot has %d events, max is %d", ErrConflictIntrospectionSnapshot, count, maxEvents)
	}
	events := make([]ConflictIntrospectionEvent, int(count))
	for eventIndex := range events {
		event, valid := readIntrospectionEvent(data[:len(data)-4], &index)
		if !valid {
			return ErrConflictIntrospectionSnapshot
		}
		if eventIndex > 0 && event.Sequence != events[eventIndex-1].Sequence+1 {
			return ErrConflictIntrospectionSnapshot
		}
		events[eventIndex] = event
	}
	if index != len(data)-4 {
		return ErrConflictIntrospectionSnapshot
	}
	if len(events) > 0 && events[len(events)-1].Sequence+1 != nextSequence {
		return ErrConflictIntrospectionSnapshot
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	log.events = make([]ConflictIntrospectionEvent, log.maxEvents)
	copy(log.events, events)
	log.head = 0
	log.count = len(events)
	log.nextSequence = nextSequence
	log.dropped = dropped
	return nil
}

func (log *ConflictIntrospectionLog) digestKey(key string) [16]byte {
	hasher, _ := log.hasherPool.Get().(hash.Hash)
	if hasher == nil {
		hasher = hmac.New(sha256.New, log.secret)
	}
	hasher.Reset()
	_, _ = hasher.Write([]byte(key))
	var digest [sha256.Size]byte
	hasher.Sum(digest[:0])
	log.hasherPool.Put(hasher)
	var result [16]byte
	copy(result[:], digest[:])
	return result
}

func (log *ConflictIntrospectionLog) recordResolution(space, key string, left, right ConflictVersion, policy ConflictPolicyMode, winner ConflictVersion, resolveErr error) {
	decision, storedWinner := introspectionDecision(winner, left, right, resolveErr)
	_ = log.Record(space, key, left, right, policy, storedWinner, decision)
}

func introspectionDecision(winner, left, right ConflictVersion, resolveErr error) (ConflictIntrospectionDecision, ConflictVersion) {
	if resolveErr != nil {
		if errors.Is(resolveErr, ErrConflictRejected) {
			return ConflictIntrospectionDecisionRejected, ConflictVersion{}
		}
		return ConflictIntrospectionDecisionError, ConflictVersion{}
	}
	if winner == left && winner == right {
		return ConflictIntrospectionDecisionEqual, winner
	}
	if winner == left {
		return ConflictIntrospectionDecisionLeft, winner
	}
	if winner == right {
		return ConflictIntrospectionDecisionRight, winner
	}
	return ConflictIntrospectionDecisionError, ConflictVersion{}
}

func validConflictPolicyMode(policy ConflictPolicyMode) bool {
	return policy <= ConflictPolicyReject
}

func validConflictIntrospectionDecision(decision ConflictIntrospectionDecision) bool {
	return decision >= ConflictIntrospectionDecisionLeft && decision <= ConflictIntrospectionDecisionError
}

func validateIntrospectionVersion(version ConflictVersion) error {
	if version.NodeID == "" {
		return ErrConflictVersionInvalid
	}
	if len(version.NodeID) > MaxConflictIntrospectionNodeBytes {
		return fmt.Errorf("%w: node ID exceeds %d bytes", ErrConflictIntrospectionSnapshot, MaxConflictIntrospectionNodeBytes)
	}
	return nil
}

func cloneIntrospectionVersion(version ConflictVersion) ConflictVersion {
	return version
}

func cloneIntrospectionEvent(event ConflictIntrospectionEvent) ConflictIntrospectionEvent {
	return event
}

func appendIntrospectionEvent(data []byte, event ConflictIntrospectionEvent) []byte {
	data = appendUint64(data, event.Sequence)
	data = append(data, byte(event.Policy), byte(event.Decision))
	data = append(data, event.KeyDigest[:]...)
	data = appendBoundedString(data, event.Space)
	data = appendIntrospectionVersion(data, event.Left)
	data = appendIntrospectionVersion(data, event.Right)
	data = appendIntrospectionVersion(data, event.Winner)
	return data
}

func appendIntrospectionVersion(data []byte, version ConflictVersion) []byte {
	data = appendUint64(data, uint64(version.Timestamp))
	data = appendUint64(data, version.Sequence)
	return appendBoundedString(data, version.NodeID)
}

func appendBoundedString(data []byte, value string) []byte {
	data = appendUvarint(data, uint64(len(value)))
	return append(data, value...)
}

func appendUvarint(data []byte, value uint64) []byte {
	var buffer [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(buffer[:], value)
	return append(data, buffer[:length]...)
}

func appendUint32(data []byte, value uint32) []byte {
	var buffer [4]byte
	binary.LittleEndian.PutUint32(buffer[:], value)
	return append(data, buffer[:]...)
}

func appendUint64(data []byte, value uint64) []byte {
	var buffer [8]byte
	binary.LittleEndian.PutUint64(buffer[:], value)
	return append(data, buffer[:]...)
}

func readIntrospectionEvent(data []byte, index *int) (ConflictIntrospectionEvent, bool) {
	sequence, ok := readUint64(data, index)
	if !ok || *index+2+16 > len(data) {
		return ConflictIntrospectionEvent{}, false
	}
	policy := ConflictPolicyMode(data[*index])
	decision := ConflictIntrospectionDecision(data[*index+1])
	*index += 2
	if !validConflictPolicyMode(policy) || !validConflictIntrospectionDecision(decision) {
		return ConflictIntrospectionEvent{}, false
	}
	var digest [16]byte
	copy(digest[:], data[*index:*index+16])
	*index += 16
	space, ok := readBoundedString(data, index, MaxConflictIntrospectionSpaceBytes)
	if !ok {
		return ConflictIntrospectionEvent{}, false
	}
	left, ok := readIntrospectionVersion(data, index)
	if !ok || validateIntrospectionVersion(left) != nil {
		return ConflictIntrospectionEvent{}, false
	}
	right, ok := readIntrospectionVersion(data, index)
	if !ok || validateIntrospectionVersion(right) != nil {
		return ConflictIntrospectionEvent{}, false
	}
	winner, ok := readIntrospectionVersion(data, index)
	if !ok {
		return ConflictIntrospectionEvent{}, false
	}
	if decision == ConflictIntrospectionDecisionLeft || decision == ConflictIntrospectionDecisionRight || decision == ConflictIntrospectionDecisionEqual {
		if validateIntrospectionVersion(winner) != nil {
			return ConflictIntrospectionEvent{}, false
		}
	} else if winner != (ConflictVersion{}) {
		return ConflictIntrospectionEvent{}, false
	}
	return ConflictIntrospectionEvent{
		Sequence:  sequence,
		Space:     space,
		KeyDigest: digest,
		Left:      left,
		Right:     right,
		Winner:    winner,
		Policy:    policy,
		Decision:  decision,
	}, true
}

func readIntrospectionVersion(data []byte, index *int) (ConflictVersion, bool) {
	timestamp, ok := readUint64(data, index)
	if !ok {
		return ConflictVersion{}, false
	}
	sequence, ok := readUint64(data, index)
	if !ok {
		return ConflictVersion{}, false
	}
	nodeID, ok := readBoundedString(data, index, MaxConflictIntrospectionNodeBytes)
	if !ok {
		return ConflictVersion{}, false
	}
	version := ConflictVersion{Timestamp: int64(timestamp), NodeID: nodeID, Sequence: sequence}
	if version.NodeID != "" && len(version.NodeID) > MaxConflictIntrospectionNodeBytes {
		return ConflictVersion{}, false
	}
	return version, true
}

func readBoundedString(data []byte, index *int, max int) (string, bool) {
	length, ok := readUvarint(data, index)
	if !ok || length > uint64(max) || length > uint64(len(data)-*index) {
		return "", false
	}
	start := *index
	*index += int(length)
	return string(data[start:*index]), true
}

func readUvarint(data []byte, index *int) (uint64, bool) {
	value, length := binary.Uvarint(data[*index:])
	if length <= 0 {
		return 0, false
	}
	*index += length
	return value, true
}

func readUint64(data []byte, index *int) (uint64, bool) {
	if *index+8 > len(data) {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(data[*index : *index+8])
	*index += 8
	return value, true
}
