package hatReplication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	maxChangefeedExactlyOnceIdentityBytes = 256
	maxChangefeedExactlyOnceSnapshotBytes = 1024
	changefeedExactlyOnceSnapshotVersion  = 1
	changefeedExactlyOnceFlagCommitted    = 1 << 0
	changefeedExactlyOnceFlagPending      = 1 << 1
)

var (
	ErrChangefeedExactlyOnceInvalid           = errors.New("invalid exactly-once changefeed state")
	ErrChangefeedExactlyOncePending           = errors.New("exactly-once changefeed batch is pending")
	ErrChangefeedExactlyOnceGap               = errors.New("exactly-once changefeed offset gap")
	ErrChangefeedExactlyOnceOverlap           = errors.New("exactly-once changefeed offset overlap")
	ErrChangefeedExactlyOnceBatchConflict     = errors.New("exactly-once changefeed batch conflict")
	ErrChangefeedExactlyOnceNoPending         = errors.New("exactly-once changefeed has no pending batch")
	ErrChangefeedExactlyOnceFrontierRegressed = errors.New("exactly-once changefeed frontier regressed")
	ErrChangefeedExactlyOnceOffsetExhausted   = errors.New("exactly-once changefeed offset exhausted")
	ErrChangefeedExactlyOnceSnapshotInvalid   = errors.New("invalid exactly-once changefeed snapshot")
)

// ChangefeedExactlyOnceBatch identifies one source batch and its inclusive
// source-offset range.
type ChangefeedExactlyOnceBatch struct {
	ID          string `json:"id"`
	StartOffset uint64 `json:"start_offset"`
	EndOffset   uint64 `json:"end_offset"`
}

// ChangefeedExactlyOnceBatchAction describes what the consumer should do with
// a batch after a restart or duplicate delivery.
type ChangefeedExactlyOnceBatchAction uint8

const (
	ChangefeedExactlyOnceApply ChangefeedExactlyOnceBatchAction = iota + 1
	ChangefeedExactlyOnceResume
	ChangefeedExactlyOnceSkip
)

// ChangefeedExactlyOnceDecision is the result of beginning a source batch.
type ChangefeedExactlyOnceDecision struct {
	Action   ChangefeedExactlyOnceBatchAction `json:"action"`
	Batch    ChangefeedExactlyOnceBatch       `json:"batch"`
	Snapshot ChangefeedExactlyOnceSnapshot    `json:"snapshot"`
}

// ChangefeedExactlyOnceSnapshot is the durable state required to resume a
// source without replaying a committed batch.
type ChangefeedExactlyOnceSnapshot struct {
	Source               string                      `json:"source"`
	HasCommittedOffset   bool                        `json:"has_committed_offset"`
	CommittedOffset      uint64                      `json:"committed_offset"`
	CommittedFrontier    uint64                      `json:"committed_frontier"`
	LastCommittedBatchID string                      `json:"last_committed_batch_id,omitempty"`
	Pending              *ChangefeedExactlyOnceBatch `json:"pending,omitempty"`
}

// ChangefeedExactlyOnceConsumer tracks one source's committed and in-flight
// batches. The caller must persist this state together with the applied data,
// or make the data mutation idempotent, to obtain exactly-once effects.
type ChangefeedExactlyOnceConsumer struct {
	mu                   sync.RWMutex
	source               string
	hasCommittedOffset   bool
	committedOffset      uint64
	committedFrontier    uint64
	lastCommittedBatchID string
	pending              *ChangefeedExactlyOnceBatch
}

// NewChangefeedExactlyOnceConsumer creates a consumer for source.
func NewChangefeedExactlyOnceConsumer(source string) (*ChangefeedExactlyOnceConsumer, error) {
	if err := validateChangefeedExactlyOnceSource(source); err != nil {
		return nil, err
	}
	return &ChangefeedExactlyOnceConsumer{source: source}, nil
}

// NewChangefeedExactlyOnceConsumerFromSnapshot restores a consumer from a
// validated snapshot.
func NewChangefeedExactlyOnceConsumerFromSnapshot(snapshot ChangefeedExactlyOnceSnapshot) (*ChangefeedExactlyOnceConsumer, error) {
	if err := validateChangefeedExactlyOnceSnapshot(snapshot); err != nil {
		return nil, err
	}
	c, err := NewChangefeedExactlyOnceConsumer(snapshot.Source)
	if err != nil {
		return nil, err
	}
	if err := c.Restore(snapshot); err != nil {
		return nil, err
	}
	return c, nil
}

// BeginBatch records a batch as in-flight, or returns the action required for
// a resumed or already committed delivery.
func (c *ChangefeedExactlyOnceConsumer) BeginBatch(id string, startOffset, endOffset uint64) (ChangefeedExactlyOnceDecision, error) {
	if c == nil {
		return ChangefeedExactlyOnceDecision{}, ErrChangefeedExactlyOnceInvalid
	}
	if err := validateChangefeedExactlyOnceID(id); err != nil {
		return ChangefeedExactlyOnceDecision{}, err
	}
	if endOffset < startOffset {
		return ChangefeedExactlyOnceDecision{}, ErrChangefeedExactlyOnceInvalid
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := validateChangefeedExactlyOnceSource(c.source); err != nil {
		return ChangefeedExactlyOnceDecision{}, err
	}

	batch := ChangefeedExactlyOnceBatch{ID: id, StartOffset: startOffset, EndOffset: endOffset}
	if c.pending != nil {
		if *c.pending == batch {
			return ChangefeedExactlyOnceDecision{
				Action:   ChangefeedExactlyOnceResume,
				Batch:    batch,
				Snapshot: c.snapshotLocked(),
			}, nil
		}
		return ChangefeedExactlyOnceDecision{}, ErrChangefeedExactlyOncePending
	}

	if c.hasCommittedOffset {
		if endOffset <= c.committedOffset {
			return ChangefeedExactlyOnceDecision{
				Action:   ChangefeedExactlyOnceSkip,
				Batch:    batch,
				Snapshot: c.snapshotLocked(),
			}, nil
		}
		if startOffset <= c.committedOffset {
			return ChangefeedExactlyOnceDecision{}, ErrChangefeedExactlyOnceOverlap
		}
		if c.committedOffset == ^uint64(0) {
			return ChangefeedExactlyOnceDecision{}, ErrChangefeedExactlyOnceOffsetExhausted
		}
		if startOffset != c.committedOffset+1 {
			return ChangefeedExactlyOnceDecision{}, ErrChangefeedExactlyOnceGap
		}
	} else if startOffset != 0 {
		return ChangefeedExactlyOnceDecision{}, ErrChangefeedExactlyOnceGap
	}

	c.pending = &batch
	return ChangefeedExactlyOnceDecision{
		Action:   ChangefeedExactlyOnceApply,
		Batch:    batch,
		Snapshot: c.snapshotLocked(),
	}, nil
}

// CommitBatch atomically advances the committed source offset and frontier.
// Repeating the last committed batch ID is idempotent.
func (c *ChangefeedExactlyOnceConsumer) CommitBatch(id string, frontier uint64) (ChangefeedExactlyOnceSnapshot, error) {
	if c == nil {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceInvalid
	}
	if err := validateChangefeedExactlyOnceID(id); err != nil {
		return ChangefeedExactlyOnceSnapshot{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := validateChangefeedExactlyOnceSource(c.source); err != nil {
		return ChangefeedExactlyOnceSnapshot{}, err
	}
	if c.pending == nil {
		if c.hasCommittedOffset && c.lastCommittedBatchID == id {
			return c.snapshotLocked(), nil
		}
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceNoPending
	}
	if c.pending.ID != id {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceBatchConflict
	}
	if c.hasCommittedOffset && frontier < c.committedFrontier {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceFrontierRegressed
	}

	c.hasCommittedOffset = true
	c.committedOffset = c.pending.EndOffset
	c.committedFrontier = frontier
	c.lastCommittedBatchID = c.pending.ID
	c.pending = nil
	return c.snapshotLocked(), nil
}

// AbortBatch discards an in-flight batch so it can be delivered again.
func (c *ChangefeedExactlyOnceConsumer) AbortBatch(id string) (ChangefeedExactlyOnceSnapshot, error) {
	if c == nil {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceInvalid
	}
	if err := validateChangefeedExactlyOnceID(id); err != nil {
		return ChangefeedExactlyOnceSnapshot{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if err := validateChangefeedExactlyOnceSource(c.source); err != nil {
		return ChangefeedExactlyOnceSnapshot{}, err
	}
	if c.pending == nil {
		if c.hasCommittedOffset && c.lastCommittedBatchID == id {
			return c.snapshotLocked(), nil
		}
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceNoPending
	}
	if c.pending.ID != id {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceBatchConflict
	}
	c.pending = nil
	return c.snapshotLocked(), nil
}

// RestartOffset returns the first source offset that is not committed.
func (c *ChangefeedExactlyOnceConsumer) RestartOffset() (uint64, error) {
	if c == nil {
		return 0, ErrChangefeedExactlyOnceInvalid
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if err := validateChangefeedExactlyOnceSource(c.source); err != nil {
		return 0, err
	}
	if !c.hasCommittedOffset {
		return 0, nil
	}
	if c.committedOffset == ^uint64(0) {
		return 0, ErrChangefeedExactlyOnceOffsetExhausted
	}
	return c.committedOffset + 1, nil
}

// Snapshot returns a copy of the current durable state.
func (c *ChangefeedExactlyOnceConsumer) Snapshot() ChangefeedExactlyOnceSnapshot {
	if c == nil {
		return ChangefeedExactlyOnceSnapshot{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.snapshotLocked()
}

// Restore atomically replaces the consumer state with snapshot.
func (c *ChangefeedExactlyOnceConsumer) Restore(snapshot ChangefeedExactlyOnceSnapshot) error {
	if c == nil {
		return ErrChangefeedExactlyOnceInvalid
	}
	if err := validateChangefeedExactlyOnceSnapshot(snapshot); err != nil {
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.source != snapshot.Source {
		return ErrChangefeedExactlyOnceSnapshotInvalid
	}
	c.hasCommittedOffset = snapshot.HasCommittedOffset
	c.committedOffset = snapshot.CommittedOffset
	c.committedFrontier = snapshot.CommittedFrontier
	c.lastCommittedBatchID = snapshot.LastCommittedBatchID
	c.pending = cloneChangefeedExactlyOnceBatch(snapshot.Pending)
	return nil
}

// MarshalBinary encodes the current state in a compact bounded binary format.
func (c *ChangefeedExactlyOnceConsumer) MarshalBinary() ([]byte, error) {
	if c == nil {
		return nil, ErrChangefeedExactlyOnceInvalid
	}
	return c.Snapshot().MarshalBinary()
}

// UnmarshalBinary decodes and atomically restores a binary snapshot.
func (c *ChangefeedExactlyOnceConsumer) UnmarshalBinary(data []byte) error {
	if c == nil {
		return ErrChangefeedExactlyOnceInvalid
	}
	snapshot, err := UnmarshalChangefeedExactlyOnceSnapshot(data)
	if err != nil {
		return err
	}
	return c.Restore(snapshot)
}

// MarshalBinary encodes a snapshot without JSON field names or reflection.
func (s ChangefeedExactlyOnceSnapshot) MarshalBinary() ([]byte, error) {
	if err := validateChangefeedExactlyOnceSnapshot(s); err != nil {
		return nil, err
	}

	var buffer bytes.Buffer
	buffer.Grow(changefeedExactlyOnceSnapshotSize(s))
	buffer.WriteString("cox1")
	buffer.WriteByte(changefeedExactlyOnceSnapshotVersion)
	var flags byte
	if s.HasCommittedOffset {
		flags |= changefeedExactlyOnceFlagCommitted
	}
	if s.Pending != nil {
		flags |= changefeedExactlyOnceFlagPending
	}
	buffer.WriteByte(flags)
	writeChangefeedExactlyOnceString(&buffer, s.Source)
	writeChangefeedExactlyOnceUint64(&buffer, s.CommittedOffset)
	writeChangefeedExactlyOnceUint64(&buffer, s.CommittedFrontier)
	writeChangefeedExactlyOnceString(&buffer, s.LastCommittedBatchID)
	if s.Pending != nil {
		writeChangefeedExactlyOnceString(&buffer, s.Pending.ID)
		writeChangefeedExactlyOnceUint64(&buffer, s.Pending.StartOffset)
		writeChangefeedExactlyOnceUint64(&buffer, s.Pending.EndOffset)
	}
	return buffer.Bytes(), nil
}

// UnmarshalBinary decodes a snapshot into the receiver without changing it
// when decoding or validation fails.
func (s *ChangefeedExactlyOnceSnapshot) UnmarshalBinary(data []byte) error {
	if s == nil {
		return ErrChangefeedExactlyOnceInvalid
	}
	decoded, err := UnmarshalChangefeedExactlyOnceSnapshot(data)
	if err != nil {
		return err
	}
	*s = decoded
	return nil
}

// UnmarshalChangefeedExactlyOnceSnapshot decodes and validates a binary
// exactly-once snapshot.
func UnmarshalChangefeedExactlyOnceSnapshot(data []byte) (ChangefeedExactlyOnceSnapshot, error) {
	var snapshot ChangefeedExactlyOnceSnapshot
	if len(data) < 6 || len(data) > maxChangefeedExactlyOnceSnapshotBytes {
		return snapshot, ErrChangefeedExactlyOnceSnapshotInvalid
	}
	if string(data[:4]) != "cox1" || data[4] != changefeedExactlyOnceSnapshotVersion {
		return snapshot, ErrChangefeedExactlyOnceSnapshotInvalid
	}
	flags := data[5]
	if flags&^(changefeedExactlyOnceFlagCommitted|changefeedExactlyOnceFlagPending) != 0 {
		return snapshot, ErrChangefeedExactlyOnceSnapshotInvalid
	}
	reader := changefeedExactlyOnceReader{data: data, offset: 6}
	var ok bool
	if snapshot.Source, ok = reader.string(); !ok {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceSnapshotInvalid
	}
	if snapshot.CommittedOffset, ok = reader.uint64(); !ok {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceSnapshotInvalid
	}
	if snapshot.CommittedFrontier, ok = reader.uint64(); !ok {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceSnapshotInvalid
	}
	if snapshot.LastCommittedBatchID, ok = reader.string(); !ok {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceSnapshotInvalid
	}
	snapshot.HasCommittedOffset = flags&changefeedExactlyOnceFlagCommitted != 0
	if flags&changefeedExactlyOnceFlagPending != 0 {
		batch := ChangefeedExactlyOnceBatch{}
		if batch.ID, ok = reader.string(); !ok {
			return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceSnapshotInvalid
		}
		if batch.StartOffset, ok = reader.uint64(); !ok {
			return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceSnapshotInvalid
		}
		if batch.EndOffset, ok = reader.uint64(); !ok {
			return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceSnapshotInvalid
		}
		snapshot.Pending = &batch
	}
	if reader.offset != len(data) {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceSnapshotInvalid
	}
	if err := validateChangefeedExactlyOnceSnapshot(snapshot); err != nil {
		return ChangefeedExactlyOnceSnapshot{}, ErrChangefeedExactlyOnceSnapshotInvalid
	}
	return snapshot, nil
}

func (c *ChangefeedExactlyOnceConsumer) snapshotLocked() ChangefeedExactlyOnceSnapshot {
	return ChangefeedExactlyOnceSnapshot{
		Source:               c.source,
		HasCommittedOffset:   c.hasCommittedOffset,
		CommittedOffset:      c.committedOffset,
		CommittedFrontier:    c.committedFrontier,
		LastCommittedBatchID: c.lastCommittedBatchID,
		Pending:              cloneChangefeedExactlyOnceBatch(c.pending),
	}
}

func cloneChangefeedExactlyOnceBatch(batch *ChangefeedExactlyOnceBatch) *ChangefeedExactlyOnceBatch {
	if batch == nil {
		return nil
	}
	clone := *batch
	return &clone
}

func validateChangefeedExactlyOnceSnapshot(snapshot ChangefeedExactlyOnceSnapshot) error {
	if err := validateChangefeedExactlyOnceSource(snapshot.Source); err != nil {
		return ErrChangefeedExactlyOnceSnapshotInvalid
	}
	if !snapshot.HasCommittedOffset {
		if snapshot.CommittedOffset != 0 || snapshot.CommittedFrontier != 0 || snapshot.LastCommittedBatchID != "" {
			return ErrChangefeedExactlyOnceSnapshotInvalid
		}
	} else if err := validateChangefeedExactlyOnceID(snapshot.LastCommittedBatchID); err != nil {
		return ErrChangefeedExactlyOnceSnapshotInvalid
	}
	if snapshot.Pending == nil {
		return nil
	}
	if err := validateChangefeedExactlyOnceBatch(*snapshot.Pending); err != nil {
		return ErrChangefeedExactlyOnceSnapshotInvalid
	}
	expected := uint64(0)
	if snapshot.HasCommittedOffset {
		if snapshot.CommittedOffset == ^uint64(0) {
			return ErrChangefeedExactlyOnceSnapshotInvalid
		}
		expected = snapshot.CommittedOffset + 1
		if snapshot.Pending.ID == snapshot.LastCommittedBatchID {
			return ErrChangefeedExactlyOnceSnapshotInvalid
		}
	}
	if snapshot.Pending.StartOffset != expected {
		return ErrChangefeedExactlyOnceSnapshotInvalid
	}
	return nil
}

func validateChangefeedExactlyOnceBatch(batch ChangefeedExactlyOnceBatch) error {
	if err := validateChangefeedExactlyOnceID(batch.ID); err != nil {
		return err
	}
	if batch.EndOffset < batch.StartOffset {
		return ErrChangefeedExactlyOnceInvalid
	}
	return nil
}

func validateChangefeedExactlyOnceSource(source string) error {
	return validateChangefeedExactlyOnceIdentity(source)
}

func validateChangefeedExactlyOnceID(id string) error {
	return validateChangefeedExactlyOnceIdentity(id)
}

func validateChangefeedExactlyOnceIdentity(value string) error {
	if value == "" || len(value) > maxChangefeedExactlyOnceIdentityBytes || !utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
		return ErrChangefeedExactlyOnceInvalid
	}
	return nil
}

func changefeedExactlyOnceSnapshotSize(snapshot ChangefeedExactlyOnceSnapshot) int {
	size := 6 + 2 + len(snapshot.Source) + 8 + 8 + 2 + len(snapshot.LastCommittedBatchID)
	if snapshot.Pending != nil {
		size += 2 + len(snapshot.Pending.ID) + 8 + 8
	}
	return size
}

func writeChangefeedExactlyOnceString(buffer *bytes.Buffer, value string) {
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(value)))
	buffer.Write(length[:])
	buffer.WriteString(value)
}

func writeChangefeedExactlyOnceUint64(buffer *bytes.Buffer, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	buffer.Write(encoded[:])
}

type changefeedExactlyOnceReader struct {
	data   []byte
	offset int
}

func (r *changefeedExactlyOnceReader) string() (string, bool) {
	if r.offset+2 > len(r.data) {
		return "", false
	}
	length := int(binary.BigEndian.Uint16(r.data[r.offset : r.offset+2]))
	r.offset += 2
	if length > maxChangefeedExactlyOnceIdentityBytes || r.offset+length > len(r.data) {
		return "", false
	}
	value := string(r.data[r.offset : r.offset+length])
	r.offset += length
	return value, true
}

func (r *changefeedExactlyOnceReader) uint64() (uint64, bool) {
	if r.offset+8 > len(r.data) {
		return 0, false
	}
	value := binary.BigEndian.Uint64(r.data[r.offset : r.offset+8])
	r.offset += 8
	return value, true
}
