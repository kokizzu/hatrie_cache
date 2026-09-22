package hatReplication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// MaxChangefeedSnapshotBoundarySourceBytes bounds one source identity.
	MaxChangefeedSnapshotBoundarySourceBytes = MaxChangefeedCheckpointSourceBytes
	changefeedSnapshotBoundaryFixedBytes     = 4 + 1 + 1 + 2 + 8*3
	maxChangefeedSnapshotBoundaryBytes       = changefeedSnapshotBoundaryFixedBytes + MaxChangefeedSnapshotBoundarySourceBytes
	changefeedSnapshotBoundaryVersion        = 1
)

var (
	// ErrChangefeedSnapshotBoundaryNil indicates a method call on a nil
	// boundary.
	ErrChangefeedSnapshotBoundaryNil = errors.New("hatriecache: changefeed snapshot boundary is nil")
	// ErrChangefeedSnapshotBoundaryInvalid identifies malformed coupling or a
	// source identity that cannot be persisted.
	ErrChangefeedSnapshotBoundaryInvalid = errors.New("hatriecache: changefeed snapshot boundary is invalid")
	// ErrChangefeedSnapshotBoundaryNotCommitted indicates that live progress
	// arrived before the snapshot boundary was atomically committed.
	ErrChangefeedSnapshotBoundaryNotCommitted = errors.New("hatriecache: changefeed snapshot boundary is not committed")
	// ErrChangefeedSnapshotBoundaryConflict indicates a retry with different
	// snapshot or first-live values.
	ErrChangefeedSnapshotBoundaryConflict = errors.New("hatriecache: changefeed snapshot boundary conflicts with the committed boundary")
	// ErrChangefeedSnapshotBoundaryRegressed indicates a lower live frontier.
	ErrChangefeedSnapshotBoundaryRegressed = errors.New("hatriecache: changefeed snapshot boundary frontier regressed")
	// ErrChangefeedSnapshotBoundarySnapshotInvalid identifies corrupt encoded
	// boundary data.
	ErrChangefeedSnapshotBoundarySnapshotInvalid = errors.New("hatriecache: changefeed snapshot boundary snapshot is invalid")
)

var changefeedSnapshotBoundaryMagic = [4]byte{'c', 'b', 's', '1'}

// ChangefeedSnapshotBoundarySnapshot is a detached coupled source snapshot
// and live-frontier view. Before commit, all offsets are zero and Committed is
// false. After commit, LiveFrontier is never below FirstLiveFrontier.
type ChangefeedSnapshotBoundarySnapshot struct {
	Source            string `json:"source"`
	SnapshotOffset    uint64 `json:"snapshot_offset"`
	FirstLiveFrontier uint64 `json:"first_live_frontier"`
	LiveFrontier      uint64 `json:"live_frontier"`
	Committed         bool   `json:"committed"`
}

// ChangefeedSnapshotBoundary atomically couples the source snapshot offset to
// the first live frontier. It is process-local; callers should persist its
// binary snapshot atomically with the recovered state.
type ChangefeedSnapshotBoundary struct {
	mu                sync.RWMutex
	source            string
	committed         bool
	snapshotOffset    uint64
	firstLiveFrontier uint64
	liveFrontier      uint64
}

// NewChangefeedSnapshotBoundary creates an uncommitted boundary for source.
func NewChangefeedSnapshotBoundary(source string) (*ChangefeedSnapshotBoundary, error) {
	source, err := normalizeChangefeedSnapshotBoundarySource(source)
	if err != nil {
		return nil, err
	}
	return &ChangefeedSnapshotBoundary{source: source}, nil
}

// NewChangefeedSnapshotBoundaryFromSnapshot restores a validated boundary.
func NewChangefeedSnapshotBoundaryFromSnapshot(snapshot ChangefeedSnapshotBoundarySnapshot) (*ChangefeedSnapshotBoundary, error) {
	normalized, err := normalizeChangefeedSnapshotBoundarySnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	return &ChangefeedSnapshotBoundary{
		source:            normalized.Source,
		committed:         normalized.Committed,
		snapshotOffset:    normalized.SnapshotOffset,
		firstLiveFrontier: normalized.FirstLiveFrontier,
		liveFrontier:      normalized.LiveFrontier,
	}, nil
}

// CommitSnapshotBoundary publishes snapshotOffset and firstLiveFrontier as
// one state transition. Retrying the exact pair is idempotent; a different
// pair is rejected so a partially replayed bootstrap cannot move the boundary.
func (boundary *ChangefeedSnapshotBoundary) CommitSnapshotBoundary(snapshotOffset, firstLiveFrontier uint64) (ChangefeedSnapshotBoundarySnapshot, error) {
	if boundary == nil {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundaryNil
	}
	if firstLiveFrontier < snapshotOffset {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundaryInvalid
	}
	boundary.mu.Lock()
	defer boundary.mu.Unlock()
	if boundary.committed {
		if boundary.snapshotOffset != snapshotOffset || boundary.firstLiveFrontier != firstLiveFrontier {
			return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundaryConflict
		}
		return boundary.snapshotLocked(), nil
	}
	boundary.committed = true
	boundary.snapshotOffset = snapshotOffset
	boundary.firstLiveFrontier = firstLiveFrontier
	boundary.liveFrontier = firstLiveFrontier
	return boundary.snapshotLocked(), nil
}

// AdvanceLiveFrontier records progress after the coupled boundary exists.
// Equal progress is idempotent and lower progress is rejected.
func (boundary *ChangefeedSnapshotBoundary) AdvanceLiveFrontier(frontier uint64) (ChangefeedSnapshotBoundarySnapshot, error) {
	if boundary == nil {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundaryNil
	}
	boundary.mu.Lock()
	defer boundary.mu.Unlock()
	if !boundary.committed {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundaryNotCommitted
	}
	if frontier < boundary.liveFrontier || frontier < boundary.firstLiveFrontier {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundaryRegressed
	}
	boundary.liveFrontier = frontier
	return boundary.snapshotLocked(), nil
}

// Snapshot returns a detached current boundary.
func (boundary *ChangefeedSnapshotBoundary) Snapshot() ChangefeedSnapshotBoundarySnapshot {
	if boundary == nil {
		return ChangefeedSnapshotBoundarySnapshot{}
	}
	boundary.mu.RLock()
	defer boundary.mu.RUnlock()
	return boundary.snapshotLocked()
}

// Restore atomically replaces the boundary after validating its coupling and
// source identity. A failed restore leaves the previous state untouched.
func (boundary *ChangefeedSnapshotBoundary) Restore(snapshot ChangefeedSnapshotBoundarySnapshot) error {
	if boundary == nil {
		return ErrChangefeedSnapshotBoundaryNil
	}
	normalized, err := normalizeChangefeedSnapshotBoundarySnapshot(snapshot)
	if err != nil {
		return err
	}
	boundary.mu.Lock()
	defer boundary.mu.Unlock()
	if normalized.Source != boundary.source {
		return fmt.Errorf("%w: source does not match boundary", ErrChangefeedSnapshotBoundaryInvalid)
	}
	boundary.committed = normalized.Committed
	boundary.snapshotOffset = normalized.SnapshotOffset
	boundary.firstLiveFrontier = normalized.FirstLiveFrontier
	boundary.liveFrontier = normalized.LiveFrontier
	return nil
}

// MarshalBinary encodes the boundary as a compact deterministic CBS1 record.
func (boundary *ChangefeedSnapshotBoundary) MarshalBinary() ([]byte, error) {
	if boundary == nil {
		return nil, ErrChangefeedSnapshotBoundaryNil
	}
	return boundary.Snapshot().MarshalBinary()
}

// UnmarshalBinary decodes and atomically restores a boundary.
func (boundary *ChangefeedSnapshotBoundary) UnmarshalBinary(encoded []byte) error {
	if boundary == nil {
		return ErrChangefeedSnapshotBoundaryNil
	}
	snapshot, err := UnmarshalChangefeedSnapshotBoundary(encoded)
	if err != nil {
		return err
	}
	return boundary.Restore(snapshot)
}

// MarshalBinary encodes a detached boundary snapshot.
func (snapshot ChangefeedSnapshotBoundarySnapshot) MarshalBinary() ([]byte, error) {
	normalized, err := normalizeChangefeedSnapshotBoundarySnapshot(snapshot)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, changefeedSnapshotBoundaryFixedBytes+len(normalized.Source))
	copyMagic := changefeedSnapshotBoundaryMagic[:]
	encoded = append(encoded, copyMagic...)
	encoded = append(encoded, changefeedSnapshotBoundaryVersion)
	if normalized.Committed {
		encoded = append(encoded, 1)
	} else {
		encoded = append(encoded, 0)
	}
	var sourceLength [2]byte
	binary.BigEndian.PutUint16(sourceLength[:], uint16(len(normalized.Source)))
	encoded = append(encoded, sourceLength[:]...)
	encoded = append(encoded, normalized.Source...)
	encoded = appendChangefeedSnapshotBoundaryUint64(encoded, normalized.SnapshotOffset)
	encoded = appendChangefeedSnapshotBoundaryUint64(encoded, normalized.FirstLiveFrontier)
	encoded = appendChangefeedSnapshotBoundaryUint64(encoded, normalized.LiveFrontier)
	return encoded, nil
}

// UnmarshalChangefeedSnapshotBoundary decodes a strict CBS1 boundary record.
func UnmarshalChangefeedSnapshotBoundary(encoded []byte) (ChangefeedSnapshotBoundarySnapshot, error) {
	if len(encoded) < changefeedSnapshotBoundaryFixedBytes || len(encoded) > maxChangefeedSnapshotBoundaryBytes {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundarySnapshotInvalid
	}
	if string(encoded[:4]) != string(changefeedSnapshotBoundaryMagic[:]) || encoded[4] != changefeedSnapshotBoundaryVersion || encoded[5] > 1 {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundarySnapshotInvalid
	}
	committed := encoded[5] == 1
	sourceLength := int(binary.BigEndian.Uint16(encoded[6:8]))
	if sourceLength == 0 || sourceLength > MaxChangefeedSnapshotBoundarySourceBytes || changefeedSnapshotBoundaryFixedBytes+sourceLength != len(encoded) {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundarySnapshotInvalid
	}
	offset := 8
	source := string(encoded[offset : offset+sourceLength])
	offset += sourceLength
	snapshotOffset, ok := readChangefeedSnapshotBoundaryUint64(encoded, &offset)
	if !ok {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundarySnapshotInvalid
	}
	firstLiveFrontier, ok := readChangefeedSnapshotBoundaryUint64(encoded, &offset)
	if !ok {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundarySnapshotInvalid
	}
	liveFrontier, ok := readChangefeedSnapshotBoundaryUint64(encoded, &offset)
	if !ok || offset != len(encoded) {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundarySnapshotInvalid
	}
	snapshot, err := normalizeChangefeedSnapshotBoundarySnapshot(ChangefeedSnapshotBoundarySnapshot{
		Source:            source,
		SnapshotOffset:    snapshotOffset,
		FirstLiveFrontier: firstLiveFrontier,
		LiveFrontier:      liveFrontier,
		Committed:         committed,
	})
	if err != nil {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundarySnapshotInvalid
	}
	return snapshot, nil
}

func (boundary *ChangefeedSnapshotBoundary) snapshotLocked() ChangefeedSnapshotBoundarySnapshot {
	return ChangefeedSnapshotBoundarySnapshot{
		Source:            boundary.source,
		SnapshotOffset:    boundary.snapshotOffset,
		FirstLiveFrontier: boundary.firstLiveFrontier,
		LiveFrontier:      boundary.liveFrontier,
		Committed:         boundary.committed,
	}
}

func normalizeChangefeedSnapshotBoundarySource(source string) (string, error) {
	trimmed := strings.TrimSpace(source)
	if trimmed == "" || len(trimmed) > MaxChangefeedSnapshotBoundarySourceBytes || trimmed != source || !utf8.ValidString(trimmed) || strings.IndexByte(trimmed, 0) >= 0 {
		return "", ErrChangefeedSnapshotBoundaryInvalid
	}
	return trimmed, nil
}

func normalizeChangefeedSnapshotBoundarySnapshot(snapshot ChangefeedSnapshotBoundarySnapshot) (ChangefeedSnapshotBoundarySnapshot, error) {
	source, err := normalizeChangefeedSnapshotBoundarySource(snapshot.Source)
	if err != nil {
		return ChangefeedSnapshotBoundarySnapshot{}, err
	}
	if !snapshot.Committed && (snapshot.SnapshotOffset != 0 || snapshot.FirstLiveFrontier != 0 || snapshot.LiveFrontier != 0) {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundaryInvalid
	}
	if snapshot.Committed && (snapshot.FirstLiveFrontier < snapshot.SnapshotOffset || snapshot.LiveFrontier < snapshot.FirstLiveFrontier) {
		return ChangefeedSnapshotBoundarySnapshot{}, ErrChangefeedSnapshotBoundaryInvalid
	}
	snapshot.Source = source
	return snapshot, nil
}

func appendChangefeedSnapshotBoundaryUint64(encoded []byte, value uint64) []byte {
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], value)
	return append(encoded, number[:]...)
}

func readChangefeedSnapshotBoundaryUint64(encoded []byte, offset *int) (uint64, bool) {
	if *offset < 0 || len(encoded)-*offset < 8 {
		return 0, false
	}
	value := binary.BigEndian.Uint64(encoded[*offset : *offset+8])
	*offset += 8
	return value, true
}
