package hatPipeline

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
)

const (
	// DefaultLateDataReclockMaxBindings bounds retained source-to-processing
	// remap points. Call CompactBefore to release old points before this bound
	// is reached.
	DefaultLateDataReclockMaxBindings = 4096
	maxLateDataReclockBindings        = 1 << 20
	maxLateDataReclockSnapshotBytes   = 64 << 20
	lateDataReclockSnapshotHeaderSize = 5
)

var (
	// ErrLateDataReclockNil indicates a nil reclock receiver.
	ErrLateDataReclockNil = errors.New("hatPipeline: late-data reclock is nil")
	// ErrLateDataReclockOptionsInvalid indicates an invalid binding bound.
	ErrLateDataReclockOptionsInvalid = errors.New("hatPipeline: late-data reclock options are invalid")
	// ErrLateDataReclockSourceRegression indicates a source frontier moved
	// backwards.
	ErrLateDataReclockSourceRegression = errors.New("hatPipeline: late-data reclock source frontier regressed")
	// ErrLateDataReclockProcessingRegression indicates a processing frontier
	// moved backwards.
	ErrLateDataReclockProcessingRegression = errors.New("hatPipeline: late-data reclock processing frontier regressed")
	// ErrLateDataReclockBindingConflict indicates an attempt to publish a new
	// source frontier at an already-published processing frontier.
	ErrLateDataReclockBindingConflict = errors.New("hatPipeline: late-data reclock binding conflicts")
	// ErrLateDataReclockBindingLimit indicates that another distinct remap
	// point would exceed the configured bound.
	ErrLateDataReclockBindingLimit = errors.New("hatPipeline: late-data reclock binding limit reached")
	// ErrLateDataReclockHistoryCompacted indicates that a historical processing
	// point is older than the retained remap history.
	ErrLateDataReclockHistoryCompacted = errors.New("hatPipeline: late-data reclock history is compacted")
	// ErrLateDataReclockProcessingNotCovered indicates that no published
	// processing frontier covers the requested target point.
	ErrLateDataReclockProcessingNotCovered = errors.New("hatPipeline: late-data reclock processing point is not covered")
	// ErrLateDataReclockSourceNotCovered indicates that no published source
	// frontier covers the requested source point.
	ErrLateDataReclockSourceNotCovered = errors.New("hatPipeline: late-data reclock source point is not covered")
	// ErrLateDataReclockCompactionRegression indicates that the compaction
	// boundary moved backwards.
	ErrLateDataReclockCompactionRegression = errors.New("hatPipeline: late-data reclock compaction boundary regressed")
	// ErrLateDataReclockSnapshotInvalid indicates a malformed or inconsistent
	// encoded snapshot.
	ErrLateDataReclockSnapshotInvalid = errors.New("hatPipeline: late-data reclock snapshot is invalid")
)

var lateDataReclockSnapshotMagic = [4]byte{'H', 'R', 'C', '1'}

// LateDataReclockOptions bounds a source-to-processing remap relation.
// SourceFrontier and ProcessingFrontier are exclusive upper bounds: a source
// event is covered when EventTime < SourceFrontier. Zero MaxBindings uses
// DefaultLateDataReclockMaxBindings.
type LateDataReclockOptions struct {
	MaxBindings               int
	InitialSourceFrontier     uint64
	InitialProcessingFrontier uint64
}

// LateDataReclockBinding records that all source progress below
// SourceFrontier is available at ProcessingFrontier.
type LateDataReclockBinding struct {
	ProcessingFrontier uint64
	SourceFrontier     uint64
}

// LateDataReclockAssignment is the target timestamp selected for one source
// event. A late event is assigned at its arrival processing time so it does
// not rewrite an already-published target frontier. An event that arrives
// earlier than its mapped frontier is held until that frontier.
type LateDataReclockAssignment struct {
	SourceTime              uint64
	ProcessingFrontier      uint64
	CoveredBySourceFrontier uint64
	Late                    bool
	HeldUntilProcessing     bool
}

// LateDataReclockSnapshot is a detached, deterministic copy of reclock state.
// Bindings are sorted by ProcessingFrontier and have nondecreasing
// SourceFrontier values.
type LateDataReclockSnapshot struct {
	ProcessingFrontier uint64
	SourceFrontier     uint64
	CompactedThrough   uint64
	Generation         uint64
	Bindings           []LateDataReclockBinding
}

// LateDataReclock translates a monotone source progress gauge into a
// monotone processing-time gauge. It stores only the remap sidecar, not event
// payloads, so callers can keep source data in their existing arrangement or
// journal. The zero value is not usable; construct it with
// NewLateDataReclock or NewLateDataReclockFromSnapshot.
type LateDataReclock struct {
	mu                 sync.RWMutex
	maxBindings        int
	processingFrontier uint64
	sourceFrontier     uint64
	compactedThrough   uint64
	generation         uint64
	bindings           []LateDataReclockBinding
}

// NewLateDataReclock creates a remap relation with one initial binding.
func NewLateDataReclock(options LateDataReclockOptions) (*LateDataReclock, error) {
	maxBindings, err := normalizeLateDataReclockMaxBindings(options.MaxBindings)
	if err != nil {
		return nil, err
	}
	return &LateDataReclock{
		maxBindings:        maxBindings,
		processingFrontier: options.InitialProcessingFrontier,
		sourceFrontier:     options.InitialSourceFrontier,
		bindings: []LateDataReclockBinding{{
			ProcessingFrontier: options.InitialProcessingFrontier,
			SourceFrontier:     options.InitialSourceFrontier,
		}},
	}, nil
}

// NewLateDataReclockFromSnapshot creates a reclock relation from a validated
// detached snapshot. Initial frontier options are ignored; MaxBindings still
// controls future growth.
func NewLateDataReclockFromSnapshot(snapshot LateDataReclockSnapshot, options LateDataReclockOptions) (*LateDataReclock, error) {
	maxBindings, err := normalizeLateDataReclockMaxBindings(options.MaxBindings)
	if err != nil {
		return nil, err
	}
	if err := validateLateDataReclockSnapshot(snapshot, maxBindings); err != nil {
		return nil, err
	}
	return &LateDataReclock{
		maxBindings:        maxBindings,
		processingFrontier: snapshot.ProcessingFrontier,
		sourceFrontier:     snapshot.SourceFrontier,
		compactedThrough:   snapshot.CompactedThrough,
		generation:         snapshot.Generation,
		bindings:           append([]LateDataReclockBinding(nil), snapshot.Bindings...),
	}, nil
}

// ObserveSourceFrontier advances source progress without publishing it to the
// processing timeline. The next successful processing advance exposes this
// source frontier through a new binding.
func (reclock *LateDataReclock) ObserveSourceFrontier(source uint64) (bool, error) {
	if reclock == nil {
		return false, ErrLateDataReclockNil
	}
	reclock.mu.Lock()
	defer reclock.mu.Unlock()
	if !reclock.initializedLocked() {
		return false, ErrLateDataReclockOptionsInvalid
	}
	if source < reclock.sourceFrontier {
		return false, ErrLateDataReclockSourceRegression
	}
	if source == reclock.sourceFrontier {
		return false, nil
	}
	reclock.sourceFrontier = source
	return true, nil
}

// AdvanceProcessingFrontier publishes the currently observed source frontier
// at a later processing frontier. Equal source frontiers are coalesced without
// consuming binding capacity.
func (reclock *LateDataReclock) AdvanceProcessingFrontier(processing uint64) (LateDataReclockBinding, bool, error) {
	if reclock == nil {
		return LateDataReclockBinding{}, false, ErrLateDataReclockNil
	}
	reclock.mu.Lock()
	defer reclock.mu.Unlock()
	if !reclock.initializedLocked() {
		return LateDataReclockBinding{}, false, ErrLateDataReclockOptionsInvalid
	}
	if processing < reclock.processingFrontier {
		return LateDataReclockBinding{}, false, ErrLateDataReclockProcessingRegression
	}
	last := reclock.bindings[len(reclock.bindings)-1]
	if processing == reclock.processingFrontier {
		if reclock.sourceFrontier != last.SourceFrontier {
			return LateDataReclockBinding{}, false, ErrLateDataReclockBindingConflict
		}
		return last, false, nil
	}
	if reclock.sourceFrontier != last.SourceFrontier && len(reclock.bindings) >= reclock.maxBindings {
		return LateDataReclockBinding{}, false, ErrLateDataReclockBindingLimit
	}
	reclock.processingFrontier = processing
	reclock.generation++
	if reclock.sourceFrontier != last.SourceFrontier {
		reclock.bindings = append(reclock.bindings, LateDataReclockBinding{
			ProcessingFrontier: processing,
			SourceFrontier:     reclock.sourceFrontier,
		})
	}
	return LateDataReclockBinding{
		ProcessingFrontier: processing,
		SourceFrontier:     reclock.sourceFrontier,
	}, true, nil
}

// SourceFrontierAt returns the latest source frontier published at or before
// processing. Historical points below CompactedThrough are unavailable.
func (reclock *LateDataReclock) SourceFrontierAt(processing uint64) (uint64, error) {
	if reclock == nil {
		return 0, ErrLateDataReclockNil
	}
	reclock.mu.RLock()
	defer reclock.mu.RUnlock()
	if !reclock.initializedLocked() {
		return 0, ErrLateDataReclockOptionsInvalid
	}
	if processing < reclock.compactedThrough {
		return 0, ErrLateDataReclockHistoryCompacted
	}
	if processing > reclock.processingFrontier {
		return 0, ErrLateDataReclockProcessingNotCovered
	}
	index := lateDataReclockLatestProcessingBinding(reclock.bindings, processing)
	if index < 0 {
		return 0, ErrLateDataReclockProcessingNotCovered
	}
	return reclock.bindings[index].SourceFrontier, nil
}

// ProcessingFrontierAt returns the earliest published processing frontier
// whose source frontier covers sourceTime. The strict comparison implements
// exclusive-upper-bound frontier semantics.
func (reclock *LateDataReclock) ProcessingFrontierAt(sourceTime uint64) (uint64, error) {
	if reclock == nil {
		return 0, ErrLateDataReclockNil
	}
	reclock.mu.RLock()
	defer reclock.mu.RUnlock()
	if !reclock.initializedLocked() {
		return 0, ErrLateDataReclockOptionsInvalid
	}
	index := lateDataReclockFirstCoveringSourceBinding(reclock.bindings, sourceTime)
	if index < 0 {
		return 0, ErrLateDataReclockSourceNotCovered
	}
	return reclock.bindings[index].ProcessingFrontier, nil
}

// Assign maps a source event to the processing timeline. Events arriving
// after their source frontier was published are marked Late and assigned at
// arrivalProcessing. Earlier arrivals are held at their mapped frontier.
func (reclock *LateDataReclock) Assign(sourceTime, arrivalProcessing uint64) (LateDataReclockAssignment, error) {
	if reclock == nil {
		return LateDataReclockAssignment{}, ErrLateDataReclockNil
	}
	reclock.mu.RLock()
	if !reclock.initializedLocked() {
		reclock.mu.RUnlock()
		return LateDataReclockAssignment{}, ErrLateDataReclockOptionsInvalid
	}
	index := lateDataReclockFirstCoveringSourceBinding(reclock.bindings, sourceTime)
	if index < 0 {
		reclock.mu.RUnlock()
		return LateDataReclockAssignment{}, ErrLateDataReclockSourceNotCovered
	}
	binding := reclock.bindings[index]
	reclock.mu.RUnlock()
	assignment := LateDataReclockAssignment{
		SourceTime:              sourceTime,
		ProcessingFrontier:      binding.ProcessingFrontier,
		CoveredBySourceFrontier: binding.SourceFrontier,
	}
	if arrivalProcessing > binding.ProcessingFrontier {
		assignment.ProcessingFrontier = arrivalProcessing
		assignment.Late = true
	} else if arrivalProcessing < binding.ProcessingFrontier {
		assignment.HeldUntilProcessing = true
	}
	return assignment, nil
}

// CompactBefore drops remap points strictly before processing while retaining
// the latest point at or before the boundary as an anchor. The anchor makes
// future lookups conservative and deterministic without retaining old map
// entries.
func (reclock *LateDataReclock) CompactBefore(processing uint64) error {
	if reclock == nil {
		return ErrLateDataReclockNil
	}
	reclock.mu.Lock()
	defer reclock.mu.Unlock()
	if !reclock.initializedLocked() {
		return ErrLateDataReclockOptionsInvalid
	}
	if processing < reclock.compactedThrough {
		return ErrLateDataReclockCompactionRegression
	}
	if processing > reclock.processingFrontier {
		return ErrLateDataReclockProcessingNotCovered
	}
	anchor := lateDataReclockLatestProcessingBinding(reclock.bindings, processing)
	if anchor < 0 {
		return ErrLateDataReclockProcessingNotCovered
	}
	if anchor > 0 {
		retained := make([]LateDataReclockBinding, len(reclock.bindings)-anchor)
		copy(retained, reclock.bindings[anchor:])
		reclock.bindings = retained
	}
	if processing > reclock.compactedThrough {
		reclock.compactedThrough = processing
	}
	return nil
}

// Snapshot returns a detached copy of the current remap state.
func (reclock *LateDataReclock) Snapshot() LateDataReclockSnapshot {
	if reclock == nil {
		return LateDataReclockSnapshot{}
	}
	reclock.mu.RLock()
	snapshot := LateDataReclockSnapshot{
		ProcessingFrontier: reclock.processingFrontier,
		SourceFrontier:     reclock.sourceFrontier,
		CompactedThrough:   reclock.compactedThrough,
		Generation:         reclock.generation,
		Bindings:           append([]LateDataReclockBinding(nil), reclock.bindings...),
	}
	reclock.mu.RUnlock()
	return snapshot
}

// MarshalSnapshot encodes a bounded deterministic snapshot with a versioned
// header and CRC32 integrity check.
func (reclock *LateDataReclock) MarshalSnapshot() ([]byte, error) {
	if reclock == nil {
		return nil, ErrLateDataReclockNil
	}
	return marshalLateDataReclockSnapshot(reclock.Snapshot())
}

// UnmarshalLateDataReclockSnapshot decodes and validates a snapshot. The
// maxBindings argument is an explicit resource bound; zero uses the package
// default.
func UnmarshalLateDataReclockSnapshot(payload []byte, maxBindings int) (LateDataReclockSnapshot, error) {
	maxBindings, err := normalizeLateDataReclockMaxBindings(maxBindings)
	if err != nil {
		return LateDataReclockSnapshot{}, err
	}
	if len(payload) < lateDataReclockSnapshotHeaderSize+4 || len(payload) > maxLateDataReclockSnapshotBytes {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	body := payload[:len(payload)-4]
	if !bytes.Equal(body[:4], lateDataReclockSnapshotMagic[:]) || body[4] != 1 {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	if crc32.ChecksumIEEE(body) != binary.LittleEndian.Uint32(payload[len(payload)-4:]) {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	offset := lateDataReclockSnapshotHeaderSize
	count, ok := readLateDataReclockUvarint(body, &offset)
	if !ok || count == 0 || count > uint64(maxBindings) || count > uint64(maxLateDataReclockBindings) {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	processing, ok := readLateDataReclockUvarint(body, &offset)
	if !ok {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	source, ok := readLateDataReclockUvarint(body, &offset)
	if !ok {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	compacted, ok := readLateDataReclockUvarint(body, &offset)
	if !ok {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	generation, ok := readLateDataReclockUvarint(body, &offset)
	if !ok {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	bindings := make([]LateDataReclockBinding, 0, int(count))
	for index := uint64(0); index < count; index++ {
		bindingProcessing, ok := readLateDataReclockUvarint(body, &offset)
		if !ok {
			return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
		}
		bindingSource, ok := readLateDataReclockUvarint(body, &offset)
		if !ok {
			return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
		}
		bindings = append(bindings, LateDataReclockBinding{
			ProcessingFrontier: bindingProcessing,
			SourceFrontier:     bindingSource,
		})
	}
	if offset != len(body) {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	snapshot := LateDataReclockSnapshot{
		ProcessingFrontier: processing,
		SourceFrontier:     source,
		CompactedThrough:   compacted,
		Generation:         generation,
		Bindings:           bindings,
	}
	if err := validateLateDataReclockSnapshot(snapshot, maxBindings); err != nil {
		return LateDataReclockSnapshot{}, ErrLateDataReclockSnapshotInvalid
	}
	return snapshot, nil
}

func marshalLateDataReclockSnapshot(snapshot LateDataReclockSnapshot) ([]byte, error) {
	if err := validateLateDataReclockSnapshot(snapshot, maxLateDataReclockBindings); err != nil {
		return nil, err
	}
	payload := make([]byte, 0, lateDataReclockSnapshotHeaderSize+5*binary.MaxVarintLen64+len(snapshot.Bindings)*2*binary.MaxVarintLen64+4)
	payload = append(payload, lateDataReclockSnapshotMagic[:]...)
	payload = append(payload, 1)
	payload = appendLateDataReclockUvarint(payload, uint64(len(snapshot.Bindings)))
	payload = appendLateDataReclockUvarint(payload, snapshot.ProcessingFrontier)
	payload = appendLateDataReclockUvarint(payload, snapshot.SourceFrontier)
	payload = appendLateDataReclockUvarint(payload, snapshot.CompactedThrough)
	payload = appendLateDataReclockUvarint(payload, snapshot.Generation)
	for _, binding := range snapshot.Bindings {
		payload = appendLateDataReclockUvarint(payload, binding.ProcessingFrontier)
		payload = appendLateDataReclockUvarint(payload, binding.SourceFrontier)
	}
	if len(payload)+4 > maxLateDataReclockSnapshotBytes {
		return nil, ErrLateDataReclockSnapshotInvalid
	}
	checksum := crc32.ChecksumIEEE(payload)
	var encodedChecksum [4]byte
	binary.LittleEndian.PutUint32(encodedChecksum[:], checksum)
	return append(payload, encodedChecksum[:]...), nil
}

func validateLateDataReclockSnapshot(snapshot LateDataReclockSnapshot, maxBindings int) error {
	if len(snapshot.Bindings) == 0 || len(snapshot.Bindings) > maxBindings || len(snapshot.Bindings) > maxLateDataReclockBindings {
		return ErrLateDataReclockSnapshotInvalid
	}
	if snapshot.CompactedThrough > snapshot.ProcessingFrontier {
		return ErrLateDataReclockSnapshotInvalid
	}
	if snapshot.CompactedThrough != 0 && snapshot.Bindings[0].ProcessingFrontier > snapshot.CompactedThrough {
		return ErrLateDataReclockSnapshotInvalid
	}
	previous := snapshot.Bindings[0]
	for index, binding := range snapshot.Bindings {
		if index > 0 && (binding.ProcessingFrontier <= previous.ProcessingFrontier || binding.SourceFrontier < previous.SourceFrontier) {
			return ErrLateDataReclockSnapshotInvalid
		}
		if binding.ProcessingFrontier > snapshot.ProcessingFrontier || binding.SourceFrontier > snapshot.SourceFrontier {
			return ErrLateDataReclockSnapshotInvalid
		}
		previous = binding
	}
	return nil
}

func normalizeLateDataReclockMaxBindings(maxBindings int) (int, error) {
	if maxBindings == 0 {
		maxBindings = DefaultLateDataReclockMaxBindings
	}
	if maxBindings < 1 || maxBindings > maxLateDataReclockBindings {
		return 0, ErrLateDataReclockOptionsInvalid
	}
	return maxBindings, nil
}

func (reclock *LateDataReclock) initializedLocked() bool {
	return reclock.maxBindings >= 1 && len(reclock.bindings) >= 1
}

func lateDataReclockLatestProcessingBinding(bindings []LateDataReclockBinding, processing uint64) int {
	left, right := 0, len(bindings)
	for left < right {
		middle := left + (right-left)/2
		if bindings[middle].ProcessingFrontier <= processing {
			left = middle + 1
		} else {
			right = middle
		}
	}
	return left - 1
}

func lateDataReclockFirstCoveringSourceBinding(bindings []LateDataReclockBinding, source uint64) int {
	left, right := 0, len(bindings)
	for left < right {
		middle := left + (right-left)/2
		if bindings[middle].SourceFrontier > source {
			right = middle
		} else {
			left = middle + 1
		}
	}
	if left == len(bindings) {
		return -1
	}
	return left
}

func appendLateDataReclockUvarint(payload []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	return append(payload, encoded[:binary.PutUvarint(encoded[:], value)]...)
}

func readLateDataReclockUvarint(payload []byte, offset *int) (uint64, bool) {
	if *offset >= len(payload) {
		return 0, false
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 {
		return 0, false
	}
	*offset += size
	return value, true
}
