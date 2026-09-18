package hatPipeline

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSnapshotCutoverMaxCutovers bounds retained cutover plans.
	DefaultSnapshotCutoverMaxCutovers = 64
	// DefaultSnapshotCutoverMaxSources bounds sources in one cutover.
	DefaultSnapshotCutoverMaxSources = 256
	maxSnapshotCutoverMaxCutovers    = 1 << 16
	maxSnapshotCutoverMaxSources     = 1 << 16
	maxSnapshotCutoverTextBytes      = 256
)

var (
	// ErrSnapshotCutoverNil indicates a method call on a nil coordinator.
	ErrSnapshotCutoverNil = errors.New("hatPipeline: snapshot cutover coordinator is nil")
	// ErrSnapshotCutoverOptionsInvalid indicates an invalid coordinator bound.
	ErrSnapshotCutoverOptionsInvalid = errors.New("hatPipeline: snapshot cutover options are invalid")
	// ErrSnapshotCutoverIDEmpty indicates an empty cutover ID.
	ErrSnapshotCutoverIDEmpty = errors.New("hatPipeline: snapshot cutover ID is empty")
	// ErrSnapshotCutoverSourceEmpty indicates an empty source ID.
	ErrSnapshotCutoverSourceEmpty = errors.New("hatPipeline: snapshot cutover source ID is empty")
	// ErrSnapshotCutoverSpecInvalid indicates malformed or duplicate sources.
	ErrSnapshotCutoverSpecInvalid = errors.New("hatPipeline: snapshot cutover specification is invalid")
	// ErrSnapshotCutoverAcknowledgementInvalid indicates invalid frontier bounds
	// or acknowledgement text.
	ErrSnapshotCutoverAcknowledgementInvalid = errors.New("hatPipeline: snapshot cutover acknowledgement is invalid")
	// ErrSnapshotCutoverSourceUnknown indicates an acknowledgement for a source
	// that was not included in the prepared cutover.
	ErrSnapshotCutoverSourceUnknown = errors.New("hatPipeline: snapshot cutover source is unknown")
	// ErrSnapshotCutoverGenerationMismatch fences stale source instances.
	ErrSnapshotCutoverGenerationMismatch = errors.New("hatPipeline: snapshot cutover source generation mismatch")
	// ErrSnapshotCutoverNotReady indicates that not every source covers the
	// requested timestamp.
	ErrSnapshotCutoverNotReady = errors.New("hatPipeline: snapshot cutover is not ready")
	// ErrSnapshotCutoverAcknowledgementRegression indicates non-monotone source
	// progress.
	ErrSnapshotCutoverAcknowledgementRegression = errors.New("hatPipeline: snapshot cutover acknowledgement regressed")
	// ErrSnapshotCutoverCapacity indicates that the coordinator is full.
	ErrSnapshotCutoverCapacity = errors.New("hatPipeline: snapshot cutover capacity exceeded")
	// ErrSnapshotCutoverAlreadyExists indicates a duplicate cutover ID.
	ErrSnapshotCutoverAlreadyExists = errors.New("hatPipeline: snapshot cutover already exists")
	// ErrSnapshotCutoverNotFound indicates an unknown cutover ID.
	ErrSnapshotCutoverNotFound = errors.New("hatPipeline: snapshot cutover was not found")
	// ErrSnapshotCutoverTerminal indicates an operation on a committed or
	// aborted cutover that cannot accept progress.
	ErrSnapshotCutoverTerminal = errors.New("hatPipeline: snapshot cutover is terminal")
	// ErrSnapshotCutoverNotTerminal indicates that Forget was requested too
	// early.
	ErrSnapshotCutoverNotTerminal = errors.New("hatPipeline: snapshot cutover is not terminal")
)

// SnapshotCutoverOptions bounds retained cross-source cutover state. Zero
// values select conservative defaults.
type SnapshotCutoverOptions struct {
	MaxCutovers int
	MaxSources  int
}

// SnapshotCutoverState is the lifecycle state of one cross-source snapshot.
type SnapshotCutoverState string

const (
	SnapshotCutoverPrepared  SnapshotCutoverState = "prepared"
	SnapshotCutoverCommitted SnapshotCutoverState = "committed"
	SnapshotCutoverAborted   SnapshotCutoverState = "aborted"
)

// SnapshotCutoverSource identifies one source and fences acknowledgements to
// the source generation observed at Prepare time.
type SnapshotCutoverSource struct {
	ID         string
	Generation uint64
}

// SnapshotCutoverSpec describes one consistent timestamp across sources.
type SnapshotCutoverSpec struct {
	ID        string
	Timestamp uint64
	Sources   []SnapshotCutoverSource
}

// SnapshotCutoverAcknowledgement reports a source frontier that covers the
// requested timestamp.
type SnapshotCutoverAcknowledgement struct {
	SourceID   string
	Generation uint64
	Lower      uint64
	Upper      uint64
}

// SnapshotCutoverStatus is a detached, deterministic cutover status.
type SnapshotCutoverStatus struct {
	ID               string
	Timestamp        uint64
	State            SnapshotCutoverState
	Sources          []SnapshotCutoverSource
	Acknowledgements []SnapshotCutoverAcknowledgement
	TotalSources     int
	Acknowledged     int
	Remaining        int
	Reason           string
}

// SnapshotCutoverProgress is the allocation-free acknowledgement result for
// connectors that only need readiness and counts. Use Status when a detached
// source and acknowledgement snapshot is required.
type SnapshotCutoverProgress struct {
	State        SnapshotCutoverState
	TotalSources int
	Acknowledged int
	Remaining    int
	Ready        bool
}

type snapshotCutoverEntry struct {
	status           SnapshotCutoverStatus
	expected         map[string]uint64
	sourceIndexes    map[string]int
	acknowledgements []SnapshotCutoverAcknowledgement
	ackReceived      []bool
	acknowledged     int
}

// SnapshotCutoverCoordinator coordinates an opt-in, bounded cross-source
// snapshot cutover. It does not start source work or perform I/O; connectors
// call Acknowledge after independently advancing to the requested timestamp.
type SnapshotCutoverCoordinator struct {
	mu          sync.RWMutex
	maxCutovers int
	maxSources  int
	cutovers    map[string]*snapshotCutoverEntry
}

// NewSnapshotCutoverCoordinator creates a bounded coordinator.
func NewSnapshotCutoverCoordinator(options SnapshotCutoverOptions) (*SnapshotCutoverCoordinator, error) {
	if options.MaxCutovers < 0 || options.MaxSources < 0 {
		return nil, ErrSnapshotCutoverOptionsInvalid
	}
	if options.MaxCutovers == 0 {
		options.MaxCutovers = DefaultSnapshotCutoverMaxCutovers
	}
	if options.MaxSources == 0 {
		options.MaxSources = DefaultSnapshotCutoverMaxSources
	}
	if options.MaxCutovers > maxSnapshotCutoverMaxCutovers || options.MaxSources > maxSnapshotCutoverMaxSources {
		return nil, ErrSnapshotCutoverOptionsInvalid
	}
	return &SnapshotCutoverCoordinator{
		maxCutovers: options.MaxCutovers,
		maxSources:  options.MaxSources,
		cutovers:    make(map[string]*snapshotCutoverEntry, options.MaxCutovers),
	}, nil
}

// Prepare reserves a cutover and records the expected source generations.
func (coordinator *SnapshotCutoverCoordinator) Prepare(spec SnapshotCutoverSpec) (SnapshotCutoverStatus, error) {
	if coordinator == nil {
		return SnapshotCutoverStatus{}, ErrSnapshotCutoverNil
	}
	normalized, expected, err := normalizeSnapshotCutoverSpec(spec, coordinator.maxSources)
	if err != nil {
		return SnapshotCutoverStatus{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if _, exists := coordinator.cutovers[normalized.ID]; exists {
		return SnapshotCutoverStatus{}, ErrSnapshotCutoverAlreadyExists
	}
	if len(coordinator.cutovers) >= coordinator.maxCutovers {
		return SnapshotCutoverStatus{}, ErrSnapshotCutoverCapacity
	}
	entry := &snapshotCutoverEntry{
		status: SnapshotCutoverStatus{
			ID:           normalized.ID,
			Timestamp:    normalized.Timestamp,
			State:        SnapshotCutoverPrepared,
			Sources:      normalized.Sources,
			TotalSources: len(normalized.Sources),
			Remaining:    len(normalized.Sources),
		},
		expected:         expected,
		sourceIndexes:    make(map[string]int, len(normalized.Sources)),
		acknowledgements: make([]SnapshotCutoverAcknowledgement, len(normalized.Sources)),
		ackReceived:      make([]bool, len(normalized.Sources)),
	}
	for index, source := range normalized.Sources {
		entry.sourceIndexes[source.ID] = index
	}
	coordinator.cutovers[normalized.ID] = entry
	return cloneSnapshotCutoverStatus(entry), nil
}

// Acknowledge records monotone source progress. The source must use the
// generation declared in Prepare and its lower frontier must cover Timestamp.
func (coordinator *SnapshotCutoverCoordinator) Acknowledge(id string, acknowledgement SnapshotCutoverAcknowledgement) (SnapshotCutoverStatus, error) {
	if coordinator == nil {
		return SnapshotCutoverStatus{}, ErrSnapshotCutoverNil
	}
	id, acknowledgement, err := normalizeSnapshotCutoverAcknowledgement(id, acknowledgement)
	if err != nil {
		return SnapshotCutoverStatus{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	entry, err := coordinator.acknowledgeLocked(id, acknowledgement)
	if err != nil {
		if entry != nil && (err == ErrSnapshotCutoverTerminal || err == ErrSnapshotCutoverNotReady || err == ErrSnapshotCutoverAcknowledgementRegression) {
			return cloneSnapshotCutoverStatus(entry), err
		}
		return SnapshotCutoverStatus{}, err
	}
	return cloneSnapshotCutoverStatus(entry), nil
}

// AcknowledgeProgress records source progress without cloning the detached
// status slices. It is the preferred path for connectors acknowledging every
// source frontier in a tight loop.
func (coordinator *SnapshotCutoverCoordinator) AcknowledgeProgress(id string, acknowledgement SnapshotCutoverAcknowledgement) (SnapshotCutoverProgress, error) {
	if coordinator == nil {
		return SnapshotCutoverProgress{}, ErrSnapshotCutoverNil
	}
	id, acknowledgement, err := normalizeSnapshotCutoverAcknowledgement(id, acknowledgement)
	if err != nil {
		return SnapshotCutoverProgress{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	entry, err := coordinator.acknowledgeLocked(id, acknowledgement)
	if entry == nil {
		return SnapshotCutoverProgress{}, err
	}
	return snapshotCutoverProgress(entry), err
}

// Commit marks a cutover committed only after every source has acknowledged a
// frontier covering the requested timestamp. Repeated commits are idempotent.
func (coordinator *SnapshotCutoverCoordinator) Commit(id string) (SnapshotCutoverStatus, error) {
	if coordinator == nil {
		return SnapshotCutoverStatus{}, ErrSnapshotCutoverNil
	}
	id, err := normalizeSnapshotCutoverText(id, ErrSnapshotCutoverIDEmpty)
	if err != nil {
		return SnapshotCutoverStatus{}, err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	entry, ok := coordinator.cutovers[id]
	if !ok {
		return SnapshotCutoverStatus{}, ErrSnapshotCutoverNotFound
	}
	if entry.status.State == SnapshotCutoverCommitted {
		return cloneSnapshotCutoverStatus(entry), nil
	}
	if entry.status.State != SnapshotCutoverPrepared {
		return cloneSnapshotCutoverStatus(entry), ErrSnapshotCutoverTerminal
	}
	if entry.acknowledged != entry.status.TotalSources {
		return cloneSnapshotCutoverStatus(entry), ErrSnapshotCutoverNotReady
	}
	entry.status.State = SnapshotCutoverCommitted
	return cloneSnapshotCutoverStatus(entry), nil
}

// Abort marks a prepared cutover terminal with a bounded diagnostic reason.
func (coordinator *SnapshotCutoverCoordinator) Abort(id, reason string) (SnapshotCutoverStatus, error) {
	if coordinator == nil {
		return SnapshotCutoverStatus{}, ErrSnapshotCutoverNil
	}
	id, err := normalizeSnapshotCutoverText(id, ErrSnapshotCutoverIDEmpty)
	if err != nil {
		return SnapshotCutoverStatus{}, err
	}
	reason = strings.TrimSpace(reason)
	if len(reason) > maxSnapshotCutoverTextBytes {
		return SnapshotCutoverStatus{}, ErrSnapshotCutoverAcknowledgementInvalid
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	entry, ok := coordinator.cutovers[id]
	if !ok {
		return SnapshotCutoverStatus{}, ErrSnapshotCutoverNotFound
	}
	if entry.status.State == SnapshotCutoverCommitted {
		return cloneSnapshotCutoverStatus(entry), ErrSnapshotCutoverTerminal
	}
	if entry.status.State == SnapshotCutoverAborted {
		return cloneSnapshotCutoverStatus(entry), nil
	}
	entry.status.State = SnapshotCutoverAborted
	entry.status.Reason = reason
	return cloneSnapshotCutoverStatus(entry), nil
}

// Forget removes a terminal cutover and releases its bounded slot.
func (coordinator *SnapshotCutoverCoordinator) Forget(id string) error {
	if coordinator == nil {
		return ErrSnapshotCutoverNil
	}
	id, err := normalizeSnapshotCutoverText(id, ErrSnapshotCutoverIDEmpty)
	if err != nil {
		return err
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	entry, ok := coordinator.cutovers[id]
	if !ok {
		return ErrSnapshotCutoverNotFound
	}
	if entry.status.State == SnapshotCutoverPrepared {
		return ErrSnapshotCutoverNotTerminal
	}
	delete(coordinator.cutovers, id)
	return nil
}

// Status returns one detached cutover status.
func (coordinator *SnapshotCutoverCoordinator) Status(id string) (SnapshotCutoverStatus, bool) {
	if coordinator == nil {
		return SnapshotCutoverStatus{}, false
	}
	id = strings.TrimSpace(id)
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	entry, ok := coordinator.cutovers[id]
	if !ok {
		return SnapshotCutoverStatus{}, false
	}
	return cloneSnapshotCutoverStatus(entry), true
}

// Snapshot returns all statuses in deterministic cutover ID order.
func (coordinator *SnapshotCutoverCoordinator) Snapshot() []SnapshotCutoverStatus {
	if coordinator == nil {
		return nil
	}
	coordinator.mu.RLock()
	defer coordinator.mu.RUnlock()
	ids := make([]string, 0, len(coordinator.cutovers))
	for id := range coordinator.cutovers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	statuses := make([]SnapshotCutoverStatus, 0, len(ids))
	for _, id := range ids {
		statuses = append(statuses, cloneSnapshotCutoverStatus(coordinator.cutovers[id]))
	}
	return statuses
}

func normalizeSnapshotCutoverSpec(spec SnapshotCutoverSpec, maxSources int) (SnapshotCutoverSpec, map[string]uint64, error) {
	id, err := normalizeSnapshotCutoverText(spec.ID, ErrSnapshotCutoverIDEmpty)
	if err != nil {
		return SnapshotCutoverSpec{}, nil, err
	}
	if len(spec.Sources) == 0 || len(spec.Sources) > maxSources {
		return SnapshotCutoverSpec{}, nil, ErrSnapshotCutoverSpecInvalid
	}
	sources := make([]SnapshotCutoverSource, len(spec.Sources))
	expected := make(map[string]uint64, len(spec.Sources))
	for index, source := range spec.Sources {
		source.ID, err = normalizeSnapshotCutoverText(source.ID, ErrSnapshotCutoverSourceEmpty)
		if err != nil {
			return SnapshotCutoverSpec{}, nil, err
		}
		if _, exists := expected[source.ID]; exists {
			return SnapshotCutoverSpec{}, nil, ErrSnapshotCutoverSpecInvalid
		}
		expected[source.ID] = source.Generation
		sources[index] = source
	}
	sort.Slice(sources, func(left, right int) bool { return sources[left].ID < sources[right].ID })
	return SnapshotCutoverSpec{ID: id, Timestamp: spec.Timestamp, Sources: sources}, expected, nil
}

func normalizeSnapshotCutoverText(value string, empty error) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", empty
	}
	if len(value) > maxSnapshotCutoverTextBytes {
		return "", ErrSnapshotCutoverSpecInvalid
	}
	return value, nil
}

func normalizeSnapshotCutoverAcknowledgement(id string, acknowledgement SnapshotCutoverAcknowledgement) (string, SnapshotCutoverAcknowledgement, error) {
	normalizedID, err := normalizeSnapshotCutoverText(id, ErrSnapshotCutoverIDEmpty)
	if err != nil {
		return "", SnapshotCutoverAcknowledgement{}, err
	}
	acknowledgement.SourceID, err = normalizeSnapshotCutoverText(acknowledgement.SourceID, ErrSnapshotCutoverSourceEmpty)
	if err != nil {
		return "", SnapshotCutoverAcknowledgement{}, err
	}
	if acknowledgement.Lower > acknowledgement.Upper {
		return "", SnapshotCutoverAcknowledgement{}, ErrSnapshotCutoverAcknowledgementInvalid
	}
	return normalizedID, acknowledgement, nil
}

func (coordinator *SnapshotCutoverCoordinator) acknowledgeLocked(id string, acknowledgement SnapshotCutoverAcknowledgement) (*snapshotCutoverEntry, error) {
	entry, ok := coordinator.cutovers[id]
	if !ok {
		return nil, ErrSnapshotCutoverNotFound
	}
	if entry.status.State != SnapshotCutoverPrepared {
		return entry, ErrSnapshotCutoverTerminal
	}
	expectedGeneration, ok := entry.expected[acknowledgement.SourceID]
	if !ok {
		return entry, ErrSnapshotCutoverSourceUnknown
	}
	if acknowledgement.Generation != expectedGeneration {
		return entry, ErrSnapshotCutoverGenerationMismatch
	}
	if acknowledgement.Lower < entry.status.Timestamp {
		return entry, ErrSnapshotCutoverNotReady
	}
	sourceIndex := entry.sourceIndexes[acknowledgement.SourceID]
	if entry.ackReceived[sourceIndex] {
		previous := entry.acknowledgements[sourceIndex]
		if acknowledgement.Lower < previous.Lower || acknowledgement.Upper < previous.Upper {
			return entry, ErrSnapshotCutoverAcknowledgementRegression
		}
	} else {
		entry.ackReceived[sourceIndex] = true
		entry.acknowledged++
	}
	entry.acknowledgements[sourceIndex] = acknowledgement
	return entry, nil
}

func snapshotCutoverProgress(entry *snapshotCutoverEntry) SnapshotCutoverProgress {
	return SnapshotCutoverProgress{
		State:        entry.status.State,
		TotalSources: entry.status.TotalSources,
		Acknowledged: entry.acknowledged,
		Remaining:    entry.status.TotalSources - entry.acknowledged,
		Ready:        entry.acknowledged == entry.status.TotalSources,
	}
}

func cloneSnapshotCutoverStatus(entry *snapshotCutoverEntry) SnapshotCutoverStatus {
	status := entry.status
	status.Sources = append([]SnapshotCutoverSource(nil), entry.status.Sources...)
	status.Acknowledgements = make([]SnapshotCutoverAcknowledgement, 0, entry.acknowledged)
	for index := range status.Sources {
		if entry.ackReceived[index] {
			status.Acknowledgements = append(status.Acknowledgements, entry.acknowledgements[index])
		}
	}
	status.Acknowledged = len(status.Acknowledgements)
	status.Remaining = status.TotalSources - status.Acknowledged
	return status
}
