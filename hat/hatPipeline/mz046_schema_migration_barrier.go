package hatPipeline

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSchemaMigrationBarrierMaxBarriers bounds retained migration plans
	// when callers do not provide an explicit limit.
	DefaultSchemaMigrationBarrierMaxBarriers = 64
	// DefaultSchemaMigrationBarrierMaxDependencies bounds dependent dataflows
	// in one migration plan when callers do not provide an explicit limit.
	DefaultSchemaMigrationBarrierMaxDependencies = 64
	MaxSchemaMigrationBarrierMaxBarriers         = 100_000
	MaxSchemaMigrationBarrierMaxDependencies     = 1_024
	MaxSchemaMigrationBarrierTextBytes           = 256
)

var (
	ErrSchemaMigrationBarrierNil               = errors.New("schema migration barrier is nil")
	ErrSchemaMigrationBarrierOptionsInvalid    = errors.New("schema migration barrier options are invalid")
	ErrSchemaMigrationBarrierIDEmpty           = errors.New("schema migration barrier ID is empty")
	ErrSchemaMigrationBarrierVersionInvalid    = errors.New("schema migration barrier version is invalid")
	ErrSchemaMigrationBarrierDependencyEmpty   = errors.New("schema migration barrier dependency is empty")
	ErrSchemaMigrationBarrierDependencyLimit   = errors.New("schema migration barrier dependency limit exceeded")
	ErrSchemaMigrationBarrierDependencyUnknown = errors.New("schema migration barrier dependency is unknown")
	ErrSchemaMigrationBarrierNotFound          = errors.New("schema migration barrier was not found")
	ErrSchemaMigrationBarrierAlreadyExists     = errors.New("schema migration barrier already exists")
	ErrSchemaMigrationBarrierCapacity          = errors.New("schema migration barrier capacity exceeded")
	ErrSchemaMigrationBarrierVersionMismatch   = errors.New("schema migration barrier version mismatch")
	ErrSchemaMigrationBarrierNotReady          = errors.New("schema migration barrier dependencies are not ready")
	ErrSchemaMigrationBarrierTerminal          = errors.New("schema migration barrier is terminal")
	ErrSchemaMigrationBarrierNotTerminal       = errors.New("schema migration barrier is not terminal")
)

// SchemaMigrationBarrierOptions bounds the in-memory migration protocol.
// Zero values use conservative defaults. The barrier is opt-in and has no
// effect on existing dataflow or table operations.
type SchemaMigrationBarrierOptions struct {
	MaxBarriers     int
	MaxDependencies int
}

// SchemaMigrationBarrierState identifies a migration barrier phase.
type SchemaMigrationBarrierState string

const (
	SchemaMigrationBarrierPrepared  SchemaMigrationBarrierState = "prepared"
	SchemaMigrationBarrierCommitted SchemaMigrationBarrierState = "committed"
	SchemaMigrationBarrierAborted   SchemaMigrationBarrierState = "aborted"
)

// SchemaMigrationBarrierSpec describes one schema version and the dataflows
// that must acknowledge it before publication.
type SchemaMigrationBarrierSpec struct {
	ID           string
	Version      uint64
	Dependencies []string
}

// SchemaMigrationBarrierStatus is a detached point-in-time barrier state.
// Dependency slices are owned by the returned value.
type SchemaMigrationBarrierStatus struct {
	ID                       string
	Version                  uint64
	State                    SchemaMigrationBarrierState
	Dependencies             []string
	AcknowledgedDependencies []string
	Acknowledged             int
	Remaining                int
}

type schemaMigrationBarrierEntry struct {
	status       SchemaMigrationBarrierStatus
	dependencies map[string]struct{}
	acknowledged map[string]struct{}
}

// SchemaMigrationBarrier coordinates an opt-in schema cutover across named
// dependent dataflows. It is a protocol registry, not a schema store: callers
// remain responsible for applying and validating the actual schema change.
type SchemaMigrationBarrier struct {
	mu              sync.RWMutex
	maxBarriers     int
	maxDependencies int
	barriers        map[string]*schemaMigrationBarrierEntry
}

// NewSchemaMigrationBarrier creates a bounded migration barrier registry.
func NewSchemaMigrationBarrier(options SchemaMigrationBarrierOptions) (*SchemaMigrationBarrier, error) {
	if options.MaxBarriers < 0 || options.MaxDependencies < 0 {
		return nil, ErrSchemaMigrationBarrierOptionsInvalid
	}
	if options.MaxBarriers == 0 {
		options.MaxBarriers = DefaultSchemaMigrationBarrierMaxBarriers
	}
	if options.MaxDependencies == 0 {
		options.MaxDependencies = DefaultSchemaMigrationBarrierMaxDependencies
	}
	if options.MaxBarriers > MaxSchemaMigrationBarrierMaxBarriers || options.MaxDependencies > MaxSchemaMigrationBarrierMaxDependencies {
		return nil, ErrSchemaMigrationBarrierOptionsInvalid
	}
	return &SchemaMigrationBarrier{
		maxBarriers:     options.MaxBarriers,
		maxDependencies: options.MaxDependencies,
		barriers:        make(map[string]*schemaMigrationBarrierEntry, options.MaxBarriers),
	}, nil
}

// Prepare registers a pending schema version and its dependent dataflows.
func (barrier *SchemaMigrationBarrier) Prepare(spec SchemaMigrationBarrierSpec) (SchemaMigrationBarrierStatus, error) {
	if barrier == nil {
		return SchemaMigrationBarrierStatus{}, ErrSchemaMigrationBarrierNil
	}
	id, dependencies, dependencySet, err := normalizeSchemaMigrationBarrierSpec(spec, barrier.maxDependencies)
	if err != nil {
		return SchemaMigrationBarrierStatus{}, err
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	if _, exists := barrier.barriers[id]; exists {
		return SchemaMigrationBarrierStatus{}, ErrSchemaMigrationBarrierAlreadyExists
	}
	if len(barrier.barriers) >= barrier.maxBarriers {
		return SchemaMigrationBarrierStatus{}, ErrSchemaMigrationBarrierCapacity
	}
	entry := &schemaMigrationBarrierEntry{
		status: SchemaMigrationBarrierStatus{
			ID:           id,
			Version:      spec.Version,
			State:        SchemaMigrationBarrierPrepared,
			Dependencies: dependencies,
			Remaining:    len(dependencies),
		},
		dependencies: dependencySet,
		acknowledged: make(map[string]struct{}, len(dependencies)),
	}
	barrier.barriers[id] = entry
	return cloneSchemaMigrationBarrierStatus(entry.status), nil
}

// Acknowledge records that one dependent dataflow has applied the requested
// schema version. Repeated acknowledgements are idempotent. It returns only an
// error so the normal control path does not allocate detached status slices.
func (barrier *SchemaMigrationBarrier) Acknowledge(id, dependency string, version uint64) error {
	_, err := barrier.acknowledge(id, dependency, version, false)
	return err
}

// AcknowledgeStatus is the diagnostic form of Acknowledge and returns a
// detached status snapshot.
func (barrier *SchemaMigrationBarrier) AcknowledgeStatus(id, dependency string, version uint64) (SchemaMigrationBarrierStatus, error) {
	return barrier.acknowledge(id, dependency, version, true)
}

func (barrier *SchemaMigrationBarrier) acknowledge(id, dependency string, version uint64, withStatus bool) (SchemaMigrationBarrierStatus, error) {
	if barrier == nil {
		return SchemaMigrationBarrierStatus{}, ErrSchemaMigrationBarrierNil
	}
	id, err := normalizeSchemaMigrationBarrierText(id, ErrSchemaMigrationBarrierIDEmpty)
	if err != nil {
		return SchemaMigrationBarrierStatus{}, err
	}
	dependency, err = normalizeSchemaMigrationBarrierText(dependency, ErrSchemaMigrationBarrierDependencyEmpty)
	if err != nil {
		return SchemaMigrationBarrierStatus{}, err
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	entry, ok := barrier.barriers[id]
	if !ok {
		return SchemaMigrationBarrierStatus{}, ErrSchemaMigrationBarrierNotFound
	}
	if entry.status.State != SchemaMigrationBarrierPrepared {
		return schemaMigrationBarrierResult(entry.status, withStatus), ErrSchemaMigrationBarrierTerminal
	}
	if entry.status.Version != version {
		return schemaMigrationBarrierResult(entry.status, withStatus), ErrSchemaMigrationBarrierVersionMismatch
	}
	if _, ok := entry.dependencies[dependency]; !ok {
		return schemaMigrationBarrierResult(entry.status, withStatus), ErrSchemaMigrationBarrierDependencyUnknown
	}
	if _, ok := entry.acknowledged[dependency]; !ok {
		entry.acknowledged[dependency] = struct{}{}
		entry.status.AcknowledgedDependencies = append(entry.status.AcknowledgedDependencies, dependency)
		sort.Strings(entry.status.AcknowledgedDependencies)
		entry.status.Acknowledged++
		entry.status.Remaining--
	}
	return schemaMigrationBarrierResult(entry.status, withStatus), nil
}

// Commit publishes the barrier only after every dependency acknowledges the
// exact prepared version. Repeating a successful commit is idempotent.
func (barrier *SchemaMigrationBarrier) Commit(id string, version uint64) (SchemaMigrationBarrierStatus, error) {
	return barrier.finish(id, version, SchemaMigrationBarrierCommitted)
}

// Abort stops a pending migration. Repeating an abort is idempotent.
func (barrier *SchemaMigrationBarrier) Abort(id string, version uint64) (SchemaMigrationBarrierStatus, error) {
	return barrier.finish(id, version, SchemaMigrationBarrierAborted)
}

func (barrier *SchemaMigrationBarrier) finish(id string, version uint64, state SchemaMigrationBarrierState) (SchemaMigrationBarrierStatus, error) {
	if barrier == nil {
		return SchemaMigrationBarrierStatus{}, ErrSchemaMigrationBarrierNil
	}
	id, err := normalizeSchemaMigrationBarrierText(id, ErrSchemaMigrationBarrierIDEmpty)
	if err != nil {
		return SchemaMigrationBarrierStatus{}, err
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	entry, ok := barrier.barriers[id]
	if !ok {
		return SchemaMigrationBarrierStatus{}, ErrSchemaMigrationBarrierNotFound
	}
	if entry.status.Version != version {
		return cloneSchemaMigrationBarrierStatus(entry.status), ErrSchemaMigrationBarrierVersionMismatch
	}
	if entry.status.State != SchemaMigrationBarrierPrepared {
		if entry.status.State == state {
			return cloneSchemaMigrationBarrierStatus(entry.status), nil
		}
		return cloneSchemaMigrationBarrierStatus(entry.status), ErrSchemaMigrationBarrierTerminal
	}
	if state == SchemaMigrationBarrierCommitted && entry.status.Remaining != 0 {
		return cloneSchemaMigrationBarrierStatus(entry.status), ErrSchemaMigrationBarrierNotReady
	}
	entry.status.State = state
	return cloneSchemaMigrationBarrierStatus(entry.status), nil
}

// Forget removes a terminal barrier so its bounded registry slot can be reused.
func (barrier *SchemaMigrationBarrier) Forget(id string) error {
	if barrier == nil {
		return ErrSchemaMigrationBarrierNil
	}
	id, err := normalizeSchemaMigrationBarrierText(id, ErrSchemaMigrationBarrierIDEmpty)
	if err != nil {
		return err
	}
	barrier.mu.Lock()
	defer barrier.mu.Unlock()
	entry, ok := barrier.barriers[id]
	if !ok {
		return ErrSchemaMigrationBarrierNotFound
	}
	if entry.status.State == SchemaMigrationBarrierPrepared {
		return ErrSchemaMigrationBarrierNotTerminal
	}
	delete(barrier.barriers, id)
	return nil
}

// Status returns one detached barrier status.
func (barrier *SchemaMigrationBarrier) Status(id string) (SchemaMigrationBarrierStatus, bool) {
	if barrier == nil {
		return SchemaMigrationBarrierStatus{}, false
	}
	id = strings.TrimSpace(id)
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	entry, ok := barrier.barriers[id]
	if !ok {
		return SchemaMigrationBarrierStatus{}, false
	}
	return cloneSchemaMigrationBarrierStatus(entry.status), true
}

// Snapshot returns all barrier statuses in deterministic ID order.
func (barrier *SchemaMigrationBarrier) Snapshot() []SchemaMigrationBarrierStatus {
	if barrier == nil {
		return nil
	}
	barrier.mu.RLock()
	defer barrier.mu.RUnlock()
	ids := make([]string, 0, len(barrier.barriers))
	for id := range barrier.barriers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	statuses := make([]SchemaMigrationBarrierStatus, 0, len(ids))
	for _, id := range ids {
		statuses = append(statuses, cloneSchemaMigrationBarrierStatus(barrier.barriers[id].status))
	}
	return statuses
}

func normalizeSchemaMigrationBarrierSpec(spec SchemaMigrationBarrierSpec, maxDependencies int) (string, []string, map[string]struct{}, error) {
	id, err := normalizeSchemaMigrationBarrierText(spec.ID, ErrSchemaMigrationBarrierIDEmpty)
	if err != nil {
		return "", nil, nil, err
	}
	if spec.Version == 0 {
		return "", nil, nil, ErrSchemaMigrationBarrierVersionInvalid
	}
	if len(spec.Dependencies) == 0 {
		return "", nil, nil, ErrSchemaMigrationBarrierDependencyEmpty
	}
	if len(spec.Dependencies) > maxDependencies {
		return "", nil, nil, ErrSchemaMigrationBarrierDependencyLimit
	}
	dependencies := make([]string, 0, len(spec.Dependencies))
	dependencySet := make(map[string]struct{}, len(spec.Dependencies))
	for _, rawDependency := range spec.Dependencies {
		dependency, err := normalizeSchemaMigrationBarrierText(rawDependency, ErrSchemaMigrationBarrierDependencyEmpty)
		if err != nil {
			return "", nil, nil, err
		}
		if _, exists := dependencySet[dependency]; exists {
			continue
		}
		dependencySet[dependency] = struct{}{}
		dependencies = append(dependencies, dependency)
	}
	sort.Strings(dependencies)
	return id, dependencies, dependencySet, nil
}

func normalizeSchemaMigrationBarrierText(value string, empty error) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", empty
	}
	if len(value) > MaxSchemaMigrationBarrierTextBytes {
		return "", ErrSchemaMigrationBarrierOptionsInvalid
	}
	return value, nil
}

func cloneSchemaMigrationBarrierStatus(status SchemaMigrationBarrierStatus) SchemaMigrationBarrierStatus {
	status.Dependencies = append([]string(nil), status.Dependencies...)
	status.AcknowledgedDependencies = append([]string(nil), status.AcknowledgedDependencies...)
	return status
}

func schemaMigrationBarrierResult(status SchemaMigrationBarrierStatus, withStatus bool) SchemaMigrationBarrierStatus {
	if !withStatus {
		return SchemaMigrationBarrierStatus{}
	}
	return cloneSchemaMigrationBarrierStatus(status)
}
