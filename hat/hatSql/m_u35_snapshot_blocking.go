package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

const (
	// DefaultSQLSnapshotReadinessMaxObjects bounds the number of tracked source
	// objects when callers do not provide an explicit limit.
	DefaultSQLSnapshotReadinessMaxObjects = 1024
	// MaxSQLSnapshotReadinessMaxObjects prevents accidental unbounded state.
	MaxSQLSnapshotReadinessMaxObjects = 65536
	// DefaultSQLSnapshotReadinessMaxDependents bounds registered query groups.
	DefaultSQLSnapshotReadinessMaxDependents = 256
	// MaxSQLSnapshotReadinessMaxDependents prevents accidental unbounded state.
	MaxSQLSnapshotReadinessMaxDependents = 16384
	// DefaultSQLSnapshotReadinessMaxDependenciesPerDependent bounds one group.
	DefaultSQLSnapshotReadinessMaxDependenciesPerDependent = 64
	// MaxSQLSnapshotReadinessMaxDependenciesPerDependent prevents accidental
	// unbounded dependency scans.
	MaxSQLSnapshotReadinessMaxDependenciesPerDependent = 1024
	maxSQLSnapshotReadinessIDBytes                     = 256
	maxSQLSnapshotReadinessErrorCodeBytes              = 64
)

const (
	// SQLSnapshotReadinessStatePending means an object is still being built.
	SQLSnapshotReadinessStatePending = "pending"
	// SQLSnapshotReadinessStateReady means an object can participate in reads.
	SQLSnapshotReadinessStateReady = "ready"
	// SQLSnapshotReadinessStateFailed means an object cannot become ready.
	SQLSnapshotReadinessStateFailed = "failed"
	// SQLSnapshotReadinessStateCanceled means an object was canceled.
	SQLSnapshotReadinessStateCanceled = "canceled"
)

var (
	ErrSQLSnapshotReadinessNil                 = errors.New("hatSql: snapshot readiness registry is nil")
	ErrSQLSnapshotReadinessOptionsInvalid      = errors.New("hatSql: snapshot readiness options are invalid")
	ErrSQLSnapshotReadinessIDInvalid           = errors.New("hatSql: snapshot readiness identifier is invalid")
	ErrSQLSnapshotReadinessObjectLimit         = errors.New("hatSql: snapshot readiness object limit exceeded")
	ErrSQLSnapshotReadinessDependentLimit      = errors.New("hatSql: snapshot readiness dependent limit exceeded")
	ErrSQLSnapshotReadinessDependencyLimit     = errors.New("hatSql: snapshot readiness dependency limit exceeded")
	ErrSQLSnapshotReadinessDependencyEmpty     = errors.New("hatSql: snapshot readiness dependency list is empty")
	ErrSQLSnapshotReadinessDependencyDuplicate = errors.New("hatSql: snapshot readiness dependency is duplicated")
	ErrSQLSnapshotReadinessDuplicateObject     = errors.New("hatSql: snapshot readiness object is duplicated")
	ErrSQLSnapshotReadinessDuplicateDependent  = errors.New("hatSql: snapshot readiness dependent is duplicated")
	ErrSQLSnapshotReadinessUnknownObject       = errors.New("hatSql: snapshot readiness object is unknown")
	ErrSQLSnapshotReadinessUnknownDependent    = errors.New("hatSql: snapshot readiness dependent is unknown")
	ErrSQLSnapshotReadinessContextNil          = errors.New("hatSql: snapshot readiness context is nil")
	ErrSQLSnapshotReadinessProgressRegression  = errors.New("hatSql: snapshot readiness progress regressed")
	ErrSQLSnapshotReadinessInvalidTransition   = errors.New("hatSql: snapshot readiness state transition is invalid")
	ErrSQLSnapshotReadinessErrorCodeInvalid    = errors.New("hatSql: snapshot readiness error code is invalid")
	ErrSQLSnapshotReadinessFailed              = errors.New("hatSql: snapshot readiness dependency failed")
	ErrSQLSnapshotReadinessCanceled            = errors.New("hatSql: snapshot readiness dependency canceled")
)

// SQLSnapshotReadinessRegistryOptions bounds the opt-in readiness registry.
// Zero values select bounded defaults.
type SQLSnapshotReadinessRegistryOptions struct {
	MaxObjects                  int
	MaxDependents               int
	MaxDependenciesPerDependent int
}

// SQLSnapshotReadinessObjectStatus is one detached object status from a
// consistent registry snapshot.
type SQLSnapshotReadinessObjectStatus struct {
	ID        string
	State     string
	Progress  uint64
	ErrorCode string
}

// SQLSnapshotReadinessSnapshot is a read-consistent view of one dependent's
// required objects. Generation changes whenever the registry state changes.
type SQLSnapshotReadinessSnapshot struct {
	Generation uint64
	Dependent  string
	Ready      bool
	Objects    []SQLSnapshotReadinessObjectStatus
	BlockedBy  []string
}

type sqlSnapshotReadinessObject struct {
	state     string
	progress  uint64
	errorCode string
}

// SQLSnapshotReadinessRegistry tracks bounded source readiness and blocks
// dependents until every required object is ready. It is opt-in and does not
// alter ordinary query admission or execution.
type SQLSnapshotReadinessRegistry struct {
	mu                          sync.Mutex
	maxObjects                  int
	maxDependents               int
	maxDependenciesPerDependent int
	objects                     map[string]sqlSnapshotReadinessObject
	dependents                  map[string][]string
	generation                  uint64
	notify                      chan struct{}
}

// NewSQLSnapshotReadinessRegistry creates a bounded readiness registry.
func NewSQLSnapshotReadinessRegistry(options SQLSnapshotReadinessRegistryOptions) (*SQLSnapshotReadinessRegistry, error) {
	if options.MaxObjects < 0 || options.MaxDependents < 0 || options.MaxDependenciesPerDependent < 0 {
		return nil, ErrSQLSnapshotReadinessOptionsInvalid
	}
	maxObjects := options.MaxObjects
	if maxObjects == 0 {
		maxObjects = DefaultSQLSnapshotReadinessMaxObjects
	}
	maxDependents := options.MaxDependents
	if maxDependents == 0 {
		maxDependents = DefaultSQLSnapshotReadinessMaxDependents
	}
	maxDependencies := options.MaxDependenciesPerDependent
	if maxDependencies == 0 {
		maxDependencies = DefaultSQLSnapshotReadinessMaxDependenciesPerDependent
	}
	if maxObjects > MaxSQLSnapshotReadinessMaxObjects || maxDependents > MaxSQLSnapshotReadinessMaxDependents || maxDependencies > MaxSQLSnapshotReadinessMaxDependenciesPerDependent {
		return nil, ErrSQLSnapshotReadinessOptionsInvalid
	}
	return &SQLSnapshotReadinessRegistry{
		maxObjects:                  maxObjects,
		maxDependents:               maxDependents,
		maxDependenciesPerDependent: maxDependencies,
		objects:                     make(map[string]sqlSnapshotReadinessObject, maxObjects),
		dependents:                  make(map[string][]string, maxDependents),
		notify:                      make(chan struct{}),
	}, nil
}

// RegisterObject adds one initially pending object to the registry.
func (registry *SQLSnapshotReadinessRegistry) RegisterObject(id string) error {
	if registry == nil {
		return ErrSQLSnapshotReadinessNil
	}
	normalized, err := normalizeSQLSnapshotReadinessID(id)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.objects[normalized]; exists {
		return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessDuplicateObject, normalized)
	}
	if len(registry.objects) >= registry.maxObjects {
		return ErrSQLSnapshotReadinessObjectLimit
	}
	registry.objects[normalized] = sqlSnapshotReadinessObject{state: SQLSnapshotReadinessStatePending}
	registry.bumpLocked()
	return nil
}

// RegisterDependent declares the objects required before one dependent can
// be admitted. Dependencies are normalized and retained in deterministic
// order.
func (registry *SQLSnapshotReadinessRegistry) RegisterDependent(id string, dependencies []string) error {
	if registry == nil {
		return ErrSQLSnapshotReadinessNil
	}
	normalizedID, err := normalizeSQLSnapshotReadinessID(id)
	if err != nil {
		return err
	}
	normalizedDependencies, err := registry.normalizeDependencies(dependencies)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, exists := registry.dependents[normalizedID]; exists {
		return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessDuplicateDependent, normalizedID)
	}
	if len(registry.dependents) >= registry.maxDependents {
		return ErrSQLSnapshotReadinessDependentLimit
	}
	for _, dependency := range normalizedDependencies {
		if _, exists := registry.objects[dependency]; !exists {
			return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessUnknownObject, dependency)
		}
	}
	registry.dependents[normalizedID] = normalizedDependencies
	registry.bumpLocked()
	return nil
}

// Advance records monotone progress for a pending object.
func (registry *SQLSnapshotReadinessRegistry) Advance(id string, progress uint64) error {
	return registry.updateObject(id, progress, SQLSnapshotReadinessStatePending, "")
}

// MarkReady records final progress and makes an object available to its
// dependents.
func (registry *SQLSnapshotReadinessRegistry) MarkReady(id string, progress uint64) error {
	return registry.updateObject(id, progress, SQLSnapshotReadinessStateReady, "")
}

// Fail permanently blocks dependents of an object with a bounded diagnostic
// code. The code is metadata only and must not contain arbitrary text.
func (registry *SQLSnapshotReadinessRegistry) Fail(id, errorCode string) error {
	if registry == nil {
		return ErrSQLSnapshotReadinessNil
	}
	normalizedID, err := normalizeSQLSnapshotReadinessID(id)
	if err != nil {
		return err
	}
	normalizedCode, err := normalizeSQLSnapshotReadinessErrorCode(errorCode)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	object, exists := registry.objects[normalizedID]
	if !exists {
		return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessUnknownObject, normalizedID)
	}
	if object.state != SQLSnapshotReadinessStatePending {
		return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessInvalidTransition, normalizedID)
	}
	object.state = SQLSnapshotReadinessStateFailed
	object.errorCode = normalizedCode
	registry.objects[normalizedID] = object
	registry.bumpLocked()
	return nil
}

// Cancel permanently cancels an object and wakes all dependents waiting on
// it.
func (registry *SQLSnapshotReadinessRegistry) Cancel(id string) error {
	if registry == nil {
		return ErrSQLSnapshotReadinessNil
	}
	normalizedID, err := normalizeSQLSnapshotReadinessID(id)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	object, exists := registry.objects[normalizedID]
	if !exists {
		return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessUnknownObject, normalizedID)
	}
	if object.state != SQLSnapshotReadinessStatePending {
		return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessInvalidTransition, normalizedID)
	}
	object.state = SQLSnapshotReadinessStateCanceled
	registry.objects[normalizedID] = object
	registry.bumpLocked()
	return nil
}

// Snapshot returns a detached read-consistent status for one dependent.
func (registry *SQLSnapshotReadinessRegistry) Snapshot(dependent string) (SQLSnapshotReadinessSnapshot, error) {
	if registry == nil {
		return SQLSnapshotReadinessSnapshot{}, ErrSQLSnapshotReadinessNil
	}
	normalizedID, err := normalizeSQLSnapshotReadinessID(dependent)
	if err != nil {
		return SQLSnapshotReadinessSnapshot{}, err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	return registry.snapshotLocked(normalizedID)
}

// Wait blocks until every object required by dependent is ready, an object
// reaches a terminal failed/canceled state, or ctx is canceled.
func (registry *SQLSnapshotReadinessRegistry) Wait(ctx context.Context, dependent string) (SQLSnapshotReadinessSnapshot, error) {
	if registry == nil {
		return SQLSnapshotReadinessSnapshot{}, ErrSQLSnapshotReadinessNil
	}
	if ctx == nil {
		return SQLSnapshotReadinessSnapshot{}, ErrSQLSnapshotReadinessContextNil
	}
	normalizedID, err := normalizeSQLSnapshotReadinessID(dependent)
	if err != nil {
		return SQLSnapshotReadinessSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return SQLSnapshotReadinessSnapshot{}, err
	}
	for {
		registry.mu.Lock()
		snapshot, err := registry.snapshotLocked(normalizedID)
		if err != nil {
			registry.mu.Unlock()
			return SQLSnapshotReadinessSnapshot{}, err
		}
		if snapshot.Ready {
			registry.mu.Unlock()
			return snapshot, nil
		}
		if err := sqlSnapshotReadinessTerminalError(snapshot); err != nil {
			registry.mu.Unlock()
			return snapshot, err
		}
		notify := registry.notify
		registry.mu.Unlock()
		select {
		case <-ctx.Done():
			return snapshot, ctx.Err()
		case <-notify:
		}
	}
}

func (registry *SQLSnapshotReadinessRegistry) updateObject(id string, progress uint64, state, errorCode string) error {
	if registry == nil {
		return ErrSQLSnapshotReadinessNil
	}
	normalizedID, err := normalizeSQLSnapshotReadinessID(id)
	if err != nil {
		return err
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	object, exists := registry.objects[normalizedID]
	if !exists {
		return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessUnknownObject, normalizedID)
	}
	if object.state != SQLSnapshotReadinessStatePending {
		return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessInvalidTransition, normalizedID)
	}
	if progress < object.progress {
		return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessProgressRegression, normalizedID)
	}
	object.progress = progress
	object.state = state
	object.errorCode = errorCode
	registry.objects[normalizedID] = object
	registry.bumpLocked()
	return nil
}

func (registry *SQLSnapshotReadinessRegistry) normalizeDependencies(dependencies []string) ([]string, error) {
	if len(dependencies) == 0 {
		return nil, ErrSQLSnapshotReadinessDependencyEmpty
	}
	if len(dependencies) > registry.maxDependenciesPerDependent {
		return nil, ErrSQLSnapshotReadinessDependencyLimit
	}
	normalized := make([]string, len(dependencies))
	seen := make(map[string]struct{}, len(dependencies))
	for index, dependency := range dependencies {
		value, err := normalizeSQLSnapshotReadinessID(dependency)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[value]; exists {
			return nil, fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessDependencyDuplicate, value)
		}
		seen[value] = struct{}{}
		normalized[index] = value
	}
	sort.Strings(normalized)
	return normalized, nil
}

func (registry *SQLSnapshotReadinessRegistry) snapshotLocked(dependent string) (SQLSnapshotReadinessSnapshot, error) {
	dependencies, exists := registry.dependents[dependent]
	if !exists {
		return SQLSnapshotReadinessSnapshot{}, fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessUnknownDependent, dependent)
	}
	snapshot := SQLSnapshotReadinessSnapshot{
		Generation: registry.generation,
		Dependent:  dependent,
		Objects:    make([]SQLSnapshotReadinessObjectStatus, len(dependencies)),
		BlockedBy:  make([]string, 0, len(dependencies)),
	}
	for index, dependency := range dependencies {
		object := registry.objects[dependency]
		snapshot.Objects[index] = SQLSnapshotReadinessObjectStatus{
			ID:        dependency,
			State:     object.state,
			Progress:  object.progress,
			ErrorCode: object.errorCode,
		}
		if object.state != SQLSnapshotReadinessStateReady {
			snapshot.BlockedBy = append(snapshot.BlockedBy, dependency)
		}
	}
	snapshot.Ready = len(snapshot.BlockedBy) == 0
	return snapshot, nil
}

func (registry *SQLSnapshotReadinessRegistry) bumpLocked() {
	registry.generation++
	if registry.generation == 0 {
		registry.generation = 1
	}
	close(registry.notify)
	registry.notify = make(chan struct{})
}

func sqlSnapshotReadinessTerminalError(snapshot SQLSnapshotReadinessSnapshot) error {
	for _, object := range snapshot.Objects {
		switch object.State {
		case SQLSnapshotReadinessStateFailed:
			return fmt.Errorf("%w: %s (%s)", ErrSQLSnapshotReadinessFailed, object.ID, object.ErrorCode)
		case SQLSnapshotReadinessStateCanceled:
			return fmt.Errorf("%w: %s", ErrSQLSnapshotReadinessCanceled, object.ID)
		}
	}
	return nil
}

func normalizeSQLSnapshotReadinessID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxSQLSnapshotReadinessIDBytes {
		return "", ErrSQLSnapshotReadinessIDInvalid
	}
	for index := 0; index < len(value); index++ {
		if value[index] == 0 || value[index] < 0x20 {
			return "", ErrSQLSnapshotReadinessIDInvalid
		}
	}
	return value, nil
}

func normalizeSQLSnapshotReadinessErrorCode(value string) (string, error) {
	value = strings.ToLower(strings.TrimSpace(value))
	if len(value) > maxSQLSnapshotReadinessErrorCodeBytes {
		return "", ErrSQLSnapshotReadinessErrorCodeInvalid
	}
	if value == "" {
		return value, nil
	}
	for index := 0; index < len(value); index++ {
		character := value[index]
		if !(character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' || character == '-' || character == '.') {
			return "", ErrSQLSnapshotReadinessErrorCodeInvalid
		}
	}
	return value, nil
}
