package hatSchema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// SpaceMigrationPlanVersion identifies the serialized plan contract.
	SpaceMigrationPlanVersion uint16 = 1
	// SpaceMigrationSnapshotVersion identifies the serialized manager state.
	SpaceMigrationSnapshotVersion uint16 = 1
	// MaxSpaceMigrationSteps bounds validation and retained progress.
	MaxSpaceMigrationSteps = 1024
	// MaxSpaceMigrationPlans bounds the in-memory registry.
	MaxSpaceMigrationPlans = 4096
	// MaxSpaceMigrationNameBytes bounds names retained in a plan.
	MaxSpaceMigrationNameBytes = 256
)

var (
	// ErrSpaceMigrationPlanInvalid reports malformed or unsupported plan data.
	ErrSpaceMigrationPlanInvalid = errors.New("hatSchema: space migration plan is invalid")
	// ErrSpaceMigrationPlanExists reports duplicate plan registration.
	ErrSpaceMigrationPlanExists = errors.New("hatSchema: space migration plan already exists")
	// ErrSpaceMigrationNotFound reports an unknown registered plan.
	ErrSpaceMigrationNotFound = errors.New("hatSchema: space migration plan is not registered")
	// ErrSpaceMigrationAlreadyStarted reports a plan with an existing state.
	ErrSpaceMigrationAlreadyStarted = errors.New("hatSchema: space migration plan already started")
	// ErrSpaceMigrationPrecondition reports a stale or mismatched base schema.
	ErrSpaceMigrationPrecondition = errors.New("hatSchema: space migration precondition failed")
	// ErrSpaceMigrationCompatibility reports an unsafe rolling transition.
	ErrSpaceMigrationCompatibility = errors.New("hatSchema: space migration is not rolling-compatible")
	// ErrSpaceMigrationState reports an invalid lifecycle operation.
	ErrSpaceMigrationState = errors.New("hatSchema: space migration state is invalid")
	// ErrSpaceMigrationBusy reports an operation already in progress.
	ErrSpaceMigrationBusy = errors.New("hatSchema: space migration operation is already in progress")
	// ErrSpaceMigrationSnapshotInvalid reports an invalid restore image.
	ErrSpaceMigrationSnapshotInvalid = errors.New("hatSchema: space migration snapshot is invalid")
)

// SpaceMigrationCompatibility controls whether a plan may run while old and
// new clients coexist. Rolling is the safe default; exclusive mode is an
// explicit escape hatch for a caller that has already fenced old clients.
type SpaceMigrationCompatibility string

const (
	SpaceMigrationCompatibilityRolling   SpaceMigrationCompatibility = "rolling"
	SpaceMigrationCompatibilityExclusive SpaceMigrationCompatibility = "exclusive"
)

// SpaceMigrationPlan is a named, versioned sequence of schema migrations for
// one space. The base fingerprint prevents applying a valid plan to stale data.
type SpaceMigrationPlan struct {
	PlanVersion     uint16                      `json:"plan_version"`
	ID              string                      `json:"id"`
	Space           string                      `json:"space"`
	BaseVersion     uint64                      `json:"base_version"`
	BaseFingerprint string                      `json:"base_fingerprint"`
	Compatibility   SpaceMigrationCompatibility `json:"compatibility"`
	Migrations      []Migration                 `json:"migrations"`
}

// Validate checks the portable plan envelope without inspecting a concrete
// base schema. Start performs the additional sequential preview checks.
func (plan SpaceMigrationPlan) Validate() error {
	if plan.PlanVersion != SpaceMigrationPlanVersion {
		return fmt.Errorf("%w: plan version %d is unsupported", ErrSpaceMigrationPlanInvalid, plan.PlanVersion)
	}
	if err := validateSpaceMigrationName(plan.ID, "id"); err != nil {
		return err
	}
	if err := validateSpaceMigrationName(plan.Space, "space"); err != nil {
		return err
	}
	if plan.BaseVersion == 0 {
		return fmt.Errorf("%w: base version must be positive", ErrSpaceMigrationPlanInvalid)
	}
	if strings.TrimSpace(plan.BaseFingerprint) == "" || !utf8.ValidString(plan.BaseFingerprint) {
		return fmt.Errorf("%w: base fingerprint is required", ErrSpaceMigrationPlanInvalid)
	}
	compatibility := plan.Compatibility
	if compatibility == "" {
		compatibility = SpaceMigrationCompatibilityRolling
	}
	if compatibility != SpaceMigrationCompatibilityRolling && compatibility != SpaceMigrationCompatibilityExclusive {
		return fmt.Errorf("%w: compatibility %q is unsupported", ErrSpaceMigrationPlanInvalid, compatibility)
	}
	if len(plan.Migrations) == 0 || len(plan.Migrations) > MaxSpaceMigrationSteps {
		return fmt.Errorf("%w: migrations must contain 1..%d steps", ErrSpaceMigrationPlanInvalid, MaxSpaceMigrationSteps)
	}
	for index, migration := range plan.Migrations {
		if err := migration.Validate(); err != nil {
			return fmt.Errorf("%w: migration %d: %v", ErrSpaceMigrationPlanInvalid, index, err)
		}
		wantVersion := plan.BaseVersion + uint64(index) + 1
		if migration.Version != wantVersion {
			return fmt.Errorf("%w: migration %d has version %d, want %d", ErrSpaceMigrationPlanInvalid, index, migration.Version, wantVersion)
		}
	}
	return nil
}

// SpaceMigrationDirection identifies whether a callback applies or reverses a
// migration step.
type SpaceMigrationDirection string

const (
	SpaceMigrationDirectionUp   SpaceMigrationDirection = "up"
	SpaceMigrationDirectionDown SpaceMigrationDirection = "down"
)

// SpaceMigrationOperation is an immutable callback view of one step. The
// manager never exposes its internal schema backing to the callback.
type SpaceMigrationOperation struct {
	PlanID    string                  `json:"plan_id"`
	Space     string                  `json:"space"`
	Step      int                     `json:"step"`
	Direction SpaceMigrationDirection `json:"direction"`
	Migration Migration               `json:"migration"`
	From      Schema                  `json:"from"`
	To        Schema                  `json:"to"`
}

// SpaceMigrationFunc executes one schema/data conversion step. The callback
// owns row conversion, locks, durable publication, and idempotency. The
// manager publishes the next schema only after the callback succeeds.
type SpaceMigrationFunc func(context.Context, SpaceMigrationOperation) error

// SpaceMigrationStatus is the durable lifecycle state of one plan.
type SpaceMigrationStatus string

const (
	SpaceMigrationStatusPending     SpaceMigrationStatus = "pending"
	SpaceMigrationStatusRunning     SpaceMigrationStatus = "running"
	SpaceMigrationStatusFailed      SpaceMigrationStatus = "failed"
	SpaceMigrationStatusApplied     SpaceMigrationStatus = "applied"
	SpaceMigrationStatusRollingBack SpaceMigrationStatus = "rolling_back"
	SpaceMigrationStatusRolledBack  SpaceMigrationStatus = "rolled_back"
)

// SpaceMigrationProgress is a clone-safe point-in-time lifecycle report.
type SpaceMigrationProgress struct {
	PlanID         string               `json:"plan_id"`
	Space          string               `json:"space"`
	Status         SpaceMigrationStatus `json:"status"`
	BaseVersion    uint64               `json:"base_version"`
	CurrentVersion uint64               `json:"current_version"`
	TargetVersion  uint64               `json:"target_version"`
	NextStep       int                  `json:"next_step"`
	TotalSteps     int                  `json:"total_steps"`
	LastError      string               `json:"last_error,omitempty"`
}

// SpaceMigrationStateSnapshot is the persisted state for one started plan.
// Base and Current make restore self-validating without consulting live data.
type SpaceMigrationStateSnapshot struct {
	PlanID         string               `json:"plan_id"`
	Space          string               `json:"space"`
	Status         SpaceMigrationStatus `json:"status"`
	BaseVersion    uint64               `json:"base_version"`
	CurrentVersion uint64               `json:"current_version"`
	NextStep       int                  `json:"next_step"`
	LastError      string               `json:"last_error,omitempty"`
	Base           Schema               `json:"base"`
	Current        Schema               `json:"current"`
}

// SpaceMigrationSnapshot is a serializable manager image. Callers can persist
// it using their existing atomic backup mechanism; the manager performs no
// filesystem or network I/O by itself.
type SpaceMigrationSnapshot struct {
	SnapshotVersion uint16                        `json:"snapshot_version"`
	Plans           []SpaceMigrationPlan          `json:"plans"`
	States          []SpaceMigrationStateSnapshot `json:"states"`
}

type spaceMigrationState struct {
	planID    string
	space     string
	base      Schema
	current   Schema
	nextStep  int
	status    SpaceMigrationStatus
	lastError string
	inFlight  bool
}

// SpaceMigrationManager owns plans and resumable state for named spaces. It
// is an in-memory control-plane component; callers persist Snapshot images.
type SpaceMigrationManager struct {
	mu     sync.RWMutex
	plans  map[string]SpaceMigrationPlan
	states map[string]*spaceMigrationState
}

// NewSpaceMigrationManager creates an empty manager with safe bounded maps.
func NewSpaceMigrationManager() *SpaceMigrationManager {
	return &SpaceMigrationManager{
		plans:  make(map[string]SpaceMigrationPlan),
		states: make(map[string]*spaceMigrationState),
	}
}

// RegisterPlan validates and registers one plan by its stable ID.
func (manager *SpaceMigrationManager) RegisterPlan(plan SpaceMigrationPlan) error {
	if manager == nil {
		return ErrSpaceMigrationState
	}
	normalized, err := normalizeSpaceMigrationPlan(plan)
	if err != nil {
		return err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.plans) >= MaxSpaceMigrationPlans {
		return fmt.Errorf("%w: maximum plans %d exceeded", ErrSpaceMigrationPlanInvalid, MaxSpaceMigrationPlans)
	}
	if manager.plans == nil {
		manager.plans = make(map[string]SpaceMigrationPlan)
	}
	if _, exists := manager.plans[normalized.ID]; exists {
		return fmt.Errorf("%w: %q", ErrSpaceMigrationPlanExists, normalized.ID)
	}
	manager.plans[normalized.ID] = normalized
	return nil
}

// Start validates the current schema against the plan and creates pending
// state. The operation is rejected when the fingerprint is stale or a rolling
// step would break old/new client coexistence.
func (manager *SpaceMigrationManager) Start(planID string, schema Schema) error {
	if manager == nil {
		return ErrSpaceMigrationState
	}
	planID = strings.TrimSpace(planID)
	manager.mu.RLock()
	plan, exists := manager.plans[planID]
	_, started := manager.states[planID]
	manager.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w: %q", ErrSpaceMigrationNotFound, planID)
	}
	if started {
		return fmt.Errorf("%w: %q", ErrSpaceMigrationAlreadyStarted, planID)
	}
	if err := schema.Validate(); err != nil {
		return fmt.Errorf("%w: schema: %v", ErrSpaceMigrationPrecondition, err)
	}
	if schema.Version != plan.BaseVersion || schema.Fingerprint() != plan.BaseFingerprint {
		return fmt.Errorf("%w: plan %q expects version %d fingerprint %q", ErrSpaceMigrationPrecondition, planID, plan.BaseVersion, plan.BaseFingerprint)
	}
	if err := validateSpaceMigrationSequence(plan, schema); err != nil {
		return err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.states[planID]; exists {
		return fmt.Errorf("%w: %q", ErrSpaceMigrationAlreadyStarted, planID)
	}
	if manager.states == nil {
		manager.states = make(map[string]*spaceMigrationState)
	}
	manager.states[planID] = &spaceMigrationState{
		planID:   planID,
		space:    plan.Space,
		base:     schema.Clone(),
		current:  schema.Clone(),
		nextStep: 0,
		status:   SpaceMigrationStatusPending,
	}
	return nil
}

// Apply runs all remaining steps, or resumes a failed plan. A successful
// callback is the commit point for one step. Repeated calls are idempotent
// after the plan reaches Applied.
func (manager *SpaceMigrationManager) Apply(ctx context.Context, planID string, apply SpaceMigrationFunc) error {
	if manager == nil {
		return ErrSpaceMigrationState
	}
	if apply == nil {
		return fmt.Errorf("%w: apply callback is required", ErrSpaceMigrationState)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return manager.fail(planID, err)
		}
		operation, done, err := manager.claimUp(planID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		expectedFrom := operation.From.Fingerprint()
		expectedTo := operation.To.Clone()
		if err := apply(ctx, operation); err != nil {
			manager.recordFailure(planID, err)
			return err
		}
		if err := ctx.Err(); err != nil {
			manager.recordFailure(planID, err)
			return err
		}
		if err := manager.commitUp(planID, operation.Step, expectedFrom, expectedTo); err != nil {
			return err
		}
	}
}

// Rollback reverses every committed step in reverse order. A failed rollback
// remains resumable from the same step, and an already rolled-back plan is a
// successful no-op.
func (manager *SpaceMigrationManager) Rollback(ctx context.Context, planID string, rollback SpaceMigrationFunc) error {
	if manager == nil {
		return ErrSpaceMigrationState
	}
	if rollback == nil {
		return fmt.Errorf("%w: rollback callback is required", ErrSpaceMigrationState)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return manager.fail(planID, err)
		}
		operation, done, err := manager.claimDown(planID)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		expectedFrom := operation.From.Fingerprint()
		expectedTo := operation.To.Clone()
		if err := rollback(ctx, operation); err != nil {
			manager.recordFailure(planID, err)
			return err
		}
		if err := ctx.Err(); err != nil {
			manager.recordFailure(planID, err)
			return err
		}
		if err := manager.commitDown(planID, operation.Step, expectedFrom, expectedTo); err != nil {
			return err
		}
	}
}

// Progress returns a stable lifecycle report for a started plan.
func (manager *SpaceMigrationManager) Progress(planID string) (SpaceMigrationProgress, bool) {
	if manager == nil {
		return SpaceMigrationProgress{}, false
	}
	planID = strings.TrimSpace(planID)
	manager.mu.RLock()
	plan, planOK := manager.plans[planID]
	state, stateOK := manager.states[planID]
	if !stateOK {
		manager.mu.RUnlock()
		return SpaceMigrationProgress{}, false
	}
	progress := spaceMigrationProgress(plan, state)
	manager.mu.RUnlock()
	if !planOK {
		return SpaceMigrationProgress{}, false
	}
	return progress, true
}

// Schema returns an independent current schema for a started plan.
func (manager *SpaceMigrationManager) Schema(planID string) (Schema, bool) {
	if manager == nil {
		return Schema{}, false
	}
	planID = strings.TrimSpace(planID)
	manager.mu.RLock()
	state, ok := manager.states[planID]
	if !ok {
		manager.mu.RUnlock()
		return Schema{}, false
	}
	schema := state.current.Clone()
	manager.mu.RUnlock()
	return schema, true
}

// Snapshot returns an independent deterministic manager image.
func (manager *SpaceMigrationManager) Snapshot() SpaceMigrationSnapshot {
	if manager == nil {
		return SpaceMigrationSnapshot{SnapshotVersion: SpaceMigrationSnapshotVersion}
	}
	manager.mu.RLock()
	planIDs := make([]string, 0, len(manager.plans))
	for planID := range manager.plans {
		planIDs = append(planIDs, planID)
	}
	sort.Strings(planIDs)
	snapshot := SpaceMigrationSnapshot{
		SnapshotVersion: SpaceMigrationSnapshotVersion,
		Plans:           make([]SpaceMigrationPlan, 0, len(planIDs)),
		States:          make([]SpaceMigrationStateSnapshot, 0, len(manager.states)),
	}
	for _, planID := range planIDs {
		snapshot.Plans = append(snapshot.Plans, cloneSpaceMigrationPlan(manager.plans[planID]))
	}
	stateIDs := make([]string, 0, len(manager.states))
	for planID := range manager.states {
		stateIDs = append(stateIDs, planID)
	}
	sort.Strings(stateIDs)
	for _, planID := range stateIDs {
		state := manager.states[planID]
		snapshot.States = append(snapshot.States, SpaceMigrationStateSnapshot{
			PlanID:         state.planID,
			Space:          state.space,
			Status:         state.status,
			BaseVersion:    state.base.Version,
			CurrentVersion: state.current.Version,
			NextStep:       state.nextStep,
			LastError:      state.lastError,
			Base:           state.base.Clone(),
			Current:        state.current.Clone(),
		})
	}
	manager.mu.RUnlock()
	return snapshot
}

// Restore validates and atomically installs a manager image. In-flight states
// are recovered as Failed so callers can retry with idempotent callbacks.
func (manager *SpaceMigrationManager) Restore(snapshot SpaceMigrationSnapshot) error {
	if manager == nil {
		return ErrSpaceMigrationState
	}
	if snapshot.SnapshotVersion != SpaceMigrationSnapshotVersion {
		return fmt.Errorf("%w: snapshot version %d is unsupported", ErrSpaceMigrationSnapshotInvalid, snapshot.SnapshotVersion)
	}
	if len(snapshot.Plans) > MaxSpaceMigrationPlans || len(snapshot.States) > MaxSpaceMigrationPlans {
		return fmt.Errorf("%w: snapshot exceeds plan limit", ErrSpaceMigrationSnapshotInvalid)
	}
	plans := make(map[string]SpaceMigrationPlan, len(snapshot.Plans))
	for index, rawPlan := range snapshot.Plans {
		plan, err := normalizeSpaceMigrationPlan(rawPlan)
		if err != nil {
			return fmt.Errorf("%w: plan %d: %v", ErrSpaceMigrationSnapshotInvalid, index, err)
		}
		if _, exists := plans[plan.ID]; exists {
			return fmt.Errorf("%w: duplicate plan %q", ErrSpaceMigrationSnapshotInvalid, plan.ID)
		}
		plans[plan.ID] = plan
	}
	states := make(map[string]*spaceMigrationState, len(snapshot.States))
	for index, rawState := range snapshot.States {
		plan, exists := plans[rawState.PlanID]
		if !exists {
			return fmt.Errorf("%w: state %d references unknown plan %q", ErrSpaceMigrationSnapshotInvalid, index, rawState.PlanID)
		}
		state, err := restoreSpaceMigrationState(plan, rawState)
		if err != nil {
			return fmt.Errorf("%w: state %d: %v", ErrSpaceMigrationSnapshotInvalid, index, err)
		}
		if _, exists := states[rawState.PlanID]; exists {
			return fmt.Errorf("%w: duplicate state %q", ErrSpaceMigrationSnapshotInvalid, rawState.PlanID)
		}
		states[rawState.PlanID] = state
	}
	manager.mu.Lock()
	manager.plans = plans
	manager.states = states
	manager.mu.Unlock()
	return nil
}

func (manager *SpaceMigrationManager) claimUp(planID string) (SpaceMigrationOperation, bool, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	plan, state, err := manager.lookupLocked(planID)
	if err != nil {
		return SpaceMigrationOperation{}, false, err
	}
	switch state.status {
	case SpaceMigrationStatusApplied:
		return SpaceMigrationOperation{}, true, nil
	case SpaceMigrationStatusPending, SpaceMigrationStatusFailed:
		if state.nextStep >= len(plan.Migrations) {
			state.status = SpaceMigrationStatusApplied
			state.lastError = ""
			return SpaceMigrationOperation{}, true, nil
		}
	case SpaceMigrationStatusRunning, SpaceMigrationStatusRollingBack:
		return SpaceMigrationOperation{}, false, ErrSpaceMigrationBusy
	case SpaceMigrationStatusRolledBack:
		return SpaceMigrationOperation{}, false, fmt.Errorf("%w: rolled-back plan cannot be applied", ErrSpaceMigrationState)
	default:
		return SpaceMigrationOperation{}, false, fmt.Errorf("%w: unknown status %q", ErrSpaceMigrationState, state.status)
	}
	migration := plan.Migrations[state.nextStep]
	to, err := Preview(&state.current, migration)
	if err != nil {
		state.status = SpaceMigrationStatusFailed
		state.lastError = err.Error()
		return SpaceMigrationOperation{}, false, err
	}
	operation := SpaceMigrationOperation{
		PlanID:    state.planID,
		Space:     state.space,
		Step:      state.nextStep,
		Direction: SpaceMigrationDirectionUp,
		Migration: cloneMigration(migration),
		From:      state.current.Clone(),
		To:        to.Clone(),
	}
	state.status = SpaceMigrationStatusRunning
	state.lastError = ""
	state.inFlight = true
	return operation, false, nil
}

func (manager *SpaceMigrationManager) claimDown(planID string) (SpaceMigrationOperation, bool, error) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	plan, state, err := manager.lookupLocked(planID)
	if err != nil {
		return SpaceMigrationOperation{}, false, err
	}
	if state.nextStep == 0 {
		if state.status == SpaceMigrationStatusRolledBack {
			return SpaceMigrationOperation{}, true, nil
		}
		return SpaceMigrationOperation{}, false, fmt.Errorf("%w: plan has no applied steps", ErrSpaceMigrationState)
	}
	switch state.status {
	case SpaceMigrationStatusApplied, SpaceMigrationStatusFailed, SpaceMigrationStatusRollingBack:
		if state.inFlight {
			return SpaceMigrationOperation{}, false, ErrSpaceMigrationBusy
		}
	case SpaceMigrationStatusPending, SpaceMigrationStatusRunning:
		return SpaceMigrationOperation{}, false, fmt.Errorf("%w: plan is not applied", ErrSpaceMigrationState)
	case SpaceMigrationStatusRolledBack:
		return SpaceMigrationOperation{}, true, nil
	default:
		return SpaceMigrationOperation{}, false, fmt.Errorf("%w: unknown status %q", ErrSpaceMigrationState, state.status)
	}
	step := state.nextStep - 1
	migration := plan.Migrations[step]
	to := state.current.Clone()
	if err := Revert(&to, migration); err != nil {
		state.status = SpaceMigrationStatusFailed
		state.lastError = err.Error()
		return SpaceMigrationOperation{}, false, err
	}
	operation := SpaceMigrationOperation{
		PlanID:    state.planID,
		Space:     state.space,
		Step:      step,
		Direction: SpaceMigrationDirectionDown,
		Migration: cloneMigration(migration),
		From:      state.current.Clone(),
		To:        to.Clone(),
	}
	state.status = SpaceMigrationStatusRollingBack
	state.lastError = ""
	state.inFlight = true
	return operation, false, nil
}

func (manager *SpaceMigrationManager) commitUp(planID string, step int, expectedFrom string, expectedTo Schema) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	_, state, err := manager.lookupLocked(planID)
	if err != nil {
		return err
	}
	if !state.inFlight || state.nextStep != step || state.current.Fingerprint() != expectedFrom {
		err := fmt.Errorf("%w: apply step changed before commit", ErrSpaceMigrationState)
		state.inFlight = false
		state.status = SpaceMigrationStatusFailed
		state.lastError = err.Error()
		return err
	}
	state.current = expectedTo.Clone()
	state.nextStep++
	state.inFlight = false
	state.lastError = ""
	if state.nextStep == len(manager.plans[planID].Migrations) {
		state.status = SpaceMigrationStatusApplied
	} else {
		state.status = SpaceMigrationStatusPending
	}
	return nil
}

func (manager *SpaceMigrationManager) commitDown(planID string, step int, expectedFrom string, expectedTo Schema) error {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	_, state, err := manager.lookupLocked(planID)
	if err != nil {
		return err
	}
	if !state.inFlight || state.nextStep != step+1 || state.current.Fingerprint() != expectedFrom {
		err := fmt.Errorf("%w: rollback step changed before commit", ErrSpaceMigrationState)
		state.inFlight = false
		state.status = SpaceMigrationStatusFailed
		state.lastError = err.Error()
		return err
	}
	state.current = expectedTo.Clone()
	state.nextStep = step
	state.inFlight = false
	state.lastError = ""
	if state.nextStep == 0 {
		state.status = SpaceMigrationStatusRolledBack
	} else {
		state.status = SpaceMigrationStatusRollingBack
	}
	return nil
}

func (manager *SpaceMigrationManager) fail(planID string, err error) error {
	if err == nil {
		return nil
	}
	manager.recordFailure(planID, err)
	return err
}

func (manager *SpaceMigrationManager) recordFailure(planID string, err error) {
	if manager == nil || err == nil {
		return
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	state, ok := manager.states[strings.TrimSpace(planID)]
	if !ok {
		return
	}
	state.inFlight = false
	state.status = SpaceMigrationStatusFailed
	state.lastError = err.Error()
}

func (manager *SpaceMigrationManager) lookupLocked(planID string) (SpaceMigrationPlan, *spaceMigrationState, error) {
	planID = strings.TrimSpace(planID)
	plan, planOK := manager.plans[planID]
	state, stateOK := manager.states[planID]
	if !planOK || !stateOK {
		return SpaceMigrationPlan{}, nil, fmt.Errorf("%w: %q", ErrSpaceMigrationNotFound, planID)
	}
	return plan, state, nil
}

func normalizeSpaceMigrationPlan(plan SpaceMigrationPlan) (SpaceMigrationPlan, error) {
	plan.ID = strings.TrimSpace(plan.ID)
	plan.Space = strings.TrimSpace(plan.Space)
	if plan.Compatibility == "" {
		plan.Compatibility = SpaceMigrationCompatibilityRolling
	}
	if err := plan.Validate(); err != nil {
		return SpaceMigrationPlan{}, err
	}
	plan.BaseFingerprint = strings.TrimSpace(plan.BaseFingerprint)
	migrations := plan.Migrations
	plan.Migrations = make([]Migration, len(migrations))
	for index, migration := range migrations {
		plan.Migrations[index] = cloneMigration(migration)
	}
	return plan, nil
}

func validateSpaceMigrationName(value, label string) error {
	if strings.TrimSpace(value) == "" || len(value) > MaxSpaceMigrationNameBytes || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is empty, invalid, or too long", ErrSpaceMigrationPlanInvalid, label)
	}
	return nil
}

func validateSpaceMigrationSequence(plan SpaceMigrationPlan, schema Schema) error {
	current := schema.Clone()
	for index, migration := range plan.Migrations {
		next, err := Preview(&current, migration)
		if err != nil {
			return fmt.Errorf("%w: migration %d preview: %v", ErrSpaceMigrationPlanInvalid, index, err)
		}
		if plan.Compatibility == SpaceMigrationCompatibilityRolling {
			report, err := CheckRollingCompatibility(current, next)
			if err != nil {
				return fmt.Errorf("%w: migration %d: %v", ErrSpaceMigrationCompatibility, index, err)
			}
			if !report.Compatible {
				return fmt.Errorf("%w: migration %d has incompatible changes", ErrSpaceMigrationCompatibility, index)
			}
		}
		current = next
	}
	for index := len(plan.Migrations) - 1; index >= 0; index-- {
		if err := Revert(&current, plan.Migrations[index]); err != nil {
			return fmt.Errorf("%w: migration %d reverse preview: %v", ErrSpaceMigrationPlanInvalid, index, err)
		}
	}
	if current.Fingerprint() != schema.Fingerprint() {
		return fmt.Errorf("%w: reverse changes do not restore the base schema", ErrSpaceMigrationPlanInvalid)
	}
	return nil
}

func spaceMigrationProgress(plan SpaceMigrationPlan, state *spaceMigrationState) SpaceMigrationProgress {
	return SpaceMigrationProgress{
		PlanID:         state.planID,
		Space:          state.space,
		Status:         state.status,
		BaseVersion:    plan.BaseVersion,
		CurrentVersion: state.current.Version,
		TargetVersion:  plan.Migrations[len(plan.Migrations)-1].Version,
		NextStep:       state.nextStep,
		TotalSteps:     len(plan.Migrations),
		LastError:      state.lastError,
	}
}

func restoreSpaceMigrationState(plan SpaceMigrationPlan, snapshot SpaceMigrationStateSnapshot) (*spaceMigrationState, error) {
	if snapshot.PlanID != plan.ID || strings.TrimSpace(snapshot.Space) != plan.Space {
		return nil, fmt.Errorf("state identity does not match plan")
	}
	if err := snapshot.Base.Validate(); err != nil {
		return nil, fmt.Errorf("base schema: %v", err)
	}
	if err := snapshot.Current.Validate(); err != nil {
		return nil, fmt.Errorf("current schema: %v", err)
	}
	if snapshot.Base.Version != plan.BaseVersion || snapshot.Base.Fingerprint() != plan.BaseFingerprint {
		return nil, fmt.Errorf("base schema does not match plan precondition")
	}
	if snapshot.NextStep < 0 || snapshot.NextStep > len(plan.Migrations) {
		return nil, fmt.Errorf("next step %d is outside plan", snapshot.NextStep)
	}
	current := snapshot.Base.Clone()
	for index := 0; index < snapshot.NextStep; index++ {
		var err error
		current, err = Preview(&current, plan.Migrations[index])
		if err != nil {
			return nil, fmt.Errorf("replay step %d: %v", index, err)
		}
	}
	if current.Fingerprint() != snapshot.Current.Fingerprint() || snapshot.CurrentVersion != snapshot.Current.Version || snapshot.BaseVersion != snapshot.Base.Version {
		return nil, fmt.Errorf("schema version or fingerprint does not match progress")
	}
	status := snapshot.Status
	switch status {
	case SpaceMigrationStatusPending, SpaceMigrationStatusRunning, SpaceMigrationStatusFailed, SpaceMigrationStatusApplied, SpaceMigrationStatusRollingBack, SpaceMigrationStatusRolledBack:
	default:
		return nil, fmt.Errorf("unknown status %q", status)
	}
	if snapshot.NextStep == 0 && status == SpaceMigrationStatusApplied {
		return nil, fmt.Errorf("applied state has no completed steps")
	}
	if snapshot.NextStep == len(plan.Migrations) && (status == SpaceMigrationStatusPending || status == SpaceMigrationStatusRolledBack) {
		return nil, fmt.Errorf("status %q does not match completed plan", status)
	}
	if snapshot.NextStep != len(plan.Migrations) && status == SpaceMigrationStatusApplied {
		return nil, fmt.Errorf("applied state is missing steps")
	}
	if snapshot.NextStep != 0 && status == SpaceMigrationStatusRolledBack {
		return nil, fmt.Errorf("rolled-back state retains applied steps")
	}
	if status == SpaceMigrationStatusRunning || status == SpaceMigrationStatusRollingBack {
		status = SpaceMigrationStatusFailed
		if strings.TrimSpace(snapshot.LastError) == "" {
			snapshot.LastError = "recovered in-flight operation; retry with an idempotent callback"
		}
	}
	return &spaceMigrationState{
		planID:    plan.ID,
		space:     plan.Space,
		base:      snapshot.Base.Clone(),
		current:   snapshot.Current.Clone(),
		nextStep:  snapshot.NextStep,
		status:    status,
		lastError: snapshot.LastError,
	}, nil
}

func cloneSpaceMigrationPlan(plan SpaceMigrationPlan) SpaceMigrationPlan {
	clone, _ := normalizeSpaceMigrationPlan(plan)
	return clone
}

func cloneMigration(migration Migration) Migration {
	clone := migration
	clone.Up = cloneChanges(migration.Up)
	clone.Down = cloneChanges(migration.Down)
	return clone
}

func cloneChanges(changes []Change) []Change {
	if len(changes) == 0 {
		return nil
	}
	clone := make([]Change, len(changes))
	for index, change := range changes {
		clone[index] = change
		clone[index].Source = cloneSource(change.Source)
		clone[index].Column.EnumValues = append([]string(nil), change.Column.EnumValues...)
		clone[index].Constraint = cloneConstraint(change.Constraint)
	}
	return clone
}
