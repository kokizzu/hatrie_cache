package hatSchema

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// DefaultSpaceMigrationMaxPlans and DefaultSpaceMigrationMaxSteps keep the
	// optional manager bounded without requiring configuration for small nodes.
	DefaultSpaceMigrationMaxPlans         = 64
	DefaultSpaceMigrationMaxSteps         = 64
	DefaultSpaceMigrationMaxSnapshotBytes = 1 << 20

	maxSpaceMigrationPlans         = 4096
	maxSpaceMigrationSteps         = 1024
	maxSpaceMigrationSnapshotBytes = 16 << 20
	maxSpaceMigrationStringBytes   = 256
	maxSpaceMigrationErrorBytes    = 512
	spaceMigrationSnapshotVersion  = 1
)

var (
	// ErrSpaceMigrationNil indicates a nil manager receiver.
	ErrSpaceMigrationNil = errors.New("hatSchema: space migration manager is nil")
	// ErrSpaceMigrationInvalid indicates an invalid plan or state transition input.
	ErrSpaceMigrationInvalid = errors.New("hatSchema: space migration is invalid")
	// ErrSpaceMigrationNotFound indicates that a plan ID is not registered.
	ErrSpaceMigrationNotFound = errors.New("hatSchema: space migration plan is not found")
	// ErrSpaceMigrationExists indicates that a plan ID is already registered.
	ErrSpaceMigrationExists = errors.New("hatSchema: space migration plan already exists")
	// ErrSpaceMigrationLimit indicates that a configured or plan limit was exceeded.
	ErrSpaceMigrationLimit = errors.New("hatSchema: space migration limit exceeded")
	// ErrSpaceMigrationCallbacks indicates that a required callback is absent.
	ErrSpaceMigrationCallbacks = errors.New("hatSchema: space migration callback is required")
	// ErrSpaceMigrationState indicates that the requested operation is not valid
	// for the plan's current state.
	ErrSpaceMigrationState = errors.New("hatSchema: space migration state is invalid")
	// ErrSpaceMigrationBusy indicates that another operation owns the plan.
	ErrSpaceMigrationBusy = errors.New("hatSchema: space migration is busy")
	// ErrSpaceMigrationSnapshotInvalid indicates malformed or incompatible data.
	ErrSpaceMigrationSnapshotInvalid = errors.New("hatSchema: space migration snapshot is invalid")
)

// SpaceMigrationState describes the durable state of one migration plan.
type SpaceMigrationState string

const (
	SpaceMigrationPrepared   SpaceMigrationState = "prepared"
	SpaceMigrationRunning    SpaceMigrationState = "running"
	SpaceMigrationPaused     SpaceMigrationState = "paused"
	SpaceMigrationCommitted  SpaceMigrationState = "committed"
	SpaceMigrationRolledBack SpaceMigrationState = "rolled_back"
)

// SpaceMigrationManagerOptions bounds an opt-in migration manager. Zero values
// select conservative defaults.
type SpaceMigrationManagerOptions struct {
	MaxPlans         int
	MaxSteps         int
	MaxSnapshotBytes int
}

// SpaceMigrationStep is one caller-owned, sequential migration action.
type SpaceMigrationStep struct {
	ID            string
	TargetVersion uint64
}

// SpaceMigrationPlan describes a bounded, sequential migration for one space.
// CompatibleVersions lists versions that may continue to access the space while
// the plan is prepared, paused, or running. An omitted list accepts FromVersion.
type SpaceMigrationPlan struct {
	ID                 string
	Space              string
	FromVersion        uint64
	CompatibleVersions []uint64
	Steps              []SpaceMigrationStep
}

// SpaceMigrationStatus is a copy-safe view of a plan's durable state.
type SpaceMigrationStatus struct {
	PlanID         string
	Space          string
	State          SpaceMigrationState
	FromVersion    uint64
	CurrentVersion uint64
	TargetVersion  uint64
	CompletedSteps int
	Generation     uint64
	LastError      string
}

// SpaceMigrationCallbacks contains the caller-owned work for one run. The
// manager never serializes callbacks, so snapshots remain data-only.
type SpaceMigrationCallbacks struct {
	Check    func(context.Context, SpaceMigrationPlan) error
	Apply    func(context.Context, SpaceMigrationStep) error
	Rollback func(context.Context, SpaceMigrationStep) error
}

// SpaceMigrationManager tracks bounded, resumable plans. It has no effect on
// schema admission or execution unless the caller explicitly uses it.
type SpaceMigrationManager struct {
	mu               sync.RWMutex
	maxPlans         int
	maxSteps         int
	maxSnapshotBytes int
	plans            map[string]*spaceMigrationEntry
}

type spaceMigrationEntry struct {
	mu          sync.Mutex
	plan        SpaceMigrationPlan
	status      SpaceMigrationStatus
	rollingBack bool
}

// NewSpaceMigrationManager creates an empty, bounded manager.
func NewSpaceMigrationManager(options SpaceMigrationManagerOptions) (*SpaceMigrationManager, error) {
	normalized, err := normalizeSpaceMigrationOptions(options)
	if err != nil {
		return nil, err
	}
	return &SpaceMigrationManager{
		maxPlans:         normalized.MaxPlans,
		maxSteps:         normalized.MaxSteps,
		maxSnapshotBytes: normalized.MaxSnapshotBytes,
		plans:            make(map[string]*spaceMigrationEntry),
	}, nil
}

// Prepare validates and registers one plan in the prepared state.
func (manager *SpaceMigrationManager) Prepare(plan SpaceMigrationPlan) (SpaceMigrationStatus, error) {
	if manager == nil {
		return SpaceMigrationStatus{}, ErrSpaceMigrationNil
	}
	normalized, err := normalizeSpaceMigrationPlan(plan, manager.maxSteps)
	if err != nil {
		return SpaceMigrationStatus{}, err
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.plans) >= manager.maxPlans {
		return SpaceMigrationStatus{}, fmt.Errorf("%w: maximum plans %d exceeded", ErrSpaceMigrationLimit, manager.maxPlans)
	}
	if _, exists := manager.plans[normalized.ID]; exists {
		return SpaceMigrationStatus{}, fmt.Errorf("%w: %q", ErrSpaceMigrationExists, normalized.ID)
	}
	status := SpaceMigrationStatus{
		PlanID:         normalized.ID,
		Space:          normalized.Space,
		State:          SpaceMigrationPrepared,
		FromVersion:    normalized.FromVersion,
		CurrentVersion: normalized.FromVersion,
		TargetVersion:  normalized.Steps[len(normalized.Steps)-1].TargetVersion,
		Generation:     1,
	}
	manager.plans[normalized.ID] = &spaceMigrationEntry{plan: normalized, status: status}
	return cloneSpaceMigrationStatus(status), nil
}

// Status returns a copy of one plan's current state.
func (manager *SpaceMigrationManager) Status(id string) (SpaceMigrationStatus, error) {
	entry, err := manager.entry(id)
	if err != nil {
		return SpaceMigrationStatus{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return cloneSpaceMigrationStatus(entry.status), nil
}

// AllowsVersion reports whether a version is admitted by the plan's current
// compatibility window. This is an observation/helper API; callers own the
// actual request routing and schema admission decision.
func (manager *SpaceMigrationManager) AllowsVersion(id string, version uint64) (bool, error) {
	if version == 0 {
		return false, fmt.Errorf("%w: version must be positive", ErrSpaceMigrationInvalid)
	}
	entry, err := manager.entry(id)
	if err != nil {
		return false, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	switch entry.status.State {
	case SpaceMigrationCommitted:
		return version == entry.status.TargetVersion, nil
	case SpaceMigrationRolledBack:
		return version == entry.status.FromVersion, nil
	case SpaceMigrationPrepared, SpaceMigrationRunning, SpaceMigrationPaused:
		if version == entry.status.CurrentVersion {
			return true, nil
		}
		for _, compatible := range entry.plan.CompatibleVersions {
			if compatible == version {
				return true, nil
			}
		}
		return false, nil
	default:
		return false, fmt.Errorf("%w: unknown state %q", ErrSpaceMigrationState, entry.status.State)
	}
}

// Run resumes a prepared or paused plan. Apply is required; Check is optional
// and runs before the next step. A callback error pauses the plan and is
// returned unchanged to the caller.
func (manager *SpaceMigrationManager) Run(ctx context.Context, id string, callbacks SpaceMigrationCallbacks) error {
	if manager == nil {
		return ErrSpaceMigrationNil
	}
	if callbacks.Apply == nil {
		return ErrSpaceMigrationCallbacks
	}
	if ctx == nil {
		ctx = context.Background()
	}
	entry, err := manager.entry(id)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	if entry.rollingBack {
		entry.mu.Unlock()
		return ErrSpaceMigrationState
	}
	switch entry.status.State {
	case SpaceMigrationPrepared, SpaceMigrationPaused:
		entry.status.State = SpaceMigrationRunning
		entry.status.LastError = ""
		entry.status.Generation++
	case SpaceMigrationRunning:
		entry.mu.Unlock()
		return ErrSpaceMigrationBusy
	case SpaceMigrationCommitted, SpaceMigrationRolledBack:
		entry.mu.Unlock()
		return fmt.Errorf("%w: plan is %s", ErrSpaceMigrationState, entry.status.State)
	default:
		entry.mu.Unlock()
		return fmt.Errorf("%w: unknown state %q", ErrSpaceMigrationState, entry.status.State)
	}
	plan := cloneSpaceMigrationPlan(entry.plan)
	completed := entry.status.CompletedSteps
	entry.mu.Unlock()

	if callbacks.Check != nil {
		if err := callbacks.Check(ctx, plan); err != nil {
			manager.pauseSpaceMigration(entry, err)
			return err
		}
	}
	for index := completed; index < len(plan.Steps); index++ {
		if err := ctx.Err(); err != nil {
			manager.pauseSpaceMigration(entry, err)
			return err
		}
		if err := callbacks.Apply(ctx, plan.Steps[index]); err != nil {
			manager.pauseSpaceMigration(entry, err)
			return err
		}
		entry.mu.Lock()
		if entry.status.State != SpaceMigrationRunning || entry.rollingBack {
			entry.mu.Unlock()
			return ErrSpaceMigrationState
		}
		entry.status.CompletedSteps = index + 1
		entry.status.CurrentVersion = plan.Steps[index].TargetVersion
		entry.status.LastError = ""
		entry.status.Generation++
		entry.mu.Unlock()
	}

	entry.mu.Lock()
	if entry.status.State != SpaceMigrationRunning || entry.rollingBack {
		entry.mu.Unlock()
		return ErrSpaceMigrationState
	}
	entry.status.State = SpaceMigrationCommitted
	entry.status.CurrentVersion = entry.status.TargetVersion
	entry.status.LastError = ""
	entry.status.Generation++
	entry.mu.Unlock()
	return nil
}

// Rollback reverses completed steps in reverse order. A paused rollback can be
// resumed by calling Rollback again with the same callback.
func (manager *SpaceMigrationManager) Rollback(ctx context.Context, id string, callbacks SpaceMigrationCallbacks) error {
	if manager == nil {
		return ErrSpaceMigrationNil
	}
	if callbacks.Rollback == nil {
		return ErrSpaceMigrationCallbacks
	}
	if ctx == nil {
		ctx = context.Background()
	}
	entry, err := manager.entry(id)
	if err != nil {
		return err
	}

	entry.mu.Lock()
	if entry.rollingBack {
		if entry.status.State != SpaceMigrationPaused {
			entry.mu.Unlock()
			return ErrSpaceMigrationBusy
		}
		entry.status.State = SpaceMigrationRunning
		entry.status.LastError = ""
		entry.status.Generation++
	} else {
		switch entry.status.State {
		case SpaceMigrationPaused, SpaceMigrationCommitted:
			entry.rollingBack = true
			entry.status.State = SpaceMigrationRunning
			entry.status.LastError = ""
			entry.status.Generation++
		case SpaceMigrationRunning:
			entry.mu.Unlock()
			return ErrSpaceMigrationBusy
		case SpaceMigrationPrepared, SpaceMigrationRolledBack:
			entry.mu.Unlock()
			return fmt.Errorf("%w: plan is %s", ErrSpaceMigrationState, entry.status.State)
		default:
			entry.mu.Unlock()
			return fmt.Errorf("%w: unknown state %q", ErrSpaceMigrationState, entry.status.State)
		}
	}
	plan := cloneSpaceMigrationPlan(entry.plan)
	completed := entry.status.CompletedSteps
	entry.mu.Unlock()

	for index := completed - 1; index >= 0; index-- {
		if err := ctx.Err(); err != nil {
			manager.pauseSpaceMigration(entry, err)
			return err
		}
		if err := callbacks.Rollback(ctx, plan.Steps[index]); err != nil {
			manager.pauseSpaceMigration(entry, err)
			return err
		}
		entry.mu.Lock()
		if entry.status.State != SpaceMigrationRunning || !entry.rollingBack {
			entry.mu.Unlock()
			return ErrSpaceMigrationState
		}
		entry.status.CompletedSteps = index
		entry.status.CurrentVersion = migrationVersionAt(plan, index)
		entry.status.LastError = ""
		entry.status.Generation++
		entry.mu.Unlock()
	}

	entry.mu.Lock()
	if entry.status.State != SpaceMigrationRunning || !entry.rollingBack {
		entry.mu.Unlock()
		return ErrSpaceMigrationState
	}
	entry.rollingBack = false
	entry.status.State = SpaceMigrationRolledBack
	entry.status.CompletedSteps = 0
	entry.status.CurrentVersion = entry.status.FromVersion
	entry.status.LastError = ""
	entry.status.Generation++
	entry.mu.Unlock()
	return nil
}

func (manager *SpaceMigrationManager) pauseSpaceMigration(entry *spaceMigrationEntry, cause error) {
	entry.mu.Lock()
	if entry.status.State == SpaceMigrationRunning {
		entry.status.State = SpaceMigrationPaused
		entry.status.LastError = boundedSpaceMigrationError(cause)
		entry.status.Generation++
	}
	entry.mu.Unlock()
}

func (manager *SpaceMigrationManager) entry(id string) (*spaceMigrationEntry, error) {
	if manager == nil {
		return nil, ErrSpaceMigrationNil
	}
	if strings.TrimSpace(id) == "" {
		return nil, fmt.Errorf("%w: plan ID is required", ErrSpaceMigrationInvalid)
	}
	manager.mu.RLock()
	entry := manager.plans[id]
	manager.mu.RUnlock()
	if entry == nil {
		return nil, fmt.Errorf("%w: %q", ErrSpaceMigrationNotFound, id)
	}
	return entry, nil
}

func normalizeSpaceMigrationOptions(options SpaceMigrationManagerOptions) (SpaceMigrationManagerOptions, error) {
	if options.MaxPlans == 0 {
		options.MaxPlans = DefaultSpaceMigrationMaxPlans
	}
	if options.MaxSteps == 0 {
		options.MaxSteps = DefaultSpaceMigrationMaxSteps
	}
	if options.MaxSnapshotBytes == 0 {
		options.MaxSnapshotBytes = DefaultSpaceMigrationMaxSnapshotBytes
	}
	if options.MaxPlans < 1 || options.MaxPlans > maxSpaceMigrationPlans ||
		options.MaxSteps < 1 || options.MaxSteps > maxSpaceMigrationSteps ||
		options.MaxSnapshotBytes < 256 || options.MaxSnapshotBytes > maxSpaceMigrationSnapshotBytes {
		return SpaceMigrationManagerOptions{}, fmt.Errorf("%w: invalid manager bounds", ErrSpaceMigrationLimit)
	}
	return options, nil
}

func normalizeSpaceMigrationPlan(plan SpaceMigrationPlan, maxSteps int) (SpaceMigrationPlan, error) {
	plan.ID = strings.TrimSpace(plan.ID)
	plan.Space = strings.TrimSpace(plan.Space)
	if err := validateSpaceMigrationString(plan.ID, "plan ID"); err != nil {
		return SpaceMigrationPlan{}, err
	}
	if err := validateSpaceMigrationString(plan.Space, "space"); err != nil {
		return SpaceMigrationPlan{}, err
	}
	if plan.FromVersion == 0 {
		return SpaceMigrationPlan{}, fmt.Errorf("%w: from version must be positive", ErrSpaceMigrationInvalid)
	}
	if len(plan.Steps) == 0 {
		return SpaceMigrationPlan{}, fmt.Errorf("%w: at least one step is required", ErrSpaceMigrationInvalid)
	}
	if len(plan.Steps) > maxSteps {
		return SpaceMigrationPlan{}, fmt.Errorf("%w: maximum steps %d exceeded", ErrSpaceMigrationLimit, maxSteps)
	}
	if len(plan.CompatibleVersions) > maxSteps+1 {
		return SpaceMigrationPlan{}, fmt.Errorf("%w: too many compatible versions", ErrSpaceMigrationLimit)
	}
	normalized := SpaceMigrationPlan{
		ID:          plan.ID,
		Space:       plan.Space,
		FromVersion: plan.FromVersion,
		Steps:       make([]SpaceMigrationStep, len(plan.Steps)),
	}
	if len(plan.CompatibleVersions) == 0 {
		normalized.CompatibleVersions = []uint64{plan.FromVersion}
	} else {
		normalized.CompatibleVersions = append([]uint64(nil), plan.CompatibleVersions...)
	}
	seenVersions := make(map[uint64]struct{}, len(normalized.CompatibleVersions))
	for _, version := range normalized.CompatibleVersions {
		if version == 0 || version < plan.FromVersion {
			return SpaceMigrationPlan{}, fmt.Errorf("%w: invalid compatible version", ErrSpaceMigrationInvalid)
		}
		if _, exists := seenVersions[version]; exists {
			return SpaceMigrationPlan{}, fmt.Errorf("%w: duplicate compatible version", ErrSpaceMigrationInvalid)
		}
		seenVersions[version] = struct{}{}
	}
	seenSteps := make(map[string]struct{}, len(plan.Steps))
	for index, step := range plan.Steps {
		step.ID = strings.TrimSpace(step.ID)
		if err := validateSpaceMigrationString(step.ID, "step ID"); err != nil {
			return SpaceMigrationPlan{}, err
		}
		if _, exists := seenSteps[step.ID]; exists {
			return SpaceMigrationPlan{}, fmt.Errorf("%w: duplicate step %q", ErrSpaceMigrationInvalid, step.ID)
		}
		seenSteps[step.ID] = struct{}{}
		if plan.FromVersion > ^uint64(0)-uint64(index+1) {
			return SpaceMigrationPlan{}, fmt.Errorf("%w: target version overflows", ErrSpaceMigrationInvalid)
		}
		expected := plan.FromVersion + uint64(index+1)
		if step.TargetVersion != expected {
			return SpaceMigrationPlan{}, fmt.Errorf("%w: step %q targets %d, want %d", ErrSpaceMigrationInvalid, step.ID, step.TargetVersion, expected)
		}
		normalized.Steps[index] = step
	}
	targetVersion := normalized.Steps[len(normalized.Steps)-1].TargetVersion
	for _, version := range normalized.CompatibleVersions {
		if version > targetVersion {
			return SpaceMigrationPlan{}, fmt.Errorf("%w: compatible version %d exceeds target %d", ErrSpaceMigrationInvalid, version, targetVersion)
		}
	}
	return normalized, nil
}

func validateSpaceMigrationString(value, label string) error {
	if value == "" {
		return fmt.Errorf("%w: %s is required", ErrSpaceMigrationInvalid, label)
	}
	if len(value) > maxSpaceMigrationStringBytes || !utf8.ValidString(value) {
		return fmt.Errorf("%w: %s is invalid", ErrSpaceMigrationInvalid, label)
	}
	return nil
}

func cloneSpaceMigrationPlan(plan SpaceMigrationPlan) SpaceMigrationPlan {
	clone := plan
	clone.CompatibleVersions = append([]uint64(nil), plan.CompatibleVersions...)
	clone.Steps = append([]SpaceMigrationStep(nil), plan.Steps...)
	return clone
}

func cloneSpaceMigrationStatus(status SpaceMigrationStatus) SpaceMigrationStatus {
	return status
}

func migrationVersionAt(plan SpaceMigrationPlan, completed int) uint64 {
	if completed <= 0 {
		return plan.FromVersion
	}
	return plan.Steps[completed-1].TargetVersion
}

func boundedSpaceMigrationError(cause error) string {
	if cause == nil {
		return ""
	}
	message := cause.Error()
	if len(message) > maxSpaceMigrationErrorBytes {
		message = message[:maxSpaceMigrationErrorBytes]
	}
	return message
}

func spaceMigrationStateCode(state SpaceMigrationState) (byte, bool) {
	switch state {
	case SpaceMigrationPrepared:
		return 1, true
	case SpaceMigrationRunning:
		return 2, true
	case SpaceMigrationPaused:
		return 3, true
	case SpaceMigrationCommitted:
		return 4, true
	case SpaceMigrationRolledBack:
		return 5, true
	default:
		return 0, false
	}
}

func spaceMigrationStateFromCode(code byte) (SpaceMigrationState, bool) {
	switch code {
	case 1:
		return SpaceMigrationPrepared, true
	case 2:
		return SpaceMigrationRunning, true
	case 3:
		return SpaceMigrationPaused, true
	case 4:
		return SpaceMigrationCommitted, true
	case 5:
		return SpaceMigrationRolledBack, true
	default:
		return "", false
	}
}

type spaceMigrationSnapshotWriter struct {
	data  []byte
	limit int
	err   error
}

func (writer *spaceMigrationSnapshotWriter) appendBytes(data []byte) {
	if writer.err != nil {
		return
	}
	if len(data) > writer.limit-len(writer.data) {
		writer.err = ErrSpaceMigrationSnapshotInvalid
		return
	}
	writer.data = append(writer.data, data...)
}

func (writer *spaceMigrationSnapshotWriter) byte(value byte) {
	writer.appendBytes([]byte{value})
}

func (writer *spaceMigrationSnapshotWriter) u32(value uint32) {
	writer.appendBytes([]byte{byte(value), byte(value >> 8), byte(value >> 16), byte(value >> 24)})
}

func (writer *spaceMigrationSnapshotWriter) u64(value uint64) {
	writer.u32(uint32(value))
	writer.u32(uint32(value >> 32))
}

func (writer *spaceMigrationSnapshotWriter) string(value string) {
	if len(value) > maxSpaceMigrationErrorBytes {
		writer.err = ErrSpaceMigrationSnapshotInvalid
		return
	}
	writer.u32(uint32(len(value)))
	writer.appendBytes([]byte(value))
}

// MarshalSnapshot returns a deterministic, checksummed binary snapshot. It is
// intended for a caller-owned durable manifest, not for persisting callbacks.
func (manager *SpaceMigrationManager) MarshalSnapshot() ([]byte, error) {
	if manager == nil {
		return nil, ErrSpaceMigrationNil
	}
	manager.mu.RLock()
	ids := make([]string, 0, len(manager.plans))
	entries := make(map[string]*spaceMigrationEntry, len(manager.plans))
	for id, entry := range manager.plans {
		ids = append(ids, id)
		entries[id] = entry
	}
	manager.mu.RUnlock()
	sort.Strings(ids)

	writer := &spaceMigrationSnapshotWriter{limit: manager.maxSnapshotBytes - 4}
	writer.appendBytes([]byte{'H', 'S', 'M', '1'})
	writer.byte(spaceMigrationSnapshotVersion)
	writer.appendBytes([]byte{0, 0, 0})
	writer.u32(uint32(len(ids)))
	for _, id := range ids {
		entry := entries[id]
		entry.mu.Lock()
		if entry.status.State == SpaceMigrationRunning || entry.rollingBack {
			entry.mu.Unlock()
			return nil, ErrSpaceMigrationBusy
		}
		plan := cloneSpaceMigrationPlan(entry.plan)
		status := entry.status
		entry.mu.Unlock()
		writer.string(plan.ID)
		writer.string(plan.Space)
		writer.u64(plan.FromVersion)
		writer.u32(uint32(len(plan.CompatibleVersions)))
		for _, version := range plan.CompatibleVersions {
			writer.u64(version)
		}
		writer.u32(uint32(len(plan.Steps)))
		for _, step := range plan.Steps {
			writer.string(step.ID)
			writer.u64(step.TargetVersion)
		}
		stateCode, ok := spaceMigrationStateCode(status.State)
		if !ok || status.State == SpaceMigrationRunning {
			return nil, ErrSpaceMigrationSnapshotInvalid
		}
		writer.byte(stateCode)
		writer.u64(status.CurrentVersion)
		writer.u64(status.Generation)
		writer.u32(uint32(status.CompletedSteps))
		writer.string(boundedSpaceMigrationError(errors.New(status.LastError)))
		if writer.err != nil {
			return nil, fmt.Errorf("%w: snapshot exceeds configured size", writer.err)
		}
	}
	if writer.err != nil {
		return nil, writer.err
	}
	checksum := crc32.Checksum(writer.data, crc32.MakeTable(crc32.Castagnoli))
	writer.limit = manager.maxSnapshotBytes
	writer.u32(checksum)
	if writer.err != nil {
		return nil, writer.err
	}
	return writer.data, nil
}

type spaceMigrationSnapshotReader struct {
	data []byte
	pos  int
}

func (reader *spaceMigrationSnapshotReader) bytes(size int) ([]byte, error) {
	if size < 0 || size > len(reader.data)-reader.pos {
		return nil, ErrSpaceMigrationSnapshotInvalid
	}
	value := reader.data[reader.pos : reader.pos+size]
	reader.pos += size
	return value, nil
}

func (reader *spaceMigrationSnapshotReader) byte() (byte, error) {
	value, err := reader.bytes(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (reader *spaceMigrationSnapshotReader) u32() (uint32, error) {
	value, err := reader.bytes(4)
	if err != nil {
		return 0, err
	}
	return uint32(value[0]) | uint32(value[1])<<8 | uint32(value[2])<<16 | uint32(value[3])<<24, nil
}

func (reader *spaceMigrationSnapshotReader) u64() (uint64, error) {
	low, err := reader.u32()
	if err != nil {
		return 0, err
	}
	high, err := reader.u32()
	if err != nil {
		return 0, err
	}
	return uint64(low) | uint64(high)<<32, nil
}

func (reader *spaceMigrationSnapshotReader) string(maxBytes int) (string, error) {
	size, err := reader.u32()
	if err != nil || size > uint32(maxBytes) {
		return "", ErrSpaceMigrationSnapshotInvalid
	}
	value, err := reader.bytes(int(size))
	if err != nil || !utf8.Valid(value) {
		return "", ErrSpaceMigrationSnapshotInvalid
	}
	return string(value), nil
}

// RestoreSpaceMigrationManager restores a checksummed snapshot into a new
// bounded manager. Snapshots never restore executable callbacks.
func RestoreSpaceMigrationManager(encoded []byte, options SpaceMigrationManagerOptions) (*SpaceMigrationManager, error) {
	manager, err := NewSpaceMigrationManager(options)
	if err != nil {
		return nil, err
	}
	if len(encoded) < 12 || len(encoded) > manager.maxSnapshotBytes {
		return nil, ErrSpaceMigrationSnapshotInvalid
	}
	body := encoded[:len(encoded)-4]
	checksum := uint32(encoded[len(encoded)-4]) | uint32(encoded[len(encoded)-3])<<8 | uint32(encoded[len(encoded)-2])<<16 | uint32(encoded[len(encoded)-1])<<24
	if crc32.Checksum(body, crc32.MakeTable(crc32.Castagnoli)) != checksum {
		return nil, ErrSpaceMigrationSnapshotInvalid
	}
	reader := &spaceMigrationSnapshotReader{data: body}
	magic, err := reader.bytes(4)
	if err != nil || string(magic) != "HSM1" {
		return nil, ErrSpaceMigrationSnapshotInvalid
	}
	version, err := reader.byte()
	if err != nil || version != spaceMigrationSnapshotVersion {
		return nil, ErrSpaceMigrationSnapshotInvalid
	}
	if _, err := reader.bytes(3); err != nil {
		return nil, ErrSpaceMigrationSnapshotInvalid
	}
	count, err := reader.u32()
	if err != nil || count > uint32(manager.maxPlans) {
		return nil, ErrSpaceMigrationSnapshotInvalid
	}
	for index := uint32(0); index < count; index++ {
		plan, state, current, generation, completed, lastError, err := decodeSpaceMigrationSnapshotEntry(reader, manager.maxSteps)
		if err != nil {
			return nil, ErrSpaceMigrationSnapshotInvalid
		}
		if _, exists := manager.plans[plan.ID]; exists {
			return nil, ErrSpaceMigrationSnapshotInvalid
		}
		status := SpaceMigrationStatus{
			PlanID:         plan.ID,
			Space:          plan.Space,
			State:          state,
			FromVersion:    plan.FromVersion,
			CurrentVersion: current,
			TargetVersion:  plan.Steps[len(plan.Steps)-1].TargetVersion,
			CompletedSteps: completed,
			Generation:     generation,
			LastError:      boundedSpaceMigrationError(errors.New(lastError)),
		}
		if err := validateSpaceMigrationSnapshotStatus(plan, status); err != nil {
			return nil, ErrSpaceMigrationSnapshotInvalid
		}
		manager.plans[plan.ID] = &spaceMigrationEntry{plan: plan, status: status}
	}
	if reader.pos != len(reader.data) {
		return nil, ErrSpaceMigrationSnapshotInvalid
	}
	return manager, nil
}

func decodeSpaceMigrationSnapshotEntry(reader *spaceMigrationSnapshotReader, maxSteps int) (SpaceMigrationPlan, SpaceMigrationState, uint64, uint64, int, string, error) {
	id, err := reader.string(maxSpaceMigrationStringBytes)
	if err != nil {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
	}
	space, err := reader.string(maxSpaceMigrationStringBytes)
	if err != nil {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
	}
	fromVersion, err := reader.u64()
	if err != nil {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
	}
	compatibleCount, err := reader.u32()
	if err != nil || compatibleCount > uint32(maxSteps+1) {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", ErrSpaceMigrationSnapshotInvalid
	}
	compatible := make([]uint64, compatibleCount)
	for index := range compatible {
		compatible[index], err = reader.u64()
		if err != nil {
			return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
		}
	}
	stepCount, err := reader.u32()
	if err != nil || stepCount == 0 || stepCount > uint32(maxSteps) {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", ErrSpaceMigrationSnapshotInvalid
	}
	steps := make([]SpaceMigrationStep, stepCount)
	for index := range steps {
		steps[index].ID, err = reader.string(maxSpaceMigrationStringBytes)
		if err != nil {
			return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
		}
		steps[index].TargetVersion, err = reader.u64()
		if err != nil {
			return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
		}
	}
	stateCode, err := reader.byte()
	if err != nil {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
	}
	state, ok := spaceMigrationStateFromCode(stateCode)
	if !ok || state == SpaceMigrationRunning {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", ErrSpaceMigrationSnapshotInvalid
	}
	current, err := reader.u64()
	if err != nil {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
	}
	generation, err := reader.u64()
	if err != nil {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
	}
	completed, err := reader.u32()
	if err != nil || completed > stepCount || uint64(completed) > uint64(^uint(0)>>1) {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", ErrSpaceMigrationSnapshotInvalid
	}
	lastError, err := reader.string(maxSpaceMigrationErrorBytes)
	if err != nil {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
	}
	plan, err := normalizeSpaceMigrationPlan(SpaceMigrationPlan{
		ID:                 id,
		Space:              space,
		FromVersion:        fromVersion,
		CompatibleVersions: compatible,
		Steps:              steps,
	}, maxSteps)
	if err != nil {
		return SpaceMigrationPlan{}, "", 0, 0, 0, "", err
	}
	return plan, state, current, generation, int(completed), lastError, nil
}

func validateSpaceMigrationSnapshotStatus(plan SpaceMigrationPlan, status SpaceMigrationStatus) error {
	if status.Generation == 0 || status.CompletedSteps < 0 || status.CompletedSteps > len(plan.Steps) {
		return ErrSpaceMigrationSnapshotInvalid
	}
	expectedCurrent := migrationVersionAt(plan, status.CompletedSteps)
	if status.CurrentVersion != expectedCurrent {
		return ErrSpaceMigrationSnapshotInvalid
	}
	if status.TargetVersion != plan.Steps[len(plan.Steps)-1].TargetVersion {
		return ErrSpaceMigrationSnapshotInvalid
	}
	switch status.State {
	case SpaceMigrationPrepared:
		if status.CompletedSteps != 0 {
			return ErrSpaceMigrationSnapshotInvalid
		}
	case SpaceMigrationCommitted:
		if status.CompletedSteps != len(plan.Steps) {
			return ErrSpaceMigrationSnapshotInvalid
		}
	case SpaceMigrationRolledBack:
		if status.CompletedSteps != 0 || status.CurrentVersion != plan.FromVersion {
			return ErrSpaceMigrationSnapshotInvalid
		}
	case SpaceMigrationPaused:
	default:
		return ErrSpaceMigrationSnapshotInvalid
	}
	return nil
}
