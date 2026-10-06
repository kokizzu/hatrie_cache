package hatSchema

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"
)

const (
	// DefaultVersionedMigrationMaxSteps bounds one named migration plan.
	DefaultVersionedMigrationMaxSteps = 1024
	// DefaultVersionedMigrationMaxClients bounds clients tracked by one manager.
	DefaultVersionedMigrationMaxClients = 4096
	// DefaultVersionedMigrationCheckpointMaxBytes bounds one encoded checkpoint.
	DefaultVersionedMigrationCheckpointMaxBytes = 1 << 20

	maxVersionedMigrationIDBytes          = 128
	maxVersionedMigrationPreconditions    = 32
	maxVersionedMigrationCheckpointBytes  = 16 << 20
	maxVersionedMigrationClients          = 1 << 20
	versionedMigrationCheckpointFormat    = uint64(1)
	versionedMigrationCheckpointHashBytes = sha256.Size * 2
)

var (
	// ErrVersionedMigrationManagerNil indicates a nil manager receiver.
	ErrVersionedMigrationManagerNil = errors.New("hatSchema: versioned migration manager is nil")
	// ErrVersionedMigrationPlanInvalid indicates a malformed or non-reversible plan.
	ErrVersionedMigrationPlanInvalid = errors.New("hatSchema: versioned migration plan is invalid")
	// ErrVersionedMigrationPhase indicates that an operation is not valid in the
	// manager's current phase.
	ErrVersionedMigrationPhase = errors.New("hatSchema: versioned migration phase is invalid")
	// ErrVersionedMigrationPrecondition indicates that a step precondition failed.
	ErrVersionedMigrationPrecondition = errors.New("hatSchema: versioned migration precondition failed")
	// ErrVersionedMigrationConflict indicates that another operation committed
	// while this operation was evaluating its candidate state.
	ErrVersionedMigrationConflict = errors.New("hatSchema: versioned migration state changed concurrently")
	// ErrVersionedMigrationClient indicates invalid client admission state.
	ErrVersionedMigrationClient = errors.New("hatSchema: versioned migration client is invalid")
	// ErrVersionedMigrationCheckpoint indicates malformed or incompatible
	// checkpoint data.
	ErrVersionedMigrationCheckpoint = errors.New("hatSchema: versioned migration checkpoint is invalid")
	// ErrVersionedMigrationRollback indicates that rollback was rejected or
	// could not produce the original schema.
	ErrVersionedMigrationRollback = errors.New("hatSchema: versioned migration rollback failed")
	// ErrVersionedMigrationComplete indicates that there are no steps left to apply.
	ErrVersionedMigrationComplete = errors.New("hatSchema: versioned migration is complete")
)

// VersionedMigrationPhase is the durable lifecycle phase of one plan.
type VersionedMigrationPhase string

const (
	VersionedMigrationActive     VersionedMigrationPhase = "active"
	VersionedMigrationComplete   VersionedMigrationPhase = "complete"
	VersionedMigrationRolledBack VersionedMigrationPhase = "rolled_back"
)

// VersionedMigrationPrecondition validates the candidate schema before a step
// is committed. The supplied schema is a private clone and may be inspected or
// modified without changing manager state.
type VersionedMigrationPrecondition func(Schema) error

// VersionedMigrationStep combines one reversible schema migration with checks
// that must pass immediately before its commit.
type VersionedMigrationStep struct {
	Migration     Migration
	Preconditions []VersionedMigrationPrecondition
}

// VersionedMigrationPlan is an immutable, contiguous migration sequence.
// Create plans with NewVersionedMigrationPlan so every reverse step is checked
// before a manager can run it.
type VersionedMigrationPlan struct {
	ID    string
	Base  Schema
	Steps []VersionedMigrationStep
}

// VersionedMigrationClientVersion records the schema version used by one
// admitted client.
type VersionedMigrationClientVersion struct {
	ID      string `json:"id"`
	Version uint64 `json:"version"`
}

// VersionedMigrationCheckpoint is the data-only recovery record for a manager.
// The plan's callbacks are intentionally not serialized; restore requires the
// same named plan to be supplied by the process.
type VersionedMigrationCheckpoint struct {
	PlanID            string                            `json:"plan_id"`
	BaseVersion       uint64                            `json:"base_version"`
	TargetVersion     uint64                            `json:"target_version"`
	CurrentVersion    uint64                            `json:"current_version"`
	AppliedSteps      uint64                            `json:"applied_steps"`
	Generation        uint64                            `json:"generation"`
	Phase             VersionedMigrationPhase           `json:"phase"`
	SchemaFingerprint string                            `json:"schema_fingerprint"`
	Clients           []VersionedMigrationClientVersion `json:"clients,omitempty"`
}

type versionedMigrationManagerOptions struct {
	maxSteps      int
	maxClients    int
	maxCheckpoint int
}

// VersionedMigrationManager applies one validated plan atomically and keeps
// enough state to admit mixed-version clients and recover from a checkpoint.
type VersionedMigrationManager struct {
	mu            sync.RWMutex
	plan          VersionedMigrationPlan
	schema        Schema
	appliedSteps  int
	phase         VersionedMigrationPhase
	generation    uint64
	clients       map[string]uint64
	maxClients    int
	maxCheckpoint int
}

type versionedMigrationCheckpointEnvelope struct {
	Format     uint64          `json:"format"`
	Checkpoint json.RawMessage `json:"checkpoint"`
	SHA256     string          `json:"sha256"`
}

// NewVersionedMigrationPlan validates and deep-copies a migration plan.
func NewVersionedMigrationPlan(id string, base Schema, steps []VersionedMigrationStep) (VersionedMigrationPlan, error) {
	return normalizeVersionedMigrationPlan(VersionedMigrationPlan{ID: id, Base: base, Steps: steps})
}

// NewVersionedMigrationManager creates an active manager at the plan base.
func NewVersionedMigrationManager(plan VersionedMigrationPlan) (*VersionedMigrationManager, error) {
	normalized, err := normalizeVersionedMigrationPlan(plan)
	if err != nil {
		return nil, err
	}
	return &VersionedMigrationManager{
		plan:          normalized,
		schema:        normalized.Base.Clone(),
		phase:         VersionedMigrationActive,
		generation:    1,
		clients:       make(map[string]uint64),
		maxClients:    DefaultVersionedMigrationMaxClients,
		maxCheckpoint: DefaultVersionedMigrationCheckpointMaxBytes,
	}, nil
}

// Schema returns a deep copy of the manager's current schema.
func (manager *VersionedMigrationManager) Schema() Schema {
	if manager == nil {
		return Schema{}
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.schema.Clone()
}

// Snapshot returns a deterministic, data-only recovery record.
func (manager *VersionedMigrationManager) Snapshot() VersionedMigrationCheckpoint {
	if manager == nil {
		return VersionedMigrationCheckpoint{}
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.snapshotLocked()
}

// ApplyNext evaluates and commits exactly one migration step. Candidate work
// happens outside the lock; the generation check prevents a stale candidate
// from publishing after another operation wins the race.
func (manager *VersionedMigrationManager) ApplyNext(ctx context.Context) error {
	if manager == nil {
		return ErrVersionedMigrationManagerNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	manager.mu.RLock()
	if manager.phase != VersionedMigrationActive {
		phase := manager.phase
		manager.mu.RUnlock()
		if phase == VersionedMigrationComplete {
			return ErrVersionedMigrationComplete
		}
		return fmt.Errorf("%w: %s", ErrVersionedMigrationPhase, phase)
	}
	if manager.appliedSteps >= len(manager.plan.Steps) {
		manager.mu.RUnlock()
		return ErrVersionedMigrationComplete
	}
	step := cloneVersionedMigrationStep(manager.plan.Steps[manager.appliedSteps])
	candidate := manager.schema.Clone()
	generation := manager.generation
	appliedSteps := manager.appliedSteps
	manager.mu.RUnlock()

	if err := ctx.Err(); err != nil {
		return err
	}
	for _, precondition := range step.Preconditions {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := precondition(candidate.Clone()); err != nil {
			return fmt.Errorf("%w: %w", ErrVersionedMigrationPrecondition, err)
		}
	}
	updated, err := Preview(&candidate, step.Migration)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVersionedMigrationPlanInvalid, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.phase != VersionedMigrationActive || manager.generation != generation || manager.appliedSteps != appliedSteps {
		return ErrVersionedMigrationConflict
	}
	manager.schema = updated
	manager.appliedSteps++
	manager.generation++
	if manager.appliedSteps == len(manager.plan.Steps) {
		manager.phase = VersionedMigrationComplete
	}
	return nil
}

// Rollback atomically reverts every committed step. It is rejected while any
// client depends on a post-base version.
func (manager *VersionedMigrationManager) Rollback() error {
	if manager == nil {
		return ErrVersionedMigrationManagerNil
	}
	manager.mu.RLock()
	if manager.phase == VersionedMigrationRolledBack {
		manager.mu.RUnlock()
		return fmt.Errorf("%w: already rolled back", ErrVersionedMigrationPhase)
	}
	if manager.phase != VersionedMigrationActive && manager.phase != VersionedMigrationComplete {
		phase := manager.phase
		manager.mu.RUnlock()
		return fmt.Errorf("%w: %s", ErrVersionedMigrationPhase, phase)
	}
	for clientID, version := range manager.clients {
		if version > manager.plan.Base.Version {
			manager.mu.RUnlock()
			return fmt.Errorf("%w: client %q uses version %d", ErrVersionedMigrationRollback, clientID, version)
		}
	}
	plan := cloneVersionedMigrationPlan(manager.plan)
	candidate := manager.schema.Clone()
	generation := manager.generation
	appliedSteps := manager.appliedSteps
	manager.mu.RUnlock()

	for index := appliedSteps - 1; index >= 0; index-- {
		if err := Revert(&candidate, plan.Steps[index].Migration); err != nil {
			return fmt.Errorf("%w: revert step %d: %v", ErrVersionedMigrationRollback, index, err)
		}
	}
	if candidate.Fingerprint() != plan.Base.Fingerprint() {
		return fmt.Errorf("%w: reverse migrations did not restore the base schema", ErrVersionedMigrationRollback)
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.phase != VersionedMigrationActive && manager.phase != VersionedMigrationComplete ||
		manager.generation != generation || manager.appliedSteps != appliedSteps {
		return ErrVersionedMigrationConflict
	}
	for clientID, version := range manager.clients {
		if version > manager.plan.Base.Version {
			return fmt.Errorf("%w: client %q was admitted during rollback", ErrVersionedMigrationRollback, clientID)
		}
	}
	manager.schema = plan.Base.Clone()
	manager.appliedSteps = 0
	manager.phase = VersionedMigrationRolledBack
	manager.generation++
	return nil
}

// AdmitClient records the client's schema version if the current phase allows it.
func (manager *VersionedMigrationManager) AdmitClient(clientID string, version uint64) error {
	if manager == nil {
		return ErrVersionedMigrationManagerNil
	}
	clientID, err := normalizeVersionedMigrationID(clientID, "client ID")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVersionedMigrationClient, err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.clients[clientID]; !exists && len(manager.clients) >= manager.maxClients {
		return fmt.Errorf("%w: client limit exceeded", ErrVersionedMigrationClient)
	}
	if existing, exists := manager.clients[clientID]; exists {
		if existing == version {
			return nil
		}
		return fmt.Errorf("%w: client %q already uses version %d", ErrVersionedMigrationClient, clientID, existing)
	}
	if !manager.clientVersionAllowedLocked(version) {
		return fmt.Errorf("%w: version %d is not admitted in phase %s", ErrVersionedMigrationClient, version, manager.phase)
	}
	manager.clients[clientID] = version
	manager.generation++
	return nil
}

// ReleaseClient removes a client admission record.
func (manager *VersionedMigrationManager) ReleaseClient(clientID string) error {
	if manager == nil {
		return ErrVersionedMigrationManagerNil
	}
	clientID, err := normalizeVersionedMigrationID(clientID, "client ID")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrVersionedMigrationClient, err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.clients[clientID]; !exists {
		return fmt.Errorf("%w: client %q is not admitted", ErrVersionedMigrationClient, clientID)
	}
	delete(manager.clients, clientID)
	manager.generation++
	return nil
}

// ClientVersions returns a deterministic snapshot of admitted clients.
func (manager *VersionedMigrationManager) ClientVersions() []VersionedMigrationClientVersion {
	if manager == nil {
		return nil
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	clients := make([]VersionedMigrationClientVersion, 0, len(manager.clients))
	for id, version := range manager.clients {
		clients = append(clients, VersionedMigrationClientVersion{ID: id, Version: version})
	}
	sort.Slice(clients, func(left, right int) bool { return clients[left].ID < clients[right].ID })
	return clients
}

// Restore validates and atomically installs a checkpoint for this manager's plan.
func (manager *VersionedMigrationManager) Restore(checkpoint VersionedMigrationCheckpoint) error {
	if manager == nil {
		return ErrVersionedMigrationManagerNil
	}
	manager.mu.RLock()
	plan := cloneVersionedMigrationPlan(manager.plan)
	manager.mu.RUnlock()

	if err := validateVersionedMigrationCheckpointShape(checkpoint); err != nil {
		return err
	}
	if checkpoint.PlanID != plan.ID || checkpoint.BaseVersion != plan.Base.Version ||
		checkpoint.TargetVersion != plan.Steps[len(plan.Steps)-1].Migration.Version {
		return fmt.Errorf("%w: checkpoint plan does not match manager", ErrVersionedMigrationCheckpoint)
	}
	if checkpoint.AppliedSteps > uint64(len(plan.Steps)) {
		return fmt.Errorf("%w: applied step count exceeds plan", ErrVersionedMigrationCheckpoint)
	}
	candidate := plan.Base.Clone()
	for index := uint64(0); index < checkpoint.AppliedSteps; index++ {
		updated, err := Preview(&candidate, plan.Steps[index].Migration)
		if err != nil {
			return fmt.Errorf("%w: replay step %d: %v", ErrVersionedMigrationCheckpoint, index, err)
		}
		candidate = updated
	}
	if checkpoint.CurrentVersion != candidate.Version || checkpoint.SchemaFingerprint != candidate.Fingerprint() {
		return fmt.Errorf("%w: schema fingerprint or version mismatch", ErrVersionedMigrationCheckpoint)
	}
	if err := validateVersionedMigrationCheckpointAgainstPlan(checkpoint, plan); err != nil {
		return err
	}
	if len(checkpoint.Clients) > manager.maxClients {
		return fmt.Errorf("%w: checkpoint client count exceeds manager limit", ErrVersionedMigrationCheckpoint)
	}
	clients := make(map[string]uint64, len(checkpoint.Clients))
	for _, client := range checkpoint.Clients {
		if _, exists := clients[client.ID]; exists {
			return fmt.Errorf("%w: duplicate client %q", ErrVersionedMigrationCheckpoint, client.ID)
		}
		if !versionedMigrationClientVersionAllowed(checkpoint.Phase, client.Version, plan.Base.Version, checkpoint.CurrentVersion) {
			return fmt.Errorf("%w: client %q version %d is not admitted", ErrVersionedMigrationCheckpoint, client.ID, client.Version)
		}
		clients[client.ID] = client.Version
	}

	manager.mu.Lock()
	manager.schema = candidate
	manager.appliedSteps = int(checkpoint.AppliedSteps)
	manager.phase = checkpoint.Phase
	manager.generation = checkpoint.Generation
	manager.clients = clients
	manager.mu.Unlock()
	return nil
}

// MarshalVersionedMigrationCheckpoint encodes a bounded, tamper-evident checkpoint.
func MarshalVersionedMigrationCheckpoint(checkpoint VersionedMigrationCheckpoint) ([]byte, error) {
	normalized, err := normalizeVersionedMigrationCheckpoint(checkpoint)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(normalized)
	if err != nil {
		return nil, fmt.Errorf("%w: encode body: %v", ErrVersionedMigrationCheckpoint, err)
	}
	digest := sha256.Sum256(body)
	var digestHex [versionedMigrationCheckpointHashBytes]byte
	hex.Encode(digestHex[:], digest[:])
	envelope := versionedMigrationCheckpointEnvelope{
		Format:     versionedMigrationCheckpointFormat,
		Checkpoint: body,
		SHA256:     string(digestHex[:]),
	}
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("%w: encode envelope: %v", ErrVersionedMigrationCheckpoint, err)
	}
	if len(encoded) > DefaultVersionedMigrationCheckpointMaxBytes {
		return nil, fmt.Errorf("%w: checkpoint exceeds default size limit", ErrVersionedMigrationCheckpoint)
	}
	return encoded, nil
}

// UnmarshalVersionedMigrationCheckpoint validates and decodes one checkpoint.
func UnmarshalVersionedMigrationCheckpoint(encoded []byte) (VersionedMigrationCheckpoint, error) {
	if len(encoded) == 0 || len(encoded) > maxVersionedMigrationCheckpointBytes {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || len(fields) != 3 {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	if _, ok := fields["format"]; !ok {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	if _, ok := fields["checkpoint"]; !ok {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	if _, ok := fields["sha256"]; !ok {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	var envelope versionedMigrationCheckpointEnvelope
	if err := json.Unmarshal(encoded, &envelope); err != nil || envelope.Format != versionedMigrationCheckpointFormat || len(envelope.Checkpoint) == 0 || len(envelope.SHA256) != versionedMigrationCheckpointHashBytes {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	provided, err := hex.DecodeString(envelope.SHA256)
	if err != nil || len(provided) != sha256.Size {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	digest := sha256.Sum256(envelope.Checkpoint)
	if subtle.ConstantTimeCompare(provided, digest[:]) != 1 {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	var checkpoint VersionedMigrationCheckpoint
	decoder := json.NewDecoder(bytes.NewReader(envelope.Checkpoint))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&checkpoint); err != nil {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return VersionedMigrationCheckpoint{}, ErrVersionedMigrationCheckpoint
	}
	return normalizeVersionedMigrationCheckpoint(checkpoint)
}

func normalizeVersionedMigrationPlan(plan VersionedMigrationPlan) (VersionedMigrationPlan, error) {
	id, err := normalizeVersionedMigrationID(plan.ID, "plan ID")
	if err != nil {
		return VersionedMigrationPlan{}, fmt.Errorf("%w: %v", ErrVersionedMigrationPlanInvalid, err)
	}
	if len(plan.Steps) == 0 || len(plan.Steps) > DefaultVersionedMigrationMaxSteps {
		return VersionedMigrationPlan{}, fmt.Errorf("%w: step count must be between 1 and %d", ErrVersionedMigrationPlanInvalid, DefaultVersionedMigrationMaxSteps)
	}
	base := plan.Base.Clone()
	if err := base.Validate(); err != nil {
		return VersionedMigrationPlan{}, fmt.Errorf("%w: base schema: %v", ErrVersionedMigrationPlanInvalid, err)
	}
	normalized := VersionedMigrationPlan{ID: id, Base: base, Steps: make([]VersionedMigrationStep, len(plan.Steps))}
	current := base.Clone()
	for index, step := range plan.Steps {
		if len(step.Preconditions) > maxVersionedMigrationPreconditions {
			return VersionedMigrationPlan{}, fmt.Errorf("%w: too many preconditions at step %d", ErrVersionedMigrationPlanInvalid, index)
		}
		for _, precondition := range step.Preconditions {
			if precondition == nil {
				return VersionedMigrationPlan{}, fmt.Errorf("%w: nil precondition at step %d", ErrVersionedMigrationPlanInvalid, index)
			}
		}
		migration := cloneVersionedMigration(step.Migration)
		if err := migration.Validate(); err != nil {
			return VersionedMigrationPlan{}, fmt.Errorf("%w: step %d: %v", ErrVersionedMigrationPlanInvalid, index, err)
		}
		if migration.Version != current.Version+1 {
			return VersionedMigrationPlan{}, fmt.Errorf("%w: step %d version %d requires %d", ErrVersionedMigrationPlanInvalid, index, migration.Version, current.Version+1)
		}
		previousFingerprint := current.Fingerprint()
		updated, err := Preview(&current, migration)
		if err != nil {
			return VersionedMigrationPlan{}, fmt.Errorf("%w: step %d preview: %v", ErrVersionedMigrationPlanInvalid, index, err)
		}
		reverted := updated.Clone()
		if err := Revert(&reverted, migration); err != nil || reverted.Fingerprint() != previousFingerprint {
			return VersionedMigrationPlan{}, fmt.Errorf("%w: step %d reverse changes do not restore the previous schema", ErrVersionedMigrationPlanInvalid, index)
		}
		normalized.Steps[index] = VersionedMigrationStep{
			Migration:     migration,
			Preconditions: append([]VersionedMigrationPrecondition(nil), step.Preconditions...),
		}
		current = updated
	}
	return normalized, nil
}

func cloneVersionedMigrationPlan(plan VersionedMigrationPlan) VersionedMigrationPlan {
	clone := VersionedMigrationPlan{ID: plan.ID, Base: plan.Base.Clone(), Steps: make([]VersionedMigrationStep, len(plan.Steps))}
	for index, step := range plan.Steps {
		clone.Steps[index] = cloneVersionedMigrationStep(step)
	}
	return clone
}

func cloneVersionedMigrationStep(step VersionedMigrationStep) VersionedMigrationStep {
	return VersionedMigrationStep{
		Migration:     cloneVersionedMigration(step.Migration),
		Preconditions: append([]VersionedMigrationPrecondition(nil), step.Preconditions...),
	}
}

func cloneVersionedMigration(migration Migration) Migration {
	clone := migration
	clone.Up = cloneVersionedMigrationChanges(migration.Up)
	clone.Down = cloneVersionedMigrationChanges(migration.Down)
	return clone
}

func cloneVersionedMigrationChanges(changes []Change) []Change {
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

func (manager *VersionedMigrationManager) snapshotLocked() VersionedMigrationCheckpoint {
	clients := make([]VersionedMigrationClientVersion, 0, len(manager.clients))
	for id, version := range manager.clients {
		clients = append(clients, VersionedMigrationClientVersion{ID: id, Version: version})
	}
	sort.Slice(clients, func(left, right int) bool { return clients[left].ID < clients[right].ID })
	return VersionedMigrationCheckpoint{
		PlanID:            manager.plan.ID,
		BaseVersion:       manager.plan.Base.Version,
		TargetVersion:     manager.plan.Steps[len(manager.plan.Steps)-1].Migration.Version,
		CurrentVersion:    manager.schema.Version,
		AppliedSteps:      uint64(manager.appliedSteps),
		Generation:        manager.generation,
		Phase:             manager.phase,
		SchemaFingerprint: manager.schema.Fingerprint(),
		Clients:           clients,
	}
}

func (manager *VersionedMigrationManager) clientVersionAllowedLocked(version uint64) bool {
	return versionedMigrationClientVersionAllowed(manager.phase, version, manager.plan.Base.Version, manager.schema.Version)
}

func versionedMigrationClientVersionAllowed(phase VersionedMigrationPhase, version, baseVersion, currentVersion uint64) bool {
	if version < baseVersion || version > currentVersion {
		return false
	}
	if phase == VersionedMigrationRolledBack {
		return version == baseVersion
	}
	return phase == VersionedMigrationActive || phase == VersionedMigrationComplete
}

func normalizeVersionedMigrationID(value, label string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" || !utf8.ValidString(value) || len(value) > maxVersionedMigrationIDBytes {
		return "", fmt.Errorf("%s is empty, invalid UTF-8, or too long", label)
	}
	return value, nil
}

func normalizeVersionedMigrationCheckpoint(checkpoint VersionedMigrationCheckpoint) (VersionedMigrationCheckpoint, error) {
	if err := validateVersionedMigrationCheckpointShape(checkpoint); err != nil {
		return VersionedMigrationCheckpoint{}, err
	}
	clone := checkpoint
	clone.Clients = append([]VersionedMigrationClientVersion(nil), checkpoint.Clients...)
	sort.Slice(clone.Clients, func(left, right int) bool { return clone.Clients[left].ID < clone.Clients[right].ID })
	return clone, nil
}

func validateVersionedMigrationCheckpointShape(checkpoint VersionedMigrationCheckpoint) error {
	if _, err := normalizeVersionedMigrationID(checkpoint.PlanID, "plan ID"); err != nil {
		return fmt.Errorf("%w: %v", ErrVersionedMigrationCheckpoint, err)
	}
	if checkpoint.Generation == 0 || checkpoint.TargetVersion < checkpoint.BaseVersion || checkpoint.CurrentVersion < checkpoint.BaseVersion || checkpoint.CurrentVersion > checkpoint.TargetVersion || checkpoint.SchemaFingerprint == "" {
		return ErrVersionedMigrationCheckpoint
	}
	if len(checkpoint.Clients) > maxVersionedMigrationClients {
		return ErrVersionedMigrationCheckpoint
	}
	seen := make(map[string]struct{}, len(checkpoint.Clients))
	for index, client := range checkpoint.Clients {
		id, err := normalizeVersionedMigrationID(client.ID, "client ID")
		if err != nil || id != client.ID {
			return fmt.Errorf("%w: invalid client at index %d", ErrVersionedMigrationCheckpoint, index)
		}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("%w: duplicate client %q", ErrVersionedMigrationCheckpoint, id)
		}
		seen[id] = struct{}{}
	}
	switch checkpoint.Phase {
	case VersionedMigrationActive:
	case VersionedMigrationComplete:
		if checkpoint.CurrentVersion != checkpoint.TargetVersion {
			return ErrVersionedMigrationCheckpoint
		}
	case VersionedMigrationRolledBack:
		if checkpoint.CurrentVersion != checkpoint.BaseVersion || checkpoint.AppliedSteps != 0 {
			return ErrVersionedMigrationCheckpoint
		}
	default:
		return ErrVersionedMigrationCheckpoint
	}
	return nil
}

func validateVersionedMigrationCheckpointAgainstPlan(checkpoint VersionedMigrationCheckpoint, plan VersionedMigrationPlan) error {
	if checkpoint.Phase == VersionedMigrationActive && checkpoint.AppliedSteps >= uint64(len(plan.Steps)) {
		return fmt.Errorf("%w: active checkpoint has no remaining step", ErrVersionedMigrationCheckpoint)
	}
	if checkpoint.Phase == VersionedMigrationComplete && checkpoint.AppliedSteps != uint64(len(plan.Steps)) {
		return fmt.Errorf("%w: complete checkpoint has unapplied steps", ErrVersionedMigrationCheckpoint)
	}
	if checkpoint.Phase == VersionedMigrationRolledBack && checkpoint.AppliedSteps != 0 {
		return fmt.Errorf("%w: rolled back checkpoint has applied steps", ErrVersionedMigrationCheckpoint)
	}
	for _, client := range checkpoint.Clients {
		if !versionedMigrationClientVersionAllowed(checkpoint.Phase, client.Version, plan.Base.Version, checkpoint.CurrentVersion) {
			return fmt.Errorf("%w: client %q version %d is not admitted", ErrVersionedMigrationCheckpoint, client.ID, client.Version)
		}
	}
	return nil
}
