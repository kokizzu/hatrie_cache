package hatDataStructure

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sort"
	"strings"
	"sync"
)

const (
	// MaxVersionedMigrationSnapshotBytes bounds one encoded migration manager
	// snapshot before its JSON payload is decoded.
	MaxVersionedMigrationSnapshotBytes      = 16 << 20
	migrationSnapshotVersion           byte = 1
)

var (
	// ErrVersionedMigrationNil reports an operation on a nil manager.
	ErrVersionedMigrationNil = errors.New("versioned migration manager is nil")
	// ErrVersionedMigrationInvalidPlan reports invalid migration metadata.
	ErrVersionedMigrationInvalidPlan = errors.New("versioned migration plan is invalid")
	// ErrVersionedMigrationInvalidClient reports invalid client capability metadata.
	ErrVersionedMigrationInvalidClient = errors.New("versioned migration client is invalid")
	// ErrVersionedMigrationExists reports a duplicate plan name.
	ErrVersionedMigrationExists = errors.New("versioned migration already exists")
	// ErrVersionedMigrationNotFound reports an unknown plan name.
	ErrVersionedMigrationNotFound = errors.New("versioned migration was not found")
	// ErrVersionedMigrationInvalidPhase reports an invalid lifecycle transition.
	ErrVersionedMigrationInvalidPhase = errors.New("versioned migration phase does not allow this operation")
	// ErrVersionedMigrationDependency reports an unsatisfied or unsafe dependency.
	ErrVersionedMigrationDependency = errors.New("versioned migration dependency is not satisfied")
	// ErrVersionedMigrationPrecondition reports an unacknowledged precondition.
	ErrVersionedMigrationPrecondition = errors.New("versioned migration precondition is not satisfied")
	// ErrVersionedMigrationClientCompatibility reports mixed-version incompatibility.
	ErrVersionedMigrationClientCompatibility = errors.New("versioned migration client is incompatible")
	// ErrVersionedMigrationProgress reports invalid progress changes.
	ErrVersionedMigrationProgress = errors.New("versioned migration progress is invalid")
	// ErrVersionedMigrationSnapshot reports a malformed or corrupt snapshot.
	ErrVersionedMigrationSnapshot = errors.New("versioned migration snapshot is invalid")
	// ErrVersionedMigrationSnapshotLimit reports a snapshot size limit violation.
	ErrVersionedMigrationSnapshotLimit = errors.New("versioned migration snapshot exceeds limit")
)

var migrationSnapshotMagic = [4]byte{'H', 'M', 'G', '1'}
var migrationSnapshotCRC32CTable = crc32.MakeTable(crc32.Castagnoli)

// MigrationPhase is the durable lifecycle state of a migration plan.
type MigrationPhase uint8

const (
	MigrationPending MigrationPhase = iota + 1
	MigrationRunning
	MigrationPaused
	MigrationCompleted
	MigrationRolledBack
)

// String returns the stable human-readable phase name.
func (phase MigrationPhase) String() string {
	switch phase {
	case MigrationPending:
		return "pending"
	case MigrationRunning:
		return "running"
	case MigrationPaused:
		return "paused"
	case MigrationCompleted:
		return "completed"
	case MigrationRolledBack:
		return "rolled_back"
	default:
		return "unknown"
	}
}

// MigrationPlan names a version transition and the checks required before it
// can run. TotalUnits is an application-defined work estimate used for durable
// progress; the manager does not modify application records itself.
type MigrationPlan struct {
	Name          string   `json:"name"`
	FromVersion   uint64   `json:"from_version"`
	ToVersion     uint64   `json:"to_version"`
	TotalUnits    uint64   `json:"total_units"`
	Dependencies  []string `json:"dependencies,omitempty"`
	Preconditions []string `json:"preconditions,omitempty"`
}

// MigrationClient describes the schema versions a client can read and write
// during a mixed-version migration window.
type MigrationClient struct {
	Name       string `json:"name"`
	MinVersion uint64 `json:"min_version"`
	MaxVersion uint64 `json:"max_version"`
}

// MigrationStatus is an immutable copy of one plan's current lifecycle state.
type MigrationStatus struct {
	Plan                      MigrationPlan  `json:"plan"`
	Phase                     MigrationPhase `json:"phase"`
	CompletedUnits            uint64         `json:"completed_units"`
	AcknowledgedPreconditions []string       `json:"acknowledged_preconditions,omitempty"`
}

type versionedMigrationRecord struct {
	plan         MigrationPlan
	phase        MigrationPhase
	completed    uint64
	acknowledged map[string]struct{}
}

// VersionedMigrationManager tracks named, resumable schema migrations. It is
// thread-safe and persists only coordination metadata; callers perform the
// actual conversion and report units through Advance.
type VersionedMigrationManager struct {
	mu      sync.Mutex
	plans   map[string]*versionedMigrationRecord
	clients map[string]MigrationClient
}

// NewVersionedMigrationManager creates an empty migration manager.
func NewVersionedMigrationManager() *VersionedMigrationManager {
	return &VersionedMigrationManager{
		plans:   make(map[string]*versionedMigrationRecord),
		clients: make(map[string]MigrationClient),
	}
}

// RegisterPlan adds a plan in the pending phase. Plan names are unique.
func (manager *VersionedMigrationManager) RegisterPlan(plan MigrationPlan) error {
	if manager == nil {
		return ErrVersionedMigrationNil
	}
	if err := validateMigrationPlan(plan); err != nil {
		return err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.ensureMapsLocked()
	if _, exists := manager.plans[plan.Name]; exists {
		return fmt.Errorf("%w: %s", ErrVersionedMigrationExists, plan.Name)
	}
	manager.plans[plan.Name] = &versionedMigrationRecord{
		plan:         copyMigrationPlan(plan),
		phase:        MigrationPending,
		acknowledged: make(map[string]struct{}),
	}
	return nil
}

// RegisterClient registers or updates a client's supported version range. An
// update that would make a running migration unsafe is rejected.
func (manager *VersionedMigrationManager) RegisterClient(client MigrationClient) error {
	if manager == nil {
		return ErrVersionedMigrationNil
	}
	if err := validateMigrationClient(client); err != nil {
		return err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	manager.ensureMapsLocked()
	for _, record := range manager.plans {
		if isActiveMigrationPhase(record.phase) && !clientSupportsPlan(client, record.plan) {
			return fmt.Errorf("%w: client %s cannot support %s", ErrVersionedMigrationClientCompatibility, client.Name, record.plan.Name)
		}
	}
	manager.clients[client.Name] = client
	return nil
}

// UnregisterClient removes a client capability record and reports whether it
// existed. Membership and liveness policy remain the caller's responsibility.
func (manager *VersionedMigrationManager) UnregisterClient(name string) bool {
	if manager == nil {
		return false
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.clients[name]; !exists {
		return false
	}
	for _, record := range manager.plans {
		if isActiveMigrationPhase(record.phase) {
			return false
		}
	}
	delete(manager.clients, name)
	return true
}

// AcknowledgePrecondition records a plan-specific precondition before start.
func (manager *VersionedMigrationManager) AcknowledgePrecondition(name, precondition string) error {
	if manager == nil {
		return ErrVersionedMigrationNil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	record, err := manager.planLocked(name)
	if err != nil {
		return err
	}
	if record.phase != MigrationPending && record.phase != MigrationRolledBack {
		return fmt.Errorf("%w: %s is %s", ErrVersionedMigrationInvalidPhase, name, record.phase)
	}
	if !containsString(record.plan.Preconditions, precondition) {
		return fmt.Errorf("%w: %s", ErrVersionedMigrationPrecondition, precondition)
	}
	record.acknowledged[precondition] = struct{}{}
	return nil
}

// Start moves a pending or rolled-back plan to running after dependencies,
// preconditions, and all registered client version ranges pass validation.
func (manager *VersionedMigrationManager) Start(name string) error {
	if manager == nil {
		return ErrVersionedMigrationNil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	record, err := manager.planLocked(name)
	if err != nil {
		return err
	}
	if record.phase != MigrationPending && record.phase != MigrationRolledBack {
		return fmt.Errorf("%w: %s is %s", ErrVersionedMigrationInvalidPhase, name, record.phase)
	}
	if err := manager.validateStartLocked(record); err != nil {
		return err
	}
	record.phase = MigrationRunning
	record.completed = 0
	return nil
}

// Pause pauses a running plan without changing its progress.
func (manager *VersionedMigrationManager) Pause(name string) error {
	if manager == nil {
		return ErrVersionedMigrationNil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	record, err := manager.planLocked(name)
	if err != nil {
		return err
	}
	if record.phase != MigrationRunning {
		return fmt.Errorf("%w: %s is %s", ErrVersionedMigrationInvalidPhase, name, record.phase)
	}
	record.phase = MigrationPaused
	return nil
}

// Resume revalidates safety gates and resumes a paused plan.
func (manager *VersionedMigrationManager) Resume(name string) error {
	if manager == nil {
		return ErrVersionedMigrationNil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	record, err := manager.planLocked(name)
	if err != nil {
		return err
	}
	if record.phase != MigrationPaused {
		return fmt.Errorf("%w: %s is %s", ErrVersionedMigrationInvalidPhase, name, record.phase)
	}
	if err := manager.validateStartLocked(record); err != nil {
		return err
	}
	record.phase = MigrationRunning
	return nil
}

// Advance adds application-reported work to a running plan. Progress changes
// are checked before mutation and completion is recorded exactly at TotalUnits.
func (manager *VersionedMigrationManager) Advance(name string, units uint64) error {
	if manager == nil {
		return ErrVersionedMigrationNil
	}
	if units == 0 {
		return ErrVersionedMigrationProgress
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	record, err := manager.planLocked(name)
	if err != nil {
		return err
	}
	if record.phase != MigrationRunning {
		return fmt.Errorf("%w: %s is %s", ErrVersionedMigrationInvalidPhase, name, record.phase)
	}
	if units > record.plan.TotalUnits-record.completed {
		return ErrVersionedMigrationProgress
	}
	record.completed += units
	if record.completed == record.plan.TotalUnits {
		record.phase = MigrationCompleted
	}
	return nil
}

// Rollback marks a plan rolled back and clears progress and acknowledgements.
// A plan cannot roll back while a dependent plan has started or completed.
func (manager *VersionedMigrationManager) Rollback(name string) error {
	if manager == nil {
		return ErrVersionedMigrationNil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	record, err := manager.planLocked(name)
	if err != nil {
		return err
	}
	if record.phase != MigrationRunning && record.phase != MigrationPaused && record.phase != MigrationCompleted {
		return fmt.Errorf("%w: %s is %s", ErrVersionedMigrationInvalidPhase, name, record.phase)
	}
	for _, dependent := range manager.plans {
		if containsString(dependent.plan.Dependencies, name) && dependent.phase != MigrationPending && dependent.phase != MigrationRolledBack {
			return fmt.Errorf("%w: %s depends on %s", ErrVersionedMigrationDependency, dependent.plan.Name, name)
		}
	}
	record.phase = MigrationRolledBack
	record.completed = 0
	record.acknowledged = make(map[string]struct{})
	return nil
}

// Status returns an independent copy of one plan's state.
func (manager *VersionedMigrationManager) Status(name string) (MigrationStatus, error) {
	if manager == nil {
		return MigrationStatus{}, ErrVersionedMigrationNil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	record, err := manager.planLocked(name)
	if err != nil {
		return MigrationStatus{}, err
	}
	return migrationStatus(record), nil
}

// List returns independent statuses sorted by plan name.
func (manager *VersionedMigrationManager) List() []MigrationStatus {
	if manager == nil {
		return nil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	statuses := make([]MigrationStatus, 0, len(manager.plans))
	for _, record := range manager.plans {
		statuses = append(statuses, migrationStatus(record))
	}
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Plan.Name < statuses[j].Plan.Name })
	return statuses
}

// MarshalBinary encodes coordination state as a bounded HTM1 JSON payload
// protected by a CRC32C checksum.
func (manager *VersionedMigrationManager) MarshalBinary() ([]byte, error) {
	if manager == nil {
		return nil, ErrVersionedMigrationNil
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	snapshot := manager.snapshotLocked()
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrVersionedMigrationSnapshot, err)
	}
	if len(payload) > MaxVersionedMigrationSnapshotBytes-13 {
		return nil, ErrVersionedMigrationSnapshotLimit
	}
	encoded := make([]byte, 13+len(payload))
	copy(encoded[:4], migrationSnapshotMagic[:])
	encoded[4] = migrationSnapshotVersion
	binary.BigEndian.PutUint32(encoded[5:9], uint32(len(payload)))
	copy(encoded[9:9+len(payload)], payload)
	binary.BigEndian.PutUint32(encoded[9+len(payload):], crc32.Checksum(payload, migrationSnapshotCRC32CTable))
	return encoded, nil
}

// UnmarshalBinary validates and atomically restores an HTM1 migration snapshot.
func (manager *VersionedMigrationManager) UnmarshalBinary(encoded []byte) error {
	if manager == nil {
		return ErrVersionedMigrationNil
	}
	if len(encoded) < 13 {
		return ErrVersionedMigrationSnapshot
	}
	if len(encoded) > MaxVersionedMigrationSnapshotBytes {
		return ErrVersionedMigrationSnapshotLimit
	}
	if !bytes.Equal(encoded[:4], migrationSnapshotMagic[:]) || encoded[4] != migrationSnapshotVersion {
		return ErrVersionedMigrationSnapshot
	}
	payloadLength := binary.BigEndian.Uint32(encoded[5:9])
	if payloadLength > uint32(MaxVersionedMigrationSnapshotBytes-13) || uint64(13)+uint64(payloadLength) != uint64(len(encoded)) {
		return ErrVersionedMigrationSnapshot
	}
	payloadEnd := 9 + int(payloadLength)
	payload := encoded[9:payloadEnd]
	checksum := binary.BigEndian.Uint32(encoded[payloadEnd:])
	if crc32.Checksum(payload, migrationSnapshotCRC32CTable) != checksum {
		return ErrVersionedMigrationSnapshot
	}
	var snapshot versionedMigrationSnapshot
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return fmt.Errorf("%w: %v", ErrVersionedMigrationSnapshot, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return ErrVersionedMigrationSnapshot
	}
	plans, clients, err := restoreMigrationSnapshot(snapshot)
	if err != nil {
		return err
	}
	manager.mu.Lock()
	manager.plans = plans
	manager.clients = clients
	manager.mu.Unlock()
	return nil
}

type versionedMigrationSnapshot struct {
	Version uint8                            `json:"version"`
	Plans   []versionedMigrationSnapshotPlan `json:"plans"`
	Clients []MigrationClient                `json:"clients"`
}

type versionedMigrationSnapshotPlan struct {
	Plan                      MigrationPlan  `json:"plan"`
	Phase                     MigrationPhase `json:"phase"`
	CompletedUnits            uint64         `json:"completed_units"`
	AcknowledgedPreconditions []string       `json:"acknowledged_preconditions,omitempty"`
}

func (manager *VersionedMigrationManager) ensureMapsLocked() {
	if manager.plans == nil {
		manager.plans = make(map[string]*versionedMigrationRecord)
	}
	if manager.clients == nil {
		manager.clients = make(map[string]MigrationClient)
	}
}

func (manager *VersionedMigrationManager) planLocked(name string) (*versionedMigrationRecord, error) {
	manager.ensureMapsLocked()
	record, ok := manager.plans[name]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrVersionedMigrationNotFound, name)
	}
	return record, nil
}

func (manager *VersionedMigrationManager) validateStartLocked(record *versionedMigrationRecord) error {
	for _, dependency := range record.plan.Dependencies {
		other, ok := manager.plans[dependency]
		if !ok || other.phase != MigrationCompleted {
			return fmt.Errorf("%w: %s", ErrVersionedMigrationDependency, dependency)
		}
	}
	for _, precondition := range record.plan.Preconditions {
		if _, ok := record.acknowledged[precondition]; !ok {
			return fmt.Errorf("%w: %s", ErrVersionedMigrationPrecondition, precondition)
		}
	}
	for _, client := range manager.clients {
		if !clientSupportsPlan(client, record.plan) {
			return fmt.Errorf("%w: client %s cannot support %s", ErrVersionedMigrationClientCompatibility, client.Name, record.plan.Name)
		}
	}
	return nil
}

func (manager *VersionedMigrationManager) snapshotLocked() versionedMigrationSnapshot {
	snapshot := versionedMigrationSnapshot{Version: migrationSnapshotVersion}
	planNames := make([]string, 0, len(manager.plans))
	for name := range manager.plans {
		planNames = append(planNames, name)
	}
	sort.Strings(planNames)
	for _, name := range planNames {
		record := manager.plans[name]
		status := migrationStatus(record)
		snapshot.Plans = append(snapshot.Plans, versionedMigrationSnapshotPlan{
			Plan:                      status.Plan,
			Phase:                     status.Phase,
			CompletedUnits:            status.CompletedUnits,
			AcknowledgedPreconditions: status.AcknowledgedPreconditions,
		})
	}
	clientNames := make([]string, 0, len(manager.clients))
	for name := range manager.clients {
		clientNames = append(clientNames, name)
	}
	sort.Strings(clientNames)
	for _, name := range clientNames {
		snapshot.Clients = append(snapshot.Clients, manager.clients[name])
	}
	return snapshot
}

func restoreMigrationSnapshot(snapshot versionedMigrationSnapshot) (map[string]*versionedMigrationRecord, map[string]MigrationClient, error) {
	if snapshot.Version != migrationSnapshotVersion {
		return nil, nil, ErrVersionedMigrationSnapshot
	}
	clients := make(map[string]MigrationClient, len(snapshot.Clients))
	for _, client := range snapshot.Clients {
		if err := validateMigrationClient(client); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrVersionedMigrationSnapshot, err)
		}
		if _, exists := clients[client.Name]; exists {
			return nil, nil, ErrVersionedMigrationSnapshot
		}
		clients[client.Name] = client
	}
	plans := make(map[string]*versionedMigrationRecord, len(snapshot.Plans))
	for _, item := range snapshot.Plans {
		if err := validateMigrationPlan(item.Plan); err != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrVersionedMigrationSnapshot, err)
		}
		if _, exists := plans[item.Plan.Name]; exists {
			return nil, nil, ErrVersionedMigrationSnapshot
		}
		if !validMigrationPhase(item.Phase) || item.CompletedUnits > item.Plan.TotalUnits {
			return nil, nil, ErrVersionedMigrationSnapshot
		}
		if (item.Phase == MigrationPending || item.Phase == MigrationRolledBack) && item.CompletedUnits != 0 {
			return nil, nil, ErrVersionedMigrationSnapshot
		}
		if item.Phase == MigrationCompleted && item.CompletedUnits != item.Plan.TotalUnits {
			return nil, nil, ErrVersionedMigrationSnapshot
		}
		if (item.Phase == MigrationRunning || item.Phase == MigrationPaused) && item.CompletedUnits == item.Plan.TotalUnits {
			return nil, nil, ErrVersionedMigrationSnapshot
		}
		acknowledged := make(map[string]struct{}, len(item.AcknowledgedPreconditions))
		for _, precondition := range item.AcknowledgedPreconditions {
			if !containsString(item.Plan.Preconditions, precondition) {
				return nil, nil, ErrVersionedMigrationSnapshot
			}
			if _, exists := acknowledged[precondition]; exists {
				return nil, nil, ErrVersionedMigrationSnapshot
			}
			acknowledged[precondition] = struct{}{}
		}
		plans[item.Plan.Name] = &versionedMigrationRecord{
			plan:         copyMigrationPlan(item.Plan),
			phase:        item.Phase,
			completed:    item.CompletedUnits,
			acknowledged: acknowledged,
		}
	}
	for _, record := range plans {
		if record.phase == MigrationRolledBack && len(record.acknowledged) != 0 {
			return nil, nil, ErrVersionedMigrationSnapshot
		}
		if record.phase == MigrationRunning || record.phase == MigrationPaused || record.phase == MigrationCompleted {
			for _, precondition := range record.plan.Preconditions {
				if _, ok := record.acknowledged[precondition]; !ok {
					return nil, nil, ErrVersionedMigrationSnapshot
				}
			}
			for _, dependency := range record.plan.Dependencies {
				other, ok := plans[dependency]
				if !ok || other.phase != MigrationCompleted {
					return nil, nil, ErrVersionedMigrationSnapshot
				}
			}
		}
		if isActiveMigrationPhase(record.phase) {
			for _, client := range clients {
				if !clientSupportsPlan(client, record.plan) {
					return nil, nil, fmt.Errorf("%w: active plan %s and client %s", ErrVersionedMigrationSnapshot, record.plan.Name, client.Name)
				}
			}
		}
	}
	return plans, clients, nil
}

func migrationStatus(record *versionedMigrationRecord) MigrationStatus {
	acknowledged := make([]string, 0, len(record.acknowledged))
	for precondition := range record.acknowledged {
		acknowledged = append(acknowledged, precondition)
	}
	sort.Strings(acknowledged)
	return MigrationStatus{
		Plan:                      copyMigrationPlan(record.plan),
		Phase:                     record.phase,
		CompletedUnits:            record.completed,
		AcknowledgedPreconditions: acknowledged,
	}
}

func validateMigrationPlan(plan MigrationPlan) error {
	if strings.TrimSpace(plan.Name) == "" || plan.FromVersion == 0 || plan.ToVersion <= plan.FromVersion || plan.TotalUnits == 0 {
		return fmt.Errorf("%w: name, versions, and total units are invalid", ErrVersionedMigrationInvalidPlan)
	}
	if containsDuplicateOrEmpty(plan.Dependencies) || containsString(plan.Dependencies, plan.Name) {
		return fmt.Errorf("%w: dependencies are invalid", ErrVersionedMigrationInvalidPlan)
	}
	if containsDuplicateOrEmpty(plan.Preconditions) {
		return fmt.Errorf("%w: preconditions are invalid", ErrVersionedMigrationInvalidPlan)
	}
	return nil
}

func validateMigrationClient(client MigrationClient) error {
	if strings.TrimSpace(client.Name) == "" || client.MinVersion == 0 || client.MaxVersion < client.MinVersion {
		return fmt.Errorf("%w: name or version range is invalid", ErrVersionedMigrationInvalidClient)
	}
	return nil
}

func containsDuplicateOrEmpty(values []string) bool {
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			return true
		}
		if _, exists := seen[value]; exists {
			return true
		}
		seen[value] = struct{}{}
	}
	return false
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func copyMigrationPlan(plan MigrationPlan) MigrationPlan {
	plan.Dependencies = append([]string(nil), plan.Dependencies...)
	plan.Preconditions = append([]string(nil), plan.Preconditions...)
	return plan
}

func clientSupportsPlan(client MigrationClient, plan MigrationPlan) bool {
	return client.MinVersion <= plan.FromVersion && client.MaxVersion >= plan.ToVersion
}

func isActiveMigrationPhase(phase MigrationPhase) bool {
	return phase == MigrationRunning || phase == MigrationPaused
}

func validMigrationPhase(phase MigrationPhase) bool {
	return phase >= MigrationPending && phase <= MigrationRolledBack
}
