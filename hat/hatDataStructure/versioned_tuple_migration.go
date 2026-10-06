package hatDataStructure

import (
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrVersionedTupleMigrationFormat indicates an unknown or invalid schema
	// format in a migration manager.
	ErrVersionedTupleMigrationFormat = errors.New("hatDataStructure: versioned tuple migration format is invalid")
	// ErrVersionedTupleMigrationDuplicate indicates a duplicate format or
	// outgoing migration step.
	ErrVersionedTupleMigrationDuplicate = errors.New("hatDataStructure: versioned tuple migration is already registered")
	// ErrVersionedTupleMigrationPath indicates that no forward migration path
	// connects the requested versions.
	ErrVersionedTupleMigrationPath = errors.New("hatDataStructure: versioned tuple migration path is unavailable")
	// ErrVersionedTupleMigrationCycle indicates that a new step would create a
	// cycle in the forward migration graph.
	ErrVersionedTupleMigrationCycle = errors.New("hatDataStructure: versioned tuple migration would create a cycle")
	// ErrVersionedTupleMigrationCallback indicates that a registered migration
	// callback rejected a record.
	ErrVersionedTupleMigrationCallback = errors.New("hatDataStructure: versioned tuple migration callback failed")
	// ErrVersionedTupleMigrationValidation indicates that a record did not
	// validate against the format at one migration boundary.
	ErrVersionedTupleMigrationValidation = errors.New("hatDataStructure: versioned tuple migration validation failed")
	// ErrVersionedTupleMigrationTarget indicates that a callback returned
	// values which cannot be packed by the destination format.
	ErrVersionedTupleMigrationTarget = errors.New("hatDataStructure: versioned tuple migration target is invalid")
)

// VersionedTupleMigrationFunc converts unpacked values from one registered
// format to the values accepted by the next registered format. The input is
// an independent copy and the manager publishes no partial result if the
// callback or destination format rejects it.
type VersionedTupleMigrationFunc func([]TupleFieldValue) ([]TupleFieldValue, error)

// VersionedTupleMigrationPlan describes the exact forward version path that
// a manager will use. Versions is an independent copy and includes both ends.
type VersionedTupleMigrationPlan struct {
	FromVersion uint64
	ToVersion   uint64
	Versions    []uint64
}

type versionedTupleMigrationStep struct {
	to    uint64
	apply VersionedTupleMigrationFunc
}

type versionedTupleMigrationExecution struct {
	from   uint64
	to     uint64
	source TupleFormat
	target TupleFormat
	apply  VersionedTupleMigrationFunc
}

// VersionedTupleMigrationManager keeps immutable tuple formats and one
// forward migration step per source version. It is an opt-in control-plane
// helper: normal VersionedTuple validation and updates remain unchanged, and
// each Migrate call returns a new tuple without mutating its input.
type VersionedTupleMigrationManager struct {
	mu             sync.RWMutex
	formats        map[uint64]TupleFormat
	steps          map[uint64]versionedTupleMigrationStep
	currentVersion uint64
}

// NewVersionedTupleMigrationManager creates a manager whose current target is
// format. Older formats and forward steps can be registered afterward.
func NewVersionedTupleMigrationManager(format TupleFormat) (*VersionedTupleMigrationManager, error) {
	if err := format.validateDefinition(); err != nil {
		return nil, fmt.Errorf("%w: current format: %v", ErrVersionedTupleMigrationFormat, err)
	}
	return &VersionedTupleMigrationManager{
		formats:        map[uint64]TupleFormat{format.version: format},
		steps:          make(map[uint64]versionedTupleMigrationStep),
		currentVersion: format.version,
	}, nil
}

// RegisterFormat adds one immutable schema version to the migration catalog.
func (manager *VersionedTupleMigrationManager) RegisterFormat(format TupleFormat) error {
	if manager == nil {
		return ErrVersionedTupleMigrationFormat
	}
	if err := format.validateDefinition(); err != nil {
		return fmt.Errorf("%w: %v", ErrVersionedTupleMigrationFormat, err)
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.formats[format.version]; exists {
		return fmt.Errorf("%w: format version %d", ErrVersionedTupleMigrationDuplicate, format.version)
	}
	manager.formats[format.version] = format
	return nil
}

// SetCurrentVersion changes the target used by Migrate after the format has
// already been registered.
func (manager *VersionedTupleMigrationManager) SetCurrentVersion(version uint64) error {
	if manager == nil {
		return ErrVersionedTupleMigrationFormat
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.formats[version]; !exists {
		return fmt.Errorf("%w: format version %d", ErrVersionedTupleMigrationFormat, version)
	}
	manager.currentVersion = version
	return nil
}

// CurrentVersion returns the target used by Migrate. It returns zero for a
// nil manager.
func (manager *VersionedTupleMigrationManager) CurrentVersion() uint64 {
	if manager == nil {
		return 0
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.currentVersion
}

// RegisterMigration registers one outgoing step. Requiring one step per
// source keeps planning deterministic and makes accidental branching visible
// at registration time.
func (manager *VersionedTupleMigrationManager) RegisterMigration(fromVersion, toVersion uint64, apply VersionedTupleMigrationFunc) error {
	if manager == nil {
		return ErrVersionedTupleMigrationFormat
	}
	if fromVersion == 0 || toVersion == 0 {
		return fmt.Errorf("%w: versions must be positive", ErrVersionedTupleMigrationFormat)
	}
	if fromVersion == toVersion {
		return ErrVersionedTupleMigrationPath
	}
	if apply == nil {
		return ErrVersionedTupleMigrationCallback
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if _, exists := manager.formats[fromVersion]; !exists {
		return fmt.Errorf("%w: source format version %d", ErrVersionedTupleMigrationFormat, fromVersion)
	}
	if _, exists := manager.formats[toVersion]; !exists {
		return fmt.Errorf("%w: target format version %d", ErrVersionedTupleMigrationFormat, toVersion)
	}
	if _, exists := manager.steps[fromVersion]; exists {
		return fmt.Errorf("%w: source format version %d", ErrVersionedTupleMigrationDuplicate, fromVersion)
	}
	for version := toVersion; ; {
		if version == fromVersion {
			return fmt.Errorf("%w: %d -> %d", ErrVersionedTupleMigrationCycle, fromVersion, toVersion)
		}
		step, exists := manager.steps[version]
		if !exists {
			break
		}
		version = step.to
	}
	manager.steps[fromVersion] = versionedTupleMigrationStep{to: toVersion, apply: apply}
	return nil
}

// Plan returns the deterministic forward path from one registered version to
// another. Both endpoints may be equal.
func (manager *VersionedTupleMigrationManager) Plan(fromVersion, toVersion uint64) (VersionedTupleMigrationPlan, error) {
	if manager == nil {
		return VersionedTupleMigrationPlan{}, ErrVersionedTupleMigrationFormat
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	return manager.planLocked(fromVersion, toVersion)
}

func (manager *VersionedTupleMigrationManager) planLocked(fromVersion, toVersion uint64) (VersionedTupleMigrationPlan, error) {
	if _, exists := manager.formats[fromVersion]; !exists {
		return VersionedTupleMigrationPlan{}, fmt.Errorf("%w: source format version %d", ErrVersionedTupleMigrationFormat, fromVersion)
	}
	if _, exists := manager.formats[toVersion]; !exists {
		return VersionedTupleMigrationPlan{}, fmt.Errorf("%w: target format version %d", ErrVersionedTupleMigrationFormat, toVersion)
	}
	plan := VersionedTupleMigrationPlan{FromVersion: fromVersion, ToVersion: toVersion, Versions: []uint64{fromVersion}}
	if fromVersion == toVersion {
		return plan, nil
	}
	visited := map[uint64]struct{}{fromVersion: {}}
	version := fromVersion
	for version != toVersion {
		step, exists := manager.steps[version]
		if !exists {
			return VersionedTupleMigrationPlan{}, fmt.Errorf("%w: %d -> %d", ErrVersionedTupleMigrationPath, fromVersion, toVersion)
		}
		version = step.to
		if _, seen := visited[version]; seen {
			return VersionedTupleMigrationPlan{}, fmt.Errorf("%w: at version %d", ErrVersionedTupleMigrationCycle, version)
		}
		visited[version] = struct{}{}
		plan.Versions = append(plan.Versions, version)
	}
	return plan, nil
}

// Migrate upgrades tuple to the manager's current version.
func (manager *VersionedTupleMigrationManager) Migrate(tuple VersionedTuple) (VersionedTuple, error) {
	if manager == nil {
		return VersionedTuple{}, ErrVersionedTupleMigrationFormat
	}
	return manager.MigrateTo(tuple, manager.CurrentVersion())
}

// MigrateTo applies the registered forward path to targetVersion. Input is
// never changed; a callback, validation, or packing error returns no partial
// tuple.
func (manager *VersionedTupleMigrationManager) MigrateTo(tuple VersionedTuple, targetVersion uint64) (VersionedTuple, error) {
	if manager == nil {
		return VersionedTuple{}, ErrVersionedTupleMigrationFormat
	}
	manager.mu.RLock()
	plan, err := manager.planLocked(tuple.Version(), targetVersion)
	if err != nil {
		manager.mu.RUnlock()
		return VersionedTuple{}, err
	}
	executions := make([]versionedTupleMigrationExecution, len(plan.Versions)-1)
	for index := range executions {
		fromVersion := plan.Versions[index]
		toVersion := plan.Versions[index+1]
		step := manager.steps[fromVersion]
		executions[index] = versionedTupleMigrationExecution{
			from:   fromVersion,
			to:     toVersion,
			source: manager.formats[fromVersion],
			target: manager.formats[toVersion],
			apply:  step.apply,
		}
	}
	sourceFormat := manager.formats[tuple.Version()]
	manager.mu.RUnlock()

	if len(executions) == 0 {
		if err := tuple.Validate(sourceFormat); err != nil {
			return VersionedTuple{}, fmt.Errorf("%w: version %d: %v", ErrVersionedTupleMigrationValidation, tuple.Version(), err)
		}
		return tuple, nil
	}
	current := tuple
	for _, execution := range executions {
		if err := current.Validate(execution.source); err != nil {
			return VersionedTuple{}, fmt.Errorf("%w: version %d: %v", ErrVersionedTupleMigrationValidation, execution.from, err)
		}
		values, err := execution.source.Unpack(current.Tuple())
		if err != nil {
			return VersionedTuple{}, fmt.Errorf("%w: unpack version %d: %v", ErrVersionedTupleMigrationValidation, execution.from, err)
		}
		migrated, err := execution.apply(values)
		if err != nil {
			return VersionedTuple{}, fmt.Errorf("%w: %d -> %d: %v", ErrVersionedTupleMigrationCallback, execution.from, execution.to, err)
		}
		next, err := execution.target.PackVersioned(migrated)
		if err != nil {
			return VersionedTuple{}, fmt.Errorf("%w: %d -> %d: %v", ErrVersionedTupleMigrationTarget, execution.from, execution.to, err)
		}
		current = next
	}
	return current, nil
}
