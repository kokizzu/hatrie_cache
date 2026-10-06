package hatDataStructure

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

const (
	// DefaultVersionedTupleSpaceUpgradeBatch is used when UpgradeStep receives
	// a zero limit. The upgrade remains caller-driven and bounded.
	DefaultVersionedTupleSpaceUpgradeBatch = 128
	// MaxVersionedTupleSpaceKeyBytes prevents an accidental unbounded key from
	// turning an operational migration into an allocation attack.
	MaxVersionedTupleSpaceKeyBytes = 1 << 20
)

var (
	// ErrVersionedTupleSpaceNil indicates a nil space or migration manager.
	ErrVersionedTupleSpaceNil = errors.New("hatDataStructure: versioned tuple space is nil")
	// ErrVersionedTupleSpaceKey indicates an empty or oversized record key.
	ErrVersionedTupleSpaceKey = errors.New("hatDataStructure: versioned tuple space key is invalid")
	// ErrVersionedTupleSpaceNotFound indicates that a key is absent.
	ErrVersionedTupleSpaceNotFound = errors.New("hatDataStructure: versioned tuple space key was not found")
	// ErrVersionedTupleSpaceUpgradeActive indicates that another upgrade is in
	// progress and must be finished or abandoned by the caller.
	ErrVersionedTupleSpaceUpgradeActive = errors.New("hatDataStructure: versioned tuple space upgrade is already active")
	// ErrVersionedTupleSpaceUpgradeInactive indicates that there is no upgrade
	// to advance.
	ErrVersionedTupleSpaceUpgradeInactive = errors.New("hatDataStructure: versioned tuple space upgrade is inactive")
	// ErrVersionedTupleSpaceUpgradeLimit indicates a negative step limit.
	ErrVersionedTupleSpaceUpgradeLimit = errors.New("hatDataStructure: versioned tuple space upgrade limit is invalid")
	// ErrVersionedTupleSpaceUpgradeRecord wraps a record-specific migration
	// failure while leaving that record at its previous version.
	ErrVersionedTupleSpaceUpgradeRecord = errors.New("hatDataStructure: versioned tuple space upgrade record failed")
)

type versionedTupleSpaceEntry struct {
	tuple      VersionedTuple
	generation uint64
}

type versionedTupleSpaceUpgrade struct {
	targetVersion uint64
	keys          []string
	cursor        int
	total         int
	scanned       int
	migrated      int
	failed        map[string]struct{}
	migratedKeys  map[string]struct{}
	active        bool
	complete      bool
	lastError     string
}

// VersionedTupleSpaceUpgradeStatus is a stable snapshot of an online tuple
// upgrade. The upgrade is intentionally not a hidden goroutine: a service can
// schedule bounded UpgradeStep calls according to its own latency budget.
type VersionedTupleSpaceUpgradeStatus struct {
	TargetVersion uint64 `json:"target_version"`
	Total         int    `json:"total"`
	Scanned       int    `json:"scanned"`
	Migrated      int    `json:"migrated"`
	Failed        int    `json:"failed"`
	Pending       int    `json:"pending"`
	Active        bool   `json:"active"`
	Complete      bool   `json:"complete"`
	LastError     string `json:"last_error,omitempty"`
}

// VersionedTupleSpace stores keyed versioned tuples and provides an opt-in,
// online migration controller. Writes are normalized to the current upgrade
// target, while Restore explicitly preserves the supplied version for import
// and recovery workflows.
type VersionedTupleSpace struct {
	manager *VersionedTupleMigrationManager

	mu         sync.RWMutex
	records    map[string]versionedTupleSpaceEntry
	generation uint64
	upgrade    *versionedTupleSpaceUpgrade
}

// NewVersionedTupleSpace creates an empty space using manager's current
// version as the write and upgrade target.
func NewVersionedTupleSpace(manager *VersionedTupleMigrationManager) (*VersionedTupleSpace, error) {
	if manager == nil || manager.CurrentVersion() == 0 {
		return nil, ErrVersionedTupleSpaceNil
	}
	return &VersionedTupleSpace{
		manager: manager,
		records: make(map[string]versionedTupleSpaceEntry),
	}, nil
}

// Restore validates and stores a tuple without changing its schema version.
// This is useful when loading a snapshot that contains older records. Use
// Upsert for normal writes.
func (space *VersionedTupleSpace) Restore(key string, tuple VersionedTuple) error {
	if err := validateVersionedTupleSpaceKey(key); err != nil {
		return err
	}
	if space == nil || space.manager == nil {
		return ErrVersionedTupleSpaceNil
	}
	if _, err := space.manager.MigrateTo(tuple, tuple.Version()); err != nil {
		return err
	}
	space.mu.Lock()
	space.generation++
	space.records[key] = versionedTupleSpaceEntry{tuple: tuple.Clone(), generation: space.generation}
	space.mu.Unlock()
	return nil
}

// Upsert validates and stores a tuple at the active upgrade target, or at the
// manager's current version when no upgrade is active.
func (space *VersionedTupleSpace) Upsert(key string, tuple VersionedTuple) error {
	if err := validateVersionedTupleSpaceKey(key); err != nil {
		return err
	}
	if space == nil || space.manager == nil {
		return ErrVersionedTupleSpaceNil
	}
	targetVersion := space.manager.CurrentVersion()
	space.mu.RLock()
	if space.upgrade != nil && space.upgrade.active {
		targetVersion = space.upgrade.targetVersion
	}
	space.mu.RUnlock()
	migrated, err := space.manager.MigrateTo(tuple, targetVersion)
	if err != nil {
		return err
	}
	space.mu.Lock()
	space.generation++
	space.records[key] = versionedTupleSpaceEntry{tuple: migrated.Clone(), generation: space.generation}
	space.mu.Unlock()
	return nil
}

// Get returns an independent tuple copy. During an active upgrade an older
// record is migrated lazily, so hot records pay the migration cost only when
// read and are then published conditionally using their generation.
func (space *VersionedTupleSpace) Get(key string) (VersionedTuple, error) {
	if err := validateVersionedTupleSpaceKey(key); err != nil {
		return VersionedTuple{}, err
	}
	if space == nil || space.manager == nil {
		return VersionedTuple{}, ErrVersionedTupleSpaceNil
	}
	space.mu.RLock()
	entry, exists := space.records[key]
	targetVersion := uint64(0)
	active := space.upgrade != nil && space.upgrade.active
	if active {
		targetVersion = space.upgrade.targetVersion
	}
	space.mu.RUnlock()
	if !exists {
		return VersionedTuple{}, ErrVersionedTupleSpaceNotFound
	}
	if !active || entry.tuple.Version() == targetVersion {
		return entry.tuple.Clone(), nil
	}

	migrated, err := space.manager.MigrateTo(entry.tuple, targetVersion)
	if err != nil {
		wrapped := newVersionedTupleSpaceUpgradeRecordError(key, err)
		space.mu.Lock()
		space.recordUpgradeFailureLocked(key, wrapped)
		space.mu.Unlock()
		return VersionedTuple{}, wrapped
	}

	space.mu.Lock()
	current, stillExists := space.records[key]
	if stillExists && current.generation == entry.generation {
		space.generation++
		space.records[key] = versionedTupleSpaceEntry{tuple: migrated.Clone(), generation: space.generation}
		space.recordUpgradeSuccessLocked(key)
	} else if stillExists && current.tuple.Version() == targetVersion {
		space.recordUpgradeSuccessLocked(key)
	}
	space.mu.Unlock()
	return migrated.Clone(), nil
}

// Delete removes key and reports whether a record was present.
func (space *VersionedTupleSpace) Delete(key string) bool {
	if space == nil || validateVersionedTupleSpaceKey(key) != nil {
		return false
	}
	space.mu.Lock()
	_, exists := space.records[key]
	if exists {
		delete(space.records, key)
		space.generation++
	}
	space.mu.Unlock()
	return exists
}

// Len returns the number of records currently stored.
func (space *VersionedTupleSpace) Len() int {
	if space == nil {
		return 0
	}
	space.mu.RLock()
	length := len(space.records)
	space.mu.RUnlock()
	return length
}

// StartUpgrade snapshots the older keys and starts a caller-driven upgrade to
// manager's current version. It performs no record migration itself.
func (space *VersionedTupleSpace) StartUpgrade() (VersionedTupleSpaceUpgradeStatus, error) {
	if space == nil || space.manager == nil {
		return VersionedTupleSpaceUpgradeStatus{}, ErrVersionedTupleSpaceNil
	}
	targetVersion := space.manager.CurrentVersion()
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.upgrade != nil && space.upgrade.active {
		return space.upgradeStatusLocked(), ErrVersionedTupleSpaceUpgradeActive
	}
	keys := make([]string, 0, len(space.records))
	for key, entry := range space.records {
		if entry.tuple.Version() != targetVersion {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	upgrade := &versionedTupleSpaceUpgrade{
		targetVersion: targetVersion,
		keys:          keys,
		total:         len(space.records),
		failed:        make(map[string]struct{}),
		migratedKeys:  make(map[string]struct{}),
		active:        len(keys) > 0,
		complete:      len(keys) == 0,
	}
	space.upgrade = upgrade
	return space.upgradeStatusLocked(), nil
}

// UpgradeStep migrates at most limit records. A zero limit uses
// DefaultVersionedTupleSpaceUpgradeBatch. Migration callbacks execute outside
// the space lock, and a generation check prevents an online step from
// overwriting a newer write or restore.
func (space *VersionedTupleSpace) UpgradeStep(limit int) (VersionedTupleSpaceUpgradeStatus, error) {
	if space == nil || space.manager == nil {
		return VersionedTupleSpaceUpgradeStatus{}, ErrVersionedTupleSpaceNil
	}
	if limit < 0 {
		return space.Status(), ErrVersionedTupleSpaceUpgradeLimit
	}
	if limit == 0 {
		limit = DefaultVersionedTupleSpaceUpgradeBatch
	}

	var firstErr error
	for processed := 0; processed < limit; processed++ {
		space.mu.Lock()
		if space.upgrade == nil || !space.upgrade.active {
			status := space.upgradeStatusLocked()
			space.mu.Unlock()
			if status.Complete {
				return status, firstErr
			}
			if firstErr != nil {
				return status, firstErr
			}
			return status, ErrVersionedTupleSpaceUpgradeInactive
		}
		if space.upgrade.cursor >= len(space.upgrade.keys) {
			space.finishUpgradeLocked()
			status := space.upgradeStatusLocked()
			space.mu.Unlock()
			return status, firstErr
		}
		key := space.upgrade.keys[space.upgrade.cursor]
		space.upgrade.cursor++
		space.upgrade.scanned++
		entry, exists := space.records[key]
		targetVersion := space.upgrade.targetVersion
		if !exists {
			space.mu.Unlock()
			continue
		}
		if entry.tuple.Version() == targetVersion {
			space.recordUpgradeSuccessLocked(key)
			space.mu.Unlock()
			continue
		}
		candidate := entry.tuple.Clone()
		generation := entry.generation
		space.mu.Unlock()

		migrated, err := space.manager.MigrateTo(candidate, targetVersion)
		if err != nil {
			wrapped := newVersionedTupleSpaceUpgradeRecordError(key, err)
			space.mu.Lock()
			space.recordUpgradeFailureLocked(key, wrapped)
			space.mu.Unlock()
			if firstErr == nil {
				firstErr = wrapped
			}
			continue
		}

		space.mu.Lock()
		current, stillExists := space.records[key]
		if stillExists && current.generation == generation {
			space.generation++
			space.records[key] = versionedTupleSpaceEntry{tuple: migrated.Clone(), generation: space.generation}
			space.recordUpgradeSuccessLocked(key)
		} else if stillExists && current.tuple.Version() == targetVersion {
			space.recordUpgradeSuccessLocked(key)
		}
		space.mu.Unlock()
	}

	space.mu.Lock()
	if space.upgrade != nil && space.upgrade.active && space.upgrade.cursor >= len(space.upgrade.keys) {
		space.finishUpgradeLocked()
	}
	status := space.upgradeStatusLocked()
	space.mu.Unlock()
	return status, firstErr
}

// Status returns the latest upgrade snapshot. Before StartUpgrade it returns
// an inactive, incomplete status with the current record count.
func (space *VersionedTupleSpace) Status() VersionedTupleSpaceUpgradeStatus {
	if space == nil {
		return VersionedTupleSpaceUpgradeStatus{}
	}
	space.mu.RLock()
	status := space.upgradeStatusLocked()
	space.mu.RUnlock()
	return status
}

func (space *VersionedTupleSpace) upgradeStatusLocked() VersionedTupleSpaceUpgradeStatus {
	status := VersionedTupleSpaceUpgradeStatus{Total: len(space.records)}
	if space.upgrade == nil {
		if space.manager != nil {
			status.TargetVersion = space.manager.CurrentVersion()
		}
		return status
	}
	upgrade := space.upgrade
	status.TargetVersion = upgrade.targetVersion
	status.Total = len(space.records)
	status.Scanned = upgrade.scanned
	status.Migrated = upgrade.migrated
	status.Failed = len(upgrade.failed)
	status.Active = upgrade.active
	status.Complete = upgrade.complete
	status.LastError = upgrade.lastError
	for _, entry := range space.records {
		if entry.tuple.Version() != upgrade.targetVersion {
			status.Pending++
		}
	}
	return status
}

func (space *VersionedTupleSpace) finishUpgradeLocked() {
	if space.upgrade == nil {
		return
	}
	space.upgrade.active = false
	status := space.upgradeStatusLocked()
	space.upgrade.complete = status.Pending == 0 && status.Failed == 0
}

func (space *VersionedTupleSpace) recordUpgradeSuccessLocked(key string) {
	if space.upgrade == nil || !space.upgradeKeyLocked(key) {
		return
	}
	if _, exists := space.upgrade.migratedKeys[key]; !exists {
		space.upgrade.migratedKeys[key] = struct{}{}
		space.upgrade.migrated++
	}
	delete(space.upgrade.failed, key)
}

func (space *VersionedTupleSpace) recordUpgradeFailureLocked(key string, err error) {
	if space.upgrade == nil || !space.upgradeKeyLocked(key) {
		return
	}
	space.upgrade.failed[key] = struct{}{}
	space.upgrade.lastError = err.Error()
}

func (space *VersionedTupleSpace) upgradeKeyLocked(key string) bool {
	if space.upgrade == nil {
		return false
	}
	index := sort.SearchStrings(space.upgrade.keys, key)
	return index < len(space.upgrade.keys) && space.upgrade.keys[index] == key
}

func validateVersionedTupleSpaceKey(key string) error {
	if len(key) == 0 || len(key) > MaxVersionedTupleSpaceKeyBytes {
		return fmt.Errorf("%w: length %d", ErrVersionedTupleSpaceKey, len(key))
	}
	return nil
}

type versionedTupleSpaceUpgradeRecordError struct {
	key string
	err error
}

func newVersionedTupleSpaceUpgradeRecordError(key string, err error) error {
	return &versionedTupleSpaceUpgradeRecordError{key: key, err: err}
}

func (err *versionedTupleSpaceUpgradeRecordError) Error() string {
	return fmt.Sprintf("%v: key %q: %v", ErrVersionedTupleSpaceUpgradeRecord, err.key, err.err)
}

func (err *versionedTupleSpaceUpgradeRecordError) Unwrap() error { return err.err }

func (err *versionedTupleSpaceUpgradeRecordError) Is(target error) bool {
	return target == ErrVersionedTupleSpaceUpgradeRecord || errors.Is(err.err, target)
}
