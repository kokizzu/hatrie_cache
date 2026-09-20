package hatDataStructure

import (
	"errors"
	"sync"
)

var (
	// ErrOnlineSpaceNil reports an operation on a nil upgrade space.
	ErrOnlineSpaceNil = errors.New("online space upgrade is nil")
	// ErrOnlineSpaceInvalidVersion reports an invalid schema version transition.
	ErrOnlineSpaceInvalidVersion = errors.New("online space schema version is invalid")
	// ErrOnlineSpaceMissingConverter reports a missing row converter.
	ErrOnlineSpaceMissingConverter = errors.New("online space upgrade converter is missing")
	// ErrOnlineSpaceUpgradePhase reports an operation that is invalid in the current phase.
	ErrOnlineSpaceUpgradePhase = errors.New("online space upgrade phase does not allow this operation")
	// ErrOnlineSpaceUpgradePending reports a cutover attempted with old records remaining.
	ErrOnlineSpaceUpgradePending = errors.New("online space upgrade still has pending records")
	// ErrOnlineSpaceInvalidBatch reports a non-positive conversion batch size.
	ErrOnlineSpaceInvalidBatch = errors.New("online space upgrade batch size must be positive")
)

// OnlineUpgradePhase is the lifecycle phase of an online format conversion.
type OnlineUpgradePhase uint8

const (
	OnlineUpgradeIdle OnlineUpgradePhase = iota + 1
	OnlineUpgradeRunning
	OnlineUpgradePaused
	OnlineUpgradeCompleted
)

// String returns the stable human-readable upgrade phase.
func (phase OnlineUpgradePhase) String() string {
	switch phase {
	case OnlineUpgradeIdle:
		return "idle"
	case OnlineUpgradeRunning:
		return "running"
	case OnlineUpgradePaused:
		return "paused"
	case OnlineUpgradeCompleted:
		return "completed"
	default:
		return "unknown"
	}
}

// OnlineSpaceUpgradeStats reports the current conversion state. PendingRecords
// is the number of records still stored below TargetVersion. ConvertedRecords
// counts successful conversions in the current upgrade.
type OnlineSpaceUpgradeStats struct {
	CurrentVersion   uint64
	TargetVersion    uint64
	Phase            OnlineUpgradePhase
	Items            int
	PendingRecords   uint64
	ConvertedRecords uint64
	ScannedRecords   int
	ScanTotal        int
}

type onlineSpaceUpgradeEntry[V any] struct {
	version uint64
	value   V
}

// OnlineSpaceUpgrade is a thread-safe generic space with online versioned
// values. During an upgrade, Set writes the target format while Get lazily
// converts old values and UpgradeBatch performs bounded background work. The
// converter must not call back into this space while it is running.
type OnlineSpaceUpgrade[K comparable, V any] struct {
	mu             sync.Mutex
	currentVersion uint64
	targetVersion  uint64
	phase          OnlineUpgradePhase
	converter      func(V) (V, error)
	records        map[K]onlineSpaceUpgradeEntry[V]
	scanKeys       []K
	scanPosition   int
	pendingRecords uint64
	converted      uint64
}

// NewOnlineSpaceUpgrade creates a space at initialVersion.
func NewOnlineSpaceUpgrade[K comparable, V any](initialVersion uint64) (*OnlineSpaceUpgrade[K, V], error) {
	if initialVersion == 0 {
		return nil, ErrOnlineSpaceInvalidVersion
	}
	return &OnlineSpaceUpgrade[K, V]{
		currentVersion: initialVersion,
		phase:          OnlineUpgradeIdle,
		records:        make(map[K]onlineSpaceUpgradeEntry[V]),
	}, nil
}

// Begin starts a target-version conversion. The target must be newer than the
// current version and converter must return the target representation.
func (space *OnlineSpaceUpgrade[K, V]) Begin(targetVersion uint64, converter func(V) (V, error)) error {
	if space == nil {
		return ErrOnlineSpaceNil
	}
	if targetVersion == 0 {
		return ErrOnlineSpaceInvalidVersion
	}
	if converter == nil {
		return ErrOnlineSpaceMissingConverter
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if targetVersion <= space.currentVersion {
		return ErrOnlineSpaceInvalidVersion
	}
	if space.phase == OnlineUpgradeRunning || space.phase == OnlineUpgradePaused {
		return ErrOnlineSpaceUpgradePhase
	}
	space.targetVersion = targetVersion
	space.converter = converter
	space.phase = OnlineUpgradeRunning
	space.scanKeys = make([]K, 0, len(space.records))
	space.pendingRecords = 0
	space.converted = 0
	for key, entry := range space.records {
		space.scanKeys = append(space.scanKeys, key)
		if entry.version < targetVersion {
			space.pendingRecords++
		}
	}
	space.scanPosition = 0
	return nil
}

// Set writes a value using the current format, or the target format while an
// upgrade is running or paused.
func (space *OnlineSpaceUpgrade[K, V]) Set(key K, value V) error {
	if space == nil {
		return ErrOnlineSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	version := space.currentVersion
	if space.phase == OnlineUpgradeRunning || space.phase == OnlineUpgradePaused {
		version = space.targetVersion
	}
	if existing, ok := space.records[key]; ok && existing.version < version {
		space.pendingRecords--
	}
	space.records[key] = onlineSpaceUpgradeEntry[V]{version: version, value: value}
	return nil
}

// Get returns a value and lazily converts an old record when an upgrade is
// active. Conversion errors leave the stored value and progress unchanged.
func (space *OnlineSpaceUpgrade[K, V]) Get(key K) (V, bool, error) {
	var zero V
	if space == nil {
		return zero, false, ErrOnlineSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	entry, ok := space.records[key]
	if !ok {
		return zero, false, nil
	}
	if (space.phase == OnlineUpgradeRunning || space.phase == OnlineUpgradePaused) && entry.version < space.targetVersion {
		converted, err := space.converter(entry.value)
		if err != nil {
			return zero, false, err
		}
		entry.value = converted
		entry.version = space.targetVersion
		space.records[key] = entry
		space.pendingRecords--
		space.converted++
	}
	return entry.value, true, nil
}

// Delete removes key and updates pending conversion accounting.
func (space *OnlineSpaceUpgrade[K, V]) Delete(key K) bool {
	if space == nil {
		return false
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	entry, ok := space.records[key]
	if !ok {
		return false
	}
	if (space.phase == OnlineUpgradeRunning || space.phase == OnlineUpgradePaused) && entry.version < space.targetVersion {
		space.pendingRecords--
	}
	delete(space.records, key)
	return true
}

// UpgradeBatch converts at most limit old records and returns converted and
// remaining counts. It stops before mutating a record whose converter errors.
func (space *OnlineSpaceUpgrade[K, V]) UpgradeBatch(limit int) (int, int, error) {
	if space == nil {
		return 0, 0, ErrOnlineSpaceNil
	}
	if limit <= 0 {
		return 0, 0, ErrOnlineSpaceInvalidBatch
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.phase != OnlineUpgradeRunning {
		return 0, int(space.pendingRecords), ErrOnlineSpaceUpgradePhase
	}
	processed := 0
	for space.scanPosition < len(space.scanKeys) && processed < limit {
		key := space.scanKeys[space.scanPosition]
		entry, ok := space.records[key]
		if !ok || entry.version >= space.targetVersion {
			space.scanPosition++
			continue
		}
		converted, err := space.converter(entry.value)
		if err != nil {
			return processed, int(space.pendingRecords), err
		}
		entry.value = converted
		entry.version = space.targetVersion
		space.records[key] = entry
		space.pendingRecords--
		space.converted++
		space.scanPosition++
		processed++
	}
	return processed, int(space.pendingRecords), nil
}

// Pause stops background batches while keeping target-format writes and lazy
// reads available for compatibility.
func (space *OnlineSpaceUpgrade[K, V]) Pause() error {
	if space == nil {
		return ErrOnlineSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.phase != OnlineUpgradeRunning {
		return ErrOnlineSpaceUpgradePhase
	}
	space.phase = OnlineUpgradePaused
	return nil
}

// Resume allows background batches to continue.
func (space *OnlineSpaceUpgrade[K, V]) Resume() error {
	if space == nil {
		return ErrOnlineSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.phase != OnlineUpgradePaused {
		return ErrOnlineSpaceUpgradePhase
	}
	space.phase = OnlineUpgradeRunning
	return nil
}

// Complete atomically cuts over to the target version after all old records
// have been converted or deleted.
func (space *OnlineSpaceUpgrade[K, V]) Complete() error {
	if space == nil {
		return ErrOnlineSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.phase != OnlineUpgradeRunning && space.phase != OnlineUpgradePaused {
		return ErrOnlineSpaceUpgradePhase
	}
	if space.pendingRecords != 0 {
		return ErrOnlineSpaceUpgradePending
	}
	space.currentVersion = space.targetVersion
	space.targetVersion = 0
	space.converter = nil
	space.phase = OnlineUpgradeCompleted
	space.scanKeys = nil
	space.scanPosition = 0
	return nil
}

// Stats returns a point-in-time copy of upgrade and record counts.
func (space *OnlineSpaceUpgrade[K, V]) Stats() OnlineSpaceUpgradeStats {
	if space == nil {
		return OnlineSpaceUpgradeStats{}
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	return OnlineSpaceUpgradeStats{
		CurrentVersion:   space.currentVersion,
		TargetVersion:    space.targetVersion,
		Phase:            space.phase,
		Items:            len(space.records),
		PendingRecords:   space.pendingRecords,
		ConvertedRecords: space.converted,
		ScannedRecords:   space.scanPosition,
		ScanTotal:        len(space.scanKeys),
	}
}

// Len returns the number of stored records.
func (space *OnlineSpaceUpgrade[K, V]) Len() int {
	if space == nil {
		return 0
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	return len(space.records)
}
