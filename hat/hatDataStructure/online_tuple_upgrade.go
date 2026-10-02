package hatDataStructure

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	// ErrOnlineTupleUpgradeInvalid indicates invalid upgrade configuration or
	// an invalid batch limit.
	ErrOnlineTupleUpgradeInvalid = errors.New("hatDataStructure: online tuple upgrade is invalid")
	// ErrOnlineTupleUpgradeState indicates that an operation is not valid in
	// the coordinator's current state.
	ErrOnlineTupleUpgradeState = errors.New("hatDataStructure: online tuple upgrade state is invalid")
	// ErrOnlineTupleUpgradeStore indicates a missing or failed store operation.
	ErrOnlineTupleUpgradeStore = errors.New("hatDataStructure: online tuple upgrade store operation failed")
	// ErrOnlineTupleUpgradeVersion indicates a row is not one of the supported
	// source or target versions.
	ErrOnlineTupleUpgradeVersion = errors.New("hatDataStructure: online tuple upgrade row version is unsupported")
	// ErrOnlineTupleUpgradeBatchComplete is returned by a store when the
	// coordinator intentionally stops a bounded scan after its batch limit.
	ErrOnlineTupleUpgradeBatchComplete = errors.New("hatDataStructure: online tuple upgrade batch limit reached")
)

// OnlineTupleUpgradeState describes the lifecycle of an online upgrade.
type OnlineTupleUpgradeState uint8

const (
	OnlineTupleUpgradePlanned OnlineTupleUpgradeState = iota + 1
	OnlineTupleUpgradeRunning
	OnlineTupleUpgradeReady
	OnlineTupleUpgradeCutover
	OnlineTupleUpgradeFailed
)

// String returns a stable human-readable state name.
func (state OnlineTupleUpgradeState) String() string {
	switch state {
	case OnlineTupleUpgradePlanned:
		return "planned"
	case OnlineTupleUpgradeRunning:
		return "running"
	case OnlineTupleUpgradeReady:
		return "ready"
	case OnlineTupleUpgradeCutover:
		return "cutover"
	case OnlineTupleUpgradeFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// OnlineTupleUpgradeStore is the caller-owned storage boundary. Scan must
// stop and return the callback error; Put should atomically replace one key as
// required by the storage engine. The coordinator never opens files or owns a
// transaction.
type OnlineTupleUpgradeStore interface {
	Get(context.Context, []byte) (VersionedTuple, bool, error)
	Put(context.Context, []byte, VersionedTuple) error
	Scan(context.Context, func([]byte, VersionedTuple) error) error
}

// OnlineTupleUpgradeBatchResult reports one bounded background scan.
type OnlineTupleUpgradeBatchResult struct {
	Scanned  uint64
	Migrated uint64
	Current  uint64
	Complete bool
	State    OnlineTupleUpgradeState
}

// OnlineTupleUpgradeStats is a copy-safe cumulative progress snapshot.
type OnlineTupleUpgradeStats struct {
	Name          string
	SourceVersion uint64
	TargetVersion uint64
	State         OnlineTupleUpgradeState
	Scanned       uint64
	Migrated      uint64
	Current       uint64
	ReadRepairs   uint64
	LastError     string
}

// OnlineTupleUpgrade coordinates dual-format reads and writes while a
// TupleMigrationPlan converts existing records in bounded batches. It is
// storage-agnostic and opt-in; callers must route concurrent writes through
// Read/Write or provide an equivalent storage fence before Cutover.
type OnlineTupleUpgrade struct {
	mu            sync.RWMutex
	batchMu       sync.Mutex
	plan          *TupleMigrationPlan
	name          string
	sourceVersion uint64
	targetVersion uint64
	state         OnlineTupleUpgradeState
	scanComplete  bool
	stats         OnlineTupleUpgradeStats
}

// NewOnlineTupleUpgrade constructs a forward online upgrade coordinator.
func NewOnlineTupleUpgrade(plan *TupleMigrationPlan, sourceVersion, targetVersion uint64) (*OnlineTupleUpgrade, error) {
	if plan == nil || plan.Name() == "" {
		return nil, fmt.Errorf("%w: migration plan is required", ErrOnlineTupleUpgradeInvalid)
	}
	if sourceVersion == 0 || targetVersion == 0 || targetVersion <= sourceVersion {
		return nil, fmt.Errorf("%w: target version must be greater than source version", ErrOnlineTupleUpgradeInvalid)
	}
	return &OnlineTupleUpgrade{
		plan:          plan,
		name:          plan.Name(),
		sourceVersion: sourceVersion,
		targetVersion: targetVersion,
		state:         OnlineTupleUpgradePlanned,
		stats: OnlineTupleUpgradeStats{
			Name:          plan.Name(),
			SourceVersion: sourceVersion,
			TargetVersion: targetVersion,
			State:         OnlineTupleUpgradePlanned,
		},
	}, nil
}

// Begin moves a planned upgrade to its resumable running state. Repeated Begin
// calls while running are harmless.
func (upgrade *OnlineTupleUpgrade) Begin() error {
	if upgrade == nil {
		return ErrOnlineTupleUpgradeInvalid
	}
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	switch upgrade.state {
	case OnlineTupleUpgradePlanned, OnlineTupleUpgradeRunning:
		upgrade.state = OnlineTupleUpgradeRunning
		upgrade.stats.State = upgrade.state
		return nil
	default:
		return fmt.Errorf("%w: cannot begin from %s", ErrOnlineTupleUpgradeState, upgrade.state)
	}
}

// Resume clears a failed state after the caller repairs the storage error.
// Previously converted rows remain safe because batches accept target-version
// rows as already current.
func (upgrade *OnlineTupleUpgrade) Resume() error {
	if upgrade == nil {
		return ErrOnlineTupleUpgradeInvalid
	}
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	if upgrade.state != OnlineTupleUpgradeFailed {
		return fmt.Errorf("%w: cannot resume from %s", ErrOnlineTupleUpgradeState, upgrade.state)
	}
	upgrade.state = OnlineTupleUpgradeRunning
	upgrade.scanComplete = false
	upgrade.stats.State = upgrade.state
	upgrade.stats.LastError = ""
	return nil
}

// State returns the current lifecycle state.
func (upgrade *OnlineTupleUpgrade) State() OnlineTupleUpgradeState {
	if upgrade == nil {
		return 0
	}
	upgrade.mu.RLock()
	defer upgrade.mu.RUnlock()
	return upgrade.state
}

// Stats returns an independent cumulative progress snapshot.
func (upgrade *OnlineTupleUpgrade) Stats() OnlineTupleUpgradeStats {
	if upgrade == nil {
		return OnlineTupleUpgradeStats{}
	}
	upgrade.mu.RLock()
	defer upgrade.mu.RUnlock()
	return upgrade.stats
}

// Read returns a row and repairs a source-version row to the target format
// while the upgrade is running or ready. After cutover, source rows are
// rejected rather than silently reintroduced.
func (upgrade *OnlineTupleUpgrade) Read(ctx context.Context, store OnlineTupleUpgradeStore, key []byte) (VersionedTuple, bool, error) {
	if err := upgrade.validateOperation(ctx, store); err != nil {
		return VersionedTuple{}, false, err
	}
	state := upgrade.State()
	if state == OnlineTupleUpgradeFailed || state == OnlineTupleUpgradePlanned {
		return VersionedTuple{}, false, fmt.Errorf("%w: read is unavailable in %s", ErrOnlineTupleUpgradeState, state)
	}
	tuple, found, err := store.Get(ctx, key)
	if err != nil {
		return VersionedTuple{}, false, fmt.Errorf("%w: get: %v", ErrOnlineTupleUpgradeStore, err)
	}
	if !found {
		return VersionedTuple{}, false, nil
	}
	if tuple.Version() == upgrade.targetVersion {
		if _, err := upgrade.plan.Migrate(tuple, upgrade.targetVersion); err != nil {
			return VersionedTuple{}, false, err
		}
		return tuple, true, nil
	}
	if tuple.Version() != upgrade.sourceVersion || state == OnlineTupleUpgradeCutover {
		return VersionedTuple{}, false, fmt.Errorf("%w: got version %d in state %s", ErrOnlineTupleUpgradeVersion, tuple.Version(), state)
	}
	migrated, err := upgrade.plan.Migrate(tuple, upgrade.targetVersion)
	if err != nil {
		return VersionedTuple{}, false, err
	}
	if err := store.Put(ctx, key, migrated); err != nil {
		return VersionedTuple{}, false, fmt.Errorf("%w: read repair: %v", ErrOnlineTupleUpgradeStore, err)
	}
	upgrade.mu.Lock()
	upgrade.stats.ReadRepairs++
	upgrade.mu.Unlock()
	return migrated, true, nil
}

// Write normalizes a source-version tuple to the target format while dual
// reads/writes are active. After Cutover only target-version tuples are
// accepted.
func (upgrade *OnlineTupleUpgrade) Write(ctx context.Context, store OnlineTupleUpgradeStore, key []byte, tuple VersionedTuple) error {
	if err := upgrade.validateOperation(ctx, store); err != nil {
		return err
	}
	state := upgrade.State()
	if state == OnlineTupleUpgradeFailed || state == OnlineTupleUpgradePlanned {
		return fmt.Errorf("%w: write is unavailable in %s", ErrOnlineTupleUpgradeState, state)
	}
	if tuple.Version() == upgrade.sourceVersion {
		if state == OnlineTupleUpgradeCutover {
			return fmt.Errorf("%w: source version %d after cutover", ErrOnlineTupleUpgradeVersion, tuple.Version())
		}
		var err error
		tuple, err = upgrade.plan.Migrate(tuple, upgrade.targetVersion)
		if err != nil {
			return err
		}
	} else if tuple.Version() != upgrade.targetVersion {
		return fmt.Errorf("%w: got version %d", ErrOnlineTupleUpgradeVersion, tuple.Version())
	} else if _, err := upgrade.plan.Migrate(tuple, upgrade.targetVersion); err != nil {
		return err
	}
	if err := store.Put(ctx, key, tuple); err != nil {
		return fmt.Errorf("%w: put: %v", ErrOnlineTupleUpgradeStore, err)
	}
	return nil
}

// MigrateBatch converts at most limit source-version rows. Target-version
// rows are skipped and do not consume the migration limit. A store must return
// ErrOnlineTupleUpgradeBatchComplete when the callback asks it to stop early.
func (upgrade *OnlineTupleUpgrade) MigrateBatch(ctx context.Context, store OnlineTupleUpgradeStore, limit int) (OnlineTupleUpgradeBatchResult, error) {
	result := OnlineTupleUpgradeBatchResult{}
	if err := upgrade.validateOperation(ctx, store); err != nil {
		return result, err
	}
	if limit <= 0 {
		return result, fmt.Errorf("%w: batch limit must be positive", ErrOnlineTupleUpgradeInvalid)
	}
	upgrade.batchMu.Lock()
	defer upgrade.batchMu.Unlock()
	if state := upgrade.State(); state != OnlineTupleUpgradeRunning {
		return result, fmt.Errorf("%w: batch requires running state, got %s", ErrOnlineTupleUpgradeState, state)
	}
	scanErr := store.Scan(ctx, func(key []byte, tuple VersionedTuple) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		result.Scanned++
		upgrade.addScanned()
		switch tuple.Version() {
		case upgrade.targetVersion:
			result.Current++
			upgrade.addCurrent()
			return nil
		case upgrade.sourceVersion:
			if result.Migrated >= uint64(limit) {
				return ErrOnlineTupleUpgradeBatchComplete
			}
			migrated, err := upgrade.plan.Migrate(tuple, upgrade.targetVersion)
			if err != nil {
				return err
			}
			if err := store.Put(ctx, key, migrated); err != nil {
				return fmt.Errorf("%w: migrate put: %v", ErrOnlineTupleUpgradeStore, err)
			}
			result.Migrated++
			upgrade.addMigrated()
			return nil
		default:
			return fmt.Errorf("%w: got version %d", ErrOnlineTupleUpgradeVersion, tuple.Version())
		}
	})
	if errors.Is(scanErr, ErrOnlineTupleUpgradeBatchComplete) {
		result.State = upgrade.State()
		return result, nil
	}
	if scanErr != nil {
		if !errors.Is(scanErr, context.Canceled) && !errors.Is(scanErr, context.DeadlineExceeded) {
			upgrade.fail(scanErr)
		}
		result.State = upgrade.State()
		return result, scanErr
	}
	upgrade.mu.Lock()
	upgrade.scanComplete = true
	upgrade.state = OnlineTupleUpgradeReady
	upgrade.stats.State = upgrade.state
	upgrade.mu.Unlock()
	result.Complete = true
	result.State = OnlineTupleUpgradeReady
	return result, nil
}

// Cutover fences old-format reads and writes after a complete scan. All
// concurrent writers must use Write or an equivalent storage-level fence.
func (upgrade *OnlineTupleUpgrade) Cutover() error {
	if upgrade == nil {
		return ErrOnlineTupleUpgradeInvalid
	}
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	if upgrade.state != OnlineTupleUpgradeReady || !upgrade.scanComplete {
		return fmt.Errorf("%w: cutover requires a completed scan, got %s", ErrOnlineTupleUpgradeState, upgrade.state)
	}
	upgrade.state = OnlineTupleUpgradeCutover
	upgrade.stats.State = upgrade.state
	return nil
}

func (upgrade *OnlineTupleUpgrade) validateOperation(ctx context.Context, store OnlineTupleUpgradeStore) error {
	if upgrade == nil {
		return ErrOnlineTupleUpgradeInvalid
	}
	if ctx == nil {
		return fmt.Errorf("%w: context is required", ErrOnlineTupleUpgradeInvalid)
	}
	if store == nil {
		return fmt.Errorf("%w: store is required", ErrOnlineTupleUpgradeStore)
	}
	return nil
}

func (upgrade *OnlineTupleUpgrade) addScanned() {
	upgrade.mu.Lock()
	upgrade.stats.Scanned++
	upgrade.mu.Unlock()
}

func (upgrade *OnlineTupleUpgrade) addCurrent() {
	upgrade.mu.Lock()
	upgrade.stats.Current++
	upgrade.mu.Unlock()
}

func (upgrade *OnlineTupleUpgrade) addMigrated() {
	upgrade.mu.Lock()
	upgrade.stats.Migrated++
	upgrade.mu.Unlock()
}

func (upgrade *OnlineTupleUpgrade) fail(err error) {
	upgrade.mu.Lock()
	defer upgrade.mu.Unlock()
	upgrade.state = OnlineTupleUpgradeFailed
	upgrade.stats.State = upgrade.state
	upgrade.stats.LastError = err.Error()
}
