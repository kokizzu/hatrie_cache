package hatSchema

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

const (
	DefaultOnlineSpaceUpgradeBatch   = 256
	MaxOnlineSpaceUpgradeBatch       = 4096
	MaxOnlineSpaceUpgradeSpaceBytes  = 256
	MaxOnlineSpaceUpgradeKeyBytes    = 1 << 20
	MaxOnlineSpaceUpgradeCursorBytes = 4096
)

var (
	ErrOnlineSpaceUpgradeInvalid  = errors.New("invalid online space upgrade")
	ErrOnlineSpaceUpgradePhase    = errors.New("invalid online space upgrade phase")
	ErrOnlineSpaceUpgradeProgress = errors.New("online space upgrade made no progress")
	ErrOnlineSpaceUpgradeContext  = errors.New("invalid online space upgrade context")
	ErrOnlineSpaceUpgradeBackend  = errors.New("online space upgrade backend error")
)

// OnlineSpaceUpgradePhase describes the durable lifecycle of an online upgrade.
type OnlineSpaceUpgradePhase uint8

const (
	OnlineSpaceUpgradePending OnlineSpaceUpgradePhase = iota + 1
	OnlineSpaceUpgradeRunning
	OnlineSpaceUpgradeReady
	OnlineSpaceUpgradeCutover
	OnlineSpaceUpgradeRolledBack
)

func (phase OnlineSpaceUpgradePhase) String() string {
	switch phase {
	case OnlineSpaceUpgradePending:
		return "pending"
	case OnlineSpaceUpgradeRunning:
		return "running"
	case OnlineSpaceUpgradeReady:
		return "ready"
	case OnlineSpaceUpgradeCutover:
		return "cutover"
	case OnlineSpaceUpgradeRolledBack:
		return "rolled_back"
	default:
		return "unknown"
	}
}

// OnlineSpaceUpgradeRecord is one source-space value returned by a scan.
type OnlineSpaceUpgradeRecord[T any] struct {
	Key   string
	Value T
}

// OnlineSpaceUpgradeBackend contains the storage-specific operations required
// by OnlineSpaceUpgrade. Implementations should make WriteDual idempotent.
type OnlineSpaceUpgradeBackend[Old, New any] interface {
	ReadOld(context.Context, string) (Old, bool, error)
	ReadNew(context.Context, string) (New, bool, error)
	ScanOld(context.Context, string, int) ([]OnlineSpaceUpgradeRecord[Old], string, bool, error)
	WriteDual(context.Context, string, Old, New) error
	WriteNew(context.Context, string, New) error
	Cutover(context.Context, string, uint64) error
	Rollback(context.Context, string, uint64) error
}

// OnlineSpaceUpgradeOptions configures an online conversion from Old to New.
type OnlineSpaceUpgradeOptions[Old, New any] struct {
	Space           string
	PreviousVersion uint64
	NextVersion     uint64
	BatchSize       int
	Convert         func(Old) (New, error)
	Backend         OnlineSpaceUpgradeBackend[Old, New]
}

// OnlineSpaceUpgradeProgress is a point-in-time view of an upgrade.
type OnlineSpaceUpgradeProgress struct {
	Space           string
	PreviousVersion uint64
	NextVersion     uint64
	Phase           OnlineSpaceUpgradePhase
	Cursor          string
	Migrated        uint64
	Ready           bool
}

// OnlineSpaceUpgradeCheckpoint is safe to persist and later pass to Restore.
type OnlineSpaceUpgradeCheckpoint struct {
	Space           string
	PreviousVersion uint64
	NextVersion     uint64
	Phase           OnlineSpaceUpgradePhase
	Cursor          string
	Migrated        uint64
}

// OnlineSpaceUpgrade coordinates dual-read, dual-write conversion and a
// single explicit cutover. It is opt-in and does not alter existing storage.
type OnlineSpaceUpgrade[Old, New any] struct {
	mu    sync.RWMutex
	runMu sync.Mutex
	gate  sync.RWMutex

	space           string
	previousVersion uint64
	nextVersion     uint64
	batchSize       int
	convert         func(Old) (New, error)
	backend         OnlineSpaceUpgradeBackend[Old, New]

	phase    OnlineSpaceUpgradePhase
	cursor   string
	migrated uint64
}

func NewOnlineSpaceUpgrade[Old, New any](options OnlineSpaceUpgradeOptions[Old, New]) (*OnlineSpaceUpgrade[Old, New], error) {
	if len(options.Space) == 0 || len(options.Space) > MaxOnlineSpaceUpgradeSpaceBytes {
		return nil, fmt.Errorf("%w: space must be between 1 and %d bytes", ErrOnlineSpaceUpgradeInvalid, MaxOnlineSpaceUpgradeSpaceBytes)
	}
	if options.PreviousVersion == 0 || options.NextVersion <= options.PreviousVersion {
		return nil, fmt.Errorf("%w: versions must be positive and increasing", ErrOnlineSpaceUpgradeInvalid)
	}
	if options.Convert == nil {
		return nil, fmt.Errorf("%w: converter is required", ErrOnlineSpaceUpgradeInvalid)
	}
	if options.Backend == nil {
		return nil, fmt.Errorf("%w: backend is required", ErrOnlineSpaceUpgradeInvalid)
	}
	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = DefaultOnlineSpaceUpgradeBatch
	}
	if batchSize < 0 || batchSize > MaxOnlineSpaceUpgradeBatch {
		return nil, fmt.Errorf("%w: batch size must be between 1 and %d", ErrOnlineSpaceUpgradeInvalid, MaxOnlineSpaceUpgradeBatch)
	}
	return &OnlineSpaceUpgrade[Old, New]{
		space:           options.Space,
		previousVersion: options.PreviousVersion,
		nextVersion:     options.NextVersion,
		batchSize:       batchSize,
		convert:         options.Convert,
		backend:         options.Backend,
		phase:           OnlineSpaceUpgradePending,
	}, nil
}

// Read prefers the new space and falls back to converting the old value until
// cutover. After cutover only the new space is consulted.
func (upgrade *OnlineSpaceUpgrade[Old, New]) Read(ctx context.Context, key string) (New, bool, error) {
	var zero New
	if err := upgrade.validateKey(key); err != nil {
		return zero, false, err
	}
	ctx = onlineSpaceUpgradeContext(ctx)
	if err := ctx.Err(); err != nil {
		return zero, false, err
	}
	upgrade.gate.RLock()
	defer upgrade.gate.RUnlock()
	phase, err := upgrade.currentPhase()
	if err != nil {
		return zero, false, err
	}
	if phase != OnlineSpaceUpgradeRolledBack {
		value, found, err := upgrade.backend.ReadNew(ctx, key)
		if err != nil {
			return zero, false, onlineSpaceUpgradeBackendError("read new", err)
		}
		if found || phase == OnlineSpaceUpgradeCutover {
			return value, found, nil
		}
	}
	oldValue, found, err := upgrade.backend.ReadOld(ctx, key)
	if err != nil {
		return zero, false, onlineSpaceUpgradeBackendError("read old", err)
	}
	if !found {
		return zero, false, nil
	}
	converted, err := upgrade.convert(oldValue)
	if err != nil {
		return zero, false, onlineSpaceUpgradeBackendError("convert read", err)
	}
	return converted, true, nil
}

// Write converts one old value and routes it through the current migration
// phase. Before cutover the backend receives a dual write; afterward it only
// receives the new value.
func (upgrade *OnlineSpaceUpgrade[Old, New]) Write(ctx context.Context, key string, oldValue Old) error {
	if err := upgrade.validateKey(key); err != nil {
		return err
	}
	ctx = onlineSpaceUpgradeContext(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}
	converted, err := upgrade.convert(oldValue)
	if err != nil {
		return onlineSpaceUpgradeBackendError("convert write", err)
	}
	upgrade.gate.RLock()
	defer upgrade.gate.RUnlock()
	phase, err := upgrade.currentPhase()
	if err != nil {
		return err
	}
	switch phase {
	case OnlineSpaceUpgradePending, OnlineSpaceUpgradeRunning, OnlineSpaceUpgradeReady:
		if err := upgrade.backend.WriteDual(ctx, key, oldValue, converted); err != nil {
			return onlineSpaceUpgradeBackendError("write dual", err)
		}
		return nil
	case OnlineSpaceUpgradeCutover:
		if err := upgrade.backend.WriteNew(ctx, key, converted); err != nil {
			return onlineSpaceUpgradeBackendError("write new", err)
		}
		return nil
	case OnlineSpaceUpgradeRolledBack:
		return fmt.Errorf("%w: writes are disabled after rollback", ErrOnlineSpaceUpgradePhase)
	default:
		return fmt.Errorf("%w: unknown phase %d", ErrOnlineSpaceUpgradePhase, phase)
	}
}

// RunBatch converts and dual-writes at most the configured number of old
// records. The cursor and migrated count advance only after the whole batch
// succeeds, making checkpoints safe to retry after a partial backend failure.
func (upgrade *OnlineSpaceUpgrade[Old, New]) RunBatch(ctx context.Context) (OnlineSpaceUpgradeProgress, error) {
	ctx = onlineSpaceUpgradeContext(ctx)
	upgrade.runMu.Lock()
	defer upgrade.runMu.Unlock()
	if err := ctx.Err(); err != nil {
		return upgrade.Progress(), err
	}
	phase, cursor, err := upgrade.snapshotRunState()
	if err != nil {
		return upgrade.Progress(), err
	}
	if phase == OnlineSpaceUpgradeReady {
		return upgrade.Progress(), nil
	}
	if phase == OnlineSpaceUpgradePending {
		upgrade.setPhase(OnlineSpaceUpgradeRunning)
		phase = OnlineSpaceUpgradeRunning
	}
	if phase != OnlineSpaceUpgradeRunning {
		return upgrade.Progress(), fmt.Errorf("%w: run batch requires running phase, got %s", ErrOnlineSpaceUpgradePhase, phase)
	}
	records, nextCursor, done, err := upgrade.backend.ScanOld(ctx, cursor, upgrade.batchSize)
	if err != nil {
		return upgrade.Progress(), onlineSpaceUpgradeBackendError("scan old", err)
	}
	if err := ctx.Err(); err != nil {
		return upgrade.Progress(), err
	}
	if len(records) == 0 {
		if !done {
			return upgrade.Progress(), fmt.Errorf("%w: empty batch at cursor %q", ErrOnlineSpaceUpgradeProgress, cursor)
		}
		if nextCursor != "" {
			if err := upgrade.validateCursor(nextCursor); err != nil {
				return upgrade.Progress(), err
			}
		}
		upgrade.mu.Lock()
		if nextCursor != "" {
			upgrade.cursor = nextCursor
		}
		upgrade.phase = OnlineSpaceUpgradeReady
		upgrade.mu.Unlock()
		return upgrade.Progress(), nil
	}

	lastKey := cursor
	for _, record := range records {
		if err := ctx.Err(); err != nil {
			return upgrade.Progress(), err
		}
		if err := upgrade.validateKey(record.Key); err != nil {
			return upgrade.Progress(), err
		}
		if lastKey != "" && record.Key <= lastKey {
			return upgrade.Progress(), fmt.Errorf("%w: record key %q does not advance cursor %q", ErrOnlineSpaceUpgradeProgress, record.Key, lastKey)
		}
		converted, err := upgrade.convert(record.Value)
		if err != nil {
			return upgrade.Progress(), onlineSpaceUpgradeBackendError("convert batch", err)
		}
		upgrade.gate.RLock()
		writeErr := upgrade.backend.WriteDual(ctx, record.Key, record.Value, converted)
		upgrade.gate.RUnlock()
		if writeErr != nil {
			return upgrade.Progress(), onlineSpaceUpgradeBackendError("write batch", writeErr)
		}
		lastKey = record.Key
	}
	if nextCursor == "" {
		nextCursor = lastKey
	}
	if err := upgrade.validateCursor(nextCursor); err != nil {
		return upgrade.Progress(), err
	}
	if !done && nextCursor <= cursor {
		return upgrade.Progress(), fmt.Errorf("%w: next cursor %q does not advance %q", ErrOnlineSpaceUpgradeProgress, nextCursor, cursor)
	}

	upgrade.mu.Lock()
	upgrade.cursor = nextCursor
	upgrade.migrated += uint64(len(records))
	if done {
		upgrade.phase = OnlineSpaceUpgradeReady
	} else {
		upgrade.phase = OnlineSpaceUpgradeRunning
	}
	upgrade.mu.Unlock()
	return upgrade.Progress(), nil
}

// Progress returns the current in-memory migration state.
func (upgrade *OnlineSpaceUpgrade[Old, New]) Progress() OnlineSpaceUpgradeProgress {
	if upgrade == nil {
		return OnlineSpaceUpgradeProgress{}
	}
	upgrade.mu.RLock()
	defer upgrade.mu.RUnlock()
	return OnlineSpaceUpgradeProgress{
		Space:           upgrade.space,
		PreviousVersion: upgrade.previousVersion,
		NextVersion:     upgrade.nextVersion,
		Phase:           upgrade.phase,
		Cursor:          upgrade.cursor,
		Migrated:        upgrade.migrated,
		Ready:           upgrade.phase == OnlineSpaceUpgradeReady || upgrade.phase == OnlineSpaceUpgradeCutover,
	}
}

// Checkpoint returns the state needed to resume an upgrade after a restart.
func (upgrade *OnlineSpaceUpgrade[Old, New]) Checkpoint() OnlineSpaceUpgradeCheckpoint {
	progress := upgrade.Progress()
	return OnlineSpaceUpgradeCheckpoint{
		Space:           progress.Space,
		PreviousVersion: progress.PreviousVersion,
		NextVersion:     progress.NextVersion,
		Phase:           progress.Phase,
		Cursor:          progress.Cursor,
		Migrated:        progress.Migrated,
	}
}

// Restore replaces the in-memory progress with a validated checkpoint.
func (upgrade *OnlineSpaceUpgrade[Old, New]) Restore(checkpoint OnlineSpaceUpgradeCheckpoint) error {
	if upgrade == nil {
		return fmt.Errorf("%w: nil coordinator", ErrOnlineSpaceUpgradeInvalid)
	}
	upgrade.runMu.Lock()
	defer upgrade.runMu.Unlock()
	upgrade.gate.Lock()
	defer upgrade.gate.Unlock()
	if checkpoint.Space != upgrade.space || checkpoint.PreviousVersion != upgrade.previousVersion || checkpoint.NextVersion != upgrade.nextVersion {
		return fmt.Errorf("%w: checkpoint identity does not match coordinator", ErrOnlineSpaceUpgradeInvalid)
	}
	if checkpoint.Phase < OnlineSpaceUpgradePending || checkpoint.Phase > OnlineSpaceUpgradeRolledBack {
		return fmt.Errorf("%w: checkpoint phase %d", ErrOnlineSpaceUpgradeInvalid, checkpoint.Phase)
	}
	if err := upgrade.validateCursor(checkpoint.Cursor); err != nil {
		return err
	}
	upgrade.mu.Lock()
	upgrade.phase = checkpoint.Phase
	upgrade.cursor = checkpoint.Cursor
	upgrade.migrated = checkpoint.Migrated
	upgrade.mu.Unlock()
	return nil
}

// Cutover makes the new space authoritative after the scanner reports ready.
func (upgrade *OnlineSpaceUpgrade[Old, New]) Cutover(ctx context.Context) error {
	ctx = onlineSpaceUpgradeContext(ctx)
	upgrade.runMu.Lock()
	defer upgrade.runMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	phase, _, err := upgrade.snapshotRunState()
	if err != nil {
		return err
	}
	if phase == OnlineSpaceUpgradeCutover {
		return nil
	}
	if phase != OnlineSpaceUpgradeReady {
		return fmt.Errorf("%w: cutover requires ready phase, got %s", ErrOnlineSpaceUpgradePhase, phase)
	}
	upgrade.gate.Lock()
	defer upgrade.gate.Unlock()
	if err := upgrade.backend.Cutover(ctx, upgrade.space, upgrade.nextVersion); err != nil {
		return onlineSpaceUpgradeBackendError("cutover", err)
	}
	upgrade.setPhase(OnlineSpaceUpgradeCutover)
	return nil
}

// Rollback asks the backend to discard the new version and disables further
// writes through the coordinator. It is idempotent before cutover.
func (upgrade *OnlineSpaceUpgrade[Old, New]) Rollback(ctx context.Context) error {
	ctx = onlineSpaceUpgradeContext(ctx)
	upgrade.runMu.Lock()
	defer upgrade.runMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	phase, _, err := upgrade.snapshotRunState()
	if err != nil {
		return err
	}
	if phase == OnlineSpaceUpgradeRolledBack {
		return nil
	}
	if phase == OnlineSpaceUpgradeCutover {
		return fmt.Errorf("%w: cannot roll back after cutover", ErrOnlineSpaceUpgradePhase)
	}
	upgrade.gate.Lock()
	defer upgrade.gate.Unlock()
	if err := upgrade.backend.Rollback(ctx, upgrade.space, upgrade.nextVersion); err != nil {
		return onlineSpaceUpgradeBackendError("rollback", err)
	}
	upgrade.setPhase(OnlineSpaceUpgradeRolledBack)
	return nil
}

func (upgrade *OnlineSpaceUpgrade[Old, New]) currentPhase() (OnlineSpaceUpgradePhase, error) {
	if upgrade == nil {
		return 0, fmt.Errorf("%w: nil coordinator", ErrOnlineSpaceUpgradeInvalid)
	}
	upgrade.mu.RLock()
	phase := upgrade.phase
	upgrade.mu.RUnlock()
	if phase == 0 {
		return 0, fmt.Errorf("%w: uninitialized coordinator", ErrOnlineSpaceUpgradeInvalid)
	}
	return phase, nil
}

func (upgrade *OnlineSpaceUpgrade[Old, New]) snapshotRunState() (OnlineSpaceUpgradePhase, string, error) {
	if upgrade == nil {
		return 0, "", fmt.Errorf("%w: nil coordinator", ErrOnlineSpaceUpgradeInvalid)
	}
	upgrade.mu.RLock()
	phase, cursor := upgrade.phase, upgrade.cursor
	upgrade.mu.RUnlock()
	if phase == 0 {
		return 0, "", fmt.Errorf("%w: uninitialized coordinator", ErrOnlineSpaceUpgradeInvalid)
	}
	return phase, cursor, nil
}

func (upgrade *OnlineSpaceUpgrade[Old, New]) setPhase(phase OnlineSpaceUpgradePhase) {
	upgrade.mu.Lock()
	upgrade.phase = phase
	upgrade.mu.Unlock()
}

func (upgrade *OnlineSpaceUpgrade[Old, New]) validateKey(key string) error {
	if upgrade == nil {
		return fmt.Errorf("%w: nil coordinator", ErrOnlineSpaceUpgradeInvalid)
	}
	if len(key) == 0 || len(key) > MaxOnlineSpaceUpgradeKeyBytes {
		return fmt.Errorf("%w: key must be between 1 and %d bytes", ErrOnlineSpaceUpgradeInvalid, MaxOnlineSpaceUpgradeKeyBytes)
	}
	return nil
}

func (upgrade *OnlineSpaceUpgrade[Old, New]) validateCursor(cursor string) error {
	if len(cursor) > MaxOnlineSpaceUpgradeCursorBytes {
		return fmt.Errorf("%w: cursor exceeds %d bytes", ErrOnlineSpaceUpgradeInvalid, MaxOnlineSpaceUpgradeCursorBytes)
	}
	return nil
}

func onlineSpaceUpgradeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func onlineSpaceUpgradeBackendError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %v", ErrOnlineSpaceUpgradeBackend, operation, err)
}
