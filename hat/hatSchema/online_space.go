package hatSchema

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	ErrOnlineSpaceInvalid                  = errors.New("hatSchema: online space is invalid")
	ErrOnlineSpaceKeyRequired              = errors.New("hatSchema: online space key is required")
	ErrOnlineSpaceUpgradeActive            = errors.New("hatSchema: online space upgrade is already active")
	ErrOnlineSpaceUpgradeConverterRequired = errors.New("hatSchema: online space upgrade converter is required")
	ErrOnlineSpaceUpgradeVersion           = errors.New("hatSchema: online space upgrade version is invalid")
	ErrOnlineSpaceUpgradeIncompatible      = errors.New("hatSchema: online space upgrade is incompatible")
	ErrOnlineSpaceUpgradeState             = errors.New("hatSchema: online space upgrade state is invalid")
	ErrOnlineSpaceUpgradeTargetKeyMismatch = errors.New("hatSchema: online space upgrade changed a row key")
)

// OnlineSpaceKey returns the stable identity of one row. The key must be
// deterministic and non-empty for the lifetime of the space.
type OnlineSpaceKey func(Row) (string, error)

// OnlineSpaceConvertFunc converts a row from the active definition to the
// target definition in place. The row is an owned copy and is consumed by the
// space after the callback returns; implementations must not retain it.
// Implementations should honor ctx so cancellation can stop a slow conversion.
type OnlineSpaceConvertFunc func(ctx context.Context, row Row) error

// OnlineSpaceUpgradePhase describes the lifecycle of one background upgrade.
type OnlineSpaceUpgradePhase uint8

const (
	OnlineSpaceUpgradeRunning OnlineSpaceUpgradePhase = iota + 1
	OnlineSpaceUpgradeComplete
	OnlineSpaceUpgradeFailed
	OnlineSpaceUpgradeCanceled
)

func (phase OnlineSpaceUpgradePhase) String() string {
	switch phase {
	case OnlineSpaceUpgradeRunning:
		return "running"
	case OnlineSpaceUpgradeComplete:
		return "complete"
	case OnlineSpaceUpgradeFailed:
		return "failed"
	case OnlineSpaceUpgradeCanceled:
		return "canceled"
	default:
		return "unknown"
	}
}

// OnlineSpaceUpgradeStatus is a point-in-time copy of upgrade progress.
type OnlineSpaceUpgradeStatus struct {
	CurrentVersion uint64                  `json:"current_version"`
	TargetVersion  uint64                  `json:"target_version"`
	Phase          OnlineSpaceUpgradePhase `json:"phase"`
	TotalRows      int                     `json:"total_rows"`
	ConvertedRows  int                     `json:"converted_rows"`
	Err            error                   `json:"-"`
}

type onlineSpaceRow struct {
	active      Row
	target      Row
	targetReady bool
	revision    uint64
}

// OnlineSpace stores keyed rows under one versioned space definition. It is
// intentionally independent from the existing MaterializedSource so callers
// can opt into online schema conversion without changing existing storage.
type OnlineSpace struct {
	mu         sync.RWMutex
	definition SpaceDefinition
	key        OnlineSpaceKey
	rows       map[string]*onlineSpaceRow
	upgrade    *OnlineSpaceUpgrade
}

// NewOnlineSpace validates a definition and creates an empty keyed space.
func NewOnlineSpace(definition SpaceDefinition, key OnlineSpaceKey) (*OnlineSpace, error) {
	if key == nil {
		return nil, ErrOnlineSpaceKeyRequired
	}
	normalized, err := normalizeOnlineSpaceDefinition(definition)
	if err != nil {
		return nil, err
	}
	return &OnlineSpace{
		definition: normalized,
		key:        key,
		rows:       make(map[string]*onlineSpaceRow),
	}, nil
}

// Definition returns an independent copy of the active definition.
func (space *OnlineSpace) Definition() SpaceDefinition {
	if space == nil {
		return SpaceDefinition{}
	}
	space.mu.RLock()
	definition := cloneSpaceDefinition(space.definition)
	space.mu.RUnlock()
	return definition
}

// Upsert publishes one row under the current definition. During an online
// upgrade, its target representation is prepared before publication so the
// write remains covered by the eventual atomic cutover.
func (space *OnlineSpace) Upsert(row Row) error {
	if space == nil {
		return ErrOnlineSpaceInvalid
	}
	active := cloneRow(row)
	key, err := space.rowKey(active)
	if err != nil {
		return err
	}
	for {
		space.mu.RLock()
		upgrade := space.upgrade
		space.mu.RUnlock()
		if upgrade == nil {
			space.mu.Lock()
			if space.upgrade != nil {
				space.mu.Unlock()
				continue
			}
			record := space.rows[key]
			if record == nil {
				record = &onlineSpaceRow{}
				space.rows[key] = record
			}
			record.active = active
			record.target = nil
			record.targetReady = false
			record.revision++
			space.mu.Unlock()
			return nil
		}

		target := cloneRow(active)
		convertErr := upgrade.convert(upgrade.ctx, target)
		if convertErr != nil {
			return fmt.Errorf("hatSchema: convert row %q: %w", key, convertErr)
		}
		targetKey, keyErr := space.rowKey(target)
		if keyErr != nil {
			return fmt.Errorf("hatSchema: convert row %q: %w", key, keyErr)
		}
		if targetKey != key {
			return fmt.Errorf("%w: %q became %q", ErrOnlineSpaceUpgradeTargetKeyMismatch, key, targetKey)
		}

		space.mu.Lock()
		if space.upgrade != upgrade {
			if space.definition.Version == upgrade.target.Version {
				record := space.rows[key]
				if record == nil {
					record = &onlineSpaceRow{}
					space.rows[key] = record
				}
				record.active = target
				record.target = nil
				record.targetReady = false
				record.revision++
				space.mu.Unlock()
				return nil
			}
			space.mu.Unlock()
			continue
		}
		record := space.rows[key]
		newRow := record == nil
		if record == nil {
			record = &onlineSpaceRow{}
			space.rows[key] = record
		}
		wasReady := record.targetReady
		record.active = active
		record.target = target
		record.targetReady = true
		record.revision++
		space.mu.Unlock()
		upgrade.noteUpsert(newRow, wasReady)
		return nil
	}
}

// Get returns a copy of the active-schema row for key.
func (space *OnlineSpace) Get(key string) (Row, bool) {
	if space == nil || key == "" {
		return nil, false
	}
	space.mu.RLock()
	record, ok := space.rows[key]
	if ok {
		record = &onlineSpaceRow{active: cloneRow(record.active)}
	}
	space.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return record.active, true
}

// Delete removes one row and reports whether it existed.
func (space *OnlineSpace) Delete(key string) bool {
	if space == nil || key == "" {
		return false
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if _, ok := space.rows[key]; !ok {
		return false
	}
	upgrade := space.upgrade
	wasReady := space.rows[key].targetReady
	delete(space.rows, key)
	if upgrade != nil {
		upgrade.totalRows.Add(-1)
		if wasReady {
			upgrade.convertedRows.Add(-1)
		}
	}
	return true
}

// Rows returns active-schema row copies in deterministic key order.
func (space *OnlineSpace) Rows() []Row {
	if space == nil {
		return nil
	}
	space.mu.RLock()
	keys := make([]string, 0, len(space.rows))
	for key := range space.rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]Row, 0, len(keys))
	for _, key := range keys {
		rows = append(rows, cloneRow(space.rows[key].active))
	}
	space.mu.RUnlock()
	return rows
}

// BeginUpgrade starts a background conversion to a compatible higher-version
// definition. Reads continue serving the old rows until every target row is
// ready, then the active definition and all rows switch under one short lock.
func (space *OnlineSpace) BeginUpgrade(ctx context.Context, target SpaceDefinition, convert OnlineSpaceConvertFunc) (*OnlineSpaceUpgrade, error) {
	if space == nil {
		return nil, ErrOnlineSpaceInvalid
	}
	if convert == nil {
		return nil, ErrOnlineSpaceUpgradeConverterRequired
	}
	normalized, err := normalizeOnlineSpaceDefinition(target)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.upgrade != nil {
		return nil, ErrOnlineSpaceUpgradeActive
	}
	if normalized.Name != space.definition.Name || normalized.Version <= space.definition.Version {
		return nil, fmt.Errorf("%w: current=%d target=%d", ErrOnlineSpaceUpgradeVersion, space.definition.Version, normalized.Version)
	}
	previous := Schema{Version: space.definition.Version, Sources: map[string]Source{space.definition.Name: onlineSpaceCompatibilitySource(space.definition.Source)}}
	next := Schema{Version: normalized.Version, Sources: map[string]Source{normalized.Name: onlineSpaceCompatibilitySource(normalized.Source)}}
	report, compatibilityErr := CheckRollingCompatibility(previous, next)
	if compatibilityErr != nil {
		return nil, fmt.Errorf("%w: %v", ErrOnlineSpaceUpgradeIncompatible, compatibilityErr)
	}
	if !report.Compatible {
		return nil, ErrOnlineSpaceUpgradeIncompatible
	}
	upgradeContext, cancel := context.WithCancel(ctx)
	upgrade := &OnlineSpaceUpgrade{
		space:   space,
		target:  normalized,
		convert: convert,
		ctx:     upgradeContext,
		cancel:  cancel,
		done:    make(chan struct{}),
		status: OnlineSpaceUpgradeStatus{
			CurrentVersion: space.definition.Version,
			TargetVersion:  normalized.Version,
			Phase:          OnlineSpaceUpgradeRunning,
			TotalRows:      len(space.rows),
		},
	}
	upgrade.totalRows.Store(int64(len(space.rows)))
	keys := make([]string, 0, len(space.rows))
	for key := range space.rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	space.upgrade = upgrade
	go upgrade.run(keys)
	return upgrade, nil
}

func normalizeOnlineSpaceDefinition(definition SpaceDefinition) (SpaceDefinition, error) {
	if definition.Version == 0 {
		return SpaceDefinition{}, ErrOnlineSpaceUpgradeVersion
	}
	catalog, err := NewSpaceCatalog([]SpaceDefinition{definition})
	if err != nil {
		return SpaceDefinition{}, err
	}
	normalized, ok := catalog.Lookup(definition.Name)
	if !ok {
		return SpaceDefinition{}, ErrOnlineSpaceInvalid
	}
	return normalized, nil
}

func onlineSpaceCompatibilitySource(source Source) Source {
	normalized := cloneSource(source)
	for index := range normalized.Columns {
		normalized.Columns[index].Type = Type(strings.ToUpper(string(normalized.Columns[index].Type)))
	}
	return normalized
}

func (space *OnlineSpace) rowKey(row Row) (string, error) {
	key, err := space.key(row)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(key) == "" {
		return "", ErrOnlineSpaceKeyRequired
	}
	return key, nil
}

// OnlineSpaceUpgrade tracks one asynchronous conversion and is safe for
// concurrent status, cancellation, and wait calls.
type OnlineSpaceUpgrade struct {
	space         *OnlineSpace
	target        SpaceDefinition
	convert       OnlineSpaceConvertFunc
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	once          sync.Once
	mu            sync.RWMutex
	status        OnlineSpaceUpgradeStatus
	totalRows     atomic.Int64
	convertedRows atomic.Int64
}

// Status returns a stable copy of the current progress.
func (upgrade *OnlineSpaceUpgrade) Status() OnlineSpaceUpgradeStatus {
	if upgrade == nil {
		return OnlineSpaceUpgradeStatus{}
	}
	upgrade.mu.RLock()
	status := upgrade.status
	upgrade.mu.RUnlock()
	status.TotalRows = int(upgrade.totalRows.Load())
	status.ConvertedRows = int(upgrade.convertedRows.Load())
	return status
}

// Cancel requests cancellation. A converter that honors its context stops
// promptly; the active definition remains unchanged.
func (upgrade *OnlineSpaceUpgrade) Cancel() {
	if upgrade == nil {
		return
	}
	upgrade.once.Do(upgrade.cancel)
}

// Wait blocks until the upgrade reaches a terminal state or ctx is canceled.
func (upgrade *OnlineSpaceUpgrade) Wait(ctx context.Context) error {
	if upgrade == nil {
		return ErrOnlineSpaceInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-upgrade.done:
		return upgrade.Status().Err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (upgrade *OnlineSpaceUpgrade) run(keys []string) {
	defer close(upgrade.done)
	for _, key := range keys {
		for {
			if err := upgrade.ctx.Err(); err != nil {
				upgrade.abort(err)
				return
			}
			row, revision, ready, ok := upgrade.snapshot(key)
			if !ok || ready {
				break
			}
			converted := row
			err := upgrade.convert(upgrade.ctx, converted)
			if err != nil {
				upgrade.abort(err)
				return
			}
			done, publishErr := upgrade.publish(key, revision, converted)
			if publishErr != nil {
				upgrade.abort(publishErr)
				return
			}
			if done {
				break
			}
		}
	}
	if err := upgrade.ctx.Err(); err != nil {
		upgrade.abort(err)
		return
	}
	if !upgrade.finish() {
		upgrade.abort(ErrOnlineSpaceUpgradeState)
		return
	}
	upgrade.complete()
}

func (upgrade *OnlineSpaceUpgrade) snapshot(key string) (Row, uint64, bool, bool) {
	upgrade.space.mu.RLock()
	defer upgrade.space.mu.RUnlock()
	if upgrade.space.upgrade != upgrade {
		return nil, 0, false, false
	}
	record, ok := upgrade.space.rows[key]
	if !ok {
		return nil, 0, false, false
	}
	if record.targetReady {
		return nil, record.revision, true, true
	}
	return cloneRow(record.active), record.revision, false, true
}

func (upgrade *OnlineSpaceUpgrade) publish(key string, revision uint64, converted Row) (bool, error) {
	targetKey, err := upgrade.space.rowKey(converted)
	if err != nil {
		return false, err
	}
	if targetKey != key {
		return false, fmt.Errorf("%w: %q became %q", ErrOnlineSpaceUpgradeTargetKeyMismatch, key, targetKey)
	}
	upgrade.space.mu.Lock()
	defer upgrade.space.mu.Unlock()
	if upgrade.space.upgrade != upgrade {
		return true, nil
	}
	record, ok := upgrade.space.rows[key]
	if !ok || record.targetReady {
		return true, nil
	}
	if record.revision != revision {
		return false, nil
	}
	record.target = converted
	record.targetReady = true
	upgrade.recordConverted()
	return true, nil
}

func (upgrade *OnlineSpaceUpgrade) finish() bool {
	upgrade.space.mu.Lock()
	defer upgrade.space.mu.Unlock()
	if upgrade.space.upgrade != upgrade {
		return false
	}
	for _, record := range upgrade.space.rows {
		if !record.targetReady {
			return false
		}
	}
	for _, record := range upgrade.space.rows {
		record.active = record.target
		record.target = nil
		record.targetReady = false
		record.revision++
	}
	upgrade.space.definition = cloneSpaceDefinition(upgrade.target)
	upgrade.space.upgrade = nil
	return true
}

func (upgrade *OnlineSpaceUpgrade) abort(err error) {
	if errors.Is(err, context.Canceled) || upgrade.ctx.Err() != nil {
		err = context.Canceled
	}
	upgrade.space.mu.Lock()
	if upgrade.space.upgrade == upgrade {
		for _, record := range upgrade.space.rows {
			record.target = nil
			record.targetReady = false
		}
		upgrade.space.upgrade = nil
	}
	upgrade.space.mu.Unlock()
	upgrade.mu.Lock()
	if errors.Is(err, context.Canceled) {
		upgrade.status.Phase = OnlineSpaceUpgradeCanceled
	} else {
		upgrade.status.Phase = OnlineSpaceUpgradeFailed
	}
	upgrade.status.Err = err
	upgrade.mu.Unlock()
}

func (upgrade *OnlineSpaceUpgrade) complete() {
	upgrade.convertedRows.Store(upgrade.totalRows.Load())
	upgrade.mu.Lock()
	upgrade.status.Phase = OnlineSpaceUpgradeComplete
	upgrade.status.ConvertedRows = upgrade.status.TotalRows
	upgrade.status.Err = nil
	upgrade.mu.Unlock()
}

func (upgrade *OnlineSpaceUpgrade) noteUpsert(newRow, wasReady bool) {
	if newRow {
		upgrade.totalRows.Add(1)
	}
	if !wasReady {
		upgrade.convertedRows.Add(1)
	}
}

func (upgrade *OnlineSpaceUpgrade) recordConverted() {
	upgrade.convertedRows.Add(1)
}
