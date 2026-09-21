package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrTypedTableSQLSnapshotRegistryRequired reports a nil registry receiver.
	ErrTypedTableSQLSnapshotRegistryRequired = errors.New("typed table SQL snapshot registry is required")
	// ErrTypedTableSQLSnapshotTableRequired reports an attempt to register nil.
	ErrTypedTableSQLSnapshotTableRequired = errors.New("typed table SQL snapshot table is required")
	// ErrTypedTableSQLSnapshotMVCCRequired reports a table without retained history.
	ErrTypedTableSQLSnapshotMVCCRequired = errors.New("typed table SQL snapshot requires MVCC")
	// ErrTypedTableSQLSnapshotDuplicate reports a duplicate SQL source identity.
	ErrTypedTableSQLSnapshotDuplicate = errors.New("typed table SQL snapshot source is already registered")
)

type typedTableSQLSourceIdentity struct {
	source string
	key    string
}

type typedTableSQLSnapshotRegistration struct {
	identity typedTableSQLSourceIdentity
	table    *TypedTable
}

// BeginSQLSnapshotAt opens one immutable retained-state view directly from a
// typed table. This is the zero-wrapper path for single-source AS OF queries.
func (table *TypedTable) BeginSQLSnapshotAt(ctx context.Context, frontier uint64) (SQLSourceResolver, func(), error) {
	if ctx != nil && ctx != context.Background() {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
	}
	snapshot, err := table.SnapshotAt(frontier)
	if err != nil {
		return nil, nil, err
	}
	return snapshot, releaseTypedTableSQLSnapshot, nil
}

func releaseTypedTableSQLSnapshot() {}

// TypedTableSQLSnapshotRegistry adapts multiple MVCC-enabled TypedTables to
// one SQL AS OF provider. A frontier is interpreted as the shared logical
// sequence across all registered tables.
//
// Registration is opt-in. Ordinary reads through the registry remain live;
// SQLQueryOptions.AsOfFrontier opens an immutable view at the requested
// sequence. Tables must therefore use the same logical sequence domain when
// they are registered together.
type TypedTableSQLSnapshotRegistry struct {
	mu     sync.RWMutex
	tables map[typedTableSQLSourceIdentity]*TypedTable
	order  []typedTableSQLSnapshotRegistration
}

var _ SQLFrontierSnapshotProvider = (*TypedTableSQLSnapshotRegistry)(nil)

// NewTypedTableSQLSnapshotRegistry creates an empty retained-state registry.
func NewTypedTableSQLSnapshotRegistry() *TypedTableSQLSnapshotRegistry {
	return &TypedTableSQLSnapshotRegistry{
		tables: make(map[typedTableSQLSourceIdentity]*TypedTable),
	}
}

// Register adds one MVCC-enabled table using its schema's SQL source name and
// key. A source identity can only be registered once.
func (registry *TypedTableSQLSnapshotRegistry) Register(table *TypedTable) error {
	if registry == nil {
		return ErrTypedTableSQLSnapshotRegistryRequired
	}
	if table == nil {
		return ErrTypedTableSQLSnapshotTableRequired
	}
	schema := table.Schema()
	if !schema.MVCC.Enabled {
		return fmt.Errorf("%w: %s", ErrTypedTableSQLSnapshotMVCCRequired, schema.Name)
	}
	identity := typedTableSQLSourceIdentityFromSchema(schema)
	if identity.source == "" || identity.key == "" {
		return fmt.Errorf("%w: table schema identity is empty", ErrTypedTableSQLSnapshotTableRequired)
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.tables == nil {
		registry.tables = make(map[typedTableSQLSourceIdentity]*TypedTable)
	}
	if _, exists := registry.tables[identity]; exists {
		return fmt.Errorf("%w: %s(%s)", ErrTypedTableSQLSnapshotDuplicate, identity.source, identity.key)
	}
	registry.tables[identity] = table
	registry.order = append(registry.order, typedTableSQLSnapshotRegistration{identity: identity, table: table})
	return nil
}

// ResolveSQLSource exposes the currently live table through SourceResolver.
func (registry *TypedTableSQLSnapshotRegistry) ResolveSQLSource(name string, key string) ([]Row, error) {
	if registry == nil {
		return nil, nil
	}
	identity := typedTableSQLSourceIdentityFromParts(name, key)
	registry.mu.RLock()
	table := registry.tables[identity]
	registry.mu.RUnlock()
	if table == nil {
		return nil, nil
	}
	return table.ResolveSQLSource(name, key)
}

// BeginSQLSnapshotAt captures every registered table at frontier and returns
// a resolver whose source reads cannot observe later writes. The returned
// release function is always non-nil for lifecycle symmetry; snapshots own
// their immutable data and need no explicit release.
func (registry *TypedTableSQLSnapshotRegistry) BeginSQLSnapshotAt(ctx context.Context, frontier uint64) (SQLSourceResolver, func(), error) {
	if registry == nil {
		return nil, nil, ErrTypedTableSQLSnapshotRegistryRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	registry.mu.RLock()
	view := &typedTableSQLSnapshotView{
		frontier:  frontier,
		snapshots: make([]typedTableSQLSnapshotEntry, 0, len(registry.order)),
	}
	for _, registration := range registry.order {
		if err := ctx.Err(); err != nil {
			registry.mu.RUnlock()
			return nil, nil, err
		}
		snapshot, err := registration.table.SnapshotAt(frontier)
		if err != nil {
			registry.mu.RUnlock()
			return nil, nil, fmt.Errorf("snapshot %s(%s) at frontier %d: %w", registration.identity.source, registration.identity.key, frontier, err)
		}
		view.snapshots = append(view.snapshots, typedTableSQLSnapshotEntry{identity: registration.identity, snapshot: snapshot})
	}
	registry.mu.RUnlock()
	return view, releaseTypedTableSQLSnapshot, nil
}

type typedTableSQLSnapshotEntry struct {
	identity typedTableSQLSourceIdentity
	snapshot *TypedTableSnapshot
}

type typedTableSQLSnapshotView struct {
	frontier  uint64
	snapshots []typedTableSQLSnapshotEntry
}

var (
	_ SQLSourceResolver                = (*typedTableSQLSnapshotView)(nil)
	_ ColumnarSourceResolver           = (*typedTableSQLSnapshotView)(nil)
	_ BorrowedColumnarSourceResolver   = (*typedTableSQLSnapshotView)(nil)
	_ ColumnarSourcePreferenceResolver = (*typedTableSQLSnapshotView)(nil)
	_ SourceVersionResolver            = (*typedTableSQLSnapshotView)(nil)
)

// Frontier reports the logical sequence captured by this immutable view.
func (view *typedTableSQLSnapshotView) Frontier() uint64 {
	if view == nil {
		return 0
	}
	return view.frontier
}

func (view *typedTableSQLSnapshotView) snapshot(name, key string) *TypedTableSnapshot {
	if view == nil {
		return nil
	}
	identity := typedTableSQLSourceIdentityFromParts(name, key)
	for _, entry := range view.snapshots {
		if entry.identity == identity {
			return entry.snapshot
		}
	}
	return nil
}

func (view *typedTableSQLSnapshotView) ResolveSQLSource(name string, key string) ([]Row, error) {
	snapshot := view.snapshot(name, key)
	if snapshot == nil {
		return nil, nil
	}
	return snapshot.ResolveSQLSource(name, key)
}

func (view *typedTableSQLSnapshotView) ResolveSQLColumnarSource(name, key string, fields []string) (ColumnarBatch, bool, error) {
	snapshot := view.snapshot(name, key)
	if snapshot == nil {
		return ColumnarBatch{}, false, nil
	}
	return snapshot.ResolveSQLColumnarSource(name, key, fields)
}

func (view *typedTableSQLSnapshotView) BorrowSQLColumnarSource(name, key string, fields []string) (ColumnarBatch, bool, error) {
	snapshot := view.snapshot(name, key)
	if snapshot == nil {
		return ColumnarBatch{}, false, nil
	}
	return snapshot.BorrowSQLColumnarSource(name, key, fields)
}

func (view *typedTableSQLSnapshotView) PreferSQLColumnarSource(name, key string, fields []string) bool {
	snapshot := view.snapshot(name, key)
	return snapshot != nil && snapshot.PreferSQLColumnarSource(name, key, fields)
}

func (view *typedTableSQLSnapshotView) SQLSourceVersion(name, key string) (string, bool, error) {
	snapshot := view.snapshot(name, key)
	if snapshot == nil {
		return "", false, nil
	}
	return snapshot.SQLSourceVersion(name, key)
}

func typedTableSQLSourceIdentityFromSchema(schema TypedTableSchema) typedTableSQLSourceIdentity {
	return typedTableSQLSourceIdentityFromParts(schema.SourceName, schema.Name)
}

func typedTableSQLSourceIdentityFromParts(name, key string) typedTableSQLSourceIdentity {
	return typedTableSQLSourceIdentity{
		source: strings.ToUpper(strings.TrimSpace(name)),
		key:    strings.TrimSpace(key),
	}
}
