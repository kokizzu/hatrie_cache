package hatSql

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// TypedTableMVCCOptions controls optional immutable historical snapshots.
// Disabled is the compatibility default because retaining overwritten values
// increases write and memory cost.
type TypedTableMVCCOptions struct {
	Enabled bool
}

var (
	// ErrTypedTableMVCCDisabled means the table was created without MVCC.
	ErrTypedTableMVCCDisabled = errors.New("typed table MVCC is disabled")
	// ErrTypedTableMVCCCompacted means the requested sequence is no longer
	// available for a new snapshot.
	ErrTypedTableMVCCCompacted = errors.New("typed table MVCC history compacted")
)

type typedTableMVCCVersion struct {
	sequence uint64
	values   []TypedTableValue
	deleted  bool
	previous *typedTableMVCCVersion
}

// typedTableMVCCState is mutated only while TypedTable.mu is held. Version
// nodes become immutable before publication, so snapshots can read them
// without retaining the table lock.
type typedTableMVCCState struct {
	heads            map[string]*typedTableMVCCVersion
	keys             []string
	knownKeys        map[string]struct{}
	compactedThrough uint64
}

func newTypedTableMVCCState() *typedTableMVCCState {
	return &typedTableMVCCState{
		heads:     make(map[string]*typedTableMVCCVersion),
		knownKeys: make(map[string]struct{}),
	}
}

func (state *typedTableMVCCState) record(change TypedTableChange) {
	if _, known := state.knownKeys[change.Key]; !known {
		state.knownKeys[change.Key] = struct{}{}
		state.keys = append(state.keys, change.Key)
	}
	state.heads[change.Key] = &typedTableMVCCVersion{
		sequence: change.Sequence,
		values:   cloneTypedTableValues(change.After),
		deleted:  len(change.After) == 0,
		previous: state.heads[change.Key],
	}
}

// TypedTableSnapshot is an immutable, sequence-consistent view of a
// MVCC-enabled TypedTable. It implements SourceResolver and
// ColumnarSourceResolver, so it can be passed directly to ExecuteQuery.
type TypedTableSnapshot struct {
	schema   TypedTableSchema
	sequence uint64
	keys     []string
	heads    map[string]*typedTableMVCCVersion
}

var _ SourceResolver = (*TypedTableSnapshot)(nil)
var _ ColumnarSourceResolver = (*TypedTableSnapshot)(nil)

// Snapshot returns a view at the latest committed table sequence.
func (table *TypedTable) Snapshot() (*TypedTableSnapshot, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	sequence := table.sequence
	table.mu.RUnlock()
	return table.SnapshotAt(sequence)
}

// SnapshotAt returns a view at sequence. Sequence zero represents the empty
// table. A sequence older than explicit MVCC compaction is rejected rather
// than returning an incomplete result.
func (table *TypedTable) SnapshotAt(sequence uint64) (*TypedTableSnapshot, error) {
	if table == nil {
		return nil, fmt.Errorf("typed table is nil")
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if table.mvcc == nil {
		return nil, ErrTypedTableMVCCDisabled
	}
	if sequence > table.sequence {
		return nil, fmt.Errorf("typed table MVCC sequence %d is newer than %d", sequence, table.sequence)
	}
	if sequence < table.mvcc.compactedThrough {
		return nil, ErrTypedTableMVCCCompacted
	}
	snapshot := &TypedTableSnapshot{
		schema:   cloneTypedTableSchema(table.schema),
		sequence: sequence,
		keys:     append([]string(nil), table.mvcc.keys...),
		heads:    make(map[string]*typedTableMVCCVersion, len(table.mvcc.heads)),
	}
	for key, head := range table.mvcc.heads {
		snapshot.heads[key] = head
	}
	return snapshot, nil
}

// CompactMVCCThrough discards historical links older than sequence from the
// table's current version chains. Snapshots created before compaction retain
// their immutable head pointers and remain valid until released by the
// caller/GC. New snapshots before sequence are rejected.
func (table *TypedTable) CompactMVCCThrough(sequence uint64) error {
	if table == nil {
		return fmt.Errorf("typed table is nil")
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.mvcc == nil {
		return ErrTypedTableMVCCDisabled
	}
	if sequence < table.mvcc.compactedThrough || sequence > table.sequence {
		return fmt.Errorf("typed table MVCC compaction sequence %d is outside %d..%d", sequence, table.mvcc.compactedThrough, table.sequence)
	}
	if sequence == table.mvcc.compactedThrough {
		return nil
	}
	for key, head := range table.mvcc.heads {
		retained := cloneTypedTableMVCCChainThrough(head, sequence)
		if retained == nil {
			delete(table.mvcc.heads, key)
			continue
		}
		table.mvcc.heads[key] = retained
	}
	table.mvcc.compactedThrough = sequence
	return nil
}

func cloneTypedTableMVCCChainThrough(head *typedTableMVCCVersion, sequence uint64) *typedTableMVCCVersion {
	retained := make([]*typedTableMVCCVersion, 0, 2)
	for version := head; version != nil; version = version.previous {
		retained = append(retained, version)
		if version.sequence <= sequence {
			break
		}
	}
	var cloned *typedTableMVCCVersion
	for index := len(retained) - 1; index >= 0; index-- {
		version := retained[index]
		cloned = &typedTableMVCCVersion{
			sequence: version.sequence,
			values:   cloneTypedTableValues(version.values),
			deleted:  version.deleted,
			previous: cloned,
		}
	}
	return cloned
}

// Sequence returns the immutable sequence represented by the snapshot.
func (snapshot *TypedTableSnapshot) Sequence() uint64 {
	if snapshot == nil {
		return 0
	}
	return snapshot.sequence
}

func (snapshot *TypedTableSnapshot) valuesAt(key string) ([]TypedTableValue, bool) {
	if snapshot == nil {
		return nil, false
	}
	version := snapshot.heads[key]
	for version != nil && version.sequence > snapshot.sequence {
		version = version.previous
	}
	if version == nil || version.deleted {
		return nil, false
	}
	return cloneTypedTableValues(version.values), true
}

// Rows returns independent row maps in the snapshot's insertion order.
func (snapshot *TypedTableSnapshot) Rows() []Row {
	if snapshot == nil {
		return nil
	}
	rows := make([]Row, 0, len(snapshot.keys))
	for _, key := range snapshot.keys {
		values, found := snapshot.valuesAt(key)
		if !found {
			continue
		}
		rows = append(rows, typedTableSnapshotRowMap(snapshot.schema, values))
	}
	return rows
}

// ResolveSQLSource exposes the immutable snapshot through the normal row
// resolver contract.
func (snapshot *TypedTableSnapshot) ResolveSQLSource(name string, key string) ([]Row, error) {
	if snapshot == nil || strings.ToUpper(strings.TrimSpace(name)) != snapshot.schema.SourceName || key != snapshot.schema.Name {
		return nil, nil
	}
	return snapshot.Rows(), nil
}

// ResolveSQLColumnarSource exposes the requested columns without referring to
// the mutable table, allowing a query to run after the writer advances.
func (snapshot *TypedTableSnapshot) ResolveSQLColumnarSource(name string, key string, fields []string) (ColumnarBatch, bool, error) {
	if snapshot == nil || strings.ToUpper(strings.TrimSpace(name)) != snapshot.schema.SourceName || key != snapshot.schema.Name {
		return ColumnarBatch{}, false, nil
	}
	batch := ColumnarBatch{Columns: make(map[string][]interface{}, len(fields))}
	for _, field := range fields {
		column, found := snapshot.columnIndex(field)
		if !found {
			continue
		}
		values := make([]interface{}, 0, len(snapshot.keys))
		for _, rowKey := range snapshot.keys {
			row, found := snapshot.valuesAt(rowKey)
			if !found {
				continue
			}
			values = append(values, typedTableValueInterface(row[column]))
		}
		batch.Columns[field] = values
		if batch.Rows < len(values) {
			batch.Rows = len(values)
		}
	}
	return batch, true, nil
}

// BorrowSQLColumnarSource serves the immutable snapshot as an already-stable
// layout. There is no mutable cache admission to perform.
func (snapshot *TypedTableSnapshot) BorrowSQLColumnarSource(name string, key string, fields []string) (ColumnarBatch, bool, error) {
	return snapshot.ResolveSQLColumnarSource(name, key, fields)
}

// PreferSQLColumnarSource reports that all snapshot data is immutable.
func (snapshot *TypedTableSnapshot) PreferSQLColumnarSource(name string, key string, fields []string) bool {
	if snapshot == nil || strings.ToUpper(strings.TrimSpace(name)) != snapshot.schema.SourceName || key != snapshot.schema.Name {
		return false
	}
	for _, field := range fields {
		if _, found := snapshot.columnIndex(field); !found {
			return false
		}
	}
	return true
}

// SQLSourceVersion returns the fixed snapshot sequence for condition-cache
// admission and source-version checks.
func (snapshot *TypedTableSnapshot) SQLSourceVersion(name string, key string) (string, bool, error) {
	if snapshot == nil || strings.ToUpper(strings.TrimSpace(name)) != snapshot.schema.SourceName || key != snapshot.schema.Name {
		return "", false, nil
	}
	return strconv.FormatUint(snapshot.sequence, 10), true, nil
}

// Schema returns a detached copy of the snapshot schema.
func (snapshot *TypedTableSnapshot) Schema() TypedTableSchema {
	if snapshot == nil {
		return TypedTableSchema{}
	}
	return cloneTypedTableSchema(snapshot.schema)
}

func (snapshot *TypedTableSnapshot) columnIndex(field string) (int, bool) {
	for index, column := range snapshot.schema.Columns {
		if column.Name == field {
			return index, true
		}
	}
	return 0, false
}

func typedTableSnapshotRowMap(schema TypedTableSchema, values []TypedTableValue) Row {
	row := make(Row, len(schema.Columns))
	for index, column := range schema.Columns {
		row[column.Name] = typedTableValueInterface(values[index])
	}
	return row
}

func cloneTypedTableSchema(schema TypedTableSchema) TypedTableSchema {
	schema.Columns = append([]TypedTableColumn(nil), schema.Columns...)
	return schema
}
