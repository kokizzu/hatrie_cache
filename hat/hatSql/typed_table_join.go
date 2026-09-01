package hatSql

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
)

// TypedTableJoinDefinition declares one exact inner equi-join over two typed
// tables. Joined fields must use the same physical kind.
type TypedTableJoinDefinition struct {
	LeftField  string
	RightField string
}

// TypedTableJoinRow retains both typed source rows for one matching key pair.
type TypedTableJoinRow struct {
	LeftKey, RightKey string
	Left, Right        []TypedTableValue
}

type typedTableJoinPair struct {
	leftKey  string
	rightKey string
}

// TypedTableJoin incrementally maintains exact inner equi-join results from
// ordered changes. It is independent from SQL execution and lets consumers
// share one arrangement rather than each rescan both input tables.
type TypedTableJoin struct {
	mu                          sync.RWMutex
	left, right                 *TypedTable
	definition                  TypedTableJoinDefinition
	leftField, rightField       int
	leftCheckpoint, rightCheckpoint uint64
	leftRows, rightRows         map[string][]TypedTableValue
	leftIndex, rightIndex       map[string]map[string]struct{}
	pairs                       map[typedTableJoinPair]struct{}
}

// NewTypedTableJoin snapshots both current tables and begins tracking changes
// after their respective current sequences.
func NewTypedTableJoin(left, right *TypedTable, definition TypedTableJoinDefinition) (*TypedTableJoin, error) {
	if left == nil || right == nil {
		return nil, fmt.Errorf("typed table join requires two tables")
	}
	if left == right {
		return nil, fmt.Errorf("typed table join requires distinct tables")
	}
	leftField, leftKind, found := typedTableJoinField(left, definition.LeftField)
	if !found {
		return nil, fmt.Errorf("typed table join left field %q does not exist", definition.LeftField)
	}
	rightField, rightKind, found := typedTableJoinField(right, definition.RightField)
	if !found {
		return nil, fmt.Errorf("typed table join right field %q does not exist", definition.RightField)
	}
	if leftKind != rightKind {
		return nil, fmt.Errorf("typed table join fields %q and %q have different kinds", definition.LeftField, definition.RightField)
	}
	join := &TypedTableJoin{
		left: left, right: right, definition: definition, leftField: leftField, rightField: rightField,
		leftRows: map[string][]TypedTableValue{}, rightRows: map[string][]TypedTableValue{},
		leftIndex: map[string]map[string]struct{}{}, rightIndex: map[string]map[string]struct{}{}, pairs: map[typedTableJoinPair]struct{}{},
	}
	join.leftRows, join.leftCheckpoint = typedTableJoinSnapshot(left)
	join.rightRows, join.rightCheckpoint = typedTableJoinSnapshot(right)
	for key, values := range join.leftRows {
		join.addLeft(key, values)
	}
	for key, values := range join.rightRows {
		join.addRight(key, values)
	}
	return join, nil
}

func typedTableJoinField(table *TypedTable, field string) (int, TypedTableKind, bool) {
	table.mu.RLock()
	defer table.mu.RUnlock()
	index, found := table.byName[field]
	if !found {
		return 0, TypedTableNull, false
	}
	return index, table.columns[index].kind, true
}

func typedTableJoinSnapshot(table *TypedTable) (map[string][]TypedTableValue, uint64) {
	table.mu.RLock()
	defer table.mu.RUnlock()
	rows := make(map[string][]TypedTableValue, len(table.keys))
	for index, key := range table.keys {
		rows[key] = table.rowLocked(index)
	}
	return rows, table.sequence
}

// ApplyLeft applies strictly ordered changes from the left source.
func (join *TypedTableJoin) ApplyLeft(changes []TypedTableChange) error {
	if join == nil {
		return fmt.Errorf("typed table join is nil")
	}
	join.mu.Lock()
	defer join.mu.Unlock()
	if len(changes) == 1 {
		return join.applyLeftOneLocked(changes[0])
	}
	for index := 0; index < len(changes); {
		change := changes[index]
		if change.Sequence <= join.leftCheckpoint {
			index++
			continue
		}
		if change.Sequence != join.leftCheckpoint+1 {
			return fmt.Errorf("typed table join left change sequence gap: got %d after %d", change.Sequence, join.leftCheckpoint)
		}
		if err := typedTableJoinChange(change); err != nil {
			return err
		}
		last := change
		checkpoint := change.Sequence
		index++
		for index < len(changes) && changes[index].Key == change.Key {
			next := changes[index]
			if next.Sequence <= checkpoint {
				index++
				continue
			}
			if next.Sequence != checkpoint+1 {
				if err := join.applyLeft(last); err != nil {
					return err
				}
				join.leftCheckpoint = checkpoint
				return fmt.Errorf("typed table join left change sequence gap: got %d after %d", next.Sequence, checkpoint)
			}
			if err := typedTableJoinChange(next); err != nil {
				if applyErr := join.applyLeft(last); applyErr != nil {
					return applyErr
				}
				join.leftCheckpoint = checkpoint
				return err
			}
			last = next
			checkpoint = next.Sequence
			index++
		}
		if err := join.applyLeft(last); err != nil {
			return err
		}
		join.leftCheckpoint = checkpoint
	}
	return nil
}

func (join *TypedTableJoin) applyLeftOneLocked(change TypedTableChange) error {
	if change.Sequence <= join.leftCheckpoint {
		return nil
	}
	if change.Sequence != join.leftCheckpoint+1 {
		return fmt.Errorf("typed table join left change sequence gap: got %d after %d", change.Sequence, join.leftCheckpoint)
	}
	if err := join.applyLeft(change); err != nil {
		return err
	}
	join.leftCheckpoint = change.Sequence
	return nil
}

// ApplyRight applies strictly ordered changes from the right source.
func (join *TypedTableJoin) ApplyRight(changes []TypedTableChange) error {
	if join == nil {
		return fmt.Errorf("typed table join is nil")
	}
	join.mu.Lock()
	defer join.mu.Unlock()
	if len(changes) == 1 {
		return join.applyRightOneLocked(changes[0])
	}
	for index := 0; index < len(changes); {
		change := changes[index]
		if change.Sequence <= join.rightCheckpoint {
			index++
			continue
		}
		if change.Sequence != join.rightCheckpoint+1 {
			return fmt.Errorf("typed table join right change sequence gap: got %d after %d", change.Sequence, join.rightCheckpoint)
		}
		if err := typedTableJoinChange(change); err != nil {
			return err
		}
		last := change
		checkpoint := change.Sequence
		index++
		for index < len(changes) && changes[index].Key == change.Key {
			next := changes[index]
			if next.Sequence <= checkpoint {
				index++
				continue
			}
			if next.Sequence != checkpoint+1 {
				if err := join.applyRight(last); err != nil {
					return err
				}
				join.rightCheckpoint = checkpoint
				return fmt.Errorf("typed table join right change sequence gap: got %d after %d", next.Sequence, checkpoint)
			}
			if err := typedTableJoinChange(next); err != nil {
				if applyErr := join.applyRight(last); applyErr != nil {
					return applyErr
				}
				join.rightCheckpoint = checkpoint
				return err
			}
			last = next
			checkpoint = next.Sequence
			index++
		}
		if err := join.applyRight(last); err != nil {
			return err
		}
		join.rightCheckpoint = checkpoint
	}
	return nil
}

func (join *TypedTableJoin) applyRightOneLocked(change TypedTableChange) error {
	if change.Sequence <= join.rightCheckpoint {
		return nil
	}
	if change.Sequence != join.rightCheckpoint+1 {
		return fmt.Errorf("typed table join right change sequence gap: got %d after %d", change.Sequence, join.rightCheckpoint)
	}
	if err := join.applyRight(change); err != nil {
		return err
	}
	join.rightCheckpoint = change.Sequence
	return nil
}

func (join *TypedTableJoin) applyLeft(change TypedTableChange) error {
	if err := typedTableJoinChange(change); err != nil {
		return err
	}
	if previous, found := join.leftRows[change.Key]; found {
		join.removeLeft(change.Key, previous)
	}
	if change.Operation != "DELETE" {
		join.addLeft(change.Key, change.After)
	}
	return nil
}

func (join *TypedTableJoin) applyRight(change TypedTableChange) error {
	if err := typedTableJoinChange(change); err != nil {
		return err
	}
	if previous, found := join.rightRows[change.Key]; found {
		join.removeRight(change.Key, previous)
	}
	if change.Operation != "DELETE" {
		join.addRight(change.Key, change.After)
	}
	return nil
}

func typedTableJoinChange(change TypedTableChange) error {
	if change.Key == "" || change.Operation != "INSERT" && change.Operation != "UPDATE" && change.Operation != "DELETE" {
		return fmt.Errorf("invalid typed table join change")
	}
	if change.Operation != "DELETE" && len(change.After) == 0 {
		return fmt.Errorf("typed table join %s change has no after row", change.Operation)
	}
	return nil
}

func (join *TypedTableJoin) addLeft(key string, values []TypedTableValue) {
	values = cloneTypedTableValues(values)
	join.leftRows[key] = values
	valueKey, joined := typedTableJoinValue(values, join.leftField)
	if !joined {
		return
	}
	addTypedTableJoinIndex(join.leftIndex, valueKey, key)
	for rightKey := range join.rightIndex[valueKey] {
		join.pairs[typedTableJoinPair{leftKey: key, rightKey: rightKey}] = struct{}{}
	}
}

func (join *TypedTableJoin) removeLeft(key string, values []TypedTableValue) {
	valueKey, joined := typedTableJoinValue(values, join.leftField)
	if joined {
		for rightKey := range join.rightIndex[valueKey] {
			delete(join.pairs, typedTableJoinPair{leftKey: key, rightKey: rightKey})
		}
		removeTypedTableJoinIndex(join.leftIndex, valueKey, key)
	}
	delete(join.leftRows, key)
}

func (join *TypedTableJoin) addRight(key string, values []TypedTableValue) {
	values = cloneTypedTableValues(values)
	join.rightRows[key] = values
	valueKey, joined := typedTableJoinValue(values, join.rightField)
	if !joined {
		return
	}
	addTypedTableJoinIndex(join.rightIndex, valueKey, key)
	for leftKey := range join.leftIndex[valueKey] {
		join.pairs[typedTableJoinPair{leftKey: leftKey, rightKey: key}] = struct{}{}
	}
}

func (join *TypedTableJoin) removeRight(key string, values []TypedTableValue) {
	valueKey, joined := typedTableJoinValue(values, join.rightField)
	if joined {
		for leftKey := range join.leftIndex[valueKey] {
			delete(join.pairs, typedTableJoinPair{leftKey: leftKey, rightKey: key})
		}
		removeTypedTableJoinIndex(join.rightIndex, valueKey, key)
	}
	delete(join.rightRows, key)
}

func addTypedTableJoinIndex(index map[string]map[string]struct{}, value, key string) {
	if index[value] == nil {
		index[value] = map[string]struct{}{}
	}
	index[value][key] = struct{}{}
}

func removeTypedTableJoinIndex(index map[string]map[string]struct{}, value, key string) {
	delete(index[value], key)
	if len(index[value]) == 0 {
		delete(index, value)
	}
}

func typedTableJoinValue(values []TypedTableValue, field int) (string, bool) {
	if field < 0 || field >= len(values) || !values[field].Valid {
		return "", false
	}
	value := values[field]
	switch value.Kind {
	case TypedTableString:
		return value.String, true
	case TypedTableInt64:
		return "i:" + strconv.FormatInt(value.Int64, 10), true
	case TypedTableFloat64:
		if math.IsNaN(value.Float64) {
			return "", false
		}
		if value.Float64 == 0 {
			return "f:0", true
		}
		return "f:" + strconv.FormatFloat(value.Float64, 'g', -1, 64), true
	case TypedTableBool:
		return "b:" + strconv.FormatBool(value.Bool), true
	}
	return "", false
}

// LeftCheckpoint and RightCheckpoint identify the latest applied source changes.
func (join *TypedTableJoin) LeftCheckpoint() uint64 {
	if join == nil { return 0 }
	join.mu.RLock(); defer join.mu.RUnlock()
	return join.leftCheckpoint
}
func (join *TypedTableJoin) RightCheckpoint() uint64 {
	if join == nil { return 0 }
	join.mu.RLock(); defer join.mu.RUnlock()
	return join.rightCheckpoint
}

// Rows returns independently owned, deterministically ordered join rows.
func (join *TypedTableJoin) Rows() []TypedTableJoinRow {
	if join == nil { return nil }
	join.mu.RLock()
	defer join.mu.RUnlock()
	rows := make([]TypedTableJoinRow, 0, len(join.pairs))
	for pair := range join.pairs {
		left, leftFound := join.leftRows[pair.leftKey]
		right, rightFound := join.rightRows[pair.rightKey]
		if !leftFound || !rightFound {
			continue
		}
		rows = append(rows, TypedTableJoinRow{
			LeftKey:  pair.leftKey,
			RightKey: pair.rightKey,
			Left:     cloneTypedTableValues(left),
			Right:    cloneTypedTableValues(right),
		})
	}
	sort.Slice(rows, func(left, right int) bool { if rows[left].LeftKey != rows[right].LeftKey { return rows[left].LeftKey < rows[right].LeftKey }; return rows[left].RightKey < rows[right].RightKey })
	return rows
}
