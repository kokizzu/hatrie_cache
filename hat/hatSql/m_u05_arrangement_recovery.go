package hatSql

import (
	"errors"
	"fmt"
	"math"
	"sort"
)

const (
	TypedTableArrangementCheckpointVersion   uint8 = 1
	MaxTypedTableArrangementCheckpointGroups       = 1_000_000
	MaxTypedTableArrangementCheckpointRows         = 1_000_000
)

var (
	ErrTypedTableArrangementCheckpointInvalid       = errors.New("typed table arrangement checkpoint is invalid")
	ErrTypedTableArrangementSourceVersionMismatch   = errors.New("typed table arrangement source version mismatch")
	ErrTypedTableArrangementCheckpointDuplicate     = errors.New("typed table arrangement checkpoint is duplicated")
	ErrTypedTableArrangementCheckpointAlreadyExists = errors.New("typed table arrangement checkpoint already exists")
)

// TypedTableArrangementValueCountCheckpoint stores one counted value from an
// exact aggregate arrangement. Slices are used instead of maps so checkpoints
// remain deterministic and can be encoded by JSON or another storage format.
type TypedTableArrangementValueCountCheckpoint struct {
	Value TypedTableValue
	Count int64
}

// TypedTableAggregateGroupCheckpoint stores the complete state needed to
// continue one aggregate group without replaying source changes.
type TypedTableAggregateGroupCheckpoint struct {
	Key            string
	Values         []TypedTableValue
	Count          int64
	Sum            float64
	Minimum        TypedTableValue
	Maximum        TypedTableValue
	HasMin         bool
	HasMax         bool
	MinValues      []TypedTableArrangementValueCountCheckpoint
	MaxValues      []TypedTableArrangementValueCountCheckpoint
	DistinctValues []TypedTableArrangementValueCountCheckpoint
}

// TypedTableAggregateArrangementCheckpoint is a detached, versioned
// checkpoint for one aggregate arrangement. Checkpoint must equal
// SourceSequence for arrangement-only recovery; otherwise retained source
// changes are still required.
type TypedTableAggregateArrangementCheckpoint struct {
	Version        uint8
	TableName      string
	Definition     TypedTableAggregateDefinition
	SourceSequence uint64
	Checkpoint     uint64
	Groups         []TypedTableAggregateGroupCheckpoint
}

// TypedTableJoinCheckpointRow stores one source row retained by a join
// arrangement.
type TypedTableJoinCheckpointRow struct {
	Key    string
	Values []TypedTableValue
}

// TypedTableJoinArrangementCheckpoint is a detached, versioned checkpoint for
// both sides of one join arrangement.
type TypedTableJoinArrangementCheckpoint struct {
	Version             uint8
	LeftTableName       string
	RightTableName      string
	Definition          TypedTableJoinDefinition
	LeftSourceSequence  uint64
	RightSourceSequence uint64
	LeftCheckpoint      uint64
	RightCheckpoint     uint64
	LeftRows            []TypedTableJoinCheckpointRow
	RightRows           []TypedTableJoinCheckpointRow
}

// CaptureCheckpoint returns complete aggregate state without reading or
// replaying the source changefeed.
func (arrangement *TypedTableAggregateArrangement) CaptureCheckpoint() (TypedTableAggregateArrangementCheckpoint, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableAggregateArrangementCheckpoint{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return captureTypedTableAggregateArrangementCheckpoint(entry.aggregate)
}

// RestoreCheckpoint replaces an aggregate's internal state after validating
// its definition, source identity, and exact source version.
func (arrangement *TypedTableAggregateArrangement) RestoreCheckpoint(checkpoint TypedTableAggregateArrangementCheckpoint) error {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return restoreTypedTableAggregateArrangementCheckpointStateLocked(entry.aggregate, checkpoint)
}

// CaptureCheckpoints returns all currently maintained aggregate arrangements
// in deterministic definition order.
func (arrangements *TypedTableAggregateArrangements) CaptureCheckpoints() ([]TypedTableAggregateArrangementCheckpoint, error) {
	if arrangements == nil {
		return nil, fmt.Errorf("typed table aggregate arrangements are nil")
	}
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	keys := make([]string, 0, len(arrangements.entries))
	for key := range arrangements.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	checkpoints := make([]TypedTableAggregateArrangementCheckpoint, 0, len(keys))
	for _, key := range keys {
		entry := arrangements.entries[key]
		if entry == nil || entry.aggregate == nil {
			continue
		}
		entry.mu.Lock()
		checkpoint, err := captureTypedTableAggregateArrangementCheckpoint(entry.aggregate)
		entry.mu.Unlock()
		if err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, checkpoint)
	}
	return checkpoints, nil
}

// RestoreCheckpoints restores all supplied aggregate arrangements. It returns
// one lease per restored definition; callers release those leases normally.
// If any checkpoint fails, leases created by this call are released.
func (arrangements *TypedTableAggregateArrangements) RestoreCheckpoints(checkpoints []TypedTableAggregateArrangementCheckpoint) ([]*TypedTableAggregateArrangement, error) {
	if arrangements == nil {
		return nil, fmt.Errorf("typed table aggregate arrangements are nil")
	}
	seen := make(map[string]struct{}, len(checkpoints))
	for _, checkpoint := range checkpoints {
		key := typedTableAggregateArrangementKey(checkpoint.Definition)
		if _, exists := seen[key]; exists {
			return nil, ErrTypedTableArrangementCheckpointDuplicate
		}
		seen[key] = struct{}{}
	}
	leases := make([]*TypedTableAggregateArrangement, 0, len(checkpoints))
	releaseAll := func() {
		for _, lease := range leases {
			lease.Release()
		}
	}
	for _, checkpoint := range checkpoints {
		key := typedTableAggregateArrangementKey(checkpoint.Definition)
		arrangements.mu.Lock()
		_, exists := arrangements.entries[key]
		arrangements.mu.Unlock()
		if exists {
			releaseAll()
			return nil, ErrTypedTableArrangementCheckpointAlreadyExists
		}
		lease, err := arrangements.Acquire(checkpoint.Definition)
		if err != nil {
			releaseAll()
			return nil, err
		}
		if err := lease.RestoreCheckpoint(checkpoint); err != nil {
			lease.Release()
			releaseAll()
			return nil, err
		}
		leases = append(leases, lease)
	}
	return leases, nil
}

func captureTypedTableAggregateArrangementCheckpoint(aggregate *TypedTableAggregate) (TypedTableAggregateArrangementCheckpoint, error) {
	if aggregate == nil || aggregate.table == nil {
		return TypedTableAggregateArrangementCheckpoint{}, ErrTypedTableArrangementCheckpointInvalid
	}
	table := aggregate.table
	table.mu.RLock()
	defer table.mu.RUnlock()
	sourceSequence := table.sequence
	if aggregate.checkpoint != sourceSequence {
		return TypedTableAggregateArrangementCheckpoint{}, ErrTypedTableArrangementCheckpointInvalid
	}
	definition := typedTableAggregateDefinitionLocked(aggregate, table)
	checkpoint := TypedTableAggregateArrangementCheckpoint{
		Version:        TypedTableArrangementCheckpointVersion,
		TableName:      table.schema.Name,
		Definition:     definition,
		SourceSequence: sourceSequence,
		Checkpoint:     aggregate.checkpoint,
	}
	groups := make([]TypedTableAggregateGroupCheckpoint, 0, aggregate.groupCount)
	for _, bucket := range aggregate.groups {
		groupCheckpoint := typedTableAggregateGroupCheckpointFromGroup(bucket.group)
		if groupCheckpoint.Key == "" {
			groupCheckpoint.Key = aggregate.storedGroupKey(bucket.group)
		}
		groups = append(groups, groupCheckpoint)
		for _, group := range bucket.collisions {
			groupCheckpoint := typedTableAggregateGroupCheckpointFromGroup(group)
			if groupCheckpoint.Key == "" {
				groupCheckpoint.Key = aggregate.storedGroupKey(group)
			}
			groups = append(groups, groupCheckpoint)
		}
	}
	sort.Slice(groups, func(left, right int) bool {
		return groups[left].Key < groups[right].Key
	})
	checkpoint.Groups = groups
	return checkpoint, nil
}

func typedTableAggregateDefinitionLocked(aggregate *TypedTableAggregate, table *TypedTable) TypedTableAggregateDefinition {
	definition := TypedTableAggregateDefinition{}
	for _, index := range aggregate.groupBy {
		if index >= 0 && index < len(table.schema.Columns) {
			definition.GroupBy = append(definition.GroupBy, table.schema.Columns[index].Name)
		}
	}
	definition.SumField = typedTableColumnName(table.schema.Columns, aggregate.sumField)
	definition.MinField = typedTableColumnName(table.schema.Columns, aggregate.minField)
	definition.MaxField = typedTableColumnName(table.schema.Columns, aggregate.maxField)
	definition.DistinctField = typedTableColumnName(table.schema.Columns, aggregate.distinctField)
	return definition
}

func typedTableAggregateGroupCheckpointFromGroup(group typedTableAggregateGroup) TypedTableAggregateGroupCheckpoint {
	return TypedTableAggregateGroupCheckpoint{
		Key:            group.key,
		Values:         cloneTypedTableValues(group.values),
		Count:          group.count,
		Sum:            group.sum,
		Minimum:        group.minimum,
		Maximum:        group.maximum,
		HasMin:         group.hasMin,
		HasMax:         group.hasMax,
		MinValues:      typedTableArrangementValueCounts(group.minValues),
		MaxValues:      typedTableArrangementValueCounts(group.maxValues),
		DistinctValues: typedTableArrangementDistinctCounts(group.distinctValues),
	}
}

func typedTableArrangementValueCounts(values map[TypedTableValue]int64) []TypedTableArrangementValueCountCheckpoint {
	ordered := make([]typedTableArrangementValueCountOrder, 0, len(values))
	for value, count := range values {
		ordered = append(ordered, typedTableArrangementValueCountOrder{
			checkpoint: TypedTableArrangementValueCountCheckpoint{Value: value, Count: count},
		})
	}
	sort.Slice(ordered, func(left, right int) bool {
		return typedTableArrangementValueLess(ordered[left].checkpoint.Value, ordered[right].checkpoint.Value)
	})
	checkpoints := make([]TypedTableArrangementValueCountCheckpoint, len(ordered))
	for index, value := range ordered {
		checkpoints[index] = value.checkpoint
	}
	return checkpoints
}

func typedTableArrangementDistinctCounts(values map[typedTableDistinctValue]int64) []TypedTableArrangementValueCountCheckpoint {
	ordered := make([]typedTableArrangementValueCountOrder, 0, len(values))
	for value, count := range values {
		checkpointValue := typedTableAggregateDistinctCheckpointValue(value)
		ordered = append(ordered, typedTableArrangementValueCountOrder{
			checkpoint: TypedTableArrangementValueCountCheckpoint{Value: checkpointValue, Count: count},
		})
	}
	sort.Slice(ordered, func(left, right int) bool {
		return typedTableArrangementValueLess(ordered[left].checkpoint.Value, ordered[right].checkpoint.Value)
	})
	checkpoints := make([]TypedTableArrangementValueCountCheckpoint, len(ordered))
	for index, value := range ordered {
		checkpoints[index] = value.checkpoint
	}
	return checkpoints
}

type typedTableArrangementValueCountOrder struct {
	checkpoint TypedTableArrangementValueCountCheckpoint
}

func typedTableArrangementValueLess(left, right TypedTableValue) bool {
	if left.Valid != right.Valid {
		return !left.Valid
	}
	if !left.Valid {
		return false
	}
	if left.Kind != right.Kind {
		return left.Kind < right.Kind
	}
	switch left.Kind {
	case TypedTableString:
		return left.String < right.String
	case TypedTableInt64:
		return left.Int64 < right.Int64
	case TypedTableFloat64:
		return math.Float64bits(left.Float64) < math.Float64bits(right.Float64)
	case TypedTableBool:
		return !left.Bool && right.Bool
	default:
		return false
	}
}

func typedTableArrangementValueEqual(left, right TypedTableValue) bool {
	if left.Valid != right.Valid || left.Kind != right.Kind {
		return false
	}
	if !left.Valid {
		return true
	}
	switch left.Kind {
	case TypedTableString:
		return left.String == right.String
	case TypedTableInt64:
		return left.Int64 == right.Int64
	case TypedTableFloat64:
		return math.Float64bits(left.Float64) == math.Float64bits(right.Float64)
	case TypedTableBool:
		return left.Bool == right.Bool
	default:
		return left == right
	}
}

func typedTableArrangementCheckpointValueBudget(total *int, amount int) bool {
	if amount < 0 || amount > MaxTypedTableArrangementCheckpointRows || *total > MaxTypedTableArrangementCheckpointRows-amount {
		return false
	}
	*total += amount
	return true
}

func typedTableArrangementCountsFit(values []TypedTableArrangementValueCountCheckpoint, limit int64) bool {
	var total int64
	for _, value := range values {
		if value.Count <= 0 || total > limit-value.Count {
			return false
		}
		total += value.Count
	}
	return true
}

func typedTableAggregateDistinctCheckpointValue(value typedTableDistinctValue) TypedTableValue {
	checkpoint := TypedTableValue{Kind: value.kind, Valid: true}
	switch value.kind {
	case TypedTableString:
		checkpoint.String = value.stringValue
	case TypedTableInt64:
		checkpoint.Int64 = value.int64Value
	case TypedTableFloat64:
		checkpoint.Float64 = math.Float64frombits(value.floatBits)
	case TypedTableBool:
		checkpoint.Bool = value.boolValue
	}
	return checkpoint
}

func restoreTypedTableAggregateArrangementCheckpointState(aggregate *TypedTableAggregate, checkpoint TypedTableAggregateArrangementCheckpoint) (map[uint64]typedTableAggregateGroupBucket, int, error) {
	table := aggregate.table
	table.mu.RLock()
	defer table.mu.RUnlock()
	if checkpoint.Version != TypedTableArrangementCheckpointVersion || checkpoint.TableName != table.schema.Name || len(checkpoint.Groups) > MaxTypedTableArrangementCheckpointGroups {
		return nil, 0, ErrTypedTableArrangementCheckpointInvalid
	}
	if checkpoint.SourceSequence != table.sequence {
		return nil, 0, ErrTypedTableArrangementSourceVersionMismatch
	}
	if checkpoint.Checkpoint != checkpoint.SourceSequence {
		return nil, 0, ErrTypedTableArrangementCheckpointInvalid
	}
	expected := typedTableAggregateDefinitionLocked(aggregate, table)
	if typedTableAggregateArrangementKey(expected) != typedTableAggregateArrangementKey(checkpoint.Definition) {
		return nil, 0, ErrTypedTableArrangementCheckpointInvalid
	}
	groups := make(map[uint64]typedTableAggregateGroupBucket, len(checkpoint.Groups))
	seenKeys := make(map[string]struct{}, len(checkpoint.Groups))
	groupCount := 0
	valueCount := 0
	for _, checkpointGroup := range checkpoint.Groups {
		if checkpointGroup.Key == "" || checkpointGroup.Count <= 0 || len(checkpointGroup.Values) != len(aggregate.groupBy) {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		if !typedTableArrangementCheckpointValueBudget(&valueCount, len(checkpointGroup.MinValues)) ||
			!typedTableArrangementCheckpointValueBudget(&valueCount, len(checkpointGroup.MaxValues)) ||
			!typedTableArrangementCheckpointValueBudget(&valueCount, len(checkpointGroup.DistinctValues)) {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		if _, exists := seenKeys[checkpointGroup.Key]; exists {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		seenKeys[checkpointGroup.Key] = struct{}{}
		for index, value := range checkpointGroup.Values {
			column := aggregate.groupBy[index]
			if !typedTableArrangementValueMatchesColumn(value, table.columns[column].kind) {
				return nil, 0, ErrTypedTableArrangementCheckpointInvalid
			}
		}
		if aggregate.sumField < 0 && checkpointGroup.Sum != 0 {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		if aggregate.minField < 0 && (len(checkpointGroup.MinValues) != 0 || checkpointGroup.HasMin) {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		if aggregate.maxField < 0 && (len(checkpointGroup.MaxValues) != 0 || checkpointGroup.HasMax) {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		if aggregate.distinctField < 0 && len(checkpointGroup.DistinctValues) != 0 {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		if aggregate.minField >= 0 && !typedTableArrangementCountsFit(checkpointGroup.MinValues, checkpointGroup.Count) {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		if aggregate.maxField >= 0 && !typedTableArrangementCountsFit(checkpointGroup.MaxValues, checkpointGroup.Count) {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		if aggregate.distinctField >= 0 && !typedTableArrangementCountsFit(checkpointGroup.DistinctValues, checkpointGroup.Count) {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		group := typedTableAggregateGroup{
			key:    checkpointGroup.Key,
			values: cloneTypedTableValues(checkpointGroup.Values),
			count:  checkpointGroup.Count,
			sum:    checkpointGroup.Sum,
		}
		var err error
		if aggregate.minField >= 0 {
			group.minValues, err = restoreTypedTableArrangementValueCounts(checkpointGroup.MinValues, table.columns[aggregate.minField].kind)
			if err != nil {
				return nil, 0, err
			}
			group.minimum, group.hasMin = typedTableAggregateExtreme(group.minValues, true)
			if group.hasMin != checkpointGroup.HasMin ||
				(group.hasMin && !typedTableArrangementValueEqual(group.minimum, checkpointGroup.Minimum)) ||
				(!group.hasMin && checkpointGroup.Minimum.Valid) {
				return nil, 0, ErrTypedTableArrangementCheckpointInvalid
			}
		}
		if aggregate.maxField >= 0 {
			group.maxValues, err = restoreTypedTableArrangementValueCounts(checkpointGroup.MaxValues, table.columns[aggregate.maxField].kind)
			if err != nil {
				return nil, 0, err
			}
			group.maximum, group.hasMax = typedTableAggregateExtreme(group.maxValues, false)
			if group.hasMax != checkpointGroup.HasMax ||
				(group.hasMax && !typedTableArrangementValueEqual(group.maximum, checkpointGroup.Maximum)) ||
				(!group.hasMax && checkpointGroup.Maximum.Valid) {
				return nil, 0, ErrTypedTableArrangementCheckpointInvalid
			}
		}
		if aggregate.distinctField >= 0 {
			group.distinctValues, err = restoreTypedTableArrangementDistinctCounts(checkpointGroup.DistinctValues, table.columns[aggregate.distinctField].kind)
			if err != nil {
				return nil, 0, err
			}
		}
		fullValues := make([]TypedTableValue, len(table.columns))
		for index, column := range aggregate.groupBy {
			fullValues[column] = group.values[index]
		}
		hash := typedTableAggregateGroupHash(fullValues, aggregate.groupBy)
		bucket := groups[hash]
		if typedTableAggregateGroupValuesEqual(bucket.group.values, fullValues, aggregate.groupBy) && len(bucket.group.values) > 0 {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		duplicate := false
		for _, existing := range bucket.collisions {
			if typedTableAggregateGroupValuesEqual(existing.values, fullValues, aggregate.groupBy) {
				duplicate = true
				break
			}
		}
		if duplicate {
			return nil, 0, ErrTypedTableArrangementCheckpointInvalid
		}
		if len(bucket.group.values) == 0 {
			bucket.group = group
		} else {
			bucket.collisions = append(bucket.collisions, group)
		}
		groups[hash] = bucket
		groupCount++
	}
	return groups, groupCount, nil
}

func restoreTypedTableAggregateArrangementCheckpointStateLocked(aggregate *TypedTableAggregate, checkpoint TypedTableAggregateArrangementCheckpoint) error {
	groups, groupCount, err := restoreTypedTableAggregateArrangementCheckpointState(aggregate, checkpoint)
	if err != nil {
		return err
	}
	aggregate.groups = groups
	aggregate.groupCount = groupCount
	aggregate.checkpoint = checkpoint.Checkpoint
	return nil
}

func typedTableArrangementValueMatchesColumn(value TypedTableValue, kind TypedTableKind) bool {
	return !value.Valid || value.Kind == kind
}

func restoreTypedTableArrangementValueCounts(checkpoints []TypedTableArrangementValueCountCheckpoint, kind TypedTableKind) (map[TypedTableValue]int64, error) {
	values := make(map[TypedTableValue]int64, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if checkpoint.Count <= 0 || !checkpoint.Value.Valid || !typedTableArrangementValueMatchesColumn(checkpoint.Value, kind) {
			return nil, ErrTypedTableArrangementCheckpointInvalid
		}
		if _, exists := values[checkpoint.Value]; exists {
			return nil, ErrTypedTableArrangementCheckpointInvalid
		}
		values[checkpoint.Value] = checkpoint.Count
	}
	return values, nil
}

func restoreTypedTableArrangementDistinctCounts(checkpoints []TypedTableArrangementValueCountCheckpoint, kind TypedTableKind) (map[typedTableDistinctValue]int64, error) {
	values := make(map[typedTableDistinctValue]int64, len(checkpoints))
	for _, checkpoint := range checkpoints {
		if checkpoint.Count <= 0 || !checkpoint.Value.Valid || !typedTableArrangementValueMatchesColumn(checkpoint.Value, kind) {
			return nil, ErrTypedTableArrangementCheckpointInvalid
		}
		value, valid := typedTableAggregateDistinctValue(checkpoint.Value)
		if !valid {
			return nil, ErrTypedTableArrangementCheckpointInvalid
		}
		if _, exists := values[value]; exists {
			return nil, ErrTypedTableArrangementCheckpointInvalid
		}
		values[value] = checkpoint.Count
	}
	return values, nil
}

func typedTableJoinArrangementCheckpointRows(rows map[string][]TypedTableValue) []TypedTableJoinCheckpointRow {
	checkpoints := make([]TypedTableJoinCheckpointRow, 0, len(rows))
	for key, values := range rows {
		checkpoints = append(checkpoints, TypedTableJoinCheckpointRow{Key: key, Values: cloneTypedTableValues(values)})
	}
	sort.Slice(checkpoints, func(left, right int) bool {
		return checkpoints[left].Key < checkpoints[right].Key
	})
	return checkpoints
}

func typedTableJoinCheckpointRowsMap(rows []TypedTableJoinCheckpointRow, table *TypedTable) (map[string][]TypedTableValue, error) {
	if len(rows) > MaxTypedTableArrangementCheckpointRows {
		return nil, ErrTypedTableArrangementCheckpointInvalid
	}
	values := make(map[string][]TypedTableValue, len(rows))
	for _, row := range rows {
		if row.Key == "" || len(row.Values) != len(table.columns) {
			return nil, ErrTypedTableArrangementCheckpointInvalid
		}
		if _, exists := values[row.Key]; exists {
			return nil, ErrTypedTableArrangementCheckpointInvalid
		}
		if err := table.validateValues(row.Values); err != nil {
			return nil, ErrTypedTableArrangementCheckpointInvalid
		}
		values[row.Key] = cloneTypedTableValues(row.Values)
	}
	return values, nil
}

func captureTypedTableJoinArrangementCheckpoint(join *TypedTableJoin) (TypedTableJoinArrangementCheckpoint, error) {
	if join == nil || join.left == nil || join.right == nil {
		return TypedTableJoinArrangementCheckpoint{}, ErrTypedTableArrangementCheckpointInvalid
	}
	join.left.mu.RLock()
	defer join.left.mu.RUnlock()
	join.right.mu.RLock()
	defer join.right.mu.RUnlock()
	if join.leftCheckpoint != join.left.sequence || join.rightCheckpoint != join.right.sequence {
		return TypedTableJoinArrangementCheckpoint{}, ErrTypedTableArrangementCheckpointInvalid
	}
	return TypedTableJoinArrangementCheckpoint{
		Version:             TypedTableArrangementCheckpointVersion,
		LeftTableName:       join.left.schema.Name,
		RightTableName:      join.right.schema.Name,
		Definition:          join.definition,
		LeftSourceSequence:  join.left.sequence,
		RightSourceSequence: join.right.sequence,
		LeftCheckpoint:      join.leftCheckpoint,
		RightCheckpoint:     join.rightCheckpoint,
		LeftRows:            typedTableJoinArrangementCheckpointRows(join.leftRows),
		RightRows:           typedTableJoinArrangementCheckpointRows(join.rightRows),
	}, nil
}

func (arrangement *TypedTableJoinArrangement) CaptureCheckpoint() (TypedTableJoinArrangementCheckpoint, error) {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return TypedTableJoinArrangementCheckpoint{}, err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.join.mu.RLock()
	defer entry.join.mu.RUnlock()
	return captureTypedTableJoinArrangementCheckpoint(entry.join)
}

func (arrangement *TypedTableJoinArrangement) RestoreCheckpoint(checkpoint TypedTableJoinArrangementCheckpoint) error {
	entry, err := arrangement.activeEntry()
	if err != nil {
		return err
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.join.mu.Lock()
	defer entry.join.mu.Unlock()
	return restoreTypedTableJoinArrangementCheckpointLocked(entry.join, checkpoint)
}

func (arrangements *TypedTableJoinArrangements) CaptureCheckpoints() ([]TypedTableJoinArrangementCheckpoint, error) {
	if arrangements == nil {
		return nil, fmt.Errorf("typed table join arrangements are nil")
	}
	arrangements.mu.Lock()
	defer arrangements.mu.Unlock()
	keys := make([]string, 0, len(arrangements.entries))
	for key := range arrangements.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	checkpoints := make([]TypedTableJoinArrangementCheckpoint, 0, len(keys))
	for _, key := range keys {
		entry := arrangements.entries[key]
		if entry == nil || entry.join == nil {
			continue
		}
		entry.mu.Lock()
		entry.join.mu.RLock()
		checkpoint, err := captureTypedTableJoinArrangementCheckpoint(entry.join)
		entry.join.mu.RUnlock()
		entry.mu.Unlock()
		if err != nil {
			return nil, err
		}
		checkpoints = append(checkpoints, checkpoint)
	}
	return checkpoints, nil
}

func (arrangements *TypedTableJoinArrangements) RestoreCheckpoints(checkpoints []TypedTableJoinArrangementCheckpoint) ([]*TypedTableJoinArrangement, error) {
	if arrangements == nil {
		return nil, fmt.Errorf("typed table join arrangements are nil")
	}
	seen := make(map[string]struct{}, len(checkpoints))
	for _, checkpoint := range checkpoints {
		key := checkpoint.Definition.LeftField + "\x00" + checkpoint.Definition.RightField
		if _, exists := seen[key]; exists {
			return nil, ErrTypedTableArrangementCheckpointDuplicate
		}
		seen[key] = struct{}{}
	}
	leases := make([]*TypedTableJoinArrangement, 0, len(checkpoints))
	releaseAll := func() {
		for _, lease := range leases {
			lease.Release()
		}
	}
	for _, checkpoint := range checkpoints {
		key := checkpoint.Definition.LeftField + "\x00" + checkpoint.Definition.RightField
		arrangements.mu.Lock()
		_, exists := arrangements.entries[key]
		arrangements.mu.Unlock()
		if exists {
			releaseAll()
			return nil, ErrTypedTableArrangementCheckpointAlreadyExists
		}
		lease, err := arrangements.Acquire(checkpoint.Definition)
		if err != nil {
			releaseAll()
			return nil, err
		}
		if err := lease.RestoreCheckpoint(checkpoint); err != nil {
			lease.Release()
			releaseAll()
			return nil, err
		}
		leases = append(leases, lease)
	}
	return leases, nil
}

func restoreTypedTableJoinArrangementCheckpointLocked(join *TypedTableJoin, checkpoint TypedTableJoinArrangementCheckpoint) error {
	if join == nil || join.left == nil || join.right == nil || checkpoint.Version != TypedTableArrangementCheckpointVersion {
		return ErrTypedTableArrangementCheckpointInvalid
	}
	join.left.mu.RLock()
	defer join.left.mu.RUnlock()
	join.right.mu.RLock()
	defer join.right.mu.RUnlock()
	if checkpoint.LeftTableName != join.left.schema.Name || checkpoint.RightTableName != join.right.schema.Name || checkpoint.Definition != join.definition ||
		checkpoint.LeftSourceSequence != join.left.sequence || checkpoint.RightSourceSequence != join.right.sequence ||
		checkpoint.LeftCheckpoint != checkpoint.LeftSourceSequence || checkpoint.RightCheckpoint != checkpoint.RightSourceSequence {
		return ErrTypedTableArrangementSourceVersionMismatch
	}
	leftRows, err := typedTableJoinCheckpointRowsMap(checkpoint.LeftRows, join.left)
	if err != nil {
		return err
	}
	rightRows, err := typedTableJoinCheckpointRowsMap(checkpoint.RightRows, join.right)
	if err != nil {
		return err
	}
	join.leftRows = leftRows
	join.rightRows = rightRows
	join.leftIndex = make(map[string]map[string]struct{}, len(leftRows))
	join.rightIndex = make(map[string]map[string]struct{}, len(rightRows))
	join.pairs = make(map[typedTableJoinPair]struct{})
	for key, values := range leftRows {
		join.addLeft(key, values)
	}
	for key, values := range rightRows {
		join.addRight(key, values)
	}
	join.leftCheckpoint = checkpoint.LeftCheckpoint
	join.rightCheckpoint = checkpoint.RightCheckpoint
	return nil
}
