package hatSql

import (
	"errors"
	"math"
)

var (
	// ErrTypedTableAggregatePartialNil reports a nil partial or target.
	ErrTypedTableAggregatePartialNil = errors.New("typed table aggregate partial is nil")
	// ErrTypedTableAggregatePartialSelf reports merging an aggregate into itself.
	ErrTypedTableAggregatePartialSelf = errors.New("typed table aggregate partial is the target")
	// ErrTypedTableAggregatePartialDefinition reports incompatible aggregate
	// definitions or table schemas.
	ErrTypedTableAggregatePartialDefinition = errors.New("typed table aggregate partial definition differs")
	// ErrTypedTableAggregatePartialState reports invalid partial state.
	ErrTypedTableAggregatePartialState = errors.New("typed table aggregate partial state is invalid")
	// ErrTypedTableAggregatePartialCountOverflow reports an integer merge overflow.
	ErrTypedTableAggregatePartialCountOverflow = errors.New("typed table aggregate partial count overflowed")
)

// MergePartial merges one completed partition-local aggregate into aggregate.
// It is equivalent to MergePartials(partial). The partial must use the same
// physical table schema and aggregate definition; it is not mutated.
func (aggregate *TypedTableAggregate) MergePartial(partial *TypedTableAggregate) error {
	return aggregate.MergePartials(partial)
}

// MergePartials merges completed partition-local aggregates into aggregate.
// The operation is atomic with respect to validation and integer overflow: a
// failure leaves aggregate unchanged. Callers must not mutate the partials
// concurrently with this operation.
func (aggregate *TypedTableAggregate) MergePartials(partials ...*TypedTableAggregate) error {
	if aggregate == nil {
		return ErrTypedTableAggregatePartialNil
	}
	for _, partial := range partials {
		if partial == nil {
			return ErrTypedTableAggregatePartialNil
		}
		if partial == aggregate {
			return ErrTypedTableAggregatePartialSelf
		}
		if !typedTableAggregatePartialsCompatible(aggregate, partial) {
			return ErrTypedTableAggregatePartialDefinition
		}
	}

	working := *aggregate
	working.groups = cloneTypedTableAggregateGroups(aggregate)
	working.groupDictionaries = cloneTypedTableAggregateDictionaries(aggregate.groupDictionaries)
	groupCount := aggregate.groupCount
	for _, partial := range partials {
		for _, bucket := range partial.groups {
			if err := mergeTypedTableAggregatePartialGroup(&working, working.groups, &groupCount, partial, bucket.group); err != nil {
				return err
			}
			for _, collision := range bucket.collisions {
				if err := mergeTypedTableAggregatePartialGroup(&working, working.groups, &groupCount, partial, collision); err != nil {
					return err
				}
			}
		}
	}
	aggregate.groups = working.groups
	aggregate.groupDictionaries = working.groupDictionaries
	aggregate.groupCount = groupCount
	aggregate.groupKeysReady = false
	aggregate.compactGroupOrder = nil
	return nil
}

func typedTableAggregatePartialsCompatible(target, partial *TypedTableAggregate) bool {
	if target.table == nil || partial.table == nil || len(target.groupBy) != len(partial.groupBy) {
		return false
	}
	if target.sumField != partial.sumField || target.minField != partial.minField || target.maxField != partial.maxField || target.distinctField != partial.distinctField {
		return false
	}
	if target.dictionaryEncodeGroups != partial.dictionaryEncodeGroups {
		return false
	}
	for index, column := range target.groupBy {
		if column != partial.groupBy[index] {
			return false
		}
	}
	target.table.mu.RLock()
	partial.table.mu.RLock()
	defer target.table.mu.RUnlock()
	defer partial.table.mu.RUnlock()
	if len(target.table.columns) != len(partial.table.columns) || len(target.table.schema.Columns) != len(partial.table.schema.Columns) {
		return false
	}
	for index := range target.table.columns {
		if target.table.schema.Columns[index].Name != partial.table.schema.Columns[index].Name || target.table.columns[index].kind != partial.table.columns[index].kind {
			return false
		}
	}
	return true
}

func cloneTypedTableAggregateGroups(aggregate *TypedTableAggregate) map[uint64]typedTableAggregateGroupBucket {
	groups := make(map[uint64]typedTableAggregateGroupBucket, len(aggregate.groups))
	for hash, bucket := range aggregate.groups {
		cloned := typedTableAggregateGroupBucket{group: cloneTypedTableAggregateGroup(bucket.group, aggregate.minField >= 0 && aggregate.maxField == aggregate.minField)}
		if len(bucket.collisions) > 0 {
			cloned.collisions = make([]typedTableAggregateGroup, len(bucket.collisions))
			for index, group := range bucket.collisions {
				cloned.collisions[index] = cloneTypedTableAggregateGroup(group, aggregate.minField >= 0 && aggregate.maxField == aggregate.minField)
			}
		}
		groups[hash] = cloned
	}
	return groups
}

func cloneTypedTableAggregateGroup(group typedTableAggregateGroup, sharedExtrema bool) typedTableAggregateGroup {
	group.values = cloneTypedTableValues(group.values)
	group.codes = append([]uint32(nil), group.codes...)
	group.kinds = append([]TypedTableKind(nil), group.kinds...)
	group.minValues = cloneTypedTableAggregateValueCounts(group.minValues)
	if sharedExtrema {
		group.maxValues = group.minValues
	} else {
		group.maxValues = cloneTypedTableAggregateValueCounts(group.maxValues)
	}
	group.distinctValues = cloneTypedTableAggregateDistinctCounts(group.distinctValues)
	return group
}

func cloneTypedTableAggregateValueCounts(values map[TypedTableValue]int64) map[TypedTableValue]int64 {
	if values == nil {
		return nil
	}
	cloned := make(map[TypedTableValue]int64, len(values))
	for value, count := range values {
		cloned[value] = count
	}
	return cloned
}

func cloneTypedTableAggregateDistinctCounts(values map[typedTableDistinctValue]int64) map[typedTableDistinctValue]int64 {
	if values == nil {
		return nil
	}
	cloned := make(map[typedTableDistinctValue]int64, len(values))
	for value, count := range values {
		cloned[value] = count
	}
	return cloned
}

func mergeTypedTableAggregatePartialGroup(aggregate *TypedTableAggregate, groups map[uint64]typedTableAggregateGroupBucket, groupCount *int, partialAggregate *TypedTableAggregate, partial typedTableAggregateGroup) error {
	if partial.count <= 0 || !partialAggregate.groupShapeValid(partial) {
		return ErrTypedTableAggregatePartialState
	}
	hash := typedTableAggregateStoredGroupHash(partialAggregate, partial)
	bucket := groups[hash]
	groupIndex := -1
	var target typedTableAggregateGroup
	if bucket.group.count > 0 && typedTableAggregateStoredGroupsEqual(aggregate, bucket.group, partialAggregate, partial) {
		groupIndex = 0
		target = bucket.group
	} else {
		for index, collision := range bucket.collisions {
			if typedTableAggregateStoredGroupsEqual(aggregate, collision, partialAggregate, partial) {
				groupIndex = index + 1
				target = collision
				break
			}
		}
	}
	if groupIndex < 0 {
		cloned, err := cloneTypedTableAggregateGroupForAggregate(aggregate, partialAggregate, partial, aggregate.minField >= 0 && aggregate.maxField == aggregate.minField)
		if err != nil {
			return err
		}
		if bucket.group.count == 0 && len(bucket.collisions) == 0 {
			bucket.group = cloned
		} else {
			bucket.collisions = append(bucket.collisions, cloned)
		}
		groups[hash] = bucket
		*groupCount = *groupCount + 1
		return nil
	}
	if err := mergeTypedTableAggregateGroups(&target, partial, aggregate.minField >= 0 && aggregate.maxField == aggregate.minField); err != nil {
		return err
	}
	if groupIndex == 0 {
		bucket.group = target
	} else {
		bucket.collisions[groupIndex-1] = target
	}
	groups[hash] = bucket
	return nil
}

func typedTableAggregateGroupHashFromValues(values []TypedTableValue) uint64 {
	return typedTableAggregateGroupedValuesHash(values)
}

func typedTableAggregateGroupSlicesEqual(left, right []TypedTableValue) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if !typedTableAggregateValueEqual(left[index], right[index]) {
			return false
		}
	}
	return true
}

func mergeTypedTableAggregateGroups(target *typedTableAggregateGroup, partial typedTableAggregateGroup, sharedExtrema bool) error {
	if partial.count <= 0 {
		return ErrTypedTableAggregatePartialState
	}
	var err error
	target.count, err = addTypedTableAggregatePartialCounts(target.count, partial.count)
	if err != nil {
		return err
	}
	target.sum += partial.sum
	target.minValues, err = mergeTypedTableAggregateValueCounts(target.minValues, partial.minValues)
	if err != nil {
		return err
	}
	if sharedExtrema {
		target.maxValues = target.minValues
	} else {
		target.maxValues, err = mergeTypedTableAggregateValueCounts(target.maxValues, partial.maxValues)
		if err != nil {
			return err
		}
	}
	target.distinctValues, err = mergeTypedTableAggregateDistinctCounts(target.distinctValues, partial.distinctValues)
	if err != nil {
		return err
	}
	if target.minValues != nil {
		target.minimum, target.hasMin = typedTableAggregateExtreme(target.minValues, true)
	}
	if target.maxValues != nil {
		target.maximum, target.hasMax = typedTableAggregateExtreme(target.maxValues, false)
	}
	return nil
}

func addTypedTableAggregatePartialCounts(left, right int64) (int64, error) {
	if left < 0 || right < 0 {
		return 0, ErrTypedTableAggregatePartialState
	}
	if right > math.MaxInt64-left {
		return 0, ErrTypedTableAggregatePartialCountOverflow
	}
	return left + right, nil
}

func mergeTypedTableAggregateValueCounts(target, partial map[TypedTableValue]int64) (map[TypedTableValue]int64, error) {
	if len(partial) == 0 {
		return target, nil
	}
	if target == nil {
		target = make(map[TypedTableValue]int64, len(partial))
	}
	for value, count := range partial {
		if count <= 0 {
			return nil, ErrTypedTableAggregatePartialState
		}
		current, err := addTypedTableAggregatePartialCounts(target[value], count)
		if err != nil {
			return nil, err
		}
		target[value] = current
	}
	return target, nil
}

func mergeTypedTableAggregateDistinctCounts(target, partial map[typedTableDistinctValue]int64) (map[typedTableDistinctValue]int64, error) {
	if len(partial) == 0 {
		return target, nil
	}
	if target == nil {
		target = make(map[typedTableDistinctValue]int64, len(partial))
	}
	for value, count := range partial {
		if count <= 0 {
			return nil, ErrTypedTableAggregatePartialState
		}
		current, err := addTypedTableAggregatePartialCounts(target[value], count)
		if err != nil {
			return nil, err
		}
		target[value] = current
	}
	return target, nil
}
