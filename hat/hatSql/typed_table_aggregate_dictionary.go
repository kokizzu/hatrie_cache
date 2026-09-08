package hatSql

import (
	"sort"
	"strings"
)

// typedTableAggregateStringDictionary interns only the strings referenced by
// live aggregate groups. Codes are local to one aggregate and are remapped
// when partial aggregates are merged.
type typedTableAggregateStringDictionary struct {
	positions map[string]uint32
	values    []string
	counts    []uint32
	free      []uint32
}

func (dictionary *typedTableAggregateStringDictionary) lookup(value string) (uint32, bool) {
	if dictionary == nil || dictionary.positions == nil {
		return 0, false
	}
	code, found := dictionary.positions[value]
	return code, found
}

func (dictionary *typedTableAggregateStringDictionary) retain(value string) uint32 {
	if dictionary.positions == nil {
		dictionary.positions = make(map[string]uint32)
	}
	if code, found := dictionary.positions[value]; found {
		dictionary.counts[code-1]++
		return code
	}
	var code uint32
	if length := len(dictionary.free); length > 0 {
		code = dictionary.free[length-1]
		dictionary.free = dictionary.free[:length-1]
		index := code - 1
		dictionary.values[index] = value
		dictionary.counts[index] = 1
	} else {
		code = uint32(len(dictionary.values) + 1)
		dictionary.values = append(dictionary.values, value)
		dictionary.counts = append(dictionary.counts, 1)
	}
	dictionary.positions[value] = code
	return code
}

func (dictionary *typedTableAggregateStringDictionary) release(code uint32) {
	if dictionary == nil || code == 0 || int(code) > len(dictionary.values) {
		return
	}
	index := code - 1
	if dictionary.counts[index] == 0 {
		return
	}
	dictionary.counts[index]--
	if dictionary.counts[index] != 0 {
		return
	}
	delete(dictionary.positions, dictionary.values[index])
	dictionary.values[index] = ""
	if int(index) == len(dictionary.values)-1 {
		for len(dictionary.values) > 0 && dictionary.counts[len(dictionary.counts)-1] == 0 {
			dictionary.values = dictionary.values[:len(dictionary.values)-1]
			dictionary.counts = dictionary.counts[:len(dictionary.counts)-1]
		}
		free := dictionary.free[:0]
		for _, freeCode := range dictionary.free {
			if int(freeCode) <= len(dictionary.values) {
				free = append(free, freeCode)
			}
		}
		dictionary.free = free
		return
	}
	dictionary.free = append(dictionary.free, code)
}

func (dictionary *typedTableAggregateStringDictionary) value(code uint32) (string, bool) {
	if dictionary == nil || code == 0 || int(code) > len(dictionary.values) || dictionary.counts[code-1] == 0 {
		return "", false
	}
	return dictionary.values[code-1], true
}

func cloneTypedTableAggregateStringDictionary(source typedTableAggregateStringDictionary) typedTableAggregateStringDictionary {
	cloned := typedTableAggregateStringDictionary{
		values: append([]string(nil), source.values...),
		counts: append([]uint32(nil), source.counts...),
		free:   append([]uint32(nil), source.free...),
	}
	if source.positions != nil {
		cloned.positions = make(map[string]uint32, len(source.positions))
		for value, code := range source.positions {
			cloned.positions[value] = code
		}
	}
	return cloned
}

func cloneTypedTableAggregateDictionaries(source []typedTableAggregateStringDictionary) []typedTableAggregateStringDictionary {
	if source == nil {
		return nil
	}
	cloned := make([]typedTableAggregateStringDictionary, len(source))
	for index, dictionary := range source {
		cloned[index] = cloneTypedTableAggregateStringDictionary(dictionary)
	}
	return cloned
}

func (aggregate *TypedTableAggregate) newGroup(values []TypedTableValue) typedTableAggregateGroup {
	group := typedTableAggregateGroup{
		values: make([]TypedTableValue, aggregate.groupValueCount),
	}
	if len(aggregate.groupDictionaries) > 0 {
		group.codes = make([]uint32, len(aggregate.groupDictionaries))
	}
	for index, column := range aggregate.groupBy {
		codeIndex := aggregate.groupCodeIndexes[index]
		if codeIndex >= 0 {
			value := values[column]
			if value.Kind != TypedTableString {
				if group.kinds == nil {
					group.kinds = make([]TypedTableKind, len(aggregate.groupDictionaries))
					for kindIndex := range group.kinds {
						group.kinds[kindIndex] = TypedTableString
					}
				}
				group.kinds[codeIndex] = value.Kind
			}
			if value.Valid {
				group.codes[codeIndex] = aggregate.groupDictionaries[codeIndex].retain(value.String)
			}
			continue
		}
		group.values[aggregate.groupValueIndexes[index]] = values[column]
	}
	return group
}

func (aggregate *TypedTableAggregate) groupShapeValid(group typedTableAggregateGroup) bool {
	return len(group.values) == aggregate.groupValueCount && len(group.codes) == len(aggregate.groupDictionaries) && (len(group.kinds) == 0 || len(group.kinds) == len(aggregate.groupDictionaries))
}

func cloneTypedTableAggregateGroupForAggregate(target, source *TypedTableAggregate, group typedTableAggregateGroup, sharedExtrema bool) (typedTableAggregateGroup, error) {
	cloned := cloneTypedTableAggregateGroup(group, sharedExtrema)
	if !target.dictionaryEncodeGroups {
		return cloned, nil
	}
	for index, code := range group.codes {
		if code == 0 {
			continue
		}
		value, found := source.groupDictionaries[index].value(code)
		if !found {
			return typedTableAggregateGroup{}, ErrTypedTableAggregatePartialState
		}
		cloned.codes[index] = target.groupDictionaries[index].retain(value)
	}
	return cloned, nil
}

func (aggregate *TypedTableAggregate) releaseGroup(group typedTableAggregateGroup) {
	for dictionaryIndex, code := range group.codes {
		if dictionaryIndex < len(aggregate.groupDictionaries) {
			aggregate.groupDictionaries[dictionaryIndex].release(code)
		}
	}
}

func (aggregate *TypedTableAggregate) groupValue(group typedTableAggregateGroup, index int) TypedTableValue {
	if index < 0 || index >= len(aggregate.groupBy) {
		return TypedTableValue{}
	}
	codeIndex := aggregate.groupCodeIndexes[index]
	if codeIndex >= 0 {
		value := TypedTableValue{Kind: TypedTableString}
		if codeIndex < len(group.kinds) {
			value.Kind = group.kinds[codeIndex]
		}
		if codeIndex >= len(group.codes) {
			return value
		}
		stringValue, found := aggregate.groupDictionaries[codeIndex].value(group.codes[codeIndex])
		if found {
			value.String = stringValue
			value.Valid = true
		}
		return value
	}
	valueIndex := aggregate.groupValueIndexes[index]
	if valueIndex < 0 || valueIndex >= len(group.values) {
		return TypedTableValue{}
	}
	return group.values[valueIndex]
}

func (aggregate *TypedTableAggregate) storedGroupKey(group typedTableAggregateGroup) string {
	if len(aggregate.groupBy) == 0 {
		return "all"
	}
	var builder strings.Builder
	for index := range aggregate.groupBy {
		typedTableAggregateAppendKeyValue(&builder, aggregate.groupValue(group, index))
	}
	return builder.String()
}

func (aggregate *TypedTableAggregate) compactOrderedGroups() []typedTableAggregateGroup {
	if aggregate.groupKeysReady {
		groups := make([]typedTableAggregateGroup, 0, len(aggregate.compactGroupOrder))
		for _, reference := range aggregate.compactGroupOrder {
			bucket, found := aggregate.groups[reference.hash]
			if !found {
				continue
			}
			if reference.collision == 0 {
				if bucket.group.count > 0 {
					groups = append(groups, bucket.group)
				}
				continue
			}
			collisionIndex := reference.collision - 1
			if collisionIndex < len(bucket.collisions) && bucket.collisions[collisionIndex].count > 0 {
				groups = append(groups, bucket.collisions[collisionIndex])
			}
		}
		return groups
	}
	type sortableGroup struct {
		reference typedTableAggregateGroupReference
		key       string
	}
	sortable := make([]sortableGroup, 0, aggregate.groupCount)
	for hash, bucket := range aggregate.groups {
		if bucket.group.count > 0 {
			sortable = append(sortable, sortableGroup{
				reference: typedTableAggregateGroupReference{hash: hash},
				key:       aggregate.storedGroupKey(bucket.group),
			})
		}
		for index, collision := range bucket.collisions {
			if collision.count > 0 {
				sortable = append(sortable, sortableGroup{
					reference: typedTableAggregateGroupReference{hash: hash, collision: index + 1},
					key:       aggregate.storedGroupKey(collision),
				})
			}
		}
	}
	sort.Slice(sortable, func(left, right int) bool { return sortable[left].key < sortable[right].key })
	aggregate.compactGroupOrder = make([]typedTableAggregateGroupReference, 0, len(sortable))
	groups := make([]typedTableAggregateGroup, 0, len(sortable))
	for _, item := range sortable {
		aggregate.compactGroupOrder = append(aggregate.compactGroupOrder, item.reference)
		bucket, found := aggregate.groups[item.reference.hash]
		if !found {
			continue
		}
		if item.reference.collision == 0 {
			groups = append(groups, bucket.group)
			continue
		}
		collisionIndex := item.reference.collision - 1
		if collisionIndex < len(bucket.collisions) {
			groups = append(groups, bucket.collisions[collisionIndex])
		}
	}
	aggregate.groupKeysReady = true
	return groups
}

func (aggregate *TypedTableAggregate) groupValuesEqual(group typedTableAggregateGroup, values []TypedTableValue) bool {
	if len(values) != len(aggregate.table.columns) {
		return false
	}
	for index, column := range aggregate.groupBy {
		if aggregate.groupCodeIndexes[index] >= 0 {
			value := values[column]
			codeIndex := aggregate.groupCodeIndexes[index]
			if codeIndex >= len(group.codes) {
				return false
			}
			code := group.codes[codeIndex]
			groupKind := TypedTableString
			if codeIndex < len(group.kinds) {
				groupKind = group.kinds[codeIndex]
			}
			if groupKind != value.Kind {
				return false
			}
			if !value.Valid {
				if code != 0 {
					return false
				}
				continue
			}
			knownValue, found := aggregate.groupDictionaries[aggregate.groupCodeIndexes[index]].value(code)
			if !found || knownValue != value.String {
				return false
			}
			continue
		}
		if !typedTableAggregateValueEqual(group.values[aggregate.groupValueIndexes[index]], values[column]) {
			return false
		}
	}
	return true
}

func typedTableAggregateStoredGroupsEqual(leftAggregate *TypedTableAggregate, left typedTableAggregateGroup, rightAggregate *TypedTableAggregate, right typedTableAggregateGroup) bool {
	if len(leftAggregate.groupBy) != len(rightAggregate.groupBy) {
		return false
	}
	for index := range leftAggregate.groupBy {
		if !typedTableAggregateValueEqual(leftAggregate.groupValue(left, index), rightAggregate.groupValue(right, index)) {
			return false
		}
	}
	return true
}

func typedTableAggregateStoredGroupHash(aggregate *TypedTableAggregate, group typedTableAggregateGroup) uint64 {
	values := make([]TypedTableValue, len(aggregate.groupBy))
	for index := range values {
		values[index] = aggregate.groupValue(group, index)
	}
	return typedTableAggregateGroupedValuesHash(values)
}
