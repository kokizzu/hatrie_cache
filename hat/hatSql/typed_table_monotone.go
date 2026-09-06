package hatSql

import (
	"errors"
	"fmt"
	"math"
)

var (
	ErrTypedTableAggregateMonotoneBefore        = errors.New("typed table aggregate monotone input contains before values")
	ErrTypedTableAggregateMonotoneAfterRequired = errors.New("typed table aggregate monotone input requires after values")
	ErrTypedTableAggregateMonotoneCountOverflow = errors.New("typed table aggregate monotone count overflowed")
)

// ApplyMonotone applies an insert-only, contiguous changefeed batch to the
// aggregate. Replayed sequences are ignored just like Apply. The specialized
// path rejects deletes and updates before touching aggregate state, allowing
// callers that know their source is append-only to avoid the general before/
// after branching and delete bookkeeping.
func (aggregate *TypedTableAggregate) ApplyMonotone(changes []TypedTableChange) error {
	if aggregate == nil {
		return fmt.Errorf("typed table aggregate is nil")
	}
	for _, change := range changes {
		if change.Sequence <= aggregate.checkpoint {
			continue
		}
		if change.Sequence != aggregate.checkpoint+1 {
			return fmt.Errorf("typed table aggregate change sequence %d follows %d", change.Sequence, aggregate.checkpoint)
		}
		if len(change.Before) > 0 {
			return ErrTypedTableAggregateMonotoneBefore
		}
		if len(change.After) == 0 {
			return ErrTypedTableAggregateMonotoneAfterRequired
		}
		var err error
		if aggregate.minField < 0 && aggregate.maxField < 0 && aggregate.distinctField < 0 {
			err = aggregate.applyMonotoneFastRow(change.After)
		} else {
			err = aggregate.applyRow(change.After, 1)
		}
		if err != nil {
			return err
		}
		aggregate.checkpoint = change.Sequence
	}
	return nil
}

func (aggregate *TypedTableAggregate) applyMonotoneFastRow(values []TypedTableValue) error {
	if len(values) != len(aggregate.table.columns) {
		return fmt.Errorf("typed table aggregate row has %d values, want %d", len(values), len(aggregate.table.columns))
	}
	hash := typedTableAggregateGroupHash(values, aggregate.groupBy)
	bucket, bucketExists := aggregate.groups[hash]
	groupIndex := -1
	var group typedTableAggregateGroup
	if bucketExists {
		if typedTableAggregateGroupValuesEqual(bucket.group.values, values, aggregate.groupBy) {
			group = bucket.group
			groupIndex = 0
		} else {
			for index := range bucket.collisions {
				if typedTableAggregateGroupValuesEqual(bucket.collisions[index].values, values, aggregate.groupBy) {
					group = bucket.collisions[index]
					groupIndex = index + 1
					break
				}
			}
		}
	}
	if groupIndex < 0 {
		group.values = make([]TypedTableValue, len(aggregate.groupBy))
		for index, column := range aggregate.groupBy {
			group.values[index] = values[column]
		}
		group.key = typedTableAggregateLegacyGroupKey(values, aggregate.groupBy)
	}
	if group.count == math.MaxInt64 {
		return ErrTypedTableAggregateMonotoneCountOverflow
	}
	group.count++
	if aggregate.sumField >= 0 && values[aggregate.sumField].Valid {
		switch values[aggregate.sumField].Kind {
		case TypedTableInt64:
			group.sum += float64(values[aggregate.sumField].Int64)
		case TypedTableFloat64:
			group.sum += values[aggregate.sumField].Float64
		}
	}
	if groupIndex < 0 {
		aggregate.groupCount++
		if !bucketExists {
			aggregate.groups[hash] = typedTableAggregateGroupBucket{group: group}
		} else {
			bucket.collisions = append(bucket.collisions, group)
			aggregate.groups[hash] = bucket
		}
	} else if groupIndex == 0 {
		bucket.group = group
		aggregate.groups[hash] = bucket
	} else {
		bucket.collisions[groupIndex-1] = group
		aggregate.groups[hash] = bucket
	}
	return nil
}
