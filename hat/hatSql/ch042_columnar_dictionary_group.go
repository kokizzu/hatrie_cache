package hatSql

import (
	"fmt"
	"sync"
)

// sqlColumnarDictionaryGroupField admits the code-keyed grouping path only
// when dictionary codes are validated and binary collation makes code equality
// equivalent to value equality.
func sqlColumnarDictionaryGroupField(batch ColumnarBatch, field string, collation SQLCollation) (DictionaryColumn, bool) {
	if collation.normalized() != SQLCollationBinary {
		return DictionaryColumn{}, false
	}
	dictionary, ok := batch.Dictionaries[field]
	if !ok || !dictionary.codesTrusted || !dictionary.ValuesValid() || dictionary.RowCount() != batch.Rows {
		return DictionaryColumn{}, false
	}
	return dictionary, true
}

func executeSQLColumnarDictionaryGroupStates(q *sqlQuery, batch ColumnarBatch, projections []sqlOrderedGroupProjection, groupField string, dictionary DictionaryColumn, match func(int) (bool, error), control *sqlExecutionControl) ([]sqlHashGroupAggregateState, int, error) {
	states := make([]sqlHashGroupAggregateState, 0)
	indexes := make(map[uint32]int)
	selectionCapacity := sqlColumnarVectorGroupBlockRows
	if batch.Rows < selectionCapacity {
		selectionCapacity = batch.Rows
	}
	selection := make([]int, 0, selectionCapacity)
	matched := 0
	for blockStart := 0; blockStart < batch.Rows; blockStart += sqlColumnarVectorGroupBlockRows {
		blockEnd := blockStart + sqlColumnarVectorGroupBlockRows
		if blockEnd > batch.Rows {
			blockEnd = batch.Rows
		}
		selection = selection[:0]
		for rowIndex := blockStart; rowIndex < blockEnd; rowIndex++ {
			if control != nil {
				if err := control.check(); err != nil {
					return nil, matched, err
				}
			}
			matches, err := match(rowIndex)
			if err != nil {
				return nil, matched, err
			}
			if matches {
				selection = append(selection, rowIndex)
			}
		}
		for _, rowIndex := range selection {
			code, ok := dictionary.CodeAt(rowIndex)
			if !ok {
				return nil, matched, fmt.Errorf("SQL columnar source %q returned an invalid dictionary code for field %q", q.from.key, groupField)
			}
			stateIndex, exists := indexes[code]
			if !exists {
				groupValue, valid := dictionary.ValueAt(code)
				if !valid {
					return nil, matched, fmt.Errorf("SQL columnar source %q returned an invalid dictionary value for field %q", q.from.key, groupField)
				}
				stateIndex = len(states)
				indexes[code] = stateIndex
				state := sqlHashGroupAggregateState{value: groupValue, aggregates: make([]sqlOrderedAggregate, len(projections))}
				for projectionIndex, projection := range projections {
					if projection.aggregate != nil {
						state.aggregates[projectionIndex] = *projection.aggregate
					}
				}
				states = append(states, state)
			}
			state := &states[stateIndex]
			state.rows++
			if control != nil && control.options.MaxGroupRowsPerKey > 0 && state.rows > control.options.MaxGroupRowsPerKey {
				return nil, matched, fmt.Errorf("SQL group skew limit exceeded: group has %d rows, maximum %d", state.rows, control.options.MaxGroupRowsPerKey)
			}
			for projectionIndex, projection := range projections {
				if projection.aggregate == nil {
					continue
				}
				aggregate := &state.aggregates[projectionIndex]
				value := interface{}(nil)
				if aggregate.field.kind != "" {
					value, _ = batch.Value(aggregate.field.name, rowIndex)
				}
				if err := aggregate.addValue(value); err != nil {
					return nil, matched, err
				}
			}
			matched++
		}
	}
	return states, matched, nil
}

type sqlColumnarDictionaryTwoLevelWorkerState struct {
	states  []sqlHashGroupAggregateState
	codes   []uint32
	indexes map[uint32]int
	matched int
	err     error
}

func executeSQLColumnarDictionaryTwoLevelGroupStates(q *sqlQuery, batch ColumnarBatch, projections []sqlOrderedGroupProjection, groupField string, dictionary DictionaryColumn, match func(int) (bool, error), control *sqlExecutionControl, workers int) ([]sqlHashGroupAggregateState, int, error) {
	local := make([]sqlColumnarDictionaryTwoLevelWorkerState, workers)
	var wait sync.WaitGroup
	wait.Add(workers)
	for workerIndex := 0; workerIndex < workers; workerIndex++ {
		start := batch.Rows * workerIndex / workers
		end := batch.Rows * (workerIndex + 1) / workers
		go func(index, start, end int) {
			defer wait.Done()
			worker := &local[index]
			worker.indexes = make(map[uint32]int)
			for rowIndex := start; rowIndex < end; rowIndex++ {
				if control != nil {
					if err := control.check(); err != nil {
						worker.err = err
						return
					}
				}
				matches, err := match(rowIndex)
				if err != nil {
					worker.err = err
					return
				}
				if !matches {
					continue
				}
				code, ok := dictionary.CodeAt(rowIndex)
				if !ok {
					worker.err = fmt.Errorf("SQL columnar source %q returned an invalid dictionary code for field %q", q.from.key, groupField)
					return
				}
				stateIndex, exists := worker.indexes[code]
				if !exists {
					groupValue, valid := dictionary.ValueAt(code)
					if !valid {
						worker.err = fmt.Errorf("SQL columnar source %q returned an invalid dictionary value for field %q", q.from.key, groupField)
						return
					}
					stateIndex = len(worker.states)
					worker.indexes[code] = stateIndex
					state := sqlHashGroupAggregateState{value: groupValue, aggregates: make([]sqlOrderedAggregate, len(projections))}
					for projectionIndex, projection := range projections {
						if projection.aggregate != nil {
							state.aggregates[projectionIndex] = *projection.aggregate
						}
					}
					worker.states = append(worker.states, state)
					worker.codes = append(worker.codes, code)
				}
				state := &worker.states[stateIndex]
				state.rows++
				for projectionIndex, projection := range projections {
					if projection.aggregate == nil {
						continue
					}
					aggregate := &state.aggregates[projectionIndex]
					value := interface{}(nil)
					if aggregate.field.kind != "" {
						value, _ = batch.Value(aggregate.field.name, rowIndex)
					}
					if err := aggregate.addValue(value); err != nil {
						worker.err = err
						return
					}
				}
				worker.matched++
			}
		}(workerIndex, start, end)
	}
	wait.Wait()

	states := make([]sqlHashGroupAggregateState, 0)
	indexes := make(map[uint32]int)
	matched := 0
	for workerIndex := range local {
		worker := &local[workerIndex]
		if worker.err != nil {
			return nil, matched, worker.err
		}
		matched += worker.matched
		for stateIndex, state := range worker.states {
			code := worker.codes[stateIndex]
			globalIndex, exists := indexes[code]
			if !exists {
				indexes[code] = len(states)
				states = append(states, state)
				continue
			}
			merged := &states[globalIndex]
			merged.rows += state.rows
			for projectionIndex := range merged.aggregates {
				mergeSQLOrderedAggregate(&merged.aggregates[projectionIndex], state.aggregates[projectionIndex])
			}
		}
	}
	if control != nil && control.options.MaxGroupRowsPerKey > 0 {
		for _, state := range states {
			if state.rows > control.options.MaxGroupRowsPerKey {
				return nil, matched, fmt.Errorf("SQL group skew limit exceeded: group has %d rows, maximum %d", state.rows, control.options.MaxGroupRowsPerKey)
			}
		}
	}
	return states, matched, nil
}
