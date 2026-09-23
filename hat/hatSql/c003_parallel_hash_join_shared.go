package hatSql

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
)

const sqlParallelHashJoinSharedCheckInterval = 1024

type sqlParallelHashJoinSharedWorkerResult struct {
	rows []sqlExecRow
	err  error
}

// executeSQLParallelInnerHashJoinShared builds one immutable typed index and
// probes it from contiguous left-side chunks. Concatenating chunk results
// preserves the established left-row/right-row order without a result sort.
func executeSQLParallelInnerHashJoinShared(
	control *sqlExecutionControl,
	left []sqlExecRow,
	right []sqlExecRow,
	leftQualifier, leftField string,
	rightAlias, rightField string,
	requestedWorkers, maxRows int,
) ([]sqlExecRow, error) {
	if requestedWorkers <= 1 || len(left) == 0 || len(right) == 0 {
		return nil, nil
	}
	workerCount := requestedWorkers
	if workerCount > MaxSQLQueryThreads {
		workerCount = MaxSQLQueryThreads
	}
	if workerCount > len(left) {
		workerCount = len(left)
	}
	if workerCount <= 1 {
		return nil, nil
	}
	if err := control.addJoinWork(len(right)); err != nil {
		return nil, err
	}

	ctx := context.Background()
	if control != nil && control.ctx != nil {
		ctx = control.ctx
	}
	index := newSQLJoinHashIndex(len(right))
	for rightIndex, row := range right {
		if rightIndex%sqlParallelHashJoinSharedCheckInterval == 0 {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		index.Add(sqlField(row, rightAlias, rightField), rightIndex)
	}
	// Lookup updates the adaptive Bloom sampling counters. Disable that
	// mutable query-local state before allowing concurrent read-only probes.
	index.bloomState = sqlJoinRuntimeBloomDisabled

	results := make([]sqlParallelHashJoinSharedWorkerResult, workerCount)
	var produced atomic.Int64
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for worker := range workerCount {
		go func(worker int) {
			defer workers.Done()
			start := worker * len(left) / workerCount
			end := (worker + 1) * len(left) / workerCount
			rows := make([]sqlExecRow, 0)
			for leftIndex := start; leftIndex < end; leftIndex++ {
				if (leftIndex-start)%sqlParallelHashJoinSharedCheckInterval == 0 {
					if err := ctx.Err(); err != nil {
						results[worker].err = err
						return
					}
				}
				for _, rightIndex := range index.Lookup(sqlField(left[leftIndex], leftQualifier, leftField)) {
					count := produced.Add(1)
					if count%sqlParallelHashJoinSharedCheckInterval == 0 {
						if err := ctx.Err(); err != nil {
							results[worker].err = err
							return
						}
					}
					if maxRows > 0 && count > int64(maxRows) {
						results[worker].err = fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
						return
					}
					rows = append(rows, mergeSQLRows(left[leftIndex], right[rightIndex]))
				}
			}
			results[worker].rows = rows
		}(worker)
	}
	workers.Wait()
	for _, result := range results {
		if result.err != nil {
			return nil, result.err
		}
	}
	if err := control.check(); err != nil {
		return nil, err
	}

	matchCount := int(produced.Load())
	if err := control.addJoinWork(matchCount); err != nil {
		return nil, err
	}
	next := make([]sqlExecRow, 0, matchCount)
	for _, result := range results {
		next = append(next, result.rows...)
	}
	return next, nil
}
