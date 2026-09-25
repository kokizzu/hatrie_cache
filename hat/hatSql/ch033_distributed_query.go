package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

const (
	// DefaultSQLDistributedQueryMaxConcurrency bounds the default number of
	// shard queries that run at once.
	DefaultSQLDistributedQueryMaxConcurrency = 8
	// MaxSQLDistributedQueryConcurrency prevents an accidental unbounded fanout.
	MaxSQLDistributedQueryConcurrency = 256
)

var (
	ErrSQLDistributedQueryContextNil       = errors.New("hatSql: distributed query context is nil")
	ErrSQLDistributedQueryShardsEmpty      = errors.New("hatSql: distributed query requires at least one shard")
	ErrSQLDistributedQueryShardIDRequired  = errors.New("hatSql: distributed query shard ID is required")
	ErrSQLDistributedQueryShardIDDuplicate = errors.New("hatSql: distributed query shard IDs must be unique")
	ErrSQLDistributedQueryResolverNil      = errors.New("hatSql: distributed query shard resolver is nil")
	ErrSQLDistributedQueryConcurrency      = errors.New("hatSql: distributed query concurrency is invalid")
	ErrSQLDistributedQueryMaxRows          = errors.New("hatSql: distributed query row limit exceeded")
	ErrSQLDistributedQueryColumnsMismatch  = errors.New("hatSql: distributed query shard columns do not match")
	ErrSQLDistributedQueryPagination       = errors.New("hatSql: distributed query does not support per-shard pagination")
	ErrSQLDistributedQueryShardFailed      = errors.New("hatSql: distributed query shard failed")
)

// SQLDistributedQueryShard identifies one independent SQL source.
type SQLDistributedQueryShard struct {
	ID       string
	Resolver SQLSourceResolver
}

// SQLDistributedQueryShardResult retains one shard's result for a custom
// merge function. Results are ordered like the input shard list.
type SQLDistributedQueryShardResult struct {
	ID     string
	Result SQLQueryResult
}

// SQLDistributedQueryMergeFunc combines completed shard results. Returning a
// custom merge function is required when the query needs global ordering,
// aggregation, or another operation that cannot be implemented by row
// concatenation.
type SQLDistributedQueryMergeFunc func([]SQLDistributedQueryShardResult) (SQLQueryResult, error)

// SQLDistributedQueryOptions controls bounded fanout and the final merge.
// Zero MaxConcurrency uses DefaultSQLDistributedQueryMaxConcurrency. Zero
// MaxRows leaves the row limit to SQLQueryOptions or the normal query default.
type SQLDistributedQueryOptions struct {
	MaxConcurrency int
	MaxRows        int
	Merge          SQLDistributedQueryMergeFunc
}

// ExecuteSQLDistributedQuery executes one read query against independent
// shard resolvers. It never exposes completion order: the default merger
// concatenates rows in shard input order. A caller-provided Merge function can
// implement a global ORDER BY, LIMIT, GROUP BY, or other distributed combine.
// A shard failure cancels work that has not completed and returns no partial
// result.
func ExecuteSQLDistributedQuery(ctx context.Context, source string, shards []SQLDistributedQueryShard, parameters []interface{}, queryOptions SQLQueryOptions, options SQLDistributedQueryOptions) (SQLQueryResult, error) {
	if ctx == nil {
		return SQLQueryResult{}, ErrSQLDistributedQueryContextNil
	}
	normalized, err := normalizeSQLDistributedQueryShards(shards)
	if err != nil {
		return SQLQueryResult{}, err
	}
	maxRows, err := normalizeSQLDistributedQueryOptions(len(normalized), queryOptions, &options)
	if err != nil {
		return SQLQueryResult{}, err
	}
	if len(normalized) == 1 {
		result, err := ExecuteSQLQueryParameters(ctx, source, normalized[0].Resolver, parameters, queryOptions)
		if err != nil {
			if contextErr := ctx.Err(); contextErr != nil {
				return SQLQueryResult{}, contextErr
			}
			return SQLQueryResult{}, fmt.Errorf("%w: shard %q: %v", ErrSQLDistributedQueryShardFailed, normalized[0].ID, err)
		}
		shardResults := []SQLDistributedQueryShardResult{{ID: normalized[0].ID, Result: result}}
		return mergeSQLDistributedQueryResults(shardResults, options.Merge, maxRows)
	}

	queryCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make([]SQLDistributedQueryShardResult, len(normalized))
	jobs := make(chan int)
	workerCount := options.MaxConcurrency
	if workerCount == 0 || workerCount > len(normalized) {
		workerCount = len(normalized)
	}
	var workers sync.WaitGroup
	var firstErr error
	var errOnce sync.Once
	for worker := 0; worker < workerCount; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				result, queryErr := ExecuteSQLQueryParameters(queryCtx, source, normalized[index].Resolver, parameters, queryOptions)
				if queryErr != nil {
					errOnce.Do(func() {
						firstErr = fmt.Errorf("%w: shard %q: %v", ErrSQLDistributedQueryShardFailed, normalized[index].ID, queryErr)
						cancel()
					})
					continue
				}
				results[index] = SQLDistributedQueryShardResult{ID: normalized[index].ID, Result: result}
			}
		}()
	}
	for index := range normalized {
		select {
		case jobs <- index:
		case <-queryCtx.Done():
			break
		}
		if queryCtx.Err() != nil {
			break
		}
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return SQLQueryResult{}, err
	}
	if firstErr != nil {
		return SQLQueryResult{}, firstErr
	}
	return mergeSQLDistributedQueryResults(results, options.Merge, maxRows)
}

func normalizeSQLDistributedQueryShards(shards []SQLDistributedQueryShard) ([]SQLDistributedQueryShard, error) {
	if len(shards) == 0 {
		return nil, ErrSQLDistributedQueryShardsEmpty
	}
	normalized := make([]SQLDistributedQueryShard, len(shards))
	seen := make(map[string]struct{}, len(shards))
	for index, shard := range shards {
		shard.ID = strings.TrimSpace(shard.ID)
		if shard.ID == "" {
			return nil, ErrSQLDistributedQueryShardIDRequired
		}
		if shard.Resolver == nil {
			return nil, fmt.Errorf("%w: %q", ErrSQLDistributedQueryResolverNil, shard.ID)
		}
		if _, exists := seen[shard.ID]; exists {
			return nil, fmt.Errorf("%w: %q", ErrSQLDistributedQueryShardIDDuplicate, shard.ID)
		}
		seen[shard.ID] = struct{}{}
		normalized[index] = shard
	}
	return normalized, nil
}

func normalizeSQLDistributedQueryOptions(shardCount int, queryOptions SQLQueryOptions, options *SQLDistributedQueryOptions) (int, error) {
	if options == nil {
		return 0, ErrSQLDistributedQueryConcurrency
	}
	if options.MaxConcurrency < 0 || options.MaxConcurrency > MaxSQLDistributedQueryConcurrency {
		return 0, ErrSQLDistributedQueryConcurrency
	}
	if options.MaxRows < 0 {
		return 0, ErrSQLDistributedQueryMaxRows
	}
	if options.MaxConcurrency == 0 {
		options.MaxConcurrency = DefaultSQLDistributedQueryMaxConcurrency
	}
	if options.MaxConcurrency > shardCount {
		options.MaxConcurrency = shardCount
	}
	maxRows := options.MaxRows
	if maxRows == 0 && queryOptions.MaxRows > 0 {
		maxRows = queryOptions.MaxRows
	}
	return maxRows, nil
}

func mergeSQLDistributedQueryResults(shardResults []SQLDistributedQueryShardResult, merge SQLDistributedQueryMergeFunc, maxRows int) (SQLQueryResult, error) {
	if merge != nil {
		result, err := merge(shardResults)
		if err != nil {
			return SQLQueryResult{}, err
		}
		if maxRows > 0 && len(result.Rows) > maxRows {
			return SQLQueryResult{}, ErrSQLDistributedQueryMaxRows
		}
		return result, nil
	}
	var merged SQLQueryResult
	initialized := false
	for _, shard := range shardResults {
		if shard.Result.HasMore || shard.Result.NextCursor != "" {
			return SQLQueryResult{}, fmt.Errorf("%w: shard %q", ErrSQLDistributedQueryPagination, shard.ID)
		}
		if !initialized {
			merged.Columns = append([]string(nil), shard.Result.Columns...)
			initialized = true
		} else if !sameSQLDistributedQueryColumns(merged.Columns, shard.Result.Columns) {
			return SQLQueryResult{}, fmt.Errorf("%w: shard %q", ErrSQLDistributedQueryColumnsMismatch, shard.ID)
		}
		if maxRows > 0 && len(merged.Rows)+len(shard.Result.Rows) > maxRows {
			return SQLQueryResult{}, ErrSQLDistributedQueryMaxRows
		}
		merged.Rows = append(merged.Rows, shard.Result.Rows...)
	}
	return merged, nil
}

func sameSQLDistributedQueryColumns(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
