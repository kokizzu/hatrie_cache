package hatSql

import (
	"context"
	"errors"
	"fmt"
)

var ErrSQLDistributedQueryShardPruning = errors.New("hatSql: distributed query shard pruning failed")

// SQLDistributedQueryShardPruner optionally decides whether one remote shard
// can contain rows matching the query's safe literal partition predicates.
// Returning available=false preserves the normal fan-out path. A pruner must
// be conservative: false is valid only when the shard cannot contain a
// matching row. The full SQL predicate is still evaluated on every selected
// shard.
type SQLDistributedQueryShardPruner interface {
	ShouldQuerySQLDistributedShard(name, key string, predicates []SQLPartitionPredicate) (include bool, available bool, err error)
}

func pruneSQLDistributedQueryShards(source string, parameters []interface{}, shards []SQLDistributedQueryShard) ([]SQLDistributedQueryShard, error) {
	prunable := false
	for _, shard := range shards {
		if _, ok := shard.Resolver.(SQLDistributedQueryShardPruner); ok {
			prunable = true
			break
		}
	}
	if !prunable {
		return shards, nil
	}
	query, err := parseSQLQueryParameters(source, parameters)
	if err != nil {
		return nil, err
	}
	predicates := sqlQueryPartitionPredicates(query)
	if len(predicates) == 0 {
		return shards, nil
	}
	selected := make([]SQLDistributedQueryShard, 0, len(shards))
	for _, shard := range shards {
		pruner, ok := shard.Resolver.(SQLDistributedQueryShardPruner)
		if !ok {
			selected = append(selected, shard)
			continue
		}
		include, available, err := pruner.ShouldQuerySQLDistributedShard(query.from.kind, query.from.key, predicates)
		if err != nil {
			return nil, fmt.Errorf("%w: shard %q: %w", ErrSQLDistributedQueryShardPruning, shard.ID, err)
		}
		if !available || include {
			selected = append(selected, shard)
		}
	}
	return selected, nil
}

type ch035EmptySQLSourceResolver struct{}

func (ch035EmptySQLSourceResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, nil
}

func executeSQLDistributedQueryWithNoSelectedShards(ctx context.Context, source string, parameters []interface{}, queryOptions SQLQueryOptions, options SQLDistributedQueryOptions, maxRows int) (SQLQueryResult, error) {
	if options.Merge != nil {
		return mergeSQLDistributedQueryResults(nil, options.Merge, maxRows)
	}
	return ExecuteSQLQueryParameters(ctx, source, ch035EmptySQLSourceResolver{}, parameters, queryOptions)
}
