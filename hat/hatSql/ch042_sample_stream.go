package hatSql

import (
	"context"
	"fmt"
)

// sqlBernoulliSampleStreamResolver applies the same per-source-row decision as
// sqlSampleRows while keeping the source and sampled result out of memory.
// It intentionally does not expose optional partition-pruning interfaces: a
// partition skip would change the repeatable random sequence.
type sqlBernoulliSampleStreamResolver struct {
	base      SQLSourceResolver
	streaming SQLStreamSourceResolver
	sample    sqlTableSample
	alias     string
	maxRows   int
	control   *sqlExecutionControl
}

func (resolver *sqlBernoulliSampleStreamResolver) ResolveSQLSource(name, key string) ([]SQLRow, error) {
	return resolver.base.ResolveSQLSource(name, key)
}

func (resolver *sqlBernoulliSampleStreamResolver) StreamSQLSource(ctx context.Context, name, key string, visit func(Row) error) error {
	state := resolver.sample.seed
	inputRows := 0
	stopped := false
	return resolver.streaming.StreamSQLSource(ctx, name, key, func(row Row) error {
		if resolver.control != nil {
			if err := resolver.control.check(); err != nil {
				return err
			}
		}
		inputRows++
		if resolver.maxRows > 0 && inputRows > resolver.maxRows {
			return fmt.Errorf("SQL source %q exceeds the %d row limit", resolver.alias, resolver.maxRows)
		}
		if stopped || resolver.sample.value == 0 {
			return nil
		}
		if resolver.sample.value != 100 && sqlSampleBounded(&state, 100) >= uint64(resolver.sample.value) {
			return nil
		}
		if err := visit(row); err != nil {
			if err == errSQLStreamLimitReached {
				stopped = true
				return nil
			}
			return err
		}
		return nil
	})
}

func sqlBernoulliSampleStreamable(query *sqlQuery, resolver SQLSourceResolver) bool {
	if query == nil || query.sample == nil || query.sample.mode != "BERNOULLI" || query.from == nil || query.from.kind != "CACHE" || len(query.from.fieldTypes) != 0 || len(query.joins) != 0 {
		return false
	}
	if _, ok := resolver.(SQLStreamSourceResolver); !ok || sqlExprHasCustomFunction(query.prewhere, nil) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	for _, item := range query.selects {
		if sqlExprHasCustomFunction(item.expr, nil) {
			return false
		}
	}
	streamed := *query
	streamed.sample = nil
	return validateSQLQueryStreamable(&streamed) == nil
}

func executeSQLBernoulliSampleRows(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, visit func([]string, SQLRow) error) (bool, error) {
	if !sqlBernoulliSampleStreamable(query, resolver) {
		return false, nil
	}
	streaming := resolver.(SQLStreamSourceResolver)
	maxRows := 0
	if control != nil {
		maxRows = control.maxRows
	}
	sampledResolver := &sqlBernoulliSampleStreamResolver{
		base:      resolver,
		streaming: streaming,
		sample:    *query.sample,
		alias:     query.from.alias,
		maxRows:   maxRows,
		control:   control,
	}
	streamed := *query
	streamed.sample = nil
	return true, executeSQLQueryRowsParsed(ctx, &streamed, sampledResolver, control, visit)
}
