package hatriecache

import (
	"context"

	"hatrie_cache/hat/hatSql"
)

type SQLTimeSeriesOptions = hatSql.TimeSeriesOptions
type SQLTimeSeriesResult = hatSql.TimeSeriesResult

// QuerySQLTimeSeries evaluates SQL once, then returns gap-aware buckets and
// optional rolling means.
func QuerySQLTimeSeries(ctx context.Context, source string, resolver SQLSourceResolver, parameters []interface{}, queryOptions SQLQueryOptions, options SQLTimeSeriesOptions) (SQLTimeSeriesResult, error) {
	return hatSql.QueryTimeSeries(ctx, source, resolver, parameters, queryOptions, options)
}
