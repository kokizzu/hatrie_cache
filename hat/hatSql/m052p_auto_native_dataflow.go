package hatSql

import (
	"context"
	"fmt"
	"time"
)

// executeSQLAutoNativeDataflow selects the native batch runtime only for a
// plain scalar projection over an ordinary row resolver. Resolvers with
// columnar, streaming, lookup, index, or ordered contracts retain their more
// specialized paths, and callers can force the general executor with the
// DisableNativeDataflow option.
func executeSQLAutoNativeDataflow(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions, control *sqlExecutionControl, recordPlan bool) (SQLQueryResult, bool, error) {
	if !sqlAutoNativeDataflowEligible(query, resolver, options) {
		return SQLQueryResult{}, false, nil
	}
	if control == nil || resolver == nil {
		return SQLQueryResult{}, false, nil
	}
	rows, err := resolver.ResolveSQLSource(query.from.kind, query.from.key)
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	if err := control.check(); err != nil {
		return SQLQueryResult{}, true, err
	}
	if len(rows) > control.maxRows {
		return SQLQueryResult{}, true, fmt.Errorf("SQL source %q exceeds the %d row limit", query.from.alias, control.maxRows)
	}
	started := time.Now()
	resultRows, err := executeNativeSQLDataflow(control.ctx, query, rows)
	if err != nil {
		return SQLQueryResult{}, true, err
	}
	if err := control.check(); err != nil {
		return SQLQueryResult{}, true, err
	}
	if options.MaxResultBytes > 0 && sqlRowsBytes(resultRows) > options.MaxResultBytes {
		return SQLQueryResult{}, true, fmt.Errorf("SQL result exceeds the %d byte limit", options.MaxResultBytes)
	}
	inputRows := len(rows)
	outputRows := len(resultRows)
	elapsed := time.Since(started).Nanoseconds()
	result := SQLQueryResult{
		Columns: sqlColumns(query.selects),
		Rows:    resultRows,
	}
	if recordPlan {
		result.Plan = []SQLExplainStep{{
			Node:             "NATIVE DATAFLOW",
			Detail:           "automatic scalar batch execution",
			ActualInputRows:  &inputRows,
			ActualOutputRows: &outputRows,
			ElapsedNanos:     &elapsed,
		}}
	}
	return result, true, nil
}

func sqlAutoNativeDataflowEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if options.DisableNativeDataflow || query == nil || query.from == nil || resolver == nil {
		return false
	}
	if query.from.kind != "CACHE" && query.from.kind != "KEYS" {
		return false
	}
	if query.explain || query.sample != nil || len(query.ctes) != 0 || len(query.joins) != 0 || len(query.unions) != 0 || query.distinct || len(query.groupBy) != 0 || len(query.orderBy) != 0 || query.having.kind != "" || query.limitBy != nil || query.limitWithTies || query.limit >= 0 || query.offset > 0 || sqlQueryHasWithFill(query) {
		return false
	}
	if sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	if len(query.selects) == 0 {
		return false
	}
	for _, item := range query.selects {
		if item.expr.kind == "star" || !sqlStreamScalarExpr(item.expr) || sqlExprHasAggregate(item.expr) || sqlExprHasCustomFunction(item.expr, nil) {
			return false
		}
	}
	if options.Collation != "" || options.Optimizer != nil || options.Workers != 0 || options.IndexHint.Source != "" || options.IndexHint.Field != "" || options.IndexHint.Mode != "" || options.AdaptivePlanner != nil || options.IndexAdvisor != nil || options.ProjectionAdvisor != nil || options.IndexUseRecorder != nil || options.SlowQueryRecorder != nil {
		return false
	}
	if sqlAutoNativeDataflowHasSpecializedResolver(resolver) {
		return false
	}
	return validateNativeSQLDataflowQuery(query) == nil
}

func sqlAutoNativeDataflowHasSpecializedResolver(resolver SQLSourceResolver) bool {
	switch resolver.(type) {
	case PartitionedSourceResolver,
		PartitionedOrderedSourceResolver,
		PartitionPruningSourceResolver,
		HistoricalSourceResolver,
		BorrowedSourceResolver,
		ColumnarSourceResolver,
		BorrowedColumnarSourceResolver,
		SegmentedColumnarSourceResolver,
		SortedColumnarSourceResolver,
		CompositeSortedColumnarSourceResolver,
		DirectedCompositeSortedColumnarSourceResolver,
		ColumnarSourcePreferenceResolver,
		SourceVersionResolver,
		StreamSourceResolver,
		SnapshotLocker,
		IndexedSourceResolver,
		MultikeyIndexedSourceResolver,
		BorrowedIndexedSourceResolver,
		CoveringIndexedSourceResolver,
		RangeIndexedSourceResolver,
		PrefixIndexedSourceResolver,
		BorrowedPrefixIndexedSourceResolver,
		TextIndexedSourceResolver,
		LookupSourceResolver,
		OrderedSourceResolver,
		OrderedStreamSourceResolver,
		KeysetOrderedStreamSourceResolver,
		CompositeIndexedSourceResolver,
		CompositeRangeIndexedSourceResolver,
		BorrowedCompositeRangeIndexedSourceResolver,
		SecondaryIndexedSourceResolver,
		JSONIndexStatsResolver,
		IndexValueEstimator:
		return true
	default:
		return false
	}
}
