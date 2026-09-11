package hatSql

import (
	"context"
	"fmt"
	"time"
)

// executeSQLAutoNativeDataflow selects the native batch runtime for narrow
// scalar, aggregate/distinct, and ordered Top-N shapes over ordinary row
// resolvers. Resolvers with columnar, streaming, lookup, index, or ordered
// contracts retain their specialized paths, and callers can force the general
// executor with the DisableNativeDataflow option.
func executeSQLAutoNativeDataflow(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions, control *sqlExecutionControl, recordPlan bool) (SQLQueryResult, bool, error) {
	detail, eligible := sqlAutoNativeDataflowPlanDetail(query, resolver, options)
	if !eligible {
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
			Detail:           detail,
			ActualInputRows:  &inputRows,
			ActualOutputRows: &outputRows,
			ElapsedNanos:     &elapsed,
		}}
	}
	return result, true, nil
}

func sqlAutoNativeDataflowEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if !sqlAutoNativeDataflowBaseEligible(query, resolver, options) {
		return false
	}
	if query.distinct || len(query.groupBy) != 0 || len(query.orderBy) != 0 || query.having.kind != "" || query.limitBy != nil || query.limitWithTies || sqlQueryHasWithFill(query) {
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

func sqlAutoNativeAggregateDistinctEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if !sqlAutoNativeDataflowBaseEligible(query, resolver, options) {
		return false
	}
	if len(query.groupBy) != 0 || len(query.orderBy) != 0 || query.having.kind != "" || query.limitBy != nil || query.limitWithTies || query.limit >= 0 || query.offset > 0 || sqlQueryHasWithFill(query) {
		return false
	}
	if sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	if query.distinct {
		_, ok := nativeSQLDataflowDistinctPlanFor(query)
		return ok && validateNativeSQLDataflowQuery(query) == nil
	}
	_, ok := nativeSQLDataflowAggregatePlan(query)
	return ok && validateNativeSQLDataflowQuery(query) == nil
}

func sqlAutoNativeAggregateWindowEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if !sqlAutoNativeDataflowBaseEligible(query, resolver, options) {
		return false
	}
	if query.distinct || len(query.groupBy) != 0 || len(query.orderBy) != 0 || query.having.kind != "" || query.limitBy != nil || query.limitWithTies || sqlQueryHasWithFill(query) || (query.limit < 0 && query.offset == 0) {
		return false
	}
	if sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	_, ok := nativeSQLDataflowAggregatePlan(query)
	return ok && validateNativeSQLDataflowQuery(query) == nil
}

func sqlAutoNativeDistinctWindowEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if !sqlAutoNativeDataflowBaseEligible(query, resolver, options) {
		return false
	}
	if !query.distinct || len(query.groupBy) != 0 || len(query.orderBy) != 0 || query.having.kind != "" || query.limitBy != nil || query.limitWithTies || sqlQueryHasWithFill(query) {
		return false
	}
	if sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	_, ok := nativeSQLDataflowDistinctPlanFor(query)
	return ok && validateNativeSQLDataflowQuery(query) == nil
}

func sqlAutoNativeGroupedEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if !sqlAutoNativeDataflowBaseEligible(query, resolver, options) {
		return false
	}
	if query.distinct || len(query.groupBy) == 0 || len(query.groupBy) > 2 || len(query.orderBy) != 0 || query.having.kind != "" || query.limitBy != nil || query.limitWithTies || query.limit >= 0 || query.offset > 0 || sqlQueryHasWithFill(query) {
		return false
	}
	if sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	if len(query.groupBy) == 1 {
		_, ok := nativeSQLDataflowGroupPlanFor(query)
		return ok && validateNativeSQLDataflowQuery(query) == nil
	}
	_, ok := nativeSQLDataflowCompositeGroupPlanFor(query)
	return ok && validateNativeSQLDataflowQuery(query) == nil
}

func sqlAutoNativeGroupedOrderedEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if !sqlAutoNativeDataflowBaseEligible(query, resolver, options) {
		return false
	}
	if query.distinct || len(query.groupBy) == 0 || len(query.groupBy) > 2 || len(query.orderBy) == 0 || query.limit < 0 || query.limitWithTies || query.limitBy != nil || sqlQueryHasWithFill(query) {
		return false
	}
	if sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	if len(query.groupBy) == 1 {
		_, ok := nativeSQLDataflowGroupedOrderedPlanFor(query)
		return ok && validateNativeSQLDataflowQuery(query) == nil
	}
	_, ok := nativeSQLDataflowCompositeGroupedOrderedPlanFor(query)
	return ok && validateNativeSQLDataflowQuery(query) == nil
}

func sqlAutoNativeDataflowPlanDetail(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) (string, bool) {
	if sqlAutoNativeDataflowEligible(query, resolver, options) {
		return "automatic scalar batch execution", true
	}
	if sqlAutoNativeAggregateDistinctEligible(query, resolver, options) {
		return "automatic aggregate/distinct batch execution", true
	}
	if sqlAutoNativeAggregateWindowEligible(query, resolver, options) {
		return "automatic aggregate window batch execution", true
	}
	if sqlAutoNativeDistinctWindowEligible(query, resolver, options) {
		return "automatic distinct window batch execution", true
	}
	if sqlAutoNativeGroupedOrderedEligible(query, resolver, options) {
		return "automatic grouped ordered top-N batch execution", true
	}
	if sqlAutoNativeGroupedEligible(query, resolver, options) {
		return "automatic grouped batch execution", true
	}
	if sqlAutoNativeOrderedEligible(query, resolver, options) {
		return "automatic ordered top-N batch execution", true
	}
	return "", false
}

func sqlAutoNativeOrderedEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if !sqlAutoNativeDataflowBaseEligible(query, resolver, options) {
		return false
	}
	if query.limit < 0 || query.limitWithTies || len(query.orderBy) == 0 || query.distinct || len(query.groupBy) != 0 || query.having.kind != "" || query.limitBy != nil || sqlQueryHasWithFill(query) {
		return false
	}
	if sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || query.where.window != nil || sqlExprHasAggregate(query.where) || sqlExprHasCustomFunction(query.where, nil) {
		return false
	}
	_, ok := nativeSQLDataflowOrderedPlanFor(query)
	return ok && validateNativeSQLDataflowQuery(query) == nil
}

func sqlAutoNativeDataflowBaseEligible(query *sqlQuery, resolver SQLSourceResolver, options SQLQueryOptions) bool {
	if options.DisableNativeDataflow || query == nil || query.from == nil || resolver == nil {
		return false
	}
	if query.from.kind != "CACHE" && query.from.kind != "KEYS" {
		return false
	}
	if query.explain || query.sample != nil || query.prewhere.kind != "" || len(query.ctes) != 0 || len(query.joins) != 0 || len(query.unions) != 0 {
		return false
	}
	if options.Collation != "" || options.Optimizer != nil || options.Workers != 0 || options.IndexHint.Source != "" || options.IndexHint.Field != "" || options.IndexHint.Mode != "" || options.AdaptivePlanner != nil || options.IndexAdvisor != nil || options.ProjectionAdvisor != nil || options.IndexUseRecorder != nil || options.SlowQueryRecorder != nil {
		return false
	}
	return !sqlAutoNativeDataflowHasSpecializedResolver(resolver)
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
