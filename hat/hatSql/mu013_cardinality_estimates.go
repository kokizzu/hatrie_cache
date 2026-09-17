package hatSql

import "strings"

type sqlCardinalityEstimate struct {
	rows  int
	known bool
}

func sqlUnknownCardinalityEstimate() sqlCardinalityEstimate {
	return sqlCardinalityEstimate{}
}

func sqlKnownCardinalityEstimate(rows int) sqlCardinalityEstimate {
	if rows < 0 {
		return sqlUnknownCardinalityEstimate()
	}
	return sqlCardinalityEstimate{rows: rows, known: true}
}

func sqlCardinalityEstimateForSource(source sqlSource, resolver SQLSourceResolver) sqlCardinalityEstimate {
	switch source.kind {
	case "VALUES":
		return sqlKnownCardinalityEstimate(len(source.values))
	case "SUBQUERY":
		return sqlCardinalityEstimateForQuery(source.query, resolver)
	}
	if resolver == nil {
		return sqlUnknownCardinalityEstimate()
	}
	cardinality, ok := resolver.(SourceCardinalityResolver)
	if !ok {
		return sqlUnknownCardinalityEstimate()
	}
	rows, _, available, err := cardinality.SQLSourceCardinality(source.kind, source.key)
	if err != nil || !available {
		return sqlUnknownCardinalityEstimate()
	}
	return sqlKnownCardinalityEstimate(rows)
}

func sqlCardinalityEstimateForQuery(query *sqlQuery, resolver SQLSourceResolver) sqlCardinalityEstimate {
	if query == nil || query.from == nil {
		return sqlUnknownCardinalityEstimate()
	}
	current := sqlCardinalityEstimateForSource(*query.from, resolver)
	whereBeforeJoins := query.where.kind != "" && sqlCanPushBaseWhere(query)
	if query.prewhere.kind != "" {
		current = sqlCardinalityEstimateForFilter(current, *query.from, query.prewhere, resolver, len(query.joins) == 0)
	}
	if whereBeforeJoins {
		current = sqlCardinalityEstimateForFilter(current, *query.from, query.where, resolver, true)
	}
	leftAliases := []string{}
	if query.from.alias != "" {
		leftAliases = append(leftAliases, query.from.alias)
	}
	for _, join := range query.joins {
		current = sqlCardinalityEstimateForJoin(current, join, leftAliases, resolver)
		if join.source.alias != "" {
			leftAliases = append(leftAliases, join.source.alias)
		}
	}
	if query.where.kind != "" && !whereBeforeJoins {
		current = sqlCardinalityEstimateForFilter(current, *query.from, query.where, resolver, false)
	}
	if len(query.groupBy) > 0 || sqlQueryHasAggregate(query) {
		current = sqlCardinalityEstimateForAggregate(current, query, resolver)
	}
	if query.having.kind != "" {
		return sqlUnknownCardinalityEstimate()
	}
	return current
}

func sqlCardinalityEstimateForFilter(input sqlCardinalityEstimate, source sqlSource, condition sqlExpr, resolver SQLSourceResolver, allowIndex bool) sqlCardinalityEstimate {
	if condition.kind == "literal" {
		if value, ok := condition.value.(bool); ok {
			if value {
				return input
			}
			return sqlKnownCardinalityEstimate(0)
		}
	}
	if allowIndex {
		if estimate, err := sqlIndexedEqualityEstimate(source, condition, resolver); err == nil && estimate != nil && *estimate >= 0 {
			rows := *estimate
			if input.known && rows > input.rows {
				rows = input.rows
			}
			return sqlKnownCardinalityEstimate(rows)
		}
	}
	return sqlUnknownCardinalityEstimate()
}

func sqlCardinalityEstimateForJoin(input sqlCardinalityEstimate, join sqlJoin, leftAliases []string, resolver SQLSourceResolver) sqlCardinalityEstimate {
	if !input.known {
		return sqlUnknownCardinalityEstimate()
	}
	right := sqlCardinalityEstimateForSource(join.source, resolver)
	if join.kind == "CROSS" {
		if !right.known {
			return sqlUnknownCardinalityEstimate()
		}
		return sqlKnownCardinalityEstimate(sqlSaturatingMultiply(input.rows, right.rows))
	}
	if join.kind != "INNER" && join.kind != "LEFT" {
		return sqlUnknownCardinalityEstimate()
	}
	if input.rows == 0 {
		return sqlKnownCardinalityEstimate(0)
	}
	if right.known && right.rows == 0 {
		if join.kind == "LEFT" {
			return input
		}
		return sqlKnownCardinalityEstimate(0)
	}
	_, _, rightField, hashJoin := sqlHashJoinFields(join.on, leftAliases, join.source.alias)
	if !hashJoin || join.source.kind != "CACHE" || resolver == nil {
		return sqlUnknownCardinalityEstimate()
	}
	statsResolver, ok := resolver.(JSONIndexStatsResolver)
	if !ok {
		return sqlUnknownCardinalityEstimate()
	}
	stats, available, err := statsResolver.SQLJSONIndexStats(join.source.key, rightField)
	if err != nil || !available {
		return sqlUnknownCardinalityEstimate()
	}
	rowsPerKey, ok := sqlJSONIndexRowsPerKeyEstimate(stats)
	if !ok {
		return sqlUnknownCardinalityEstimate()
	}
	estimate := sqlSaturatingMultiply(input.rows, rowsPerKey)
	if join.kind == "LEFT" && estimate < input.rows {
		estimate = input.rows
	}
	return sqlKnownCardinalityEstimate(estimate)
}

func sqlJSONIndexRowsPerKeyEstimate(stats JSONIndexStats) (int, bool) {
	if stats.Rows < 0 || stats.NullRows < 0 || stats.DistinctKeys < 0 {
		return 0, false
	}
	if stats.DistinctKeys == 0 || stats.Rows == 0 {
		return 0, true
	}
	return sqlSaturatingAdd(stats.Rows, stats.DistinctKeys-1) / stats.DistinctKeys, true
}

func sqlCardinalityEstimateForAggregate(input sqlCardinalityEstimate, query *sqlQuery, resolver SQLSourceResolver) sqlCardinalityEstimate {
	if len(query.groupBy) == 0 && sqlQueryHasAggregate(query) {
		return sqlKnownCardinalityEstimate(1)
	}
	if query.from == nil || query.from.kind != "CACHE" || len(query.joins) != 0 || query.where.kind != "" || query.prewhere.kind != "" || resolver == nil {
		return sqlUnknownCardinalityEstimate()
	}
	fields, ok := sqlCardinalityGroupFields(query.groupBy, *query.from)
	if !ok {
		return sqlUnknownCardinalityEstimate()
	}
	statsResolver, ok := resolver.(JSONIndexStatsResolver)
	if !ok {
		return sqlUnknownCardinalityEstimate()
	}
	stats, available, err := statsResolver.SQLJSONIndexStats(query.from.key, fields...)
	if err != nil || !available || stats.DistinctKeys < 0 || stats.NullRows < 0 {
		return sqlUnknownCardinalityEstimate()
	}
	estimate := stats.DistinctKeys
	if stats.NullRows > 0 {
		estimate = sqlSaturatingAdd(estimate, 1)
	}
	if input.known && estimate > input.rows {
		estimate = input.rows
	}
	return sqlKnownCardinalityEstimate(estimate)
}

func sqlCardinalityGroupFields(expressions []sqlExpr, source sqlSource) ([]string, bool) {
	if len(expressions) == 0 {
		return nil, false
	}
	fields := make([]string, len(expressions))
	for index, expression := range expressions {
		if expression.kind != "field" || expression.qualifier != source.alias || expression.name == "" {
			return nil, false
		}
		fields[index] = expression.name
	}
	return fields, true
}

func sqlSetExplainCardinalityEstimate(step *SQLExplainStep, estimate sqlCardinalityEstimate) {
	if step == nil || !estimate.known {
		return
	}
	step.EstimatedRows = sqlExplainIntPointer(estimate.rows)
}

func sqlMergeExplainCardinalityEstimates(actual, planned []SQLExplainStep) {
	if len(actual) == 0 || len(planned) == 0 {
		return
	}
	used := make([]bool, len(planned))
	for actualIndex := range actual {
		plannedIndex := -1
		for candidate := range planned {
			if used[candidate] || planned[candidate].EstimatedRows == nil || !sqlExplainCardinalityStepMatches(actual[actualIndex], planned[candidate]) {
				continue
			}
			plannedIndex = candidate
			break
		}
		if plannedIndex < 0 {
			continue
		}
		used[plannedIndex] = true
		if actual[actualIndex].EstimatedRows != nil {
			continue
		}
		estimate := *planned[plannedIndex].EstimatedRows
		actual[actualIndex].EstimatedRows = &estimate
		if actual[actualIndex].ActualOutputRows == nil {
			continue
		}
		actual[actualIndex].EstimateErrorRows = sqlExplainIntPointer(*actual[actualIndex].ActualOutputRows - estimate)
		if estimate != 0 {
			percent := float64(*actual[actualIndex].ActualOutputRows-estimate) * 100 / float64(estimate)
			actual[actualIndex].EstimateErrorPercent = &percent
		}
	}
}

func sqlExplainCardinalityStepMatches(actual, planned SQLExplainStep) bool {
	actualNode := strings.TrimSpace(actual.Node)
	plannedNode := strings.TrimSpace(planned.Node)
	if actualNode == plannedNode {
		return actual.Detail == planned.Detail
	}
	if sqlExplainCardinalityJoinNode(actualNode) && sqlExplainCardinalityJoinNode(plannedNode) {
		return sqlExplainCardinalityJoinDetail(actual.Detail) == sqlExplainCardinalityJoinDetail(planned.Detail)
	}
	if sqlExplainCardinalityAggregateNode(actualNode) && sqlExplainCardinalityAggregateNode(plannedNode) {
		return actual.Detail == planned.Detail
	}
	return false
}

func sqlExplainCardinalityJoinNode(node string) bool {
	return strings.HasSuffix(node, "JOIN")
}

func sqlExplainCardinalityAggregateNode(node string) bool {
	return strings.HasSuffix(node, "AGGREGATE")
}

func sqlExplainCardinalityJoinDetail(detail string) string {
	if index := strings.Index(detail, "; eligible for "); index >= 0 {
		return detail[:index]
	}
	return detail
}
