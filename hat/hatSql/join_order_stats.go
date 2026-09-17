package hatSql

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// executeSQLReorderedInnerHashJoinsFromStats uses resolver-provided source
// cardinalities to choose a deterministic connected join order before loading
// rows. It keeps only the current joined rows and the next source, while the
// existing executor remains the fallback for resolvers without metadata.
func executeSQLReorderedInnerHashJoinsFromStats(q *sqlQuery, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl, maxRows int) ([]sqlExecRow, bool, error) {
	if q == nil || q.from == nil || q.sample != nil || len(q.joins) < 2 || q.where.kind != "" {
		return nil, false, nil
	}
	sources := []sqlSource{*q.from}
	aliases := []string{q.from.alias}
	for _, join := range q.joins {
		if join.kind != "INNER" || join.source.alias == "" {
			return nil, false, nil
		}
		if _, _, _, ok := sqlHashJoinFields(join.on, aliases, join.source.alias); !ok {
			return nil, false, nil
		}
		sources = append(sources, join.source)
		aliases = append(aliases, join.source.alias)
	}
	for left := range sources {
		for right := left + 1; right < len(sources); right++ {
			if sources[left].alias == sources[right].alias {
				return nil, false, nil
			}
		}
	}

	cardinalities, ok := sqlSourceCardinalities(sources, resolver)
	if !ok {
		return nil, false, nil
	}
	sourceOrder, joinOrder, ok := sqlPlanInnerHashJoinOrder(sources, q.joins, cardinalities)
	if !ok {
		return nil, false, nil
	}

	startedPlan := time.Now()
	selectedAliases := make([]string, 0, len(sources))
	var rows []sqlExecRow
	order := make([]string, 0, len(sourceOrder))
	for position, sourceIndex := range sourceOrder {
		source := sources[sourceIndex]
		started := time.Now()
		resolved, err := resolveSQLSource(source, resolver, ctes, metrics, control)
		if err != nil {
			return nil, true, err
		}
		if len(resolved) > maxRows {
			return nil, true, fmt.Errorf("SQL source %q exceeds the %d row limit", source.alias, maxRows)
		}
		if control != nil {
			if err := sqlJoinMaterializedInputBudgetError(control.options, resolved); err != nil {
				return nil, true, err
			}
		}
		wrapped := wrapSQLSource(source, resolved)
		metrics.recordScanRows(source, resolved, started)
		order = append(order, fmt.Sprintf("%s (%d rows; estimate %d)", source.alias, len(resolved), cardinalities[sourceIndex]))

		if position == 0 {
			rows = wrapped
			selectedAliases = append(selectedAliases, source.alias)
			continue
		}

		joinIndex := joinOrder[position-1]
		join := q.joins[joinIndex]
		leftQualifier, leftField, rightField, ok := sqlHashJoinFields(join.on, selectedAliases, source.alias)
		if !ok {
			return nil, true, fmt.Errorf("SQL join planner lost a connected join for source %q", source.alias)
		}
		inputRows := len(rows) + len(wrapped)
		joinStarted := time.Now()
		buckets := make(map[string][]int, len(wrapped))
		for rightIndex, row := range wrapped {
			if err := control.addJoinWork(1); err != nil {
				return nil, true, err
			}
			if key, ok := sqlHashJoinKey(sqlField(row, source.alias, rightField)); ok {
				buckets[key] = append(buckets[key], rightIndex)
			}
		}
		var next []sqlExecRow
		for _, left := range rows {
			key, ok := sqlHashJoinKey(sqlField(left, leftQualifier, leftField))
			if !ok {
				continue
			}
			for _, rightIndex := range buckets[key] {
				if err := control.addJoinWork(1); err != nil {
					return nil, true, err
				}
				next = append(next, mergeSQLRows(left, wrapped[rightIndex]))
				if len(next) > maxRows {
					return nil, true, fmt.Errorf("SQL join exceeds the %d row limit; add a more selective WHERE or ON condition", maxRows)
				}
			}
		}
		detail := "INNER JOIN " + sqlExplainSource(source) + " ON " + sqlExplainExpression(join.on)
		metrics.record("HASH JOIN", detail, inputRows, len(next), joinStarted)
		rows = next
		selectedAliases = append(selectedAliases, source.alias)
	}

	// Restore the stable nested-loop order of the original SQL source list.
	sort.SliceStable(rows, func(left, right int) bool {
		for _, alias := range aliases {
			leftOrdinal, rightOrdinal := rows[left].ordinals[alias], rows[right].ordinals[alias]
			if leftOrdinal != rightOrdinal {
				return leftOrdinal < rightOrdinal
			}
		}
		return false
	})
	metrics.record("JOIN REORDER", "statistics order: "+strings.Join(order, " -> "), len(sources), len(rows), startedPlan)
	return rows, true, nil
}

func sqlSourceCardinalities(sources []sqlSource, resolver SQLSourceResolver) ([]int, bool) {
	cardinalities := make([]int, len(sources))
	statsResolver, hasStats := resolver.(SourceCardinalityResolver)
	for index, source := range sources {
		if source.kind == "VALUES" {
			cardinalities[index] = len(source.values)
			continue
		}
		if !hasStats {
			return nil, false
		}
		rows, _, available, err := statsResolver.SQLSourceCardinality(source.kind, source.key)
		if err != nil || !available || rows < 0 {
			return nil, false
		}
		cardinalities[index] = rows
	}
	return cardinalities, true
}

func sqlPlanInnerHashJoinOrder(sources []sqlSource, joins []sqlJoin, cardinalities []int) ([]int, []int, bool) {
	if len(sources) == 0 || len(sources) != len(cardinalities) {
		return nil, nil, false
	}
	selected := make([]bool, len(sources))
	start := 0
	for index := 1; index < len(sources); index++ {
		if cardinalities[index] < cardinalities[start] {
			start = index
		}
	}
	sourceOrder := []int{start}
	joinOrder := make([]int, 0, len(sources)-1)
	selected[start] = true
	selectedAliases := []string{sources[start].alias}
	for len(sourceOrder) < len(sources) {
		bestSource := -1
		bestJoin := -1
		for sourceIndex, source := range sources {
			if selected[sourceIndex] {
				continue
			}
			for joinIndex, join := range joins {
				if _, _, _, ok := sqlHashJoinFields(join.on, selectedAliases, source.alias); !ok {
					continue
				}
				if bestSource == -1 || cardinalities[sourceIndex] < cardinalities[bestSource] || cardinalities[sourceIndex] == cardinalities[bestSource] && sourceIndex < bestSource {
					bestSource = sourceIndex
					bestJoin = joinIndex
				}
			}
		}
		if bestSource == -1 || bestJoin == -1 {
			return nil, nil, false
		}
		selected[bestSource] = true
		sourceOrder = append(sourceOrder, bestSource)
		joinOrder = append(joinOrder, bestJoin)
		selectedAliases = append(selectedAliases, sources[bestSource].alias)
	}
	return sourceOrder, joinOrder, true
}
