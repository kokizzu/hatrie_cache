package hatSql

import (
	"context"
	"fmt"
	"strings"
)

func sqlArgExtremeAggregate(name string) bool {
	return name == "ARGMAX" || name == "ARGMIN"
}

func updateSQLArgExtreme(name string, selected *interface{}, extreme *interface{}, seen *bool, argument interface{}, value interface{}, collation SQLCollation) {
	if argument == nil || value == nil {
		return
	}
	if !*seen {
		*selected, *extreme, *seen = argument, value, true
		return
	}
	comparison := sqlCompareWithCollation(collation, value, *extreme)
	if name == "ARGMIN" && comparison < 0 || name == "ARGMAX" && comparison > 0 {
		*selected, *extreme = argument, value
	}
}

func evalSQLArgExtreme(expr sqlExpr, group []sqlExecRow) (interface{}, error) {
	if len(expr.args) != 2 {
		return nil, fmt.Errorf("%s expects exactly two arguments", expr.name)
	}
	rows, err := sqlAggregateFilterRows(expr, group)
	if err != nil {
		return nil, err
	}
	var selected interface{}
	var extreme interface{}
	seen := false
	for _, row := range rows {
		argument := evalSQLExpr(expr.args[0], []sqlExecRow{row}, row)
		if err := sqlExpressionError(argument); err != nil {
			return nil, err
		}
		value := evalSQLExpr(expr.args[1], []sqlExecRow{row}, row)
		if err := sqlExpressionError(value); err != nil {
			return nil, err
		}
		updateSQLArgExtreme(expr.name, &selected, &extreme, &seen, argument, value, expr.collation)
	}
	if !seen {
		return nil, nil
	}
	return selected, nil
}

func sqlArgExtremeMaterializedPlan(query *sqlQuery) ([]sqlStreamAggregate, bool) {
	aggregates, ok := sqlGlobalStreamAggregates(query)
	if !ok || len(aggregates) == 0 {
		return nil, false
	}
	for _, aggregate := range aggregates {
		if !sqlArgExtremeAggregate(aggregate.name) {
			return nil, false
		}
	}
	return aggregates, true
}

func sqlArgExtremeDirectSourcePlan(query *sqlQuery, aggregates []sqlStreamAggregate) bool {
	if query == nil || len(aggregates) == 0 {
		return false
	}
	for _, aggregate := range aggregates {
		if !sqlArgExtremeAggregate(aggregate.name) || aggregate.arg == nil || aggregate.order == nil {
			return false
		}
	}
	if query.where.kind == "" {
		return true
	}
	field, _, ok := sqlSimpleFieldLiteralPredicate(query.where)
	if !ok {
		return false
	}
	return field.qualifier == "" || strings.EqualFold(field.qualifier, query.from.alias)
}

func sqlStreamAggregateSourcePredicate(expr sqlExpr, row SQLRow, alias string) (bool, bool, error) {
	field, literal, ok := sqlSimpleFieldLiteralPredicate(expr)
	if !ok || field.qualifier != "" && !strings.EqualFold(field.qualifier, alias) {
		return false, false, nil
	}
	left, ok := sqlStreamAggregateSourceValue(field, row, alias)
	if !ok {
		return false, false, nil
	}
	value := sqlBinaryValueWithCollation(expr.op, left, literal.value, expr.collation)
	if err := sqlExpressionError(value); err != nil {
		return false, true, err
	}
	return sqlTruthy(value), true, nil
}

func executeSQLArgExtremeMaterialized(ctx context.Context, query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, aggregates []sqlStreamAggregate) (SQLQueryResult, error) {
	result := SQLQueryResult{Columns: sqlColumns(query.selects), Rows: []SQLRow{}}
	err := executeSQLGlobalAggregateStream(ctx, query, resolver, control, func(_ []string, row SQLRow) error {
		result.Rows = append(result.Rows, row)
		return nil
	}, aggregates)
	return result, err
}
