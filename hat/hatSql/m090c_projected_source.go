package hatSql

func resolveSQLProjectedSource(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, predicates []SQLPartitionPredicate) ([]SQLRow, []string, bool, error) {
	rows, fields, handled, err := resolveSQLProjectedSourceRows(query, resolver, control, predicates)
	if !handled {
		return nil, fields, false, err
	}
	if err != nil {
		return nil, fields, true, err
	}
	finalized, err := finishSQLSourceRows(*query.from, control, rows, false, nil)
	return finalized, fields, true, err
}

func resolveSQLProjectedSourceRows(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, predicates []SQLPartitionPredicate) ([]SQLRow, []string, bool, error) {
	if query == nil || query.from == nil || resolver == nil {
		return nil, nil, false, nil
	}
	if len(predicates) != 0 {
		if _, ok := resolver.(PartitionPruningSourceResolver); ok {
			return nil, nil, false, nil
		}
		if _, ok := resolver.(PartitionedSourceResolver); ok {
			return nil, nil, false, nil
		}
	}
	fields, ok := sqlProjectedSourceFields(query)
	if !ok {
		return nil, nil, false, nil
	}
	cacheKey := query.from.kind + "\x00" + query.from.key
	if control != nil {
		if _, cached := control.sources[cacheKey]; cached {
			return nil, fields, false, nil
		}
	}
	var (
		rows      []SQLRow
		available bool
		err       error
	)
	if projected, ok := resolver.(ContextProjectedSourceResolver); ok {
		rows, available, err = projected.ResolveSQLProjectedSourceContext(sqlResolverExecutionContext(control), query.from.kind, query.from.key, fields)
	} else if projected, ok := resolver.(ProjectedSourceResolver); ok {
		rows, available, err = projected.ResolveSQLProjectedSource(query.from.kind, query.from.key, fields)
	} else {
		return nil, fields, false, nil
	}
	if err != nil {
		return nil, fields, true, err
	}
	if !available {
		return nil, fields, false, nil
	}
	return rows, fields, true, nil
}

func sqlProjectedSourceFields(query *sqlQuery) ([]string, bool) {
	if query == nil || query.from == nil || query.from.final || query.from.kind != "CACHE" && query.from.kind != "KEYS" || len(query.from.fieldTypes) != 0 || len(query.selects) == 0 || len(query.ctes) != 0 || len(query.unions) != 0 || len(query.joins) != 0 || len(query.groupBy) != 0 || len(query.groupingSets) != 0 || len(query.groupingDimensions) != 0 || query.having.kind != "" || query.qualify.kind != "" || len(query.orderBy) != 0 || query.distinct || query.sample != nil || query.limitBy != nil || query.prewhere.kind != "" || sqlQueryHasAggregate(query) || sqlQueryHasWindow(query) || sqlQueryHasSubqueryExpression(query) {
		return nil, false
	}
	seen := make(map[string]struct{}, len(query.selects))
	fields := make([]string, 0, len(query.selects)+2)
	add := func(field string) {
		if field == "" {
			return
		}
		if _, exists := seen[field]; exists {
			return
		}
		seen[field] = struct{}{}
		fields = append(fields, field)
	}
	for _, item := range query.selects {
		if item.expr.kind != "field" || item.expr.qualifier != "" && item.expr.qualifier != query.from.alias {
			return nil, false
		}
		add(item.expr.name)
	}
	if query.where.kind != "" && !sqlColumnarPredicateFields(query.where, query.from.alias, add) {
		return nil, false
	}
	return fields, len(fields) != 0
}
