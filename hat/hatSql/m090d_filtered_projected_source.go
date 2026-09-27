package hatSql

func resolveSQLPredicateProjectedSource(query *sqlQuery, resolver SQLSourceResolver, control *sqlExecutionControl, fields []string, predicates []SQLPartitionPredicate) ([]SQLRow, bool, error) {
	if query == nil || query.from == nil || resolver == nil || len(predicates) == 0 {
		return nil, false, nil
	}
	if control != nil {
		cacheKey := query.from.kind + "\x00" + query.from.key
		if _, cached := control.sources[cacheKey]; cached {
			return nil, false, nil
		}
	}
	var (
		rows      []SQLRow
		available bool
		err       error
	)
	if filtered, ok := resolver.(ContextPredicateProjectedSourceResolver); ok {
		rows, available, err = filtered.ResolveSQLProjectedSourceContextWithPredicates(sqlResolverExecutionContext(control), query.from.kind, query.from.key, fields, predicates)
	} else if filtered, ok := resolver.(PredicateProjectedSourceResolver); ok {
		rows, available, err = filtered.ResolveSQLProjectedSourceWithPredicates(query.from.kind, query.from.key, fields, predicates)
	} else {
		return nil, false, nil
	}
	if err != nil {
		return nil, true, err
	}
	if !available {
		return nil, false, nil
	}
	return rows, true, nil
}
