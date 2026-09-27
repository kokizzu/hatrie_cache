package hatSql

func resolveSQLPredicateColumnarSource(query *sqlQuery, resolver ColumnarSourceResolver, fields []string) (ColumnarBatch, bool, error) {
	if query == nil || query.from == nil || resolver == nil || len(fields) == 0 {
		return ColumnarBatch{}, false, nil
	}
	filtered, ok := resolver.(PredicateColumnarSourceResolver)
	if !ok {
		return ColumnarBatch{}, false, nil
	}
	predicates := sqlQueryPartitionPredicates(query)
	if len(predicates) == 0 {
		return ColumnarBatch{}, false, nil
	}
	return filtered.ResolveSQLColumnarSourceWithPredicates(query.from.kind, query.from.key, fields, predicates)
}
