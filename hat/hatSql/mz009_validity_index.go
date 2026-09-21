package hatSql

import "time"

func resolveSQLValidityIndexedSource(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, hint SQLIndexHint) ([]SQLRow, bool, error) {
	field, at, ok := sqlValidityIndexPredicate(source, condition)
	if !ok || !hint.allowsField(source, field) {
		return nil, false, nil
	}
	return resolveSQLIndexedComparison(source, field, "<=", at, resolver)
}

func sqlValidityIndexPredicate(source sqlSource, condition sqlExpr) (string, time.Time, bool) {
	if condition.kind != "func" || condition.name != "VALID_AT" || len(condition.args) != 3 {
		return "", time.Time{}, false
	}
	at, ok := sqlValidityIndexTimestamp(condition.args[0])
	if !ok || !sqlValidityIndexFieldBelongsToSource(source, condition.args[1]) || !sqlValidityIndexFieldBelongsToSource(source, condition.args[2]) {
		return "", time.Time{}, false
	}
	return condition.args[1].name, at, true
}

func sqlValidityIndexFieldBelongsToSource(source sqlSource, expr sqlExpr) bool {
	return expr.kind == "field" && (expr.qualifier == "" || expr.qualifier == source.alias)
}

func sqlValidityIndexTimestamp(expr sqlExpr) (time.Time, bool) {
	if expr.kind != "literal" || expr.value == nil {
		return time.Time{}, false
	}
	timestamp, ok := expr.value.(time.Time)
	return timestamp, ok
}
