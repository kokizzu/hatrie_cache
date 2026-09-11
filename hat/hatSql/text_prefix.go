package hatSql

import (
	"strings"
	"time"
)

func resolveSQLTextPrefixIndexedSource(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, metrics *sqlExecutionMetrics, hint SQLIndexHint) ([]SQLRow, bool, error) {
	if condition.kind != "func" || !strings.EqualFold(condition.name, "CONTAINS_PREFIX") || len(condition.args) != 2 {
		return nil, false, nil
	}
	field, query := condition.args[0], condition.args[1]
	if field.kind != "field" || field.qualifier != source.alias || query.kind != "literal" {
		return nil, false, nil
	}
	if !hint.allowsField(source, field.name) {
		return nil, false, nil
	}
	prefix, ok := query.value.(string)
	if !ok {
		return nil, false, nil
	}
	indexed, ok := resolver.(TextPrefixIndexedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	started := time.Now()
	rows, available, err := indexed.ResolveSQLTextPrefixSource(source.kind, source.key, field.name, prefix)
	if available && metrics != nil {
		metrics.record("TEXT PREFIX INDEX SCAN", sqlExplainSource(source)+" field="+field.name, 0, len(rows), started)
	}
	return rows, available, err
}
