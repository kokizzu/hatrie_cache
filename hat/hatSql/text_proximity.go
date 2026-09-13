package hatSql

import (
	"strings"
	"time"
)

func resolveSQLTextProximityIndexedSource(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, metrics *sqlExecutionMetrics, hint SQLIndexHint) ([]SQLRow, bool, error) {
	if condition.kind != "func" || condition.left != nil || condition.right != nil {
		return nil, false, nil
	}
	name := strings.ToUpper(condition.name)
	if name != "CONTAINS_PHRASE" && name != "CONTAINS_PROXIMITY" {
		return nil, false, nil
	}
	wantArguments := 2
	maxGap := 0
	if name == "CONTAINS_PROXIMITY" {
		wantArguments = 3
	}
	if len(condition.args) != wantArguments {
		return nil, false, nil
	}
	field, query := condition.args[0], condition.args[1]
	if field.kind != "field" || field.qualifier != source.alias || query.kind != "literal" {
		return nil, false, nil
	}
	text, ok := query.value.(string)
	if !ok || !hint.allowsField(source, field.name) {
		return nil, false, nil
	}
	if name == "CONTAINS_PROXIMITY" {
		distance := condition.args[2]
		if distance.kind != "literal" {
			return nil, false, nil
		}
		var err error
		maxGap, err = sqlTextProximityGap(distance.value)
		if err != nil {
			return nil, false, err
		}
	}
	indexed, ok := resolver.(TextProximityIndexedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	started := time.Now()
	rows, available, err := indexed.ResolveSQLTextProximitySource(source.kind, source.key, field.name, text, maxGap)
	if available && metrics != nil {
		node := "TEXT PHRASE INDEX SCAN"
		if name == "CONTAINS_PROXIMITY" {
			node = "TEXT PROXIMITY INDEX SCAN"
		}
		metrics.record(node, sqlExplainSource(source)+" field="+field.name, 0, len(rows), started)
	}
	return rows, available, err
}
