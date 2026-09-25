package hatSql

import (
	"strings"
	"time"
)

func sqlTextProximityPredicate(source sqlSource, condition sqlExpr, hint SQLIndexHint) (string, SQLTextProximityQuery, bool, error) {
	if condition.kind != "func" || condition.left != nil || condition.right != nil {
		return "", SQLTextProximityQuery{}, false, nil
	}
	name := strings.ToUpper(condition.name)
	if name != "CONTAINS_PHRASE" && name != "CONTAINS_PROXIMITY" {
		return "", SQLTextProximityQuery{}, false, nil
	}
	wantArguments := 2
	maxGap := 0
	if name == "CONTAINS_PROXIMITY" {
		wantArguments = 3
	}
	if len(condition.args) != wantArguments {
		return "", SQLTextProximityQuery{}, false, nil
	}
	field, query := condition.args[0], condition.args[1]
	if field.kind != "field" || field.qualifier != source.alias || query.kind != "literal" {
		return "", SQLTextProximityQuery{}, false, nil
	}
	text, ok := query.value.(string)
	if !ok || !hint.allowsField(source, field.name) {
		return "", SQLTextProximityQuery{}, false, nil
	}
	if name == "CONTAINS_PROXIMITY" {
		distance := condition.args[2]
		if distance.kind != "literal" {
			return "", SQLTextProximityQuery{}, false, nil
		}
		var err error
		maxGap, err = sqlTextProximityGap(distance.value)
		if err != nil {
			return "", SQLTextProximityQuery{}, false, err
		}
	}
	return field.name, SQLTextProximityQuery{Query: text, MaxGap: maxGap, Proximity: name == "CONTAINS_PROXIMITY"}, true, nil
}

func resolveSQLTextProximityIndexedSource(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, metrics *sqlExecutionMetrics, hint SQLIndexHint) ([]SQLRow, bool, error) {
	fieldName, predicate, matched, err := sqlTextProximityPredicate(source, condition, hint)
	if err != nil || !matched {
		return nil, false, err
	}
	indexed, ok := resolver.(TextProximityIndexedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	started := time.Now()
	rows, available, err := indexed.ResolveSQLTextProximitySource(source.kind, source.key, fieldName, predicate.Query, predicate.MaxGap)
	if available && metrics != nil {
		node := "TEXT PHRASE INDEX SCAN"
		if predicate.Proximity {
			node = "TEXT PROXIMITY INDEX SCAN"
		}
		metrics.record(node, sqlExplainSource(source)+" field="+fieldName, 0, len(rows), started)
	}
	return rows, available, err
}

func resolveSQLTextProximityUnionIndexedSource(source sqlSource, condition sqlExpr, resolver SQLSourceResolver, metrics *sqlExecutionMetrics, hint SQLIndexHint) ([]SQLRow, bool, error) {
	if condition.kind != "binary" || condition.op != "OR" || condition.left == nil || condition.right == nil {
		return nil, false, nil
	}
	indexed, ok := resolver.(TextProximityUnionIndexedSourceResolver)
	if !ok {
		return nil, false, nil
	}
	fieldName := ""
	allMatched := true
	queries := make([]SQLTextProximityQuery, 0, 2)
	var collect func(sqlExpr) error
	collect = func(expression sqlExpr) error {
		if expression.kind == "binary" && expression.op == "OR" && expression.left != nil && expression.right != nil {
			if err := collect(*expression.left); err != nil {
				return err
			}
			return collect(*expression.right)
		}
		field, query, matched, err := sqlTextProximityPredicate(source, expression, hint)
		if err != nil {
			return err
		}
		if !matched {
			allMatched = false
			fieldName = ""
			return nil
		}
		if fieldName == "" {
			fieldName = field
		} else if fieldName != field {
			allMatched = false
			fieldName = ""
			return nil
		}
		queries = append(queries, query)
		return nil
	}
	if err := collect(condition); err != nil {
		return nil, false, err
	}
	if !allMatched || fieldName == "" || len(queries) < 2 || !hint.allowsField(source, fieldName) {
		return nil, false, nil
	}
	started := time.Now()
	rows, available, err := indexed.ResolveSQLTextProximityUnionSource(source.kind, source.key, fieldName, queries)
	if available && metrics != nil {
		metrics.record("TEXT PROXIMITY INDEX UNION", sqlExplainSource(source)+" field="+fieldName, len(queries), len(rows), started)
	}
	return rows, available, err
}
