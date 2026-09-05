package hatSql

import "testing"

var sqlLogicalShortCircuitBenchmarkSink []interface{}

func BenchmarkSQLLogicalBatchShortCircuit(b *testing.B) {
	const rowsCount = 20000
	rows := make([]sqlExecRow, rowsCount)
	for index := range rows {
		active := true
		payload := "no-match"
		if index%100 == 0 {
			active = false
			if index%200 == 0 {
				payload = "contains-needle"
			}
		}
		rows[index] = newSQLSingleSourceExecRow("event", SQLRow{"active": active, "payload": payload})
	}
	active := sqlExpr{kind: "field", qualifier: "event", name: "active"}
	payload := sqlExpr{kind: "field", qualifier: "event", name: "payload"}
	pattern := sqlExpr{kind: "literal", value: "%needle%"}
	right := sqlExpr{kind: "binary", op: "LIKE", left: &payload, right: &pattern}
	expression := sqlExpr{kind: "binary", op: "OR", left: &active, right: &right}
	b.Run("eager_right", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			values, err := evalSQLLogicalExprBatchEager(expression, rows, nil)
			if err != nil {
				b.Fatal(err)
			}
			sqlLogicalShortCircuitBenchmarkSink = values
		}
	})
	b.Run("short_circuit", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			values, err := evalSQLExprBatch(expression, rows, nil)
			if err != nil {
				b.Fatal(err)
			}
			sqlLogicalShortCircuitBenchmarkSink = values
		}
	})
}

func evalSQLLogicalExprBatchEager(expr sqlExpr, rows []sqlExecRow, functions SQLFunctionResolver) ([]interface{}, error) {
	left, err := evalSQLExprBatch(*expr.left, rows, functions)
	if err != nil {
		return nil, err
	}
	right, err := evalSQLExprBatch(*expr.right, rows, functions)
	if err != nil {
		return nil, err
	}
	out := make([]interface{}, len(rows))
	for index := range rows {
		if err := sqlExpressionError(left[index]); err != nil {
			out[index] = sqlEvaluationFailure(err)
			continue
		}
		if err := sqlExpressionError(right[index]); err != nil {
			out[index] = sqlEvaluationFailure(err)
			continue
		}
		out[index] = sqlBinaryValueWithCollation(expr.op, left[index], right[index], expr.collation)
	}
	return out, nil
}
