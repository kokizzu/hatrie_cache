package hatSql

import "testing"

var sqlCommonSubexpressionBenchmarkSink []interface{}

func BenchmarkSQLCommonSubexpressionRewrite(b *testing.B) {
	query, err := parseSQLQueryTemplate(`FROM VALUES (1), (2), (3), (4), (5), (6), (7), (8) AS src(score) WHERE (src.score >= 1) AND (src.score >= 1) SELECT src.score`)
	if err != nil {
		b.Fatalf("parse query: %v", err)
	}
	expression := query.where
	rows := make([]sqlExecRow, 1024)
	for index := range rows {
		rows[index] = sqlExecRow{sources: map[string]SQLRow{"src": {"score": int64(index%8 + 1)}}, order: []string{"src"}}
	}
	rewritten := rewriteSQLExpr(expression)

	b.Run("baseline_duplicate", func(b *testing.B) {
		for range b.N {
			values, err := evalSQLExprBatch(expression, rows, nil)
			if err != nil {
				b.Fatal(err)
			}
			sqlCommonSubexpressionBenchmarkSink = values
		}
	})
	b.Run("cse_rewritten", func(b *testing.B) {
		for range b.N {
			values, err := evalSQLExprBatch(rewritten, rows, nil)
			if err != nil {
				b.Fatal(err)
			}
			sqlCommonSubexpressionBenchmarkSink = values
		}
	})
}
