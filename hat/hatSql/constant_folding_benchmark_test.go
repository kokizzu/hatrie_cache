package hatSql

import (
	"context"
	"strconv"
	"strings"
	"testing"
)

var sqlConstantFoldingBenchmarkSink SQLQueryResult

func BenchmarkSQLConstantFolding(b *testing.B) {
	source := sqlConstantFoldingBenchmarkSource(256)
	withoutRewrite, err := parseSQLQueryTemplate(source)
	if err != nil {
		b.Fatalf("parse baseline query: %v", err)
	}
	withRewrite, err := parseSQLQueryTemplate(source)
	if err != nil {
		b.Fatalf("parse folded query: %v", err)
	}
	rewriteSQLQuery(withRewrite)

	b.Run("without_rewrite", func(b *testing.B) {
		benchmarkSQLConstantFoldingQuery(b, withoutRewrite)
	})
	b.Run("with_constant_folding", func(b *testing.B) {
		benchmarkSQLConstantFoldingQuery(b, withRewrite)
	})
}

func benchmarkSQLConstantFoldingQuery(b *testing.B, query *sqlQuery) {
	control, cancel, err := newSQLExecutionControl(context.Background(), SQLQueryOptions{})
	if err != nil {
		b.Fatalf("create execution control: %v", err)
	}
	defer cancel()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := executeSQLQueryWithMetrics(query, nil, nil, nil, control)
		if err != nil {
			b.Fatalf("execute query: %v", err)
		}
		sqlConstantFoldingBenchmarkSink = result
	}
	b.StopTimer()
}

func sqlConstantFoldingBenchmarkSource(rows int) string {
	var source strings.Builder
	source.WriteString("FROM VALUES ")
	for i := 0; i < rows; i++ {
		if i > 0 {
			source.WriteString(", ")
		}
		source.WriteByte('(')
		source.WriteString(strconv.Itoa(i))
		source.WriteByte(')')
	}
	source.WriteString(" AS src(n) WHERE 2 IN (1, 2) AND 2 BETWEEN 1 AND 3 AND 5 IS NOT NULL SELECT CAST('42' AS TEXT) AS casted, COALESCE(NULL, 'ok') AS fallback, CASE WHEN 1 = 1 THEN 'hit' ELSE 'miss' END AS branch")
	return source.String()
}
