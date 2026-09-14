package hatSql

import "testing"

func TestCH056LiteralLikeIsPrepared(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value LIKE '%needle%' SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	if query.where.likeProgram == nil {
		t.Fatal("literal LIKE pattern was not prepared during query binding")
	}

	row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": "prefix-needle-suffix"}}, order: []string{"src"}}
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("LIKE result = %#v, want true", value)
	}
	row.sources["src"]["value"] = "prefix-other-suffix"
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != false {
		t.Fatalf("non-matching LIKE result = %#v, want false", value)
	}
}

func TestCH056DynamicLikePatternRemainsUnprepared(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value LIKE src.pattern SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	if query.where.likeProgram != nil {
		t.Fatal("dynamic LIKE pattern unexpectedly received a prepared program")
	}
	row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": "prefix-needle-suffix", "pattern": "%needle%"}}, order: []string{"src"}}
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("dynamic LIKE result = %#v, want true", value)
	}
}

func TestCH056PreparedLikePreservesPatternSemantics(t *testing.T) {
	tests := []struct {
		name  string
		value string
		like  string
		want  bool
	}{
		{name: "contains", value: "prefix-needle-suffix", like: "%needle%", want: true},
		{name: "prefix", value: "prefix-needle", like: "prefix%", want: true},
		{name: "suffix", value: "needle-suffix", like: "%suffix", want: true},
		{name: "exact", value: "needle", like: "needle", want: true},
		{name: "multiple parts", value: "a-middle-b-end", like: "a%b%", want: true},
		{name: "underscore remains literal", value: "a_b", like: "a_b", want: true},
		{name: "underscore is not wildcard", value: "axb", like: "a_b", want: false},
		{name: "empty wildcard", value: "anything", like: "%%", want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			query, err := parseSQLQueryParameters("FROM CACHE('events') AS src WHERE src.value LIKE '"+test.like+"' SELECT src.value", nil)
			if err != nil {
				t.Fatalf("parseSQLQueryParameters() error = %v", err)
			}
			if query.where.likeProgram == nil {
				t.Fatal("literal LIKE pattern was not prepared")
			}
			row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": test.value}}, order: []string{"src"}}
			if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != test.want {
				t.Fatalf("LIKE result = %#v, want %t", value, test.want)
			}
		})
	}
}

func TestCH056PreparedLikeBindsParametersAndPreservesNulls(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value LIKE $1 SELECT src.value`, []interface{}{"%needle%"})
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() parameter error = %v", err)
	}
	if query.where.likeProgram == nil {
		t.Fatal("bound literal LIKE pattern was not prepared")
	}
	row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": nil}}, order: []string{"src"}}
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != nil {
		t.Fatalf("NULL LIKE result = %#v, want nil", value)
	}

	query, err = parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value LIKE '%needle%' SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() collation error = %v", err)
	}
	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	if query.where.likeProgram == nil {
		t.Fatal("collated literal LIKE pattern was not prepared")
	}
	row.sources["src"]["value"] = "prefix-NEEDLE-suffix"
	if value := evalSQLExpr(query.where, []sqlExecRow{row}, row); value != true {
		t.Fatalf("collated LIKE result = %#v, want true", value)
	}

	program := &sqlLikeProgram{pattern: "123", parts: []string{"123"}}
	if value := program.evaluate(int64(123), SQLCollationBinary); value != true {
		t.Fatalf("non-string LIKE result = %#v, want true", value)
	}
}

func TestCH056LiteralLikeColumnarMatcherUsesPreparedProgram(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value LIKE '%needle%' SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	if query.where.likeProgram == nil {
		t.Fatal("columnar LIKE test did not prepare the literal pattern")
	}
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"value": {"prefix-NEEDLE-suffix", "other", nil}},
		Rows:    3,
	}
	matcher := sqlColumnarQueryRowsMatcher(query, batch, nil)
	want := []bool{true, false, false}
	for index, expected := range want {
		matched, err := matcher(index)
		if err != nil {
			t.Fatalf("columnar matcher row %d error = %v", index, err)
		}
		if matched != expected {
			t.Fatalf("columnar matcher row %d = %t, want %t", index, matched, expected)
		}
	}
}

func TestCH056UnicodeCILikeNGramDoesNotSkipCaseVariantSegment(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value LIKE '%needle%' SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	segment := ColumnarStringNGramBloomSegment{}
	segment.Add("prefix-NEEDLE-suffix")
	batch := ColumnarBatch{
		Columns: map[string][]interface{}{"value": {"prefix-NEEDLE-suffix"}},
		Rows:    1,
	}
	segments := &ColumnarNumericSegments{
		RowsPerSegment:          1,
		StringNGramBloomFilters: map[string][]ColumnarStringNGramBloomSegment{"value": {segment}},
	}
	result, matched, scanned := sqlColumnarStringNGramMaterialize(query, batch, []string{"value"}, segments, "value", "%needle%", "needle", false)
	if matched != 1 || scanned != 1 || len(result.Rows) != 1 {
		t.Fatalf("UnicodeCI NGram LIKE result = %#v, matched=%d scanned=%d, want one row", result, matched, scanned)
	}
}

func TestCH056LiteralLikeBatchEvaluationUsesPreparedProgram(t *testing.T) {
	query, err := parseSQLQueryParameters(`FROM CACHE('events') AS src WHERE src.value LIKE '%needle%' SELECT src.value`, nil)
	if err != nil {
		t.Fatalf("parseSQLQueryParameters() error = %v", err)
	}
	applySQLQueryCollation(query, SQLCollationUnicodeCI)
	rows := []sqlExecRow{
		{sources: map[string]SQLRow{"src": {"value": "prefix-NEEDLE-suffix"}}, order: []string{"src"}},
		{sources: map[string]SQLRow{"src": {"value": "other"}}, order: []string{"src"}},
		{sources: map[string]SQLRow{"src": {"value": nil}}, order: []string{"src"}},
	}
	values, err := evalSQLExprBatch(query.where, rows, nil)
	if err != nil {
		t.Fatalf("evalSQLExprBatch() error = %v", err)
	}
	want := []interface{}{true, false, nil}
	for index, expected := range want {
		if values[index] != expected {
			t.Fatalf("batch LIKE row %d = %#v, want %#v", index, values[index], expected)
		}
	}
}

func ch056LiteralLikeExpression() sqlExpr {
	left := sqlExpr{kind: "field", qualifier: "src", name: "value"}
	right := sqlExpr{kind: "literal", value: "%needle%"}
	return sqlExpr{kind: "binary", op: "LIKE", left: &left, right: &right}
}

func benchmarkCH056LiteralLike(b *testing.B, prepared bool) {
	expr := ch056LiteralLikeExpression()
	if prepared {
		prepareSQLLikeExpr(&expr)
	}
	row := sqlExecRow{sources: map[string]SQLRow{"src": {"value": "prefix-needle-suffix"}}, order: []string{"src"}}
	group := []sqlExecRow{row}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if value := evalSQLExpr(expr, group, row); value != true {
			b.Fatalf("LIKE result = %#v, want true", value)
		}
	}
}

func BenchmarkCH056LiteralLike(b *testing.B) {
	b.Run("baseline", func(b *testing.B) {
		benchmarkCH056LiteralLike(b, false)
	})
	b.Run("prepared", func(b *testing.B) {
		benchmarkCH056LiteralLike(b, true)
	})
}
