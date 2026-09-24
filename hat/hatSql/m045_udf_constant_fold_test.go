package hatSql

import "testing"

func TestM045DeterministicLiteralUDFIsEvaluatedOncePerBatch(t *testing.T) {
	resolver := &m045UDFResolver{definition: FunctionDefinition{
		Name:          "SCORE_BOOST",
		Deterministic: true,
		Pure:          true,
	}}
	expr := sqlExpr{
		kind: "func",
		name: "SCORE_BOOST",
		args: []sqlExpr{{kind: "literal", value: int64(7)}},
	}
	rows := make([]sqlExecRow, 32)
	values, err := evalSQLExprBatch(expr, rows, resolver)
	if err != nil {
		t.Fatalf("evalSQLExprBatch() error = %v", err)
	}
	if resolver.calls != 1 {
		t.Fatalf("EvaluateSQLFunction() calls = %d, want 1", resolver.calls)
	}
	if len(values) != len(rows) {
		t.Fatalf("result length = %d, want %d", len(values), len(rows))
	}
	for index, value := range values {
		if value != int64(8) {
			t.Fatalf("result[%d] = %#v, want 8", index, value)
		}
	}
}

func TestM045LiteralUDFWithoutPurityUsesExistingBatchPath(t *testing.T) {
	resolver := &m045UDFResolver{definition: FunctionDefinition{
		Name:          "SCORE_BOOST",
		Deterministic: true,
	}}
	expr := sqlExpr{kind: "func", name: "SCORE_BOOST", args: []sqlExpr{{kind: "literal", value: int64(7)}}}
	if _, err := evalSQLExprBatch(expr, make([]sqlExecRow, 4), resolver); err != nil {
		t.Fatalf("evalSQLExprBatch() error = %v", err)
	}
	if resolver.calls != 4 {
		t.Fatalf("EvaluateSQLFunction() calls = %d, want 4", resolver.calls)
	}
}

func TestM045PureLiteralUDFDoesNotAliasCompositeResults(t *testing.T) {
	resolver := &m045UDFResolver{
		definition: FunctionDefinition{Name: "TAGS", Deterministic: true, Pure: true},
		composite:  true,
	}
	expr := sqlExpr{kind: "func", name: "TAGS", args: []sqlExpr{{kind: "literal", value: "x"}}}
	values, err := evalSQLExprBatch(expr, make([]sqlExecRow, 2), resolver)
	if err != nil {
		t.Fatalf("evalSQLExprBatch() error = %v", err)
	}
	values[0].([]interface{})[0] = "changed"
	if values[1].([]interface{})[0] != "tag" {
		t.Fatalf("composite result was aliased across rows: %#v", values)
	}
	if resolver.calls != 1 {
		t.Fatalf("EvaluateSQLFunction() calls = %d, want 1", resolver.calls)
	}
}

func TestM045PureCapabilityRequiresDeterminism(t *testing.T) {
	definition := FunctionDefinition{Name: "unsafe", Pure: true}
	if err := NormalizeFunctionCapabilities(&definition); err == nil {
		t.Fatal("NormalizeFunctionCapabilities() error = nil, want deterministic requirement")
	}
}

type m045UDFResolver struct {
	definition FunctionDefinition
	calls      int
	composite  bool
}

func (resolver *m045UDFResolver) EvaluateSQLFunction(_ string, calls []FunctionCall) ([]interface{}, error) {
	resolver.calls += len(calls)
	values := make([]interface{}, len(calls))
	for index, call := range calls {
		if resolver.composite {
			values[index] = []interface{}{"tag"}
			continue
		}
		values[index] = call.Arguments[0].(int64) + 1
	}
	return values, nil
}

func (resolver *m045UDFResolver) FunctionCapabilities(name string) (deterministic, pure, ok bool) {
	if name != resolver.definition.Name {
		return false, false, false
	}
	return resolver.definition.Deterministic, resolver.definition.Pure, true
}

func BenchmarkM045UDFLiteralBatch(b *testing.B) {
	expr := sqlExpr{
		kind: "func",
		name: "SCORE_BOOST",
		args: []sqlExpr{{kind: "literal", value: int64(7)}},
	}
	rows := make([]sqlExecRow, 4096)
	b.Run("legacy", func(b *testing.B) {
		b.ReportAllocs()
		resolver := &m045PlainUDFResolver{}
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			values, err := evalSQLExprBatch(expr, rows, resolver)
			if err != nil || len(values) != len(rows) {
				b.Fatalf("legacy evaluation = %d values, err %v", len(values), err)
			}
		}
	})
	b.Run("candidate", func(b *testing.B) {
		b.ReportAllocs()
		resolver := &m045UDFResolver{definition: FunctionDefinition{Name: "SCORE_BOOST", Deterministic: true, Pure: true}}
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			values, err := evalSQLExprBatch(expr, rows, resolver)
			if err != nil || len(values) != len(rows) {
				b.Fatalf("candidate evaluation = %d values, err %v", len(values), err)
			}
		}
	})
	b.Run("fallback", func(b *testing.B) {
		b.ReportAllocs()
		resolver := &m045UDFResolver{definition: FunctionDefinition{Name: "SCORE_BOOST", Deterministic: true}}
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			values, err := evalSQLExprBatch(expr, rows, resolver)
			if err != nil || len(values) != len(rows) {
				b.Fatalf("fallback evaluation = %d values, err %v", len(values), err)
			}
		}
	})
}

type m045PlainUDFResolver struct{}

func (*m045PlainUDFResolver) EvaluateSQLFunction(_ string, calls []FunctionCall) ([]interface{}, error) {
	values := make([]interface{}, len(calls))
	for index, call := range calls {
		values[index] = call.Arguments[0].(int64) + 1
	}
	return values, nil
}
