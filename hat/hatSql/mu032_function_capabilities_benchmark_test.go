package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatSql"
)

func BenchmarkMU032FunctionMetadata(b *testing.B) {
	legacy := hatCache.NewSQLFunctionRegistry()
	legacyDefinition := hatCache.SQLFunctionDefinition{
		Name:          "score_boost",
		Arguments:     []string{"score"},
		ArgumentTypes: []string{"INTEGER"},
		Language:      "GO",
		Source:        "return score + 1",
	}
	if err := legacy.Register(legacyDefinition); err != nil {
		b.Fatal(err)
	}
	defer legacy.Close()

	classified := hatCache.NewSQLFunctionRegistry()
	classifiedDefinition := legacyDefinition
	classifiedDefinition.Deterministic = true
	classifiedDefinition.Monotonicity = hatSql.FunctionMonotonicityNonDecreasing
	classifiedDefinition.Retractable = true
	if err := classified.Register(classifiedDefinition); err != nil {
		b.Fatal(err)
	}
	defer classified.Close()

	calls := make([]hatCache.SQLFunctionCall, 256)
	for i := range calls {
		calls[i].Arguments = []interface{}{int64(i)}
	}

	b.Run("before_execution", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := legacy.EvaluateSQLFunction("score_boost", calls); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("after_execution", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := classified.EvaluateSQLFunction("score_boost", calls); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("definition_lookup", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, ok := classified.Definition("score_boost"); !ok {
				b.Fatal("Definition() did not find score_boost")
			}
		}
	})
}
