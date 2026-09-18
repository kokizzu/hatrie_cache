package hatSql_test

import (
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

var mz021BenchmarkTokenSink string

func BenchmarkMZ021ProvidedIdempotencyKey(b *testing.B) {
	provided := "explicit-idempotency-key-9a38a5a834083f166ecfca1e6eebee83"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		mz021BenchmarkTokenSink = provided
	}
}

func BenchmarkMZ021DerivedIdempotencyKey(b *testing.B) {
	input := hatSql.SQLSinkIdempotencyTokenInput{
		Sink:     "warehouse",
		Progress: []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 42}},
		Batch:    []byte(`{"id":42,"value":"Ada"}`),
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		key, err := hatSql.DeriveSQLSinkIdempotencyKey(input)
		if err != nil {
			b.Fatal(err)
		}
		mz021BenchmarkTokenSink = key
	}
}
