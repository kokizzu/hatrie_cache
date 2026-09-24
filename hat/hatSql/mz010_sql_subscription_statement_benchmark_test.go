package hatSql

import "testing"

var mz010SQLSubscriptionStatementBenchmarkSink SQLSubscriptionStatement

func BenchmarkMZ010ParseSQLSubscriptionStatement(b *testing.B) {
	source := "TAIL FROM CACHE('people') SELECT id, name"
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		statement, err := ParseSQLSubscriptionStatement(source)
		if err != nil {
			b.Fatal(err)
		}
		mz010SQLSubscriptionStatementBenchmarkSink = statement
	}
}
