package hatSql

import "testing"

func BenchmarkMU51BaselineParseSQLSubscription(b *testing.B) {
	query := "TAIL FROM CACHE('items') SELECT id"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ParseSQLSubscriptionStatement(query); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU51ParseSQLSubscriptionOptions(b *testing.B) {
	query := "TAIL FROM CACHE('items') SELECT id WITH (SNAPSHOT = false, PROGRESS = true, AS OF = 4, UP TO = 9, DETERMINISTIC = true)"
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ParseSQLSubscriptionStatement(query); err != nil {
			b.Fatal(err)
		}
	}
}
