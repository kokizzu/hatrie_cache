package hatSql

import "testing"

func BenchmarkCH003BaselineNamespaceLimitsApply(b *testing.B) {
	limits := NamespaceResourceLimits{MaxRows: 100, MaxJoinBytes: 1_000}
	requested := SQLQueryOptions{MaxRows: 500, MaxJoinBytes: 2_000}
	var effective SQLQueryOptions
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		effective = limits.Apply(requested)
	}
	b.StopTimer()
	if effective.MaxRows != 100 || effective.MaxJoinBytes != 1_000 {
		b.Fatalf("effective options = %#v", effective)
	}
}
