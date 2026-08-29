package hatCache

import "testing"

func BenchmarkSQLIndexValueKey(b *testing.B) {
	for _, benchmark := range []struct {
		name  string
		value interface{}
	}{
		{name: "string", value: "queued"},
		{name: "boolean", value: true},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, ok := sqlIndexValueKey(benchmark.value); !ok {
					b.Fatal("sqlIndexValueKey() returned unavailable")
				}
			}
		})
	}
}
