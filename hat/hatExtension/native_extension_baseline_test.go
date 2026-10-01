package hatExtension

import "testing"

func BenchmarkNativeExtensionMapResolve(b *testing.B) {
	entries := map[string]int{"geo": 1, "source": 2, "index": 3}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if entries["geo"] == 0 {
			b.Fatal("missing control entry")
		}
	}
}
