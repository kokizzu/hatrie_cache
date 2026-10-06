package hatCache

import "testing"

func BenchmarkTU18VolatileEngineBytes(b *testing.B) {
	engine, err := NewVolatileEngine(VolatileEngineOptions{MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	value := []byte("volatile benchmark payload")
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := engine.SetBytes("benchmark-key", value, 0); err != nil {
			b.Fatal(err)
		}
		if got, ok, err := engine.GetBytes("benchmark-key"); err != nil || !ok || len(got) != len(value) {
			b.Fatal("volatile read lost value")
		}
	}
}
