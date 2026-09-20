package hatDataStructure

import "testing"

func BenchmarkTU20AfterSpaceGet(b *testing.B) {
	space, err := NewOnlineSpaceUpgrade[string, int](1)
	if err != nil {
		b.Fatal(err)
	}
	if err := space.Set("hot", 1); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu20BenchmarkSink, _, err = space.Get("hot")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU20AfterSpaceSet(b *testing.B) {
	space, err := NewOnlineSpaceUpgrade[string, int](1)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := space.Set("hot", i); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU20AfterSpaceTargetGet(b *testing.B) {
	space, err := NewOnlineSpaceUpgrade[string, int](1)
	if err != nil {
		b.Fatal(err)
	}
	if err := space.Begin(2, func(value int) (int, error) { return value + 1, nil }); err != nil {
		b.Fatal(err)
	}
	if err := space.Set("hot", 1); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tu20BenchmarkSink, _, err = space.Get("hot")
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU20AfterOneRecordBatchConversion(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		space, err := NewOnlineSpaceUpgrade[string, int](1)
		if err != nil {
			b.Fatal(err)
		}
		if err := space.Set("old", i); err != nil {
			b.Fatal(err)
		}
		if err := space.Begin(2, func(value int) (int, error) { return value + 1, nil }); err != nil {
			b.Fatal(err)
		}
		if processed, remaining, err := space.UpgradeBatch(1); err != nil || processed != 1 || remaining != 0 {
			b.Fatalf("batch = %d, %d, %v", processed, remaining, err)
		}
	}
}
