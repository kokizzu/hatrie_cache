package hatDataStructure

import (
	"runtime"
	"sync"
	"testing"
)

func BenchmarkVolatileEngineGetInto(b *testing.B) {
	engine, err := NewVolatileEngine(VolatileEngineOptions{MaxEntries: 1024, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	value := make([]byte, 64)
	if err := engine.Set("hot", value, 0); err != nil {
		b.Fatal(err)
	}
	destination := make([]byte, 0, len(value))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var ok bool
		destination, ok = engine.GetInto("hot", destination[:0])
		if !ok {
			b.Fatal("hot key missing")
		}
	}
	runtime.KeepAlive(destination)
}

func BenchmarkMapGetInto(b *testing.B) {
	values := map[string][]byte{"hot": make([]byte, 64)}
	var mu sync.RWMutex
	destination := make([]byte, 0, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock()
		value := values["hot"]
		destination = append(destination[:0], value...)
		mu.RUnlock()
	}
	runtime.KeepAlive(destination)
}

func BenchmarkVolatileEngineSet(b *testing.B) {
	engine, err := NewVolatileEngine(VolatileEngineOptions{MaxEntries: 1024, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	value := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := engine.Set("hot", value, 0); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMapSet(b *testing.B) {
	values := map[string][]byte{"hot": make([]byte, 64)}
	var mu sync.RWMutex
	value := make([]byte, 64)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		copied := append([]byte(nil), value...)
		mu.Lock()
		values["hot"] = copied
		mu.Unlock()
	}
}
