package hatDataStructure

import (
	"strconv"
	"testing"
)

func BenchmarkTU17BeforeMapPut(b *testing.B) {
	values := []byte("value-000000000000000000000000000000")
	store := make(map[string][]byte, b.N)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		store["key-"+strconv.Itoa(index)] = append([]byte(nil), values...)
	}
	if len(store) != b.N {
		b.Fatalf("map length = %d, want %d", len(store), b.N)
	}
}

func BenchmarkTU17BeforeMapGet(b *testing.B) {
	store := make(map[string][]byte, 100_000)
	for index := 0; index < 100_000; index++ {
		store["key-"+strconv.Itoa(index)] = []byte("value-000000000000000000000000000000")
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		value, ok := store["key-4242"]
		if !ok || len(value) == 0 {
			b.Fatal("map lookup missed key-4242")
		}
	}
}

func BenchmarkTU17BeforeMapHotUpsert(b *testing.B) {
	values := []byte("value-000000000000000000000000000000")
	store := make(map[string][]byte, 1)
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		store["hot-key"] = append([]byte(nil), values...)
	}
}
