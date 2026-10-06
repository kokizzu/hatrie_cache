package hatDataStructure

import "testing"

type tu46BaselineBenchmarkValue struct {
	Key string
}

func tu46BaselineBenchmarkKeys() []string {
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = string(rune('a'+i%26)) + string(rune('a'+(i/26)%26))
	}
	return keys
}

func BenchmarkTU46BaselineHashLookupDefault(b *testing.B) {
	keys := tu46BaselineBenchmarkKeys()
	index, err := NewHashIndex(func(value tu46BaselineBenchmarkValue) string { return value.Key }, HashIndexOptions{})
	if err != nil {
		b.Fatal(err)
	}
	for i, key := range keys {
		if err := index.Upsert(uint64(i), tu46BaselineBenchmarkValue{Key: key}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := index.LookupOne(keys[i%len(keys)]); !ok {
			b.Fatal("lookup returned no value")
		}
	}
}

func BenchmarkTU46BaselineFunctionalLookupDefault(b *testing.B) {
	keys := tu46BaselineBenchmarkKeys()
	index, err := NewFunctionalIndex(func(value tu46BaselineBenchmarkValue) string { return value.Key }, 1024)
	if err != nil {
		b.Fatal(err)
	}
	for i, key := range keys {
		if err := index.Upsert(uint64(i), tu46BaselineBenchmarkValue{Key: key}); err != nil {
			b.Fatal(err)
		}
	}
	dst := make([]tu46BaselineBenchmarkValue, 0, 1)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = index.LookupInto(keys[i%len(keys)], dst)
	}
	if len(dst) == 0 {
		b.Fatal("lookup returned no value")
	}
}

func BenchmarkTU46BaselineOrderedSeekDefault(b *testing.B) {
	keys := tu46BaselineBenchmarkKeys()
	index, err := NewOrderedIndex(
		func(value tu46BaselineBenchmarkValue) string { return value.Key },
		func(left, right string) int {
			if left < right {
				return -1
			}
			if left > right {
				return 1
			}
			return 0
		},
		1024,
	)
	if err != nil {
		b.Fatal(err)
	}
	for i, key := range keys {
		if err := index.Upsert(uint64(i), tu46BaselineBenchmarkValue{Key: key}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iterator, ok := index.Seek(keys[i%len(keys)])
		if !ok {
			b.Fatal("seek returned no value")
		}
		iterator.Close()
	}
}
