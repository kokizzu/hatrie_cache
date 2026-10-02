package hatDataStructure

import (
	"strconv"
	"testing"
)

func BenchmarkTU22CrossIndexUniqueSetUpsert(b *testing.B) {
	set, err := NewCrossIndexUniqueSet(tu22Definitions(), CrossIndexUniqueOptions{Capacity: 1024, MaxEntries: 1024})
	if err != nil {
		b.Fatal(err)
	}
	values := make([]tu22Record, 1024)
	for i := range values {
		key := "user-" + strconv.Itoa(i)
		values[i] = tu22Record{
			Email:    key + "@example.test",
			Username: key,
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := set.Upsert(uint64(i%len(values)), values[i%len(values)]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU22CrossIndexUniqueSetLookupOwner(b *testing.B) {
	set, err := NewCrossIndexUniqueSet(tu22Definitions(), CrossIndexUniqueOptions{Capacity: 1, MaxEntries: 1})
	if err != nil {
		b.Fatal(err)
	}
	value := tu22Record{Email: "user@example.test", Username: "user"}
	if err := set.Upsert(1, value); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if owner, ok := set.LookupOwner("email", value.Email); !ok || owner != 1 {
			b.Fatal("owner lookup failed")
		}
	}
}

func BenchmarkTU22CrossIndexUniqueSetChangingKeys(b *testing.B) {
	set, err := NewCrossIndexUniqueSet(tu22Definitions(), CrossIndexUniqueOptions{Capacity: 1024, MaxEntries: 1024})
	if err != nil {
		b.Fatal(err)
	}
	values := [2][]tu22Record{make([]tu22Record, 1024), make([]tu22Record, 1024)}
	for version := range values {
		for i := range values[version] {
			key := "user-" + strconv.Itoa(i) + "-" + strconv.Itoa(version)
			values[version][i] = tu22Record{Email: key + "@example.test", Username: key}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := set.Upsert(uint64(i%len(values[0])), values[i%2][i%len(values[0])]); err != nil {
			b.Fatal(err)
		}
	}
}
