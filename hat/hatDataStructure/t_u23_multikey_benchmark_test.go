package hatDataStructure

import (
	"strconv"
	"testing"
)

func tu23BenchmarkValues() []tu23Record {
	values := make([]tu23Record, 1024)
	for i := range values {
		prefix := "tag-" + strconv.Itoa(i)
		values[i] = tu23Record{ID: uint64(i), Tags: []string{prefix + "-a", prefix + "-b", prefix + "-c"}}
	}
	return values
}

func BenchmarkTU23MultiKeyIndexUpsert(b *testing.B) {
	index, err := NewMultiKeyIndex(tu23Tags, MultiKeyIndexOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	values := tu23BenchmarkValues()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value := values[i%len(values)]
		if err := index.Upsert(value.ID, value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU23MultiKeyIndexLookupIDs(b *testing.B) {
	index, err := NewMultiKeyIndex(tu23Tags, MultiKeyIndexOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	values := tu23BenchmarkValues()
	for _, value := range values {
		if err := index.Upsert(value.ID, value); err != nil {
			b.Fatal(err)
		}
	}
	dst := make([]uint64, 0, 4)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = index.LookupIDsInto(values[i%len(values)].Tags[0], dst)
		if len(dst) != 1 {
			b.Fatal("unexpected posting length")
		}
	}
}

func BenchmarkTU23StringMultikeyIndexBoundedBuild(b *testing.B) {
	const itemCount = 10000
	type row struct {
		id   uint64
		keys [2]string
	}
	rows := make([]row, itemCount)
	for i := range rows {
		rows[i] = row{
			id: uint64(i),
			keys: [2]string{
				"tag-" + strconv.Itoa(i%100),
				"group-" + strconv.Itoa(i%32),
			},
		}
	}

	b.Run("sorted-postings", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			index := NewStringMultikeyIndex(StringMultikeyIndexOptions{MaxItems: itemCount})
			for _, row := range rows {
				if err := index.Set(row.id, row.keys[:]); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("map-of-sets-with-reverse", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			postings := make(map[string]map[uint64]struct{}, 132)
			entries := make(map[uint64][]string, itemCount)
			for _, row := range rows {
				keys := append([]string(nil), row.keys[:]...)
				entries[row.id] = keys
				for _, key := range keys {
					ids := postings[key]
					if ids == nil {
						ids = make(map[uint64]struct{})
						postings[key] = ids
					}
					ids[row.id] = struct{}{}
				}
			}
		}
	})
}
