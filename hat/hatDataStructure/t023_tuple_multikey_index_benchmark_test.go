package hatDataStructure

import "testing"

type t023BenchmarkRecord struct {
	keys [2]uint32
}

var t023BenchmarkSink []uint64

func t023BenchmarkRows() []t023BenchmarkRecord {
	rows := make([]t023BenchmarkRecord, 100000)
	for index := range rows {
		rows[index] = t023BenchmarkRecord{keys: [2]uint32{uint32(index % 100), uint32(index % 32)}}
	}
	return rows
}

func BenchmarkT023TupleMultikeyIndex(b *testing.B) {
	rows := t023BenchmarkRows()
	index, err := NewTupleMultikeyIndex(func(record t023BenchmarkRecord) ([]uint32, error) {
		return record.keys[:], nil
	}, TupleMultikeyIndexOptions{})
	if err != nil {
		b.Fatal(err)
	}
	for id, row := range rows {
		if err := index.Upsert(uint64(id), row); err != nil {
			b.Fatal(err)
		}
	}
	want := len(index.Lookup(42, nil))
	mapPostings := make(map[uint32]map[uint64]struct{}, 132)
	for id, row := range rows {
		for _, key := range row.keys {
			ids := mapPostings[key]
			if ids == nil {
				ids = make(map[uint64]struct{})
				mapPostings[key] = ids
			}
			ids[uint64(id)] = struct{}{}
		}
	}

	b.Run("tuple_index_lookup", func(b *testing.B) {
		b.ReportAllocs()
		destination := make([]uint64, 0, want)
		for range b.N {
			destination = index.Lookup(42, destination)
			if len(destination) != want {
				b.Fatalf("tuple index result length = %d, want %d", len(destination), want)
			}
		}
		t023BenchmarkSink = destination
	})
	b.Run("map_of_sets_lookup", func(b *testing.B) {
		b.ReportAllocs()
		destination := make([]uint64, 0, want)
		for range b.N {
			destination = destination[:0]
			for id := range mapPostings[42] {
				destination = append(destination, id)
			}
			if len(destination) != want {
				b.Fatalf("map result length = %d, want %d", len(destination), want)
			}
		}
		t023BenchmarkSink = destination
	})
	b.Run("linear_scan", func(b *testing.B) {
		b.ReportAllocs()
		destination := make([]uint64, 0, want)
		for range b.N {
			destination = destination[:0]
			for id, row := range rows {
				if row.keys[0] == 42 || row.keys[1] == 42 {
					destination = append(destination, uint64(id))
				}
			}
			if len(destination) != want {
				b.Fatalf("linear result length = %d, want %d", len(destination), want)
			}
		}
		t023BenchmarkSink = destination
	})

	const buildRows = 1024
	b.Run("tuple_index_build", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			built, err := NewTupleMultikeyIndex(func(record t023BenchmarkRecord) ([]uint32, error) {
				return record.keys[:], nil
			}, TupleMultikeyIndexOptions{})
			if err != nil {
				b.Fatal(err)
			}
			for id := 0; id < buildRows; id++ {
				if err := built.Upsert(uint64(id), rows[id]); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
	b.Run("map_of_sets_build", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			built := make(map[uint32]map[uint64]struct{}, 132)
			for id := 0; id < buildRows; id++ {
				for _, key := range rows[id].keys {
					ids := built[key]
					if ids == nil {
						ids = make(map[uint64]struct{})
						built[key] = ids
					}
					ids[uint64(id)] = struct{}{}
				}
			}
		}
	})
	b.Run("map_of_sets_with_reverse_build", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			built := make(map[uint32]map[uint64]struct{}, 132)
			reverse := make(map[uint64][]uint32, buildRows)
			for id := 0; id < buildRows; id++ {
				keys := append([]uint32(nil), rows[id].keys[:]...)
				reverse[uint64(id)] = keys
				for _, key := range keys {
					ids := built[key]
					if ids == nil {
						ids = make(map[uint64]struct{})
						built[key] = ids
					}
					ids[uint64(id)] = struct{}{}
				}
			}
		}
	})
}
