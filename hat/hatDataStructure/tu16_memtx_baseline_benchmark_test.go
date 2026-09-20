package hatDataStructure

import (
	"sync"
	"testing"
)

var tu16BenchmarkSink uint64

func tu16BaselineRows() map[uint64]uint64 {
	rows := make(map[uint64]uint64, 4096)
	for id := uint64(1); id <= 4096; id++ {
		rows[id] = id * 17
	}
	return rows
}

func BenchmarkTU16BaselineMapLookup(b *testing.B) {
	rows := tu16BaselineRows()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value := rows[uint64(index%4096)+1]
		tu16BenchmarkSink = value
	}
}

func BenchmarkTU16BaselineLockedMapLookup(b *testing.B) {
	rows := tu16BaselineRows()
	var mu sync.RWMutex
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		mu.RLock()
		value := rows[uint64(index%4096)+1]
		mu.RUnlock()
		tu16BenchmarkSink = value
	}
}

func BenchmarkTU16BaselineMapScan(b *testing.B) {
	rows := tu16BaselineRows()
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		var sum uint64
		for id, value := range rows {
			sum += id ^ value
		}
		tu16BenchmarkSink = sum
	}
}

func BenchmarkTU16BaselineLockedMapScan(b *testing.B) {
	rows := tu16BaselineRows()
	var mu sync.RWMutex
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		mu.RLock()
		var sum uint64
		for id, value := range rows {
			sum += id ^ value
		}
		mu.RUnlock()
		tu16BenchmarkSink = sum
	}
}

func BenchmarkTU16BaselineMapBuild(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		rows := make(map[uint64]uint64, 4096)
		for id := uint64(1); id <= 4096; id++ {
			rows[id] = id * 17
		}
		tu16KeepMapAlive(rows)
	}
}

func tu16KeepMapAlive(rows map[uint64]uint64) {
	tu16BenchmarkSink = uint64(len(rows))
}

func BenchmarkTU16MemtxLookup(b *testing.B) {
	table := tu16MemtxRows(b)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value, ok := table.Get(uint64(index%4096) + 1)
		if ok {
			tu16BenchmarkSink = value
		}
	}
}

func BenchmarkTU16MemtxScan(b *testing.B) {
	table := tu16MemtxRows(b)
	dst := make([]MemtxEntry[uint64], 0, 4096)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		dst = table.ScanInto(dst[:0])
		var sum uint64
		for _, row := range dst {
			sum += row.ID ^ row.Value
		}
		tu16BenchmarkSink = sum
	}
}

func BenchmarkTU16MemtxBuild(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		table, err := NewMemtxTable[uint64](MemtxTableOptions{Capacity: 4096})
		if err != nil {
			b.Fatal(err)
		}
		for id := uint64(1); id <= 4096; id++ {
			if err := table.Insert(id, id*17); err != nil {
				b.Fatal(err)
			}
		}
		tu16BenchmarkSink = uint64(table.Len())
	}
}

func tu16MemtxRows(b *testing.B) *MemtxTable[uint64] {
	b.Helper()
	table, err := NewMemtxTable[uint64](MemtxTableOptions{Capacity: 4096})
	if err != nil {
		b.Fatal(err)
	}
	for id := uint64(1); id <= 4096; id++ {
		if err := table.Insert(id, id*17); err != nil {
			b.Fatal(err)
		}
	}
	return table
}
