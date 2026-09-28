package hatDataStructure

import "testing"

var m212LogicalCompactionSink interface{}

func BenchmarkM212LogicalCompactionAdd(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		compaction := NewLogicalCompaction[int]()
		for index := 0; index < 64; index++ {
			if err := compaction.Add(index&15, uint64(index), 1); err != nil {
				b.Fatal(err)
			}
		}
		m212LogicalCompactionSink = compaction
	}
}

func BenchmarkM212LogicalCompactionFold(b *testing.B) {
	build := func() *LogicalCompaction[int] {
		compaction := NewLogicalCompaction[int]()
		for index := 0; index < 4096; index++ {
			if err := compaction.Add(index&127, uint64(index), 1); err != nil {
				b.Fatal(err)
			}
		}
		return compaction
	}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		compaction := build()
		b.StartTimer()
		removed, err := compaction.CompactThrough(4095)
		b.StopTimer()
		if err != nil || removed != 4096 {
			b.Fatalf("CompactThrough() = %d/%v, want 4096/nil", removed, err)
		}
		m212LogicalCompactionSink = compaction
	}
}
