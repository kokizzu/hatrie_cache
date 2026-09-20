package hatDataStructure

import (
	"strconv"
	"testing"
)

func BenchmarkTU17AfterLSMPut(b *testing.B) {
	table, err := NewLSMTable(LSMTableOptions{
		MemtableMaxRecords:      1 << 20,
		MaxRunsBeforeCompaction: 8,
		RunOptions:              SealedUpsertRunOptions{MaxRecords: 1 << 20},
	})
	if err != nil {
		b.Fatal(err)
	}
	value := []byte("value-000000000000000000000000000000")
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		if err := table.Put("hot-key", value); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU17AfterLSMGet(b *testing.B) {
	const itemCount = 100_000
	table, err := NewLSMTable(LSMTableOptions{
		MemtableMaxRecords:      itemCount,
		MaxRunsBeforeCompaction: 8,
		RunOptions:              SealedUpsertRunOptions{MaxRecords: itemCount},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < itemCount; index++ {
		if err := table.Put("key-"+strconv.Itoa(index), []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
	if err := table.Compact(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		value, ok := table.Get("key-4242")
		if !ok || len(value) == 0 {
			b.Fatal("LSM lookup missed key-4242")
		}
	}
}

func BenchmarkTU17AfterLSMCompaction(b *testing.B) {
	const runCount = 8
	const recordsPerRun = 2_000
	for index := 0; index < b.N; index++ {
		b.StopTimer()
		table, err := NewLSMTable(LSMTableOptions{
			MemtableMaxRecords:      recordsPerRun,
			MaxRunsBeforeCompaction: runCount + 1,
			RunOptions:              SealedUpsertRunOptions{MaxRecords: runCount * recordsPerRun},
		})
		if err != nil {
			b.Fatal(err)
		}
		for run := 0; run < runCount; run++ {
			for record := 0; record < recordsPerRun; record++ {
				key := "key-" + strconv.Itoa(run*recordsPerRun+record)
				if err := table.Put(key, []byte("value")); err != nil {
					b.Fatal(err)
				}
			}
		}
		b.StartTimer()
		if err := table.Compact(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU17AfterLSMSnapshot(b *testing.B) {
	table, err := NewLSMTable(LSMTableOptions{
		MemtableMaxRecords:      10_000,
		MaxRunsBeforeCompaction: 8,
		RunOptions:              SealedUpsertRunOptions{MaxRecords: 10_000},
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 10_000; index++ {
		if err := table.Put("key-"+strconv.Itoa(index), []byte("value")); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		wire, err := table.MarshalBinary()
		if err != nil || len(wire) == 0 {
			b.Fatalf("MarshalBinary() = %d bytes, %v", len(wire), err)
		}
		b.ReportMetric(float64(len(wire)), "snapshot-bytes")
	}
}
