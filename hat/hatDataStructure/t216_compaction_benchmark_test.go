package hatDataStructure

import "testing"

func BenchmarkT216CompactionDeferredWrites(b *testing.B) {
	runT216CompactionWorkload(b, LSMCompactionDeferred, false)
}

func BenchmarkT216CompactionDeferredWritesWithCompact(b *testing.B) {
	runT216CompactionWorkload(b, LSMCompactionDeferred, true)
}

func runT216CompactionWorkload(b *testing.B, mode LSMCompactionMode, compact bool) {
	b.ReportAllocs()
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		table, err := NewLSMTable(LSMTableOptions{
			MemtableMaxRecords:      t216CompactionRecordsPerRun,
			MaxRunsBeforeCompaction: 2,
			Compaction:              LSMCompactionPolicy{Mode: mode},
			RunOptions:              SealedUpsertRunOptions{MaxRecords: t216CompactionRuns * t216CompactionRecordsPerRun},
		})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		for run := 0; run < t216CompactionRuns; run++ {
			for record := 0; record < t216CompactionRecordsPerRun; record++ {
				key := benchmarkT216CompactionKey(run, record)
				if err := table.Put(key, []byte("value")); err != nil {
					b.Fatal(err)
				}
			}
		}
		if compact {
			if _, err := table.CompactIfNeeded(); err != nil {
				b.Fatal(err)
			}
		}
	}
}
