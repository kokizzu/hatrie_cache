package hatDataStructure

import "testing"

const (
	t216CompactionRuns          = 8
	t216CompactionRecordsPerRun = 128
)

func BenchmarkT216CompactionBaselineImmediateWrites(b *testing.B) {
	for iteration := 0; iteration < b.N; iteration++ {
		b.StopTimer()
		table, err := NewLSMTable(LSMTableOptions{
			MemtableMaxRecords:      t216CompactionRecordsPerRun,
			MaxRunsBeforeCompaction: 2,
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
	}
}

func benchmarkT216CompactionKey(run, record int) string {
	return "key-" + string(rune('a'+run)) + "-" + string(rune('a'+record&31))
}
