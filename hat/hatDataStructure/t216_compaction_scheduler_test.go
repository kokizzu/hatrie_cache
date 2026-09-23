package hatDataStructure_test

import (
	"errors"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func TestT216DeferredCompactionTracksDebtAndCompactsOnDemand(t *testing.T) {
	table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{
		MemtableMaxRecords:      2,
		MaxRunsBeforeCompaction: 8,
		Compaction: hatDataStructure.LSMCompactionPolicy{
			Mode:        hatDataStructure.LSMCompactionDeferred,
			MaxDebtRuns: 2,
		},
		RunOptions: hatDataStructure.SealedUpsertRunOptions{MaxRecords: 32},
	})
	if err != nil {
		t.Fatalf("NewLSMTable() error = %v", err)
	}
	for run := 0; run < 4; run++ {
		for record := 0; record < 2; record++ {
			key := string(rune('a' + run*2 + record))
			if err := table.Put(key, []byte{byte(run)}); err != nil {
				t.Fatalf("Put(%q) error = %v", key, err)
			}
		}
	}
	stats := table.Stats()
	deferredStats := stats
	if stats.RunCount != 4 || stats.CompactionDebtRuns != 3 {
		t.Fatalf("deferred stats = %#v, want four runs and three debt runs", stats)
	}
	if stats.CompactionDebtBytes == 0 || stats.CompactionCount != 0 {
		t.Fatalf("deferred debt/accounting = %#v, want debt and no compaction", stats)
	}
	if due := table.CompactionDue(); !due {
		t.Fatal("CompactionDue() = false, want true")
	}
	compacted, err := table.CompactIfNeeded()
	if err != nil || !compacted {
		t.Fatalf("CompactIfNeeded() = %t, %v, want true, nil", compacted, err)
	}
	stats = table.Stats()
	if stats.RunCount != 1 || stats.CompactionDebtRuns != 0 || stats.CompactionDebtBytes != 0 || stats.CompactionCount != 1 {
		t.Fatalf("compacted stats = %#v, want one run and zero debt", stats)
	}
	if stats.CompactionInputBytes == 0 || stats.CompactionOutputBytes == 0 {
		t.Fatalf("compaction byte accounting = %#v, want non-zero input/output", stats)
	}
	t.Logf("deferred accounting: runs=%d debt-runs=%d immutable-bytes=%d debt-bytes=%d", deferredStats.RunCount, deferredStats.CompactionDebtRuns, deferredStats.ImmutableBytes, deferredStats.CompactionDebtBytes)
	t.Logf("compacted accounting: runs=%d debt-runs=%d immutable-bytes=%d debt-bytes=%d count=%d input-bytes=%d output-bytes=%d", stats.RunCount, stats.CompactionDebtRuns, stats.ImmutableBytes, stats.CompactionDebtBytes, stats.CompactionCount, stats.CompactionInputBytes, stats.CompactionOutputBytes)
}

func TestT216CompactionSchedulerSelectsHighestDebtSpace(t *testing.T) {
	newTable := func(records int) *hatDataStructure.LSMTable {
		table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{
			MemtableMaxRecords:      1,
			MaxRunsBeforeCompaction: 100,
			Compaction: hatDataStructure.LSMCompactionPolicy{
				Mode: hatDataStructure.LSMCompactionDeferred,

				MaxDebtRuns: 2,
			},
			RunOptions: hatDataStructure.SealedUpsertRunOptions{MaxRecords: 128},
		})
		if err != nil {
			t.Fatalf("NewLSMTable() error = %v", err)
		}
		for index := 0; index < records; index++ {
			if err := table.Put(string(rune('a'+index)), []byte("value")); err != nil {
				t.Fatalf("Put(%d) error = %v", index, err)
			}
		}
		return table
	}
	low, high := newTable(2), newTable(5)
	scheduler, err := hatDataStructure.NewLSMCompactionScheduler(hatDataStructure.LSMCompactionSchedulerOptions{MaxCompactionsPerRun: 1})
	if err != nil {
		t.Fatalf("NewLSMCompactionScheduler() error = %v", err)
	}
	if err := scheduler.Register("low", low); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("high", high); err != nil {
		t.Fatal(err)
	}
	result, err := scheduler.RunOnce()
	if err != nil {
		t.Fatalf("RunOnce() error = %v", err)
	}
	if result.Compacted != 1 || len(result.CompactedSpaces) != 1 || result.CompactedSpaces[0] != "high" {
		t.Fatalf("RunOnce() result = %#v, want high only", result)
	}
	if high.Stats().RunCount != 1 || low.Stats().RunCount != 2 {
		t.Fatalf("scheduler run counts = high %d, low %d", high.Stats().RunCount, low.Stats().RunCount)
	}
}

func TestT216CompactionPolicyValidationAndImmediateCompatibility(t *testing.T) {
	for _, policy := range []hatDataStructure.LSMCompactionPolicy{
		{Mode: hatDataStructure.LSMCompactionMode(99)},
		{MaxDebtRuns: -1},
		{MaxDebtBytes: -1},
	} {
		if _, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{Compaction: policy}); !errors.Is(err, hatDataStructure.ErrLSMTableOptions) {
			t.Fatalf("invalid policy %#v error = %v, want options error", policy, err)
		}
	}
	table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{
		MemtableMaxRecords:      1,
		MaxRunsBeforeCompaction: 2,
		RunOptions:              hatDataStructure.SealedUpsertRunOptions{MaxRecords: 32},
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 12; index++ {
		if err := table.Put("key", []byte{byte(index)}); err != nil {
			t.Fatal(err)
		}
	}
	if got := table.Stats().RunCount; got > 2 {
		t.Fatalf("default immediate run count = %d, want <= 2", got)
	}
}

func TestT216MemtableAccountingAndByteThreshold(t *testing.T) {
	table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{
		MemtableMaxRecords:      4,
		MaxRunsBeforeCompaction: 100,
		Compaction: hatDataStructure.LSMCompactionPolicy{
			Mode:         hatDataStructure.LSMCompactionDeferred,
			MaxDebtRuns:  100,
			MaxDebtBytes: 1,
		},
		RunOptions: hatDataStructure.SealedUpsertRunOptions{MaxRecords: 32},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Put("same", []byte("value")); err != nil {
		t.Fatal(err)
	}
	if err := table.Put("same", []byte("longer")); err != nil {
		t.Fatal(err)
	}
	stats := table.Stats()
	if stats.MemtableRecords != 1 || stats.MemtableBytes != len("same")+len("longer") || stats.MemtableTombstones != 0 {
		t.Fatalf("replacement stats = %#v, want one value payload", stats)
	}
	if err := table.Delete("same"); err != nil {
		t.Fatal(err)
	}
	stats = table.Stats()
	if stats.MemtableRecords != 1 || stats.MemtableBytes != len("same") || stats.MemtableTombstones != 1 {
		t.Fatalf("tombstone stats = %#v, want one key payload and tombstone", stats)
	}
	if err := table.Put("other", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := table.Flush(); err != nil {
		t.Fatal(err)
	}
	stats = table.Stats()
	if stats.MemtableRecords != 0 || stats.MemtableBytes != 0 || stats.MemtableTombstones != 0 || stats.RunCount != 1 {
		t.Fatalf("flushed stats = %#v, want empty memtable and one run", stats)
	}
	if table.CompactionDue() {
		t.Fatal("CompactionDue() = true with one run")
	}
	if err := table.Put("third", []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := table.Flush(); err != nil {
		t.Fatal(err)
	}
	if !table.CompactionDue() {
		t.Fatal("CompactionDue() = false after byte debt threshold")
	}
	if _, ok := table.Get("same"); ok {
		t.Fatal("deleted key became visible")
	}
}

func TestT216SchedulerValidationAndUnregister(t *testing.T) {
	if _, err := hatDataStructure.NewLSMCompactionScheduler(hatDataStructure.LSMCompactionSchedulerOptions{MaxCompactionsPerRun: -1}); !errors.Is(err, hatDataStructure.ErrLSMCompactionSchedulerLimit) {
		t.Fatalf("negative scheduler limit error = %v", err)
	}
	scheduler, err := hatDataStructure.NewLSMCompactionScheduler(hatDataStructure.LSMCompactionSchedulerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("", nil); !errors.Is(err, hatDataStructure.ErrLSMCompactionSchedulerNameRequired) {
		t.Fatalf("empty registration error = %v", err)
	}
	if err := scheduler.Register("table", nil); !errors.Is(err, hatDataStructure.ErrLSMCompactionSchedulerTableRequired) {
		t.Fatalf("nil registration error = %v", err)
	}
	table, err := hatDataStructure.NewLSMTable(hatDataStructure.LSMTableOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("table", table); err != nil {
		t.Fatal(err)
	}
	if err := scheduler.Register("table", table); !errors.Is(err, hatDataStructure.ErrLSMCompactionSchedulerDuplicate) {
		t.Fatalf("duplicate registration error = %v", err)
	}
	if !scheduler.Unregister("table") || scheduler.Unregister("table") {
		t.Fatal("Unregister() result did not reflect registration state")
	}
}
