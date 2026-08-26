package hatCache

import (
	"path/filepath"
	"testing"
)

func TestRestorePointInTimeStopsAtRequestedJournalSequence(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "cache.snapshot")
	journalPath := filepath.Join(dir, "cache.journal")
	source := newTestTrie(t)
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETINT", Key: "counter", Value: "1"}); !response.OK {
		t.Fatalf("initial Execute() = %#v", response)
	}
	if err := source.SaveSnapshotWithJournalSequence(snapshotPath, journal.Sequence()); err != nil {
		t.Fatalf("SaveSnapshotWithJournalSequence() error = %v", err)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "INC", Key: "counter", Value: "2"}); !response.OK {
		t.Fatalf("second Execute() = %#v", response)
	}
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "INC", Key: "counter", Value: "4"}); !response.OK {
		t.Fatalf("third Execute() = %#v", response)
	}

	restored, report, err := RestorePointInTime(PointInTimeRestoreOptions{
		SnapshotPath:   snapshotPath,
		JournalPath:    journalPath,
		TargetSequence: 2,
	})
	if err != nil {
		t.Fatalf("RestorePointInTime() error = %v", err)
	}
	defer restored.Destroy()
	if !report.Verified || report.SnapshotSequence != 1 || report.AppliedThrough != 2 || report.RecoveredKeys != 1 {
		t.Fatalf("report = %#v", report)
	}
	if got := restored.GetCounter("counter"); got != 3 {
		t.Fatalf("restored counter = %d, want 3", got)
	}
}
