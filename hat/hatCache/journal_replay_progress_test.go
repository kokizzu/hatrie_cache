package hatCache

import (
	"path/filepath"
	"testing"
)

func TestCommandJournalReplayWithProgressReportsCompletion(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	trie := newTestTrie(t)
	for index := 0; index < 3; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "INC", Key: "progress-counter", Value: "1"})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
		}
	}

	replayed := newTestTrie(t)
	defer replayed.Destroy()
	sequence, err := journal.ReplayWithProgress(replayed, 0, 0)
	if err != nil {
		t.Fatalf("ReplayWithProgress() error = %v", err)
	}
	if sequence != 3 || replayed.GetCounter("progress-counter") != 3 {
		t.Fatalf("ReplayWithProgress() sequence/value = %d/%d, want 3/3", sequence, replayed.GetCounter("progress-counter"))
	}

	progress := journal.ReplayProgress()
	if progress.Active {
		t.Fatal("ReplayProgress().Active = true after replay")
	}
	if progress.Total != 3 || progress.Applied != 3 || progress.CurrentSequence != 3 {
		t.Fatalf("ReplayProgress() counts = %#v, want total/applied/current 3/3/3", progress)
	}
	if progress.StartedAt.IsZero() || progress.FinishedAt.IsZero() {
		t.Fatalf("ReplayProgress() timestamps = %#v, want both set", progress)
	}
	if progress.Elapsed < 0 || progress.EstimatedRemaining != 0 || progress.Error != "" {
		t.Fatalf("ReplayProgress() terminal timing/error = %#v, want non-negative/zero/empty", progress)
	}
}

func TestCommandJournalReplayWithProgressReportsTargetError(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		Format:              CommandJournalFormatBinary,
		GroupCommitMaxBatch: 1,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	defer journal.Close()

	trie := newTestTrie(t)
	response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "INC", Key: "progress-counter", Value: "1"})
	if !response.OK {
		t.Fatalf("ExecuteCommand() = %#v, want ok", response)
	}
	replayed := newTestTrie(t)
	defer replayed.Destroy()
	if _, err := journal.ReplayWithProgress(replayed, 0, 2); err == nil {
		t.Fatal("ReplayWithProgress(target beyond tail) error = nil")
	}

	progress := journal.ReplayProgress()
	if progress.Active || progress.Error == "" {
		t.Fatalf("ReplayProgress() terminal error state = %#v, want inactive error", progress)
	}
	if progress.Total != 1 || progress.Applied != 0 || progress.CurrentSequence != 0 {
		t.Fatalf("ReplayProgress() target error counts = %#v, want total/applied/current 1/0/0", progress)
	}
}
