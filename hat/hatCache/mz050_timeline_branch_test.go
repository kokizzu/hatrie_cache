package hatCache

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestMZ050CommandJournalBranchIsolatedAndReplayable(t *testing.T) {
	journal, source := newMZ050TestJournal(t)
	defer journal.Close()
	defer source.Destroy()

	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "answer", Value: "base"}); !response.OK {
		t.Fatalf("base write = %#v", response)
	}
	baseSequence := journal.Sequence()
	branch, err := journal.BranchAt(baseSequence)
	if err != nil {
		t.Fatalf("BranchAt() error = %v", err)
	}
	defer branch.Close()

	if got := branch.BaseSequence(); got != baseSequence {
		t.Fatalf("BaseSequence() = %d, want %d", got, baseSequence)
	}
	if response := branch.Execute(CacheCommandRequest{Command: "SETSTR", Key: "answer", Value: "what-if"}); !response.OK {
		t.Fatalf("branch answer write = %#v", response)
	}
	if response := branch.Execute(CacheCommandRequest{Command: "SETSTR", Key: "branch-only", Value: "yes"}); !response.OK {
		t.Fatalf("branch-only write = %#v", response)
	}

	if response := source.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "answer"}); response.Value != "base" {
		t.Fatalf("source answer = %#v, want base", response)
	}
	if response := source.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "branch-only"}); response.Value != "" {
		t.Fatalf("source branch-only value = %#v, want empty", response)
	}

	replayed := CreateHatTrie()
	defer replayed.Destroy()
	if err := branch.ReplayInto(replayed); err != nil {
		t.Fatalf("ReplayInto() error = %v", err)
	}
	if response := replayed.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "answer"}); response.Value != "what-if" {
		t.Fatalf("replayed answer = %#v, want what-if", response)
	}
	if response := replayed.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "branch-only"}); response.Value != "yes" {
		t.Fatalf("replayed branch-only = %#v, want yes", response)
	}
}

func TestMZ050CommandJournalBranchDoesNotFollowLaterLiveWrites(t *testing.T) {
	journal, source := newMZ050TestJournal(t)
	defer journal.Close()
	defer source.Destroy()

	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "mode", Value: "base"}); !response.OK {
		t.Fatalf("base write = %#v", response)
	}
	branch, err := journal.BranchAt(journal.Sequence())
	if err != nil {
		t.Fatalf("BranchAt() error = %v", err)
	}
	defer branch.Close()
	if response := journal.ExecuteCommand(source, CacheCommandRequest{Command: "SETSTR", Key: "mode", Value: "live"}); !response.OK {
		t.Fatalf("live write = %#v", response)
	}
	if response := branch.Execute(CacheCommandRequest{Command: "GETSTR", Key: "mode"}); response.Value != "base" {
		t.Fatalf("branch followed live write: %#v", response)
	}
}

func TestMZ050CommandJournalBranchRejectsInvalidLifecycle(t *testing.T) {
	journal, source := newMZ050TestJournal(t)
	defer journal.Close()
	defer source.Destroy()

	if _, err := journal.BranchAt(journal.Sequence() + 1); err == nil {
		t.Fatal("BranchAt() accepted a future sequence")
	}
	branch, err := journal.BranchAt(0)
	if err != nil {
		t.Fatalf("BranchAt(0) error = %v", err)
	}
	branch.Close()
	branch.Close()
	if response := branch.Execute(CacheCommandRequest{Command: "SETSTR", Key: "closed", Value: "no"}); response.OK || response.Message != ErrCommandJournalBranchClosed.Error() {
		t.Fatalf("closed Execute() response = %#v, want ErrCommandJournalBranchClosed", response)
	}
	replayTarget := CreateHatTrie()
	defer replayTarget.Destroy()
	if err := branch.ReplayInto(replayTarget); !errors.Is(err, ErrCommandJournalBranchClosed) {
		t.Fatalf("closed ReplayInto() error = %v, want ErrCommandJournalBranchClosed", err)
	}
}

func newMZ050TestJournal(t *testing.T) (*CommandJournal, *HatTrie) {
	t.Helper()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "journal.log"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	return journal, CreateHatTrie()
}
