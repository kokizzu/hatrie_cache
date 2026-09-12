package hatCache

import (
	"path/filepath"
	"testing"
)

func TestCommandJournalCachesReplayCompactionBoundary(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()

	trie := newTestTrie(t)
	for index := 1; index <= 3; index++ {
		response := journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     filepath.Join("key", string(rune('0'+index))),
			Value:   "value",
		})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) = %#v, want ok", index, response)
		}
	}

	journal.mu.Lock()
	err = journal.compactLocked(2)
	journal.mu.Unlock()
	if err != nil {
		t.Fatalf("compactLocked() error = %v", err)
	}
	if journal.compactedThrough != 2 {
		t.Fatalf("compactedThrough = %d, want 2", journal.compactedThrough)
	}

	if err := journal.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	reopened, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(reopen) error = %v", err)
	}
	defer reopened.Close()
	if reopened.compactedThrough != 2 {
		t.Fatalf("reopened compactedThrough = %d, want 2", reopened.compactedThrough)
	}
}
