package hatCache

import (
	"fmt"
	"path/filepath"
	"testing"
)

func TestCommandJournalReplayFastPath(t *testing.T) {
	requests := []CacheCommandRequest{
		{Command: "SETINT", Key: "counter", Value: "10"},
		{Command: "INC", Key: "counter", Value: "5"},
		{Command: "SET", Key: "discarded", Value: "value"},
		{Command: "DEL", Key: "discarded"},
	}

	expected := newTestTrie(t)
	actual := newTestTrie(t)
	for _, request := range requests {
		if response := expected.ExecuteCommand(request); !response.OK {
			t.Fatalf("expected ExecuteCommand(%+v) failed: %s", request, response.Message)
		}
		if err := executeCommandForReplay(actual, request); err != nil {
			t.Fatalf("executeCommandForReplay(%+v) error = %v", request, err)
		}
	}
	if got, want := actual.GetCounter("counter"), expected.GetCounter("counter"); got != want {
		t.Fatalf("counter = %d, want %d", got, want)
	}
	if actual.Exists("discarded") != expected.Exists("discarded") {
		t.Fatal("replay fast path changed deleted-key state")
	}
}

func TestCommandJournalReplayUsesFastPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	source := newTestTrie(t)
	journal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []CacheCommandRequest{
		{Command: "SETINT", Key: "counter", Value: "7"},
		{Command: "INC", Key: "counter", Value: "5"},
		{Command: "SET", Key: "temporary", Value: "value"},
		{Command: "DEL", Key: "temporary"},
	} {
		if response := journal.ExecuteCommand(source, request); !response.OK {
			t.Fatalf("journal ExecuteCommand(%+v) failed: %s", request, response.Message)
		}
	}
	if err := journal.Close(); err != nil {
		t.Fatal(err)
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetCounter("counter"); got != 12 {
		t.Fatalf("replayed counter = %d, want 12", got)
	}
	if replayed.Exists("temporary") {
		t.Fatal("replay retained a deleted key")
	}
	if err := replayJournal.Close(); err != nil {
		t.Fatal(err)
	}
}

func BenchmarkCommandJournalReplayFastPath(b *testing.B) {
	requests := []CacheCommandRequest{{Command: "SETINT", Key: "counter", Value: "0"}}
	for index := 0; index < 4096; index++ {
		requests = append(requests, CacheCommandRequest{Command: "INC", Key: "counter", Value: "1"})
	}

	b.Run("command-api", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			trie := CreateHatTrie()
			for _, request := range requests {
				if response := trie.ExecuteCommand(request); !response.OK {
					b.Fatal(response.Message)
				}
			}
			trie.Destroy()
		}
	})
	b.Run("replay-fastpath", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			trie := CreateHatTrie()
			for _, request := range requests {
				if err := executeCommandForReplay(trie, request); err != nil {
					b.Fatal(err)
				}
			}
			trie.Destroy()
		}
	})

	path := filepath.Join(b.TempDir(), "commands.journal")
	source := CreateHatTrie()
	journal, err := OpenCommandJournal(path)
	if err != nil {
		b.Fatal(err)
	}
	for index, request := range requests {
		if index > 0 {
			request.Value = fmt.Sprintf("%d", index)
		}
		if response := journal.ExecuteCommand(source, request); !response.OK {
			b.Fatal(response.Message)
		}
	}
	if err := journal.Close(); err != nil {
		b.Fatal(err)
	}
	source.Destroy()

	for _, variant := range []struct {
		name   string
		replay func(*CommandJournal, *HatTrie) error
	}{
		{name: "journal-command-api", replay: replayCommandJournalWithCommandAPI},
		{name: "journal-fastpath", replay: func(journal *CommandJournal, trie *HatTrie) error {
			_, err := journal.Replay(trie, 0)
			return err
		}},
	} {
		b.Run(variant.name, func(b *testing.B) {
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				trie := CreateHatTrie()
				replayJournal, err := OpenCommandJournal(path)
				if err != nil {
					b.Fatal(err)
				}
				if err := variant.replay(replayJournal, trie); err != nil {
					b.Fatal(err)
				}
				if err := replayJournal.Close(); err != nil {
					b.Fatal(err)
				}
				trie.Destroy()
			}
		})
	}
}

func replayCommandJournalWithCommandAPI(journal *CommandJournal, trie *HatTrie) error {
	var maximumSequence uint64
	if _, err := scanCommandJournalSet(journal.path, journal.segmented(), func(entry commandJournalEntry) error {
		if entry.Sequence > maximumSequence {
			maximumSequence = entry.Sequence
		}
		return nil
	}); err != nil {
		return err
	}
	_, err := scanCommandJournalSet(journal.path, journal.segmented(), func(entry commandJournalEntry) error {
		if entry.Checkpoint || entry.Sequence > maximumSequence {
			return nil
		}
		response := trie.ExecuteCommand(entry.Request)
		if !response.OK {
			return fmt.Errorf("replay command journal entry %d failed: %s", entry.Sequence, response.Message)
		}
		return nil
	})
	return err
}
