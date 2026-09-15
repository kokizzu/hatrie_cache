package hatCache

import (
	"context"
	"path/filepath"
	"testing"
)

func BenchmarkCHU07SubmissionMetadata(b *testing.B) {
	submission := newCommandJournalSubmission()
	submission.setSequence(1)
	submission.complete(CacheCommandResponse{OK: true, Message: "committed"})
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = submission.Sequence()
		_ = submission.Status()
		_ = submission.Error()
	}
}

func BenchmarkCHU07MutationStatusLookup(b *testing.B) {
	path := filepath.Join(b.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		b.Fatal(err)
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "benchmark-mutation", Value: "value"}); !response.OK {
		b.Fatalf("ExecuteCommand() = %#v, want ok", response)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		status, err := journal.MutationStatus(1)
		if err != nil || status.State != CommandJournalMutationCommitted {
			b.Fatalf("MutationStatus() = %#v/%v, want committed", status, err)
		}
	}
}

func BenchmarkCHU07TailLookup(b *testing.B) {
	path := filepath.Join(b.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(path)
	if err != nil {
		b.Fatal(err)
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "benchmark-tail", Value: "value"}); !response.OK {
		b.Fatalf("ExecuteCommand() = %#v, want ok", response)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		tail, err := journal.Tail(0, 1)
		if err != nil || len(tail.Entries) != 1 {
			b.Fatalf("Tail() = %#v/%v, want one entry", tail, err)
		}
	}
}

func BenchmarkCHU07AsyncCommitAndWait(b *testing.B) {
	options := CommandJournalOptions{GroupCommitMaxBatch: 64}
	for index := 0; index < b.N; index++ {
		path := filepath.Join(b.TempDir(), "commands.journal")
		journal, err := OpenCommandJournalWithOptions(path, options)
		if err != nil {
			b.Fatal(err)
		}
		trie := CreateHatTrie()
		submission, err := journal.SubmitAsyncCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "benchmark-async",
			Value:   "value",
		})
		if err != nil {
			trie.Destroy()
			journal.Close()
			b.Fatal(err)
		}
		if _, err := submission.Wait(context.Background()); err != nil {
			trie.Destroy()
			journal.Close()
			b.Fatal(err)
		}
		trie.Destroy()
		if err := journal.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
