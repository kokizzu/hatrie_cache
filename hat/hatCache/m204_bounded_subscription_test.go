package hatCache

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openM204Journal(t *testing.T) (*HatTrie, *CommandJournal) {
	t.Helper()
	trie := CreateHatTrie()
	t.Cleanup(trie.Destroy)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Errorf("journal.Close() error = %v", err)
		}
	})
	return trie, journal
}

func executeM204Command(t *testing.T, trie *HatTrie, journal *CommandJournal, key, value string) {
	t.Helper()
	response := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     key,
		Value:   value,
	})
	if !response.OK {
		t.Fatalf("journal.ExecuteCommand(%q) = %#v", key, response)
	}
}

func TestCommandJournalBoundedSubscriptionReplaysExclusiveUpperSequence(t *testing.T) {
	trie, journal := openM204Journal(t)
	for sequence := 1; sequence <= 4; sequence++ {
		executeM204Command(t, trie, journal, "key", string(rune('0'+sequence)))
	}

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		UpToSequence: 3,
		ReplayLimit:  16,
		Buffer:       8,
	})
	if err != nil {
		t.Fatal(err)
	}

	var got []uint64
	for record := range subscription.Records() {
		got = append(got, record.Sequence)
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("subscription.Err() = %v, want nil", err)
	}
	if want := []uint64{1, 2}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("bounded replay sequences = %#v, want %#v", got, want)
	}
}

func TestCommandJournalBoundedSubscriptionStopsOnLiveUpperSequence(t *testing.T) {
	trie, journal := openM204Journal(t)
	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		UpToSequence: 3,
		Buffer:       8,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	for sequence := 1; sequence <= 2; sequence++ {
		executeM204Command(t, trie, journal, "live", string(rune('0'+sequence)))
		select {
		case record := <-subscription.Records():
			if record.Sequence != uint64(sequence) {
				t.Fatalf("live record sequence = %d, want %d", record.Sequence, sequence)
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for live sequence %d", sequence)
		}
	}

	select {
	case _, ok := <-subscription.Records():
		if ok {
			t.Fatal("bounded subscription delivered a record at or after the upper sequence")
		}
	case <-time.After(time.Second):
		t.Fatal("bounded subscription did not close at the upper sequence")
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("subscription.Err() = %v, want nil", err)
	}

	executeM204Command(t, trie, journal, "after", "ignored")
}

func TestCommandJournalBoundedSubscriptionPreservesFilteredReplay(t *testing.T) {
	trie, journal := openM204Journal(t)
	executeM204Command(t, trie, journal, "user:1", "one")
	executeM204Command(t, trie, journal, "other:1", "other")
	executeM204Command(t, trie, journal, "user:2", "two")

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		UpToSequence: 4,
		ReplayLimit:  16,
		Buffer:       8,
		KeyPrefix:    "user:",
	})
	if err != nil {
		t.Fatal(err)
	}

	var got []uint64
	for record := range subscription.Records() {
		got = append(got, record.Sequence)
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("subscription.Err() = %v, want nil", err)
	}
	if want := []uint64{1, 3}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("filtered bounded sequences = %#v, want %#v", got, want)
	}
}

func TestCommandJournalBoundedSubscriptionPreservesCoalescing(t *testing.T) {
	trie, journal := openM204Journal(t)
	executeM204Command(t, trie, journal, "a", "one")
	executeM204Command(t, trie, journal, "a", "two")
	executeM204Command(t, trie, journal, "b", "three")

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		UpToSequence: 4,
		ReplayLimit:  16,
		Buffer:       8,
		Coalesce:     true,
	})
	if err != nil {
		t.Fatal(err)
	}

	var got []uint64
	for record := range subscription.Records() {
		got = append(got, record.Sequence)
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("subscription.Err() = %v, want nil", err)
	}
	if want := []uint64{2, 3}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("coalesced bounded sequences = %#v, want %#v", got, want)
	}
}

func TestCommandJournalBoundedSubscriptionRejectsInvalidRange(t *testing.T) {
	_, journal := openM204Journal(t)
	_, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		AfterSequence: 3,
		UpToSequence:  3,
	})
	if !errors.Is(err, ErrCommandJournalSubscriptionRange) {
		t.Fatalf("Subscribe() error = %v, want %v", err, ErrCommandJournalSubscriptionRange)
	}
}

func TestCommandJournalBoundedSubscriptionHonorsReplayLimit(t *testing.T) {
	trie, journal := openM204Journal(t)
	executeM204Command(t, trie, journal, "one", "one")
	executeM204Command(t, trie, journal, "two", "two")

	_, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		UpToSequence: 3,
		ReplayLimit:  1,
	})
	if !errors.Is(err, ErrCommandJournalSubscriptionReplayLimit) {
		t.Fatalf("Subscribe() error = %v, want %v", err, ErrCommandJournalSubscriptionReplayLimit)
	}
}

func TestCommandJournalBoundedSubscriptionReplayLimitAllowsLaterRecords(t *testing.T) {
	trie, journal := openM204Journal(t)
	for sequence := 1; sequence <= 3; sequence++ {
		executeM204Command(t, trie, journal, "key", string(rune('0'+sequence)))
	}

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		UpToSequence: 3,
		ReplayLimit:  2,
	})
	if err != nil {
		t.Fatal(err)
	}

	var got []uint64
	for record := range subscription.Records() {
		got = append(got, record.Sequence)
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("subscription.Err() = %v, want nil", err)
	}
	if want := []uint64{1, 2}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("bounded replay before later records = %#v, want %#v", got, want)
	}
}

func TestCommandJournalBoundedSubscriptionSkipReplayUsesCurrentTail(t *testing.T) {
	trie, journal := openM204Journal(t)
	executeM204Command(t, trie, journal, "before", "before")
	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		AfterSequence: 99,
		UpToSequence:  3,
		SkipReplay:    true,
		Buffer:        8,
	})
	if err != nil {
		t.Fatal(err)
	}

	executeM204Command(t, trie, journal, "after", "after")
	select {
	case record := <-subscription.Records():
		if record.Sequence != 2 {
			t.Fatalf("skip-replay bounded sequence = %d, want 2", record.Sequence)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for skip-replay bounded record")
	}
	select {
	case _, ok := <-subscription.Records():
		if ok {
			t.Fatal("skip-replay bounded subscription delivered an extra record")
		}
	case <-time.After(time.Second):
		t.Fatal("skip-replay bounded subscription did not close")
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("subscription.Err() = %v, want nil", err)
	}
}
