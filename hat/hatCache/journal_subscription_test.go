package hatCache

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func openJournalSubscriptionTestFixture(t *testing.T) (*CommandJournal, *HatTrie) {
	t.Helper()

	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	trie := newTestTrie(t)
	t.Cleanup(func() {
		if err := journal.Close(); err != nil {
			t.Errorf("journal.Close() error = %v", err)
		}
	})
	return journal, trie
}

func appendJournalSubscriptionTestCommand(t *testing.T, journal *CommandJournal, trie *HatTrie, key string) {
	t.Helper()
	response := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     key,
		Value:   "value-" + key,
	})
	if !response.OK {
		t.Fatalf("ExecuteCommand(%q) failed: %s", key, response.Message)
	}
}

func receiveJournalSubscriptionRecord(t *testing.T, subscription *CommandJournalSubscription) CommandJournalRecord {
	t.Helper()

	select {
	case record, ok := <-subscription.Records():
		if !ok {
			t.Fatalf("subscription closed unexpectedly: %v", subscription.Err())
		}
		return record
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for journal subscription record")
		return CommandJournalRecord{}
	}
}

func waitForJournalSubscriptionError(t *testing.T, subscription *CommandJournalSubscription, want error) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if errors.Is(subscription.Err(), want) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("subscription error = %v, want %v", subscription.Err(), want)
		case <-time.After(time.Millisecond):
		}
	}
}

func TestCommandJournalSubscriptionReplaysAndReceivesLive(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "one")
	appendJournalSubscriptionTestCommand(t, journal, trie, "two")

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		AfterSequence: 0,
		ReplayLimit:   10,
		Buffer:        4,
		PollInterval:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	for sequence, key := range []string{"one", "two"} {
		record := receiveJournalSubscriptionRecord(t, subscription)
		if record.Sequence != uint64(sequence+1) {
			t.Fatalf("replayed sequence = %d, want %d", record.Sequence, sequence+1)
		}
		if record.Request.Command != "SETSTR" || record.Request.Key != key {
			t.Fatalf("replayed request = %#v, want SETSTR for %q", record.Request, key)
		}
	}

	appendJournalSubscriptionTestCommand(t, journal, trie, "three")
	record := receiveJournalSubscriptionRecord(t, subscription)
	if record.Sequence != 3 || record.Request.Key != "three" {
		t.Fatalf("live record = %#v, want sequence 3 for three", record)
	}
	if err := subscription.Err(); err != nil {
		t.Fatalf("subscription.Err() = %v, want nil", err)
	}
}

func TestCommandJournalSubscriptionWakesOnAppend(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		Buffer:       1,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	appendJournalSubscriptionTestCommand(t, journal, trie, "wake")
	select {
	case record, ok := <-subscription.Records():
		if !ok {
			t.Fatalf("subscription closed before wake record: %v", subscription.Err())
		}
		if record.Sequence != 1 || record.Request.Key != "wake" {
			t.Fatalf("wake record = %#v, want sequence 1 for wake", record)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for append wake-up")
	}
}

func TestCommandJournalSubscriptionBroadcastsLiveAppend(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	first, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		Buffer:       1,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("first Subscribe() error = %v", err)
	}
	defer first.Close()
	second, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		Buffer:       1,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("second Subscribe() error = %v", err)
	}
	defer second.Close()

	appendJournalSubscriptionTestCommand(t, journal, trie, "broadcast")
	for name, subscription := range map[string]*CommandJournalSubscription{
		"first":  first,
		"second": second,
	} {
		record := receiveJournalSubscriptionRecord(t, subscription)
		if record.Sequence != 1 || record.Request.Key != "broadcast" {
			t.Fatalf("%s record = %#v, want sequence 1 for broadcast", name, record)
		}
	}
}

func TestCommandJournalSubscriptionReceivesGroupCommitAppend(t *testing.T) {
	journal, err := OpenCommandJournalWithOptions(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalOptions{
		GroupCommitMaxBatch: 4,
	})
	if err != nil {
		t.Fatalf("OpenCommandJournalWithOptions() error = %v", err)
	}
	trie := newTestTrie(t)
	t.Cleanup(func() {
		trie.Destroy()
		if err := journal.Close(); err != nil {
			t.Errorf("journal.Close() error = %v", err)
		}
	})

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		Buffer:       1,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	response := journal.ExecuteCommand(trie, CacheCommandRequest{
		Command: "SETSTR",
		Key:     "group-commit",
		Value:   "value-group-commit",
	})
	if !response.OK {
		t.Fatalf("ExecuteCommand() failed: %s", response.Message)
	}
	record := receiveJournalSubscriptionRecord(t, subscription)
	if record.Sequence != 1 || record.Request.Key != "group-commit" {
		t.Fatalf("group commit record = %#v, want sequence 1 for group-commit", record)
	}
}

func TestCommandJournalSubscriptionWakesOnBatchAppend(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		Buffer:       2,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	applied, response := journal.executeJournalRecordsBatch(trie, []CommandJournalRecord{
		{Request: CacheCommandRequest{Command: "SETSTR", Key: "batch-one", Value: "value-one"}},
		{Request: CacheCommandRequest{Command: "SETSTR", Key: "batch-two", Value: "value-two"}},
	})
	if applied != 2 || !response.OK {
		t.Fatalf("executeJournalRecordsBatch() = (%d, %#v), want (2, OK)", applied, response)
	}
	for sequence, key := range []string{"batch-one", "batch-two"} {
		record := receiveJournalSubscriptionRecord(t, subscription)
		if record.Sequence != uint64(sequence+1) || record.Request.Key != key {
			t.Fatalf("batch record = %#v, want sequence %d for %s", record, sequence+1, key)
		}
	}
}

func TestCommandJournalSubscriptionDeliversBufferedAppendAfterJournalClose(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		Buffer:       1,
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	appendJournalSubscriptionTestCommand(t, journal, trie, "buffered")
	if err := journal.Close(); err != nil {
		t.Fatalf("journal.Close() error = %v", err)
	}
	record := receiveJournalSubscriptionRecord(t, subscription)
	if record.Sequence != 1 || record.Request.Key != "buffered" {
		t.Fatalf("buffered record = %#v, want sequence 1 for buffered", record)
	}
}

func TestCommandJournalSubscriptionStopsWhenJournalCloses(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	if err := journal.Close(); err != nil {
		t.Fatalf("journal.Close() error = %v", err)
	}

	select {
	case _, ok := <-subscription.Records():
		if ok {
			t.Fatal("subscription delivered a record after an empty journal closed")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscription to stop after journal close")
	}
	if !errors.Is(subscription.Err(), ErrCommandJournalClosed) {
		t.Fatalf("subscription.Err() = %v, want ErrCommandJournalClosed", subscription.Err())
	}
}

func TestCommandJournalSubscriptionRejectsReplayLimit(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "one")
	appendJournalSubscriptionTestCommand(t, journal, trie, "two")

	_, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		ReplayLimit: 1,
		Buffer:      1,
	})
	if !errors.Is(err, ErrCommandJournalSubscriptionReplayLimit) {
		t.Fatalf("Subscribe() error = %v, want %v", err, ErrCommandJournalSubscriptionReplayLimit)
	}
}

func TestCommandJournalSubscriptionRejectsInvalidOptions(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	options := []CommandJournalSubscribeOptions{
		{ReplayLimit: -1},
		{ReplayLimit: MaxCommandJournalTailLimit + 1},
		{Buffer: -1},
		{Buffer: MaxCommandJournalSubscriptionBuffer + 1},
		{PollInterval: -time.Nanosecond},
	}
	for _, option := range options {
		if _, err := journal.Subscribe(context.Background(), option); err == nil {
			t.Fatalf("Subscribe(%#v) error = nil, want validation error", option)
		}
	}
}

func TestCommandJournalSubscriptionRejectsBufferOverflow(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		AfterSequence: 0,
		Buffer:        1,
		PollInterval:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()

	appendJournalSubscriptionTestCommand(t, journal, trie, "one")
	appendJournalSubscriptionTestCommand(t, journal, trie, "two")
	waitForJournalSubscriptionError(t, subscription, ErrCommandJournalSubscriptionOverflow)
}

func TestCommandJournalSubscriptionStopsOnContextCancellation(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	subscription, err := journal.Subscribe(ctx, CommandJournalSubscribeOptions{
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	cancel()

	select {
	case _, ok := <-subscription.Records():
		if ok {
			t.Fatal("subscription records channel remained open after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscription cancellation")
	}
	if !errors.Is(subscription.Err(), context.Canceled) {
		t.Fatalf("subscription.Err() = %v, want context.Canceled", subscription.Err())
	}
}
