package hatCache

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCommandJournalWithReadFenceReturnsSequenceAfterRead(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "fenced")

	var value string
	sequence, err := journal.WithReadFence(func() error {
		value = trie.GetString("fenced")
		return nil
	})
	if err != nil {
		t.Fatalf("WithReadFence() error = %v", err)
	}
	if value != "value-fenced" {
		t.Fatalf("fenced value = %q, want value-fenced", value)
	}
	if sequence != 1 {
		t.Fatalf("fenced sequence = %d, want 1", sequence)
	}
}

func TestCommandJournalWithReadFencePreventsJournaledWriteGap(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "fenced")

	entered := make(chan struct{})
	release := make(chan struct{})
	type fenceResult struct {
		sequence uint64
		err      error
	}
	fenceDone := make(chan fenceResult, 1)
	go func() {
		sequence, err := journal.WithReadFence(func() error {
			if got := trie.GetString("fenced"); got != "value-fenced" {
				return errors.New("point read observed the wrong value")
			}
			close(entered)
			<-release
			return nil
		})
		fenceDone <- fenceResult{sequence: sequence, err: err}
	}()
	<-entered

	writeDone := make(chan CacheCommandResponse, 1)
	go func() {
		writeDone <- journal.ExecuteCommand(trie, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "after-fence",
			Value:   "value-after-fence",
		})
	}()
	select {
	case <-writeDone:
		t.Fatal("journaled write completed while read fence was held")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	fence := <-fenceDone
	if fence.err != nil || fence.sequence != 1 {
		t.Fatalf("WithReadFence() = sequence %d, error %v; want sequence 1 and nil", fence.sequence, fence.err)
	}
	response := <-writeDone
	if !response.OK {
		t.Fatalf("journaled write failed: %s", response.Message)
	}

	subscription, err := journal.Subscribe(context.Background(), CommandJournalSubscribeOptions{
		AfterSequence: fence.sequence,
		Buffer:        1,
		PollInterval:  time.Hour,
	})
	if err != nil {
		t.Fatalf("Subscribe() error = %v", err)
	}
	defer subscription.Close()
	record := receiveJournalSubscriptionRecord(t, subscription)
	if record.Sequence != 2 || record.Request.Key != "after-fence" {
		t.Fatalf("post-fence record = %#v, want sequence 2 for after-fence", record)
	}
}

func TestCommandJournalWithReadFencePropagatesReadError(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	wantErr := errors.New("read failed")
	sequence, err := journal.WithReadFence(func() error { return wantErr })
	if !errors.Is(err, wantErr) {
		t.Fatalf("WithReadFence() error = %v, want %v", err, wantErr)
	}
	if sequence != 0 {
		t.Fatalf("failed fence sequence = %d, want 0", sequence)
	}
}

func TestCommandJournalWithReadFenceRejectsNilCallback(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	sequence, err := journal.WithReadFence(nil)
	if err == nil || sequence != 0 {
		t.Fatalf("WithReadFence(nil) = sequence %d, error %v; want zero and an error", sequence, err)
	}
}

func TestCommandJournalWithReadFenceRejectsClosedJournal(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	if err := journal.Close(); err != nil {
		t.Fatalf("journal.Close() error = %v", err)
	}
	sequence, err := journal.WithReadFence(func() error { return nil })
	if !errors.Is(err, ErrCommandJournalClosed) {
		t.Fatalf("WithReadFence(closed) error = %v, want %v", err, ErrCommandJournalClosed)
	}
	if sequence != 0 {
		t.Fatalf("closed fence sequence = %d, want 0", sequence)
	}
}
