package hatCache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type journalExactlyOnceTestSink struct {
	mu             sync.Mutex
	sequence       uint64
	loadCalls      int
	beginSequences []uint64
	beginErr       error
	tx             *journalExactlyOnceTestTransaction
	committed      chan *journalExactlyOnceTestTransaction
}

func (sink *journalExactlyOnceTestSink) LoadSequence(context.Context) (uint64, error) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.loadCalls++
	return sink.sequence, nil
}

func (sink *journalExactlyOnceTestSink) Begin(_ context.Context, afterSequence uint64) (CommandJournalExactlyOnceTransaction, error) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.beginSequences = append(sink.beginSequences, afterSequence)
	if sink.beginErr != nil {
		return nil, sink.beginErr
	}
	tx := sink.tx
	if tx == nil {
		tx = &journalExactlyOnceTestTransaction{sink: sink}
		sink.tx = tx
	}
	tx.sink = sink
	return tx, nil
}

type journalExactlyOnceTestTransaction struct {
	sink           *journalExactlyOnceTestSink
	mu             sync.Mutex
	records        []CommandJournalRecord
	commitSequence uint64
	committed      bool
	rolledBack     bool
	writeErr       error
	commitErr      error
	rollbackErr    error
}

func (transaction *journalExactlyOnceTestTransaction) Write(_ context.Context, records []CommandJournalRecord) error {
	transaction.mu.Lock()
	transaction.records = append(transaction.records, records...)
	transaction.mu.Unlock()
	return transaction.writeErr
}

func (transaction *journalExactlyOnceTestTransaction) Commit(_ context.Context, sequence uint64) error {
	transaction.mu.Lock()
	transaction.commitSequence = sequence
	transaction.committed = true
	commitErr := transaction.commitErr
	transaction.mu.Unlock()
	if commitErr != nil {
		return commitErr
	}
	transaction.sink.mu.Lock()
	transaction.sink.sequence = sequence
	transaction.sink.mu.Unlock()
	select {
	case transaction.sink.committed <- transaction:
	default:
	}
	return nil
}

func (transaction *journalExactlyOnceTestTransaction) Rollback(context.Context) error {
	transaction.mu.Lock()
	transaction.rolledBack = true
	rollbackErr := transaction.rollbackErr
	transaction.mu.Unlock()
	return rollbackErr
}

func (transaction *journalExactlyOnceTestTransaction) snapshot() ([]CommandJournalRecord, uint64, bool, bool) {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	return append([]CommandJournalRecord(nil), transaction.records...), transaction.commitSequence, transaction.committed, transaction.rolledBack
}

func newJournalExactlyOnceTestSink(sequence uint64) *journalExactlyOnceTestSink {
	return &journalExactlyOnceTestSink{
		sequence:  sequence,
		committed: make(chan *journalExactlyOnceTestTransaction, 4),
	}
}

func TestCommandJournalExactlyOnceSinkCommitsReplayAndWatermark(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "one")
	appendJournalSubscriptionTestCommand(t, journal, trie, "two")
	appendJournalSubscriptionTestCommand(t, journal, trie, "three")

	sink := newJournalExactlyOnceTestSink(1)
	runner, err := journal.StartCommandJournalExactlyOnceSink(context.Background(), sink, CommandJournalExactlyOnceSinkOptions{
		ReplayLimit:  10,
		Buffer:       2,
		PollInterval: time.Hour,
		BatchSize:    2,
		BatchWait:    time.Hour,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceSink() error = %v", err)
	}
	defer runner.Close()

	select {
	case transaction := <-sink.committed:
		records, sequence, committed, rolledBack := transaction.snapshot()
		if !committed || rolledBack || sequence != 3 {
			t.Fatalf("transaction state = (committed=%v, rolledBack=%v, sequence=%d), want committed sequence 3", committed, rolledBack, sequence)
		}
		if len(records) != 2 || records[0].Request.Key != "two" || records[1].Request.Key != "three" {
			t.Fatalf("transaction records = %#v, want two/three", records)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for exactly-once transaction")
	}
	sink.mu.Lock()
	loadCalls := sink.loadCalls
	beginSequences := append([]uint64(nil), sink.beginSequences...)
	sequence := sink.sequence
	sink.mu.Unlock()
	if loadCalls != 1 || len(beginSequences) != 1 || beginSequences[0] != 1 || sequence != 3 {
		t.Fatalf("sink state = (loads=%d, begins=%v, sequence=%d), want (1, [1], 3)", loadCalls, beginSequences, sequence)
	}
}

func TestCommandJournalExactlyOnceSinkCommitsLiveBatch(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	sink := newJournalExactlyOnceTestSink(0)
	runner, err := journal.StartCommandJournalExactlyOnceSink(context.Background(), sink, CommandJournalExactlyOnceSinkOptions{
		Buffer:       2,
		PollInterval: time.Hour,
		BatchSize:    2,
		BatchWait:    time.Hour,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceSink() error = %v", err)
	}
	defer runner.Close()

	appendJournalSubscriptionTestCommand(t, journal, trie, "live-one")
	appendJournalSubscriptionTestCommand(t, journal, trie, "live-two")
	select {
	case transaction := <-sink.committed:
		records, sequence, committed, rolledBack := transaction.snapshot()
		if !committed || rolledBack || sequence != 2 || len(records) != 2 {
			t.Fatalf("live transaction = (records=%#v, sequence=%d, committed=%v, rolledBack=%v), want two committed records at 2", records, sequence, committed, rolledBack)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live exactly-once transaction")
	}
}

func TestCommandJournalExactlyOnceSinkAdvancesExpectedSequence(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "first")
	appendJournalSubscriptionTestCommand(t, journal, trie, "second")
	sink := newJournalExactlyOnceTestSink(0)
	runner, err := journal.StartCommandJournalExactlyOnceSink(context.Background(), sink, CommandJournalExactlyOnceSinkOptions{
		Buffer:       2,
		PollInterval: time.Hour,
		BatchSize:    1,
		BatchWait:    time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceSink() error = %v", err)
	}
	defer runner.Close()
	for commit := 0; commit < 2; commit++ {
		select {
		case <-sink.committed:
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for transaction %d", commit+1)
		}
	}
	sink.mu.Lock()
	beginSequences := append([]uint64(nil), sink.beginSequences...)
	sink.mu.Unlock()
	if len(beginSequences) != 2 || beginSequences[0] != 0 || beginSequences[1] != 1 {
		t.Fatalf("transaction begin sequences = %v, want [0 1]", beginSequences)
	}
}

func TestCommandJournalExactlyOnceSinkRollsBackWriteFailure(t *testing.T) {
	wantErr := errors.New("transaction write failed")
	journal, trie := openJournalSubscriptionTestFixture(t)
	sink := newJournalExactlyOnceTestSink(0)
	sink.tx = &journalExactlyOnceTestTransaction{sink: sink, writeErr: wantErr}
	runner, err := journal.StartCommandJournalExactlyOnceSink(context.Background(), sink, CommandJournalExactlyOnceSinkOptions{
		Buffer:       1,
		PollInterval: time.Hour,
		BatchSize:    1,
		BatchWait:    time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceSink() error = %v", err)
	}
	appendJournalSubscriptionTestCommand(t, journal, trie, "write-failure")
	if err := runner.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("runner.Wait() error = %v, want %v", err, wantErr)
	}
	_, _, committed, rolledBack := sink.tx.snapshot()
	if committed || !rolledBack {
		t.Fatalf("transaction state = (committed=%v, rolledBack=%v), want rollback only", committed, rolledBack)
	}
}

func TestCommandJournalExactlyOnceSinkStopsOnCommitFailureWithoutRetry(t *testing.T) {
	wantErr := errors.New("transaction commit outcome is unknown")
	journal, trie := openJournalSubscriptionTestFixture(t)
	sink := newJournalExactlyOnceTestSink(0)
	sink.tx = &journalExactlyOnceTestTransaction{sink: sink, commitErr: wantErr}
	runner, err := journal.StartCommandJournalExactlyOnceSink(context.Background(), sink, CommandJournalExactlyOnceSinkOptions{
		Buffer:       1,
		PollInterval: time.Hour,
		BatchSize:    1,
		BatchWait:    time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceSink() error = %v", err)
	}
	appendJournalSubscriptionTestCommand(t, journal, trie, "commit-failure")
	if err := runner.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("runner.Wait() error = %v, want %v", err, wantErr)
	}
	_, _, committed, rolledBack := sink.tx.snapshot()
	if !committed || rolledBack {
		t.Fatalf("transaction state = (committed=%v, rolledBack=%v), want ambiguous commit without rollback", committed, rolledBack)
	}
	sink.mu.Lock()
	beginCount := len(sink.beginSequences)
	sink.mu.Unlock()
	if beginCount != 1 {
		t.Fatalf("transaction begin count = %d, want 1 after commit failure", beginCount)
	}
}

func TestCommandJournalExactlyOnceSinkStopsOnContextCancellation(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	sink := newJournalExactlyOnceTestSink(0)
	runner, err := journal.StartCommandJournalExactlyOnceSink(ctx, sink, CommandJournalExactlyOnceSinkOptions{
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceSink() error = %v", err)
	}
	cancel()
	if err := runner.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("runner.Wait() error = %v, want context.Canceled", err)
	}
}

func TestCommandJournalExactlyOnceSinkRejectsInvalidOptionsAndNilSink(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	if _, err := journal.StartCommandJournalExactlyOnceSink(context.Background(), nil, CommandJournalExactlyOnceSinkOptions{}); err == nil {
		t.Fatal("nil sink error = nil, want error")
	}
	sink := newJournalExactlyOnceTestSink(0)
	for _, options := range []CommandJournalExactlyOnceSinkOptions{
		{BatchSize: -1},
		{BatchSize: MaxCommandJournalSinkBatchSize + 1},
		{BatchWait: -time.Nanosecond},
	} {
		if _, err := journal.StartCommandJournalExactlyOnceSink(context.Background(), sink, options); err == nil {
			t.Fatalf("options %#v error = nil, want validation error", options)
		}
	}
}
