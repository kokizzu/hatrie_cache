package hatCache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type journalSinkTestSink struct {
	mu      sync.Mutex
	batches [][]CommandJournalRecord
	called  chan struct{}
	err     error
}

func (sink *journalSinkTestSink) Write(_ context.Context, records []CommandJournalRecord) error {
	batch := append([]CommandJournalRecord(nil), records...)
	sink.mu.Lock()
	sink.batches = append(sink.batches, batch)
	sink.mu.Unlock()
	select {
	case sink.called <- struct{}{}:
	default:
	}
	return sink.err
}

func (sink *journalSinkTestSink) snapshot() [][]CommandJournalRecord {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	result := make([][]CommandJournalRecord, len(sink.batches))
	for index := range sink.batches {
		result[index] = append([]CommandJournalRecord(nil), sink.batches[index]...)
	}
	return result
}

type journalSinkTestCheckpoint struct {
	mu       sync.Mutex
	sequence uint64
	loads    int
	saves    []uint64
	saved    chan uint64
	err      error
}

func (checkpoint *journalSinkTestCheckpoint) Load(context.Context) (uint64, error) {
	checkpoint.mu.Lock()
	defer checkpoint.mu.Unlock()
	checkpoint.loads++
	return checkpoint.sequence, nil
}

func (checkpoint *journalSinkTestCheckpoint) Save(_ context.Context, sequence uint64) error {
	checkpoint.mu.Lock()
	checkpoint.sequence = sequence
	checkpoint.saves = append(checkpoint.saves, sequence)
	checkpoint.mu.Unlock()
	select {
	case checkpoint.saved <- sequence:
	default:
	}
	return checkpoint.err
}

func TestCommandJournalSinkDeliversCheckpointedReplayAndPersistsSequence(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "one")
	appendJournalSubscriptionTestCommand(t, journal, trie, "two")
	appendJournalSubscriptionTestCommand(t, journal, trie, "three")

	sink := &journalSinkTestSink{called: make(chan struct{}, 1)}
	checkpoint := &journalSinkTestCheckpoint{sequence: 1, saved: make(chan uint64, 1)}
	runner, err := journal.StartCommandJournalSink(context.Background(), sink, CommandJournalSinkOptions{
		ReplayLimit:     10,
		Buffer:          2,
		PollInterval:    time.Hour,
		BatchSize:       2,
		BatchWait:       time.Hour,
		CheckpointStore: checkpoint,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalSink() error = %v", err)
	}
	defer runner.Close()

	select {
	case <-sink.called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sink batch")
	}
	select {
	case sequence := <-checkpoint.saved:
		if sequence != 3 {
			t.Fatalf("checkpoint sequence = %d, want 3", sequence)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sink checkpoint")
	}
	batches := sink.snapshot()
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("sink batches = %#v, want one batch of two records", batches)
	}
	for index, key := range []string{"two", "three"} {
		record := batches[0][index]
		if record.Sequence != uint64(index+2) || record.Request.Key != key {
			t.Fatalf("sink record = %#v, want sequence %d for %s", record, index+2, key)
		}
	}
	checkpoint.mu.Lock()
	loads := checkpoint.loads
	checkpoint.mu.Unlock()
	if loads != 1 {
		t.Fatalf("checkpoint loads = %d, want 1", loads)
	}
}

func TestCommandJournalSinkDeliversLiveBatch(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	sink := &journalSinkTestSink{called: make(chan struct{}, 1)}
	runner, err := journal.StartCommandJournalSink(context.Background(), sink, CommandJournalSinkOptions{
		Buffer:       2,
		PollInterval: time.Hour,
		BatchSize:    2,
		BatchWait:    time.Hour,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalSink() error = %v", err)
	}
	defer runner.Close()

	appendJournalSubscriptionTestCommand(t, journal, trie, "live-one")
	appendJournalSubscriptionTestCommand(t, journal, trie, "live-two")
	select {
	case <-sink.called:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for live sink batch")
	}
	batches := sink.snapshot()
	if len(batches) != 1 || len(batches[0]) != 2 {
		t.Fatalf("live sink batches = %#v, want one batch of two records", batches)
	}
	if batches[0][0].Request.Key != "live-one" || batches[0][1].Request.Key != "live-two" {
		t.Fatalf("live sink records = %#v, want live-one/live-two", batches[0])
	}
}

func TestCommandJournalSinkReportsDeliveryError(t *testing.T) {
	wantErr := errors.New("sink unavailable")
	journal, trie := openJournalSubscriptionTestFixture(t)
	sink := &journalSinkTestSink{called: make(chan struct{}, 1), err: wantErr}
	runner, err := journal.StartCommandJournalSink(context.Background(), sink, CommandJournalSinkOptions{
		Buffer:       1,
		PollInterval: time.Hour,
		BatchSize:    1,
		BatchWait:    time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalSink() error = %v", err)
	}
	appendJournalSubscriptionTestCommand(t, journal, trie, "failure")
	if err := runner.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("runner.Wait() error = %v, want %v", err, wantErr)
	}
}

func TestCommandJournalSinkReportsCheckpointError(t *testing.T) {
	wantErr := errors.New("checkpoint unavailable")
	journal, trie := openJournalSubscriptionTestFixture(t)
	sink := &journalSinkTestSink{called: make(chan struct{}, 1)}
	checkpoint := &journalSinkTestCheckpoint{saved: make(chan uint64, 1), err: wantErr}
	runner, err := journal.StartCommandJournalSink(context.Background(), sink, CommandJournalSinkOptions{
		Buffer:          1,
		PollInterval:    time.Hour,
		BatchSize:       1,
		BatchWait:       time.Nanosecond,
		CheckpointStore: checkpoint,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalSink() error = %v", err)
	}
	appendJournalSubscriptionTestCommand(t, journal, trie, "checkpoint-failure")
	if err := runner.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("runner.Wait() error = %v, want %v", err, wantErr)
	}
}

func TestCommandJournalSinkCloseIsIdempotent(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	sink := &journalSinkTestSink{called: make(chan struct{}, 1)}
	runner, err := journal.StartCommandJournalSink(context.Background(), sink, CommandJournalSinkOptions{})
	if err != nil {
		t.Fatalf("StartCommandJournalSink() error = %v", err)
	}
	if err := runner.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := runner.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := runner.Err(); err != nil {
		t.Fatalf("runner.Err() = %v, want nil", err)
	}
}

func TestCommandJournalSinkStopsOnContextCancellation(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	sink := &journalSinkTestSink{called: make(chan struct{}, 1)}
	runner, err := journal.StartCommandJournalSink(ctx, sink, CommandJournalSinkOptions{
		PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalSink() error = %v", err)
	}
	cancel()
	if err := runner.Wait(); !errors.Is(err, context.Canceled) {
		t.Fatalf("runner.Wait() error = %v, want context.Canceled", err)
	}
}

func TestCommandJournalSinkRejectsInvalidOptionsAndNilSink(t *testing.T) {
	journal, _ := openJournalSubscriptionTestFixture(t)
	if _, err := journal.StartCommandJournalSink(context.Background(), nil, CommandJournalSinkOptions{}); err == nil {
		t.Fatal("nil sink error = nil, want error")
	}
	for _, options := range []CommandJournalSinkOptions{
		{BatchSize: -1},
		{BatchSize: MaxCommandJournalSinkBatchSize + 1},
		{BatchWait: -time.Nanosecond},
	} {
		if _, err := journal.StartCommandJournalSink(context.Background(), sinkForJournalSinkTest(), options); err == nil {
			t.Fatalf("options %#v error = nil, want validation error", options)
		}
	}
}

func sinkForJournalSinkTest() CommandJournalSink {
	return &journalSinkTestSink{called: make(chan struct{}, 1)}
}
