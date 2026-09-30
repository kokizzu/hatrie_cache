package hatCache

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type m231UpsertTestSink struct {
	mu          sync.Mutex
	sequence    uint64
	commitErr   error
	outputs     map[string]CommandJournalExactlyOnceUpsertRecord
	upsertCalls int
	commitCalls int
	nilTx       bool
	lastTx      *m231UpsertTestTransaction
	committed   chan uint64
}

type m231UpsertTestTransaction struct {
	sink       *m231UpsertTestSink
	mu         sync.Mutex
	staged     map[string]CommandJournalExactlyOnceUpsertRecord
	rolledBack bool
}

func newM231UpsertTestSink() *m231UpsertTestSink {
	return &m231UpsertTestSink{
		outputs:   make(map[string]CommandJournalExactlyOnceUpsertRecord),
		committed: make(chan uint64, 4),
	}
}

func (sink *m231UpsertTestSink) LoadSequence(context.Context) (uint64, error) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.sequence, nil
}

func (sink *m231UpsertTestSink) Begin(context.Context, uint64) (CommandJournalExactlyOnceUpsertTransaction, error) {
	if sink.nilTx {
		return nil, nil
	}
	tx := &m231UpsertTestTransaction{
		sink:   sink,
		staged: make(map[string]CommandJournalExactlyOnceUpsertRecord),
	}
	sink.mu.Lock()
	sink.lastTx = tx
	sink.mu.Unlock()
	return tx, nil
}

func (transaction *m231UpsertTestTransaction) Upsert(_ context.Context, record CommandJournalExactlyOnceUpsertRecord) error {
	transaction.mu.Lock()
	transaction.staged[record.OutputIdentity] = record
	transaction.mu.Unlock()
	transaction.sink.mu.Lock()
	transaction.sink.upsertCalls++
	transaction.sink.mu.Unlock()
	return nil
}

func (transaction *m231UpsertTestTransaction) Commit(_ context.Context, sequence uint64) error {
	transaction.mu.Lock()
	staged := make(map[string]CommandJournalExactlyOnceUpsertRecord, len(transaction.staged))
	for identity, record := range transaction.staged {
		staged[identity] = record
	}
	transaction.mu.Unlock()

	transaction.sink.mu.Lock()
	for identity, record := range staged {
		transaction.sink.outputs[identity] = record
	}
	transaction.sink.commitCalls++
	commitErr := transaction.sink.commitErr
	if commitErr == nil {
		transaction.sink.sequence = sequence
	}
	transaction.sink.mu.Unlock()
	if commitErr != nil {
		return commitErr
	}
	select {
	case transaction.sink.committed <- sequence:
	default:
	}
	return nil
}

func (transaction *m231UpsertTestTransaction) Rollback(context.Context) error {
	transaction.mu.Lock()
	transaction.rolledBack = true
	transaction.mu.Unlock()
	return nil
}

func TestM231ExactlyOnceUpsertSinkUsesStableOutputIdentities(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "one")
	appendJournalSubscriptionTestCommand(t, journal, trie, "two")

	sink := newM231UpsertTestSink()
	runner, err := journal.StartCommandJournalExactlyOnceUpsertSink(context.Background(), sink, CommandJournalExactlyOnceUpsertSinkOptions{
		Buffer:       2,
		PollInterval: time.Hour,
		BatchSize:    2,
		BatchWait:    time.Hour,
		Identity: func(record CommandJournalRecord) (string, error) {
			return "order:" + record.Request.Key, nil
		},
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceUpsertSink() error = %v", err)
	}
	defer runner.Close()

	select {
	case sequence := <-sink.committed:
		if sequence != 2 {
			t.Fatalf("committed sequence = %d, want 2", sequence)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for exactly-once upsert transaction")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.sequence != 2 || len(sink.outputs) != 2 || sink.upsertCalls != 2 {
		t.Fatalf("sink state = (sequence=%d, outputs=%d, upserts=%d), want (2, 2, 2)", sink.sequence, len(sink.outputs), sink.upsertCalls)
	}
	for _, key := range []string{"one", "two"} {
		identity := "order:" + key
		record, ok := sink.outputs[identity]
		if !ok || record.OutputIdentity != identity || record.Record.Request.Key != key {
			t.Fatalf("output[%q] = %#v, want stable identity and key", identity, record)
		}
	}
}

func TestM231ExactlyOnceUpsertSinkUsesDefaultSequenceIdentity(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "default-identity")
	sink := newM231UpsertTestSink()
	runner, err := journal.StartCommandJournalExactlyOnceUpsertSink(context.Background(), sink, CommandJournalExactlyOnceUpsertSinkOptions{
		Buffer:       1,
		PollInterval: time.Hour,
		BatchSize:    1,
		BatchWait:    time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceUpsertSink() error = %v", err)
	}
	defer runner.Close()
	select {
	case <-sink.committed:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for default-identity transaction")
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	record, ok := sink.outputs["journal:1"]
	if !ok || record.OutputIdentity != "journal:1" || record.JournalSequence != 1 || record.Record.Sequence != 1 {
		t.Fatalf("default output = %#v, want journal:1 for sequence 1", record)
	}
}

func TestM231ExactlyOnceUpsertSinkRetriesCommitUncertaintyByIdentity(t *testing.T) {
	wantErr := errors.New("transaction commit outcome is unknown")
	journal, trie := openJournalSubscriptionTestFixture(t)
	appendJournalSubscriptionTestCommand(t, journal, trie, "commit-failure")
	sink := newM231UpsertTestSink()
	sink.commitErr = wantErr
	identity := func(record CommandJournalRecord) (string, error) {
		return "order:" + record.Request.Key, nil
	}
	options := CommandJournalExactlyOnceUpsertSinkOptions{
		Buffer:       1,
		PollInterval: time.Hour,
		BatchSize:    1,
		BatchWait:    time.Nanosecond,
		Identity:     identity,
	}

	first, err := journal.StartCommandJournalExactlyOnceUpsertSink(context.Background(), sink, options)
	if err != nil {
		t.Fatalf("first StartCommandJournalExactlyOnceUpsertSink() error = %v", err)
	}
	if err := first.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("first runner.Wait() error = %v, want %v", err, wantErr)
	}

	sink.mu.Lock()
	if sink.sequence != 0 || len(sink.outputs) != 1 || sink.upsertCalls != 1 {
		t.Fatalf("after uncertain commit = (sequence=%d, outputs=%d, upserts=%d), want (0, 1, 1)", sink.sequence, len(sink.outputs), sink.upsertCalls)
	}
	sink.commitErr = nil
	sink.mu.Unlock()

	second, err := journal.StartCommandJournalExactlyOnceUpsertSink(context.Background(), sink, options)
	if err != nil {
		t.Fatalf("second StartCommandJournalExactlyOnceUpsertSink() error = %v", err)
	}
	select {
	case sequence := <-sink.committed:
		if sequence != 1 {
			t.Fatalf("retry committed sequence = %d, want 1", sequence)
		}
	case <-time.After(time.Second):
		second.Close()
		t.Fatal("timed out waiting for retry transaction")
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second.Close() error = %v", err)
	}

	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.sequence != 1 || len(sink.outputs) != 1 || sink.upsertCalls != 2 || sink.commitCalls != 2 {
		t.Fatalf("after retry = (sequence=%d, outputs=%d, upserts=%d, commits=%d), want (1, 1, 2, 2)", sink.sequence, len(sink.outputs), sink.upsertCalls, sink.commitCalls)
	}
}

func TestM231ExactlyOnceUpsertSinkRejectsInvalidOutputIdentity(t *testing.T) {
	tests := []struct {
		name     string
		identity CommandJournalExactlyOnceUpsertIdentityFunc
		wantErr  error
		commands []string
	}{
		{
			name: "empty",
			identity: func(CommandJournalRecord) (string, error) {
				return "", nil
			},
			wantErr:  ErrCommandJournalExactlyOnceUpsertIdentityInvalid,
			commands: []string{"empty-identity"},
		},
		{
			name: "duplicate",
			identity: func(CommandJournalRecord) (string, error) {
				return "same-output", nil
			},
			wantErr:  ErrCommandJournalExactlyOnceUpsertIdentityDuplicate,
			commands: []string{"duplicate-one", "duplicate-two"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			journal, trie := openJournalSubscriptionTestFixture(t)
			for _, command := range test.commands {
				appendJournalSubscriptionTestCommand(t, journal, trie, command)
			}
			sink := newM231UpsertTestSink()
			runner, err := journal.StartCommandJournalExactlyOnceUpsertSink(context.Background(), sink, CommandJournalExactlyOnceUpsertSinkOptions{
				Buffer:       2,
				PollInterval: time.Hour,
				BatchSize:    len(test.commands),
				BatchWait:    time.Nanosecond,
				Identity:     test.identity,
			})
			if err != nil {
				t.Fatalf("StartCommandJournalExactlyOnceUpsertSink() error = %v", err)
			}
			if err := runner.Wait(); !errors.Is(err, test.wantErr) {
				t.Fatalf("runner.Wait() error = %v, want %v", err, test.wantErr)
			}
			sink.mu.Lock()
			lastTx := sink.lastTx
			outputs := len(sink.outputs)
			sink.mu.Unlock()
			if lastTx == nil {
				t.Fatal("sink did not create a transaction")
			}
			lastTx.mu.Lock()
			rolledBack := lastTx.rolledBack
			lastTx.mu.Unlock()
			if !rolledBack || outputs != 0 {
				t.Fatalf("invalid identity transaction = (rolledBack=%v, outputs=%d), want rollback and no output", rolledBack, outputs)
			}
		})
	}
}

func TestM231ExactlyOnceUpsertSinkRejectsNilTransaction(t *testing.T) {
	journal, trie := openJournalSubscriptionTestFixture(t)
	sink := newM231UpsertTestSink()
	sink.nilTx = true
	runner, err := journal.StartCommandJournalExactlyOnceUpsertSink(context.Background(), sink, CommandJournalExactlyOnceUpsertSinkOptions{
		Buffer:       1,
		PollInterval: time.Hour,
		BatchSize:    1,
		BatchWait:    time.Nanosecond,
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceUpsertSink() error = %v", err)
	}
	appendJournalSubscriptionTestCommand(t, journal, trie, "nil-transaction")
	if err := runner.Wait(); !errors.Is(err, ErrNilCommandJournalExactlyOnceUpsertTransaction) {
		t.Fatalf("runner.Wait() error = %v, want nil-transaction error", err)
	}
}

func TestM231ExactlyOnceUpsertSinkRejectsUnsafeIdentity(t *testing.T) {
	for _, test := range []struct {
		name     string
		identity string
	}{
		{name: "empty", identity: ""},
		{name: "nul", identity: "safe\x00unsafe"},
		{name: "too-long", identity: strings.Repeat("x", MaxCommandJournalExactlyOnceUpsertIdentityBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			journal, trie := openJournalSubscriptionTestFixture(t)
			sink := newM231UpsertTestSink()
			runner, err := journal.StartCommandJournalExactlyOnceUpsertSink(context.Background(), sink, CommandJournalExactlyOnceUpsertSinkOptions{
				Buffer:       1,
				PollInterval: time.Hour,
				BatchSize:    1,
				BatchWait:    time.Nanosecond,
				Identity: func(CommandJournalRecord) (string, error) {
					return test.identity, nil
				},
			})
			if err != nil {
				t.Fatalf("StartCommandJournalExactlyOnceUpsertSink() error = %v", err)
			}
			appendJournalSubscriptionTestCommand(t, journal, trie, test.name)
			if err := runner.Wait(); !errors.Is(err, ErrCommandJournalExactlyOnceUpsertIdentityInvalid) {
				t.Fatalf("runner.Wait() error = %v, want invalid-identity error", err)
			}
		})
	}
}

func TestM231ExactlyOnceUpsertSinkPropagatesIdentityError(t *testing.T) {
	wantErr := errors.New("identity lookup failed")
	journal, trie := openJournalSubscriptionTestFixture(t)
	sink := newM231UpsertTestSink()
	runner, err := journal.StartCommandJournalExactlyOnceUpsertSink(context.Background(), sink, CommandJournalExactlyOnceUpsertSinkOptions{
		Buffer:       1,
		PollInterval: time.Hour,
		BatchSize:    1,
		BatchWait:    time.Nanosecond,
		Identity: func(CommandJournalRecord) (string, error) {
			return "", wantErr
		},
	})
	if err != nil {
		t.Fatalf("StartCommandJournalExactlyOnceUpsertSink() error = %v", err)
	}
	appendJournalSubscriptionTestCommand(t, journal, trie, "identity-error")
	if err := runner.Wait(); !errors.Is(err, wantErr) {
		t.Fatalf("runner.Wait() error = %v, want %v", err, wantErr)
	}
}
