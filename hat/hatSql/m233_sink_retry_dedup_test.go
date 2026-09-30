package hatSql

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"
)

var errM233Permanent = errors.New("permanent sink rejection")

func TestM233RetryDeduplicatesConnectionDropAfterApply(t *testing.T) {
	ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewSQLSinkRetryExecutor(ledger, SQLSinkRetryOptions{
		MaxAttempts:    3,
		InitialBackoff: 1,
		MaxBackoff:     1,
		Sleep:          func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	commit := SQLSinkCommit{
		Sink:           "orders",
		TransactionID:  "txn-1",
		IdempotencyKey: "event-1",
		Progress:       []SQLSinkProgress{{Sink: "orders", Partition: "region-a", Frontier: 7}},
	}
	var calls int32
	seen := make(map[string]int)
	result, err := executor.Commit(context.Background(), commit, func(key string) error {
		calls++
		seen[key]++
		if calls == 1 {
			return io.EOF
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if result.Attempts != 2 || result.Deduplicated || !result.Committed {
		t.Fatalf("result = %#v, want two attempts and committed non-duplicate", result)
	}
	if atomic.LoadInt32(&calls) != 2 || seen["event-1"] != 2 {
		t.Fatalf("calls = %d, keys = %#v, want same key twice", calls, seen)
	}
}

func TestM233RetryDoesNotRepeatPermanentErrors(t *testing.T) {
	ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewSQLSinkRetryExecutor(ledger, SQLSinkRetryOptions{
		MaxAttempts: 3,
		Retryable:   func(error) bool { return false },
	})
	if err != nil {
		t.Fatal(err)
	}
	commit := m233TestCommit()
	var calls int
	result, err := executor.Commit(context.Background(), commit, func(string) error {
		calls++
		return errM233Permanent
	})
	if !errors.Is(err, errM233Permanent) || calls != 1 || result.Attempts != 1 {
		t.Fatalf("error = %v, calls = %d, result = %#v", err, calls, result)
	}
}

func TestM233RetryExhaustsBoundedAttempts(t *testing.T) {
	ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewSQLSinkRetryExecutor(ledger, SQLSinkRetryOptions{
		MaxAttempts:    2,
		InitialBackoff: 1,
		MaxBackoff:     1,
		Retryable:      func(error) bool { return true },
		Sleep:          func(context.Context, time.Duration) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	commit := m233TestCommit()
	var calls int
	result, err := executor.Commit(context.Background(), commit, func(string) error {
		calls++
		return io.ErrUnexpectedEOF
	})
	if !errors.Is(err, io.ErrUnexpectedEOF) || calls != 2 || result.Attempts != 2 || result.Committed {
		t.Fatalf("error = %v, calls = %d, result = %#v", err, calls, result)
	}
}

func TestM233RetryShortCircuitsCommittedTransaction(t *testing.T) {
	ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewSQLSinkRetryExecutor(ledger, SQLSinkRetryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	commit := m233TestCommit()
	if _, err := executor.Commit(context.Background(), commit, func(string) error { return nil }); err != nil {
		t.Fatal(err)
	}
	var calls int
	result, err := executor.Commit(context.Background(), commit, func(string) error {
		calls++
		return errM233Permanent
	})
	if err != nil || calls != 0 || result.Attempts != 1 || !result.Committed || !result.Deduplicated {
		t.Fatalf("error = %v, calls = %d, result = %#v", err, calls, result)
	}
}

func TestM233RetryDefaultClassifier(t *testing.T) {
	if !DefaultSQLSinkRetryable(io.EOF) || !DefaultSQLSinkRetryable(io.ErrUnexpectedEOF) {
		t.Fatal("default classifier must retry disconnected stream errors")
	}
	if DefaultSQLSinkRetryable(errM233Permanent) || DefaultSQLSinkRetryable(context.Canceled) {
		t.Fatal("default classifier retried a permanent or canceled error")
	}
}

func TestM233RetryRejectsInvalidOptionsAndNilCalls(t *testing.T) {
	if _, err := NewSQLSinkRetryExecutor(nil, SQLSinkRetryOptions{}); !errors.Is(err, ErrSQLSinkRetryLedgerRequired) {
		t.Fatalf("nil ledger error = %v", err)
	}
	ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, options := range []SQLSinkRetryOptions{
		{MaxAttempts: -1},
		{MaxAttempts: MaxSQLSinkRetryAttempts + 1},
		{InitialBackoff: -1},
		{InitialBackoff: 2, MaxBackoff: 1},
	} {
		if _, err := NewSQLSinkRetryExecutor(ledger, options); !errors.Is(err, ErrSQLSinkRetryOptionsInvalid) {
			t.Fatalf("options %#v error = %v", options, err)
		}
	}
	executor, err := NewSQLSinkRetryExecutor(ledger, SQLSinkRetryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executor.Commit(context.Background(), m233TestCommit(), nil); !errors.Is(err, ErrSQLSinkRetryApplyRequired) {
		t.Fatalf("nil callback error = %v", err)
	}
	var nilExecutor *SQLSinkRetryExecutor
	if _, err := nilExecutor.Commit(context.Background(), m233TestCommit(), func(string) error { return nil }); !errors.Is(err, ErrSQLSinkRetryExecutorNil) {
		t.Fatalf("nil executor error = %v", err)
	}
}

func TestM233RetryStopsWhenSleepContextIsCanceled(t *testing.T) {
	ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := NewSQLSinkRetryExecutor(ledger, SQLSinkRetryOptions{
		MaxAttempts:    3,
		InitialBackoff: 1,
		MaxBackoff:     1,
		Retryable:      func(error) bool { return true },
		Sleep:          func(context.Context, time.Duration) error { return context.Canceled },
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := executor.Commit(context.Background(), m233TestCommit(), func(string) error { return io.EOF })
	if !errors.Is(err, context.Canceled) || result.Attempts != 1 || result.Committed {
		t.Fatalf("error = %v, result = %#v", err, result)
	}
}

func m233TestCommit() SQLSinkCommit {
	return SQLSinkCommit{
		Sink:           "orders",
		TransactionID:  "txn-test",
		IdempotencyKey: "event-test",
		Progress:       []SQLSinkProgress{{Sink: "orders", Partition: "region-a", Frontier: 1}},
	}
}
