package hatSql

import (
	"context"
	"io"
	"testing"
	"time"
)

func BenchmarkM233ExistingManualRetry(b *testing.B) {
	commit := m233BenchmarkCommit()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
		if err != nil {
			b.Fatal(err)
		}
		attempt := 0
		b.StartTimer()
		for {
			_, err = ledger.CommitContext(context.Background(), commit, func(string) error {
				attempt++
				if attempt == 1 {
					return io.EOF
				}
				return nil
			})
			if err == nil {
				break
			}
		}
	}
}

func BenchmarkM233ExistingSingleAttempt(b *testing.B) {
	commit := m233BenchmarkCommit()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := ledger.CommitContext(context.Background(), commit, func(string) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM233RetryExecutor(b *testing.B) {
	commit := m233BenchmarkCommit()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
		if err != nil {
			b.Fatal(err)
		}
		executor, err := NewSQLSinkRetryExecutor(ledger, SQLSinkRetryOptions{
			InitialBackoff: 1,
			MaxBackoff:     1,
			Sleep:          func(context.Context, time.Duration) error { return nil },
		})
		if err != nil {
			b.Fatal(err)
		}
		attempt := 0
		b.StartTimer()
		if _, err := executor.Commit(context.Background(), commit, func(string) error {
			attempt++
			if attempt == 1 {
				return io.EOF
			}
			return nil
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM233RetryExecutorSingleAttempt(b *testing.B) {
	commit := m233BenchmarkCommit()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		b.StopTimer()
		ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
		if err != nil {
			b.Fatal(err)
		}
		executor, err := NewSQLSinkRetryExecutor(ledger, SQLSinkRetryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := executor.Commit(context.Background(), commit, func(string) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkM233ExistingAlreadyCommitted(b *testing.B) {
	commit := m233BenchmarkCommit()
	ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := ledger.Commit(commit, func(string) error { return nil }); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		committed, err := ledger.Commit(commit, func(string) error { b.Fatal("duplicate invoked callback"); return nil })
		if err != nil || committed {
			b.Fatalf("duplicate result = %v, %v", committed, err)
		}
	}
}

func BenchmarkM233RetryExecutorAlreadyCommitted(b *testing.B) {
	commit := m233BenchmarkCommit()
	ledger, err := NewSQLSinkExactlyOnceLedger(context.Background(), SQLSinkExactlyOnceOptions{})
	if err != nil {
		b.Fatal(err)
	}
	executor, err := NewSQLSinkRetryExecutor(ledger, SQLSinkRetryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := executor.Commit(context.Background(), commit, func(string) error { return nil }); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := executor.Commit(context.Background(), commit, func(string) error { b.Fatal("duplicate invoked callback"); return nil })
		if err != nil || !result.Deduplicated {
			b.Fatalf("duplicate result = %#v, %v", result, err)
		}
	}
}

func m233BenchmarkCommit() SQLSinkCommit {
	return SQLSinkCommit{
		Sink:           "orders",
		TransactionID:  "txn-benchmark",
		IdempotencyKey: "event-benchmark",
		Progress:       []SQLSinkProgress{{Sink: "orders", Partition: "region-a", Frontier: 1}},
	}
}
