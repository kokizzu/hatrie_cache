package hatSql_test

import (
	"context"
	"path/filepath"
	"strconv"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

type mu018BenchmarkCheckpointStore struct {
	snapshot hatSql.SQLSinkExactlyOnceCheckpoint
}

func (store *mu018BenchmarkCheckpointStore) LoadSQLSinkExactlyOnceCheckpoint(context.Context, string) (hatSql.SQLSinkExactlyOnceCheckpoint, bool, error) {
	return store.snapshot, store.snapshot.Capacity != 0, nil
}

func (store *mu018BenchmarkCheckpointStore) SaveSQLSinkExactlyOnceCheckpoint(_ context.Context, _ string, snapshot hatSql.SQLSinkExactlyOnceCheckpoint) error {
	store.snapshot = snapshot
	return nil
}

func mu018BenchmarkCommit(index int) hatSql.SQLSinkCommit {
	return hatSql.SQLSinkCommit{
		Sink:           "warehouse",
		TransactionID:  "txn-" + strconv.Itoa(index),
		IdempotencyKey: "event-" + strconv.Itoa(index),
		Progress:       []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: uint64(index + 1)}},
	}
}

func mu018SeedLedger(b *testing.B, ledger interface {
	Commit(hatSql.SQLSinkCommit, func(string) error) (bool, error)
}, count int) {
	b.Helper()
	b.StopTimer()
	for index := 0; index < count; index++ {
		if committed, err := ledger.Commit(mu018BenchmarkCommit(index), func(string) error { return nil }); err != nil || !committed {
			b.Fatalf("seed Commit(%d) = %t/%v, want true/nil", index, committed, err)
		}
	}
	b.StartTimer()
}

func BenchmarkMU018AfterExactlyOnceLedger(b *testing.B) {
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(nil, hatSql.SQLSinkExactlyOnceOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		commit := mu018BenchmarkCommit(index)
		if committed, err := ledger.Commit(commit, func(string) error { return nil }); err != nil || !committed {
			b.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
		}
	}
}

func BenchmarkMU018AfterExactlyOnceLedgerDuplicate(b *testing.B) {
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(nil, hatSql.SQLSinkExactlyOnceOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	commit := mu018Commit(1, "txn-1", "event-1")
	if committed, err := ledger.Commit(commit, func(string) error { return nil }); err != nil || !committed {
		b.Fatalf("seed Commit() = %t/%v, want true/nil", committed, err)
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if committed, err := ledger.Commit(commit, func(string) error { return nil }); err != nil || committed {
			b.Fatalf("duplicate Commit() = %t/%v, want false/nil", committed, err)
		}
	}
}

func BenchmarkMU018AfterExactlyOnceLedgerMemoryCheckpoint(b *testing.B) {
	store := &mu018BenchmarkCheckpointStore{}
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(nil, hatSql.SQLSinkExactlyOnceOptions{
		Capacity:        1024,
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		b.Fatal(err)
	}
	mu018SeedLedger(b, ledger, 1024)
	b.ResetTimer()
	for index := 1024; index < 1024+b.N; index++ {
		commit := mu018BenchmarkCommit(index)
		if committed, err := ledger.Commit(commit, func(string) error { return nil }); err != nil || !committed {
			b.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
		}
	}
}

func BenchmarkMU018AfterExactlyOnceLedgerFileCheckpoint(b *testing.B) {
	store, err := hatSql.NewFileSQLSinkExactlyOnceCheckpointStore(filepath.Join(b.TempDir(), "sink-checkpoints.json"))
	if err != nil {
		b.Fatal(err)
	}
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(nil, hatSql.SQLSinkExactlyOnceOptions{
		Capacity:        1024,
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		b.Fatal(err)
	}
	mu018SeedLedger(b, ledger, 1024)
	b.ResetTimer()
	for index := 1024; index < 1024+b.N; index++ {
		commit := mu018BenchmarkCommit(index)
		if committed, err := ledger.Commit(commit, func(string) error { return nil }); err != nil || !committed {
			b.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
		}
	}
}
