package hatSql_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

type mu018CheckpointStore struct {
	mu       sync.Mutex
	snapshot hatSql.SQLSinkExactlyOnceCheckpoint
	found    bool
	saveErr  error
}

func (store *mu018CheckpointStore) LoadSQLSinkExactlyOnceCheckpoint(_ context.Context, _ string) (hatSql.SQLSinkExactlyOnceCheckpoint, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.snapshot, store.found, nil
}

func (store *mu018CheckpointStore) SaveSQLSinkExactlyOnceCheckpoint(_ context.Context, _ string, snapshot hatSql.SQLSinkExactlyOnceCheckpoint) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveErr != nil {
		return store.saveErr
	}
	store.snapshot = snapshot
	store.found = true
	return nil
}

func mu018Commit(frontier uint64, transactionID, idempotencyKey string) hatSql.SQLSinkCommit {
	return hatSql.SQLSinkCommit{
		Sink:           "warehouse",
		TransactionID:  transactionID,
		IdempotencyKey: idempotencyKey,
		Progress:       []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: frontier}},
	}
}

func TestSQLSinkExactlyOnceCommitPassesDurableTokenAndRestores(t *testing.T) {
	store := &mu018CheckpointStore{}
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{
		Capacity:        8,
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		t.Fatalf("NewSQLSinkExactlyOnceLedger() error = %v", err)
	}
	commit := mu018Commit(10, "txn-1", "event-1")
	calls := 0
	var callbackKey string
	if committed, err := ledger.Commit(commit, func(key string) error {
		calls++
		callbackKey = key
		return nil
	}); err != nil || !committed {
		t.Fatalf("first Commit() = %t/%v, want true/nil", committed, err)
	}
	if callbackKey != "event-1" || calls != 1 {
		t.Fatalf("callback = %q/%d, want event-1/1", callbackKey, calls)
	}
	if committed, err := ledger.Commit(commit, func(string) error {
		calls++
		return nil
	}); err != nil || committed {
		t.Fatalf("duplicate Commit() = %t/%v, want false/nil", committed, err)
	}
	if calls != 1 {
		t.Fatalf("duplicate callback calls = %d, want 1", calls)
	}

	restored, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{
		Capacity:        8,
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		t.Fatalf("restored NewSQLSinkExactlyOnceLedger() error = %v", err)
	}
	if committed, err := restored.Commit(commit, func(string) error {
		calls++
		return nil
	}); err != nil || committed {
		t.Fatalf("restored duplicate Commit() = %t/%v, want false/nil", committed, err)
	}
	if calls != 1 {
		t.Fatalf("restored callback calls = %d, want 1", calls)
	}
	if frontier, found := restored.Frontier("warehouse", "0"); !found || frontier != 10 {
		t.Fatalf("restored Frontier() = %d/%t, want 10/true", frontier, found)
	}
}

func TestSQLSinkExactlyOnceRejectsConflictsAndStaleFrontiers(t *testing.T) {
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{Capacity: 2})
	if err != nil {
		t.Fatalf("NewSQLSinkExactlyOnceLedger() error = %v", err)
	}
	for index := uint64(1); index <= 3; index++ {
		commit := mu018Commit(index, "txn-"+strconv.FormatUint(index, 10), "event-"+strconv.FormatUint(index, 10))
		if committed, err := ledger.Commit(commit, func(string) error { return nil }); err != nil || !committed {
			t.Fatalf("Commit(%d) = %t/%v, want true/nil", index, committed, err)
		}
	}
	conflict := mu018Commit(4, "txn-4", "event-3")
	if _, err := ledger.Commit(conflict, func(string) error { return nil }); !errors.Is(err, hatSql.ErrSQLSinkExactlyOnceConflict) {
		t.Fatalf("conflicting Commit() error = %v, want conflict", err)
	}
	stale := mu018Commit(2, "txn-stale", "event-stale")
	if _, err := ledger.Commit(stale, func(string) error { return nil }); !errors.Is(err, hatSql.ErrSQLSinkExactlyOnceStale) {
		t.Fatalf("stale Commit() error = %v, want stale", err)
	}
	snapshot := ledger.Snapshot()
	if len(snapshot.Commits) != 2 || snapshot.Commits[0].IdempotencyKey != "event-2" || snapshot.Commits[1].IdempotencyKey != "event-3" {
		t.Fatalf("Snapshot().Commits = %#v, want bounded oldest-first history", snapshot.Commits)
	}
	restored, err := hatSql.NewSQLSinkExactlyOnceLedgerFromSnapshot(snapshot)
	if err != nil {
		t.Fatalf("NewSQLSinkExactlyOnceLedgerFromSnapshot() error = %v", err)
	}
	if frontier, found := restored.Frontier("warehouse", "0"); !found || frontier != 3 {
		t.Fatalf("restored Frontier() = %d/%t, want 3/true", frontier, found)
	}
	snapshot.Progress[0].Frontier = 0
	if err := restored.Restore(snapshot); !errors.Is(err, hatSql.ErrSQLSinkExactlyOnceInvalid) {
		t.Fatalf("invalid Restore() error = %v, want invalid", err)
	}
	if frontier, found := restored.Frontier("warehouse", "0"); !found || frontier != 3 {
		t.Fatalf("failed Restore() changed Frontier() = %d/%t, want 3/true", frontier, found)
	}
}

func TestSQLSinkExactlyOnceRetryAfterCallbackAndCheckpointFailures(t *testing.T) {
	store := &mu018CheckpointStore{}
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		t.Fatalf("NewSQLSinkExactlyOnceLedger() error = %v", err)
	}
	commit := mu018Commit(1, "txn-1", "event-1")
	boom := errors.New("sink unavailable")
	if _, err := ledger.Commit(commit, func(string) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("failed Commit() error = %v, want sink error", err)
	}
	if committed, err := ledger.Commit(commit, func(string) error { return nil }); err != nil || !committed {
		t.Fatalf("retry Commit() = %t/%v, want true/nil", committed, err)
	}

	store.saveErr = errors.New("checkpoint unavailable")
	second := mu018Commit(2, "txn-2", "event-2")
	if _, err := ledger.Commit(second, func(string) error { return nil }); !errors.Is(err, store.saveErr) {
		t.Fatalf("checkpoint failure Commit() error = %v, want checkpoint error", err)
	}
	store.saveErr = nil
	if committed, err := ledger.Commit(second, func(string) error { return nil }); err != nil || !committed {
		t.Fatalf("checkpoint retry Commit() = %t/%v, want true/nil", committed, err)
	}
}

func TestFileSQLSinkExactlyOnceCheckpointStoreRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sink-checkpoints.json")
	store, err := hatSql.NewFileSQLSinkExactlyOnceCheckpointStore(path)
	if err != nil {
		t.Fatalf("NewFileSQLSinkExactlyOnceCheckpointStore() error = %v", err)
	}
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		t.Fatalf("NewSQLSinkExactlyOnceLedger() error = %v", err)
	}
	commit := mu018Commit(7, "txn-7", "event-7")
	if committed, err := ledger.Commit(commit, func(string) error { return nil }); err != nil || !committed {
		t.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}
	if len(data) < 5 || !bytes.Equal(data[:4], []byte("HSE1")) || data[4] != 1 {
		header := data
		if len(header) > 5 {
			header = header[:5]
		}
		t.Fatalf("checkpoint header = %q, want HSE1 version 1", header)
	}
	corruptPath := filepath.Join(t.TempDir(), "corrupt-checkpoints.bin")
	corrupt := append([]byte(nil), data...)
	corrupt[4] = 2
	if err := os.WriteFile(corruptPath, corrupt, 0o600); err != nil {
		t.Fatalf("WriteFile(corrupt) error = %v", err)
	}
	corruptStore, err := hatSql.NewFileSQLSinkExactlyOnceCheckpointStore(corruptPath)
	if err != nil {
		t.Fatalf("NewFileSQLSinkExactlyOnceCheckpointStore(corrupt) error = %v", err)
	}
	if _, _, err := corruptStore.LoadSQLSinkExactlyOnceCheckpoint(context.Background(), "warehouse"); err == nil {
		t.Fatal("corrupt LoadSQLSinkExactlyOnceCheckpoint() error = nil, want invalid format")
	}

	reopenedStore, err := hatSql.NewFileSQLSinkExactlyOnceCheckpointStore(path)
	if err != nil {
		t.Fatalf("reopened NewFileSQLSinkExactlyOnceCheckpointStore() error = %v", err)
	}
	reopened, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{
		CheckpointStore: reopenedStore,
		Name:            "warehouse",
	})
	if err != nil {
		t.Fatalf("reopened NewSQLSinkExactlyOnceLedger() error = %v", err)
	}
	if committed, err := reopened.Commit(commit, func(string) error { t.Fatal("replayed external callback"); return nil }); err != nil || committed {
		t.Fatalf("reopened duplicate Commit() = %t/%v, want false/nil", committed, err)
	}
}

func TestFileSQLSinkExactlyOnceCheckpointStoreReadsLegacyJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy-checkpoints.json")
	legacy := []byte(`{"checkpoints":{"warehouse":{"capacity":8,"commits":[{"sink":"warehouse","transaction_id":"txn-legacy","progress":[{"sink":"warehouse","partition":"0","frontier":9}]}],"progress":[{"sink":"warehouse","partition":"0","frontier":9}]}}}`)
	if err := os.WriteFile(path, legacy, 0o600); err != nil {
		t.Fatalf("WriteFile(legacy) error = %v", err)
	}
	store, err := hatSql.NewFileSQLSinkExactlyOnceCheckpointStore(path)
	if err != nil {
		t.Fatalf("NewFileSQLSinkExactlyOnceCheckpointStore() error = %v", err)
	}
	snapshot, found, err := store.LoadSQLSinkExactlyOnceCheckpoint(context.Background(), "warehouse")
	if err != nil || !found || len(snapshot.Commits) != 1 || snapshot.Commits[0].IdempotencyKey != "txn-legacy" {
		t.Fatalf("legacy LoadSQLSinkExactlyOnceCheckpoint() = %#v/%t/%v, want normalized legacy checkpoint", snapshot, found, err)
	}
}

func TestSQLSinkExactlyOnceUsesTransactionFallbackAndSerializesPartitionDelivery(t *testing.T) {
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{Capacity: 8})
	if err != nil {
		t.Fatalf("NewSQLSinkExactlyOnceLedger() error = %v", err)
	}
	first := mu018Commit(1, "txn-1", "")
	started := make(chan struct{})
	release := make(chan struct{})
	firstResult := make(chan error, 1)
	go func() {
		_, err := ledger.Commit(first, func(key string) error {
			if key != "txn-1" {
				return errors.New("unexpected fallback key")
			}
			close(started)
			<-release
			return nil
		})
		firstResult <- err
	}()
	<-started
	second := mu018Commit(2, "txn-2", "event-2")
	if _, err := ledger.Commit(second, func(string) error { return nil }); !errors.Is(err, hatSql.ErrSQLSinkExactlyOnceInFlight) {
		t.Fatalf("concurrent partition Commit() error = %v, want in-flight", err)
	}
	close(release)
	if err := <-firstResult; err != nil {
		t.Fatalf("first concurrent Commit() error = %v", err)
	}
	if committed, err := ledger.Commit(second, func(string) error { return nil }); err != nil || !committed {
		t.Fatalf("second retry Commit() = %t/%v, want true/nil", committed, err)
	}
}

func TestSQLSinkExactlyOnceDeduplicatesConcurrentSameKey(t *testing.T) {
	ledger, err := hatSql.NewSQLSinkExactlyOnceLedger(context.Background(), hatSql.SQLSinkExactlyOnceOptions{Capacity: 8})
	if err != nil {
		t.Fatalf("NewSQLSinkExactlyOnceLedger() error = %v", err)
	}
	commit := mu018Commit(1, "txn-1", "event-1")
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	results := make(chan struct {
		committed bool
		err       error
	}, 2)
	apply := func(string) error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}
	go func() {
		committed, err := ledger.Commit(commit, apply)
		results <- struct {
			committed bool
			err       error
		}{committed, err}
	}()
	<-started
	go func() {
		committed, err := ledger.Commit(commit, apply)
		results <- struct {
			committed bool
			err       error
		}{committed, err}
	}()
	close(release)
	first := <-results
	second := <-results
	if first.err != nil || second.err != nil || first.committed == second.committed {
		t.Fatalf("concurrent Commit() results = %#v/%#v, want one true and one false without errors", first, second)
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent callback calls = %d, want 1", calls.Load())
	}
}

func TestFileSQLSinkExactlyOnceCheckpointStoreRejectsInsecureFiles(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sink-checkpoints.json")
	if err := os.WriteFile(path, []byte(`{"checkpoints":{}}`), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	store, err := hatSql.NewFileSQLSinkExactlyOnceCheckpointStore(path)
	if err != nil {
		t.Fatalf("NewFileSQLSinkExactlyOnceCheckpointStore() error = %v", err)
	}
	if _, _, err := store.LoadSQLSinkExactlyOnceCheckpoint(context.Background(), "warehouse"); err == nil {
		t.Fatal("LoadSQLSinkExactlyOnceCheckpoint() error = nil, want insecure permission rejection")
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}
	link := filepath.Join(t.TempDir(), "sink-checkpoints-link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}
	linked, err := hatSql.NewFileSQLSinkExactlyOnceCheckpointStore(link)
	if err != nil {
		t.Fatalf("NewFileSQLSinkExactlyOnceCheckpointStore(link) error = %v", err)
	}
	if _, _, err := linked.LoadSQLSinkExactlyOnceCheckpoint(context.Background(), "warehouse"); err == nil {
		t.Fatal("symlink LoadSQLSinkExactlyOnceCheckpoint() error = nil, want rejection")
	}
}
