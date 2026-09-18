package hatSql_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

type mz020CheckpointStore struct {
	mu       sync.Mutex
	snapshot hatSql.SQLSinkTwoPhaseCheckpoint
	found    bool
	saves    int
	saveErr  error
}

func (store *mz020CheckpointStore) LoadSQLSinkTwoPhaseCheckpoint(_ context.Context, _ string) (hatSql.SQLSinkTwoPhaseCheckpoint, bool, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.snapshot, store.found, nil
}

func (store *mz020CheckpointStore) SaveSQLSinkTwoPhaseCheckpoint(_ context.Context, _ string, snapshot hatSql.SQLSinkTwoPhaseCheckpoint) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.saveErr != nil {
		return store.saveErr
	}
	store.snapshot = snapshot
	store.found = true
	store.saves++
	return nil
}

type mz020Participant struct {
	mu           sync.Mutex
	prepareCalls int
	commitCalls  int
	prepareErr   error
	commitErrors []error
}

type mz020BlockingParticipant struct {
	prepareStarted chan struct{}
	prepareRelease chan struct{}
	prepareCalls   atomic.Int32
}

func (participant *mz020BlockingParticipant) Prepare(context.Context, string) error {
	if participant.prepareCalls.Add(1) == 1 {
		close(participant.prepareStarted)
		<-participant.prepareRelease
	}
	return nil
}

func (participant *mz020BlockingParticipant) Commit(context.Context, string) error { return nil }

func (participant *mz020Participant) Prepare(context.Context, string) error {
	participant.mu.Lock()
	defer participant.mu.Unlock()
	participant.prepareCalls++
	return participant.prepareErr
}

func (participant *mz020Participant) Commit(context.Context, string) error {
	participant.mu.Lock()
	defer participant.mu.Unlock()
	index := participant.commitCalls
	participant.commitCalls++
	if index < len(participant.commitErrors) {
		return participant.commitErrors[index]
	}
	return nil
}

func mz020Commit(frontier uint64, transactionID, idempotencyKey string) hatSql.SQLSinkCommit {
	return hatSql.SQLSinkCommit{
		Sink:           "warehouse",
		TransactionID:  transactionID,
		IdempotencyKey: idempotencyKey,
		Progress:       []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: frontier}},
	}
}

func TestMZ020PreparePersistsBeforePublicationAndCommitAdvancesProgress(t *testing.T) {
	store := &mz020CheckpointStore{}
	coordinator, err := hatSql.NewSQLSinkTwoPhaseCoordinator(context.Background(), hatSql.SQLSinkTwoPhaseOptions{
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		t.Fatalf("NewSQLSinkTwoPhaseCoordinator() error = %v", err)
	}
	participant := &mz020Participant{}
	commit := mz020Commit(10, "txn-1", "event-1")
	prepared, err := coordinator.Prepare(context.Background(), commit, participant)
	if err != nil || !prepared {
		t.Fatalf("Prepare() = %t/%v, want true/nil", prepared, err)
	}
	if participant.prepareCalls != 1 || participant.commitCalls != 0 {
		t.Fatalf("participant calls after Prepare() = %d/%d, want 1/0", participant.prepareCalls, participant.commitCalls)
	}
	if frontier, found := coordinator.Frontier("warehouse", "0"); found || frontier != 0 {
		t.Fatalf("Frontier() after Prepare() = %d/%t, want 0/false", frontier, found)
	}
	preparedSnapshot := coordinator.Snapshot()
	if len(preparedSnapshot.Entries) != 1 || preparedSnapshot.Entries[0].State != hatSql.SQLSinkTwoPhasePrepared || len(preparedSnapshot.Progress) != 0 {
		t.Fatalf("prepared Snapshot() = %#v, want prepared entry without progress", preparedSnapshot)
	}

	committed, err := coordinator.Commit(context.Background(), commit, participant)
	if err != nil || !committed {
		t.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
	}
	if participant.commitCalls != 1 {
		t.Fatalf("participant commit calls = %d, want 1", participant.commitCalls)
	}
	if frontier, found := coordinator.Frontier("warehouse", "0"); !found || frontier != 10 {
		t.Fatalf("Frontier() after Commit() = %d/%t, want 10/true", frontier, found)
	}
	committedSnapshot := coordinator.Snapshot()
	if len(committedSnapshot.Entries) != 1 || committedSnapshot.Entries[0].State != hatSql.SQLSinkTwoPhaseCommitted || len(committedSnapshot.Progress) != 1 {
		t.Fatalf("committed Snapshot() = %#v, want committed entry with progress", committedSnapshot)
	}
	duplicate, err := coordinator.Commit(context.Background(), commit, participant)
	if err != nil || duplicate {
		t.Fatalf("duplicate Commit() = %t/%v, want false/nil", duplicate, err)
	}
	if participant.commitCalls != 1 {
		t.Fatalf("duplicate participant commit calls = %d, want 1", participant.commitCalls)
	}
}

func TestMZ020PreparedCheckpointRecoversAndRetriesPublication(t *testing.T) {
	store := &mz020CheckpointStore{}
	first, err := hatSql.NewSQLSinkTwoPhaseCoordinator(context.Background(), hatSql.SQLSinkTwoPhaseOptions{
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		t.Fatalf("first coordinator error = %v", err)
	}
	commit := mz020Commit(20, "txn-2", "event-2")
	if prepared, err := first.Prepare(context.Background(), commit, &mz020Participant{}); err != nil || !prepared {
		t.Fatalf("first Prepare() = %t/%v, want true/nil", prepared, err)
	}

	second, err := hatSql.NewSQLSinkTwoPhaseCoordinator(context.Background(), hatSql.SQLSinkTwoPhaseOptions{
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		t.Fatalf("recovered coordinator error = %v", err)
	}
	boom := errors.New("sink unavailable")
	participant := &mz020Participant{commitErrors: []error{boom}}
	if _, err := second.Commit(context.Background(), commit, participant); !errors.Is(err, boom) {
		t.Fatalf("failed recovered Commit() error = %v, want sink error", err)
	}
	if snapshot := second.Snapshot(); len(snapshot.Entries) != 1 || snapshot.Entries[0].State != hatSql.SQLSinkTwoPhasePrepared || len(snapshot.Progress) != 0 {
		t.Fatalf("failed recovered Snapshot() = %#v, want prepared state retained", snapshot)
	}
	if committed, err := second.Commit(context.Background(), commit, participant); err != nil || !committed {
		t.Fatalf("retry recovered Commit() = %t/%v, want true/nil", committed, err)
	}
	if participant.commitCalls != 2 {
		t.Fatalf("recovered participant commit calls = %d, want 2", participant.commitCalls)
	}
}

func TestMZ020CommitCheckpointFailureKeepsPreparedState(t *testing.T) {
	store := &mz020CheckpointStore{}
	coordinator, err := hatSql.NewSQLSinkTwoPhaseCoordinator(context.Background(), hatSql.SQLSinkTwoPhaseOptions{
		CheckpointStore: store,
		Name:            "warehouse",
	})
	if err != nil {
		t.Fatalf("NewSQLSinkTwoPhaseCoordinator() error = %v", err)
	}
	commit := mz020Commit(30, "txn-3", "event-3")
	participant := &mz020Participant{}
	if _, err := coordinator.Prepare(context.Background(), commit, participant); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	store.saveErr = errors.New("checkpoint unavailable")
	if _, err := coordinator.Commit(context.Background(), commit, participant); !errors.Is(err, store.saveErr) {
		t.Fatalf("checkpoint failure Commit() error = %v, want checkpoint error", err)
	}
	if snapshot := coordinator.Snapshot(); len(snapshot.Entries) != 1 || snapshot.Entries[0].State != hatSql.SQLSinkTwoPhasePrepared || len(snapshot.Progress) != 0 {
		t.Fatalf("checkpoint failure Snapshot() = %#v, want prepared state retained", snapshot)
	}
	store.saveErr = nil
	if committed, err := coordinator.Commit(context.Background(), commit, participant); err != nil || !committed {
		t.Fatalf("checkpoint retry Commit() = %t/%v, want true/nil", committed, err)
	}
	if participant.commitCalls != 2 {
		t.Fatalf("checkpoint retry participant commit calls = %d, want 2", participant.commitCalls)
	}
}

func TestMZ020RejectsStaleAndConflictingProgress(t *testing.T) {
	coordinator, err := hatSql.NewSQLSinkTwoPhaseCoordinator(context.Background(), hatSql.SQLSinkTwoPhaseOptions{})
	if err != nil {
		t.Fatalf("NewSQLSinkTwoPhaseCoordinator() error = %v", err)
	}
	participant := &mz020Participant{}
	first := mz020Commit(40, "txn-4", "event-4")
	if _, err := coordinator.Prepare(context.Background(), first, participant); err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	if _, err := coordinator.Commit(context.Background(), first, participant); err != nil {
		t.Fatalf("first Commit() error = %v", err)
	}
	stale := mz020Commit(40, "txn-stale", "event-stale")
	if _, err := coordinator.Prepare(context.Background(), stale, participant); !errors.Is(err, hatSql.ErrSQLSinkTwoPhaseStale) {
		t.Fatalf("stale Prepare() error = %v, want stale", err)
	}
	conflict := mz020Commit(41, "txn-4", "event-other")
	if _, err := coordinator.Prepare(context.Background(), conflict, participant); !errors.Is(err, hatSql.ErrSQLSinkTwoPhaseConflict) {
		t.Fatalf("conflicting Prepare() error = %v, want conflict", err)
	}
}

func TestMZ020FailedPrepareDoesNotEvictCommittedState(t *testing.T) {
	coordinator, err := hatSql.NewSQLSinkTwoPhaseCoordinator(context.Background(), hatSql.SQLSinkTwoPhaseOptions{Capacity: 1})
	if err != nil {
		t.Fatalf("NewSQLSinkTwoPhaseCoordinator() error = %v", err)
	}
	first := mz020Commit(50, "txn-5", "event-5")
	participant := &mz020Participant{}
	if _, err := coordinator.Prepare(context.Background(), first, participant); err != nil {
		t.Fatalf("first Prepare() error = %v", err)
	}
	if _, err := coordinator.Commit(context.Background(), first, participant); err != nil {
		t.Fatalf("first Commit() error = %v", err)
	}
	second := mz020Commit(51, "txn-6", "event-6")
	participant.prepareErr = errors.New("prepare unavailable")
	if _, err := coordinator.Prepare(context.Background(), second, participant); !errors.Is(err, participant.prepareErr) {
		t.Fatalf("failed second Prepare() error = %v, want prepare error", err)
	}
	snapshot := coordinator.Snapshot()
	if len(snapshot.Entries) != 1 || snapshot.Entries[0].Commit.TransactionID != "txn-5" || snapshot.Entries[0].State != hatSql.SQLSinkTwoPhaseCommitted {
		t.Fatalf("snapshot after failed eviction Prepare() = %#v, want first commit retained", snapshot)
	}
}

func TestMZ020InvalidRestoreLeavesStateUnchanged(t *testing.T) {
	coordinator, err := hatSql.NewSQLSinkTwoPhaseCoordinator(context.Background(), hatSql.SQLSinkTwoPhaseOptions{})
	if err != nil {
		t.Fatalf("NewSQLSinkTwoPhaseCoordinator() error = %v", err)
	}
	commit := mz020Commit(60, "txn-7", "event-7")
	participant := &mz020Participant{}
	if _, err := coordinator.Prepare(context.Background(), commit, participant); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	before := coordinator.Snapshot()
	invalid := before
	invalid.Entries = append([]hatSql.SQLSinkTwoPhaseEntry(nil), before.Entries...)
	invalid.Entries[0].State = "unknown"
	if err := coordinator.Restore(invalid); !errors.Is(err, hatSql.ErrSQLSinkTwoPhaseInvalid) {
		t.Fatalf("invalid Restore() error = %v, want invalid", err)
	}
	after := coordinator.Snapshot()
	if len(after.Entries) != 1 || after.Entries[0].Commit.TransactionID != "txn-7" || after.Entries[0].State != hatSql.SQLSinkTwoPhasePrepared {
		t.Fatalf("state after invalid Restore() = %#v, want original prepared entry", after)
	}
}

func TestMZ020ConcurrentPrepareCallsSingleFlight(t *testing.T) {
	coordinator, err := hatSql.NewSQLSinkTwoPhaseCoordinator(context.Background(), hatSql.SQLSinkTwoPhaseOptions{})
	if err != nil {
		t.Fatalf("NewSQLSinkTwoPhaseCoordinator() error = %v", err)
	}
	commit := mz020Commit(70, "txn-8", "event-8")
	participant := &mz020BlockingParticipant{
		prepareStarted: make(chan struct{}),
		prepareRelease: make(chan struct{}),
	}
	type result struct {
		prepared bool
		err      error
	}
	results := make(chan result, 2)
	go func() {
		prepared, err := coordinator.Prepare(context.Background(), commit, participant)
		results <- result{prepared: prepared, err: err}
	}()
	<-participant.prepareStarted
	go func() {
		prepared, err := coordinator.Prepare(context.Background(), commit, participant)
		results <- result{prepared: prepared, err: err}
	}()
	close(participant.prepareRelease)
	first, second := <-results, <-results
	if first.err != nil || second.err != nil || first.prepared == second.prepared {
		t.Fatalf("concurrent Prepare() results = %#v/%#v, want one true and one false without errors", first, second)
	}
	if participant.prepareCalls.Load() != 1 {
		t.Fatalf("participant prepare calls = %d, want 1", participant.prepareCalls.Load())
	}
}
