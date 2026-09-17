package hatSql_test

import (
	"errors"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestSQLSinkDeliveryAuditTracksCoordinatorOutcomesAndRestores(t *testing.T) {
	audit := hatSql.NewSQLSinkDeliveryAudit(hatSql.SQLSinkDeliveryAuditOptions{Capacity: 8})
	coordinator := hatSql.NewSQLSinkCommitCoordinatorWithOptions(hatSql.SQLSinkCommitCoordinatorOptions{Audit: audit})
	commit := hatSql.SQLSinkCommit{
		Sink:          "warehouse",
		TransactionID: "txn-1",
		Progress: []hatSql.SQLSinkProgress{
			{Sink: "warehouse", Partition: "1", Frontier: 11},
			{Sink: "warehouse", Partition: "0", Frontier: 10},
		},
	}
	if committed, err := coordinator.Commit(commit, func() error { return nil }); err != nil || !committed {
		t.Fatalf("Commit() = %t/%v, want true/nil", committed, err)
	}
	if committed, err := coordinator.Commit(commit, func() error { return errors.New("must not run") }); err != nil || committed {
		t.Fatalf("duplicate Commit() = %t/%v, want false/nil", committed, err)
	}

	snapshot := audit.Snapshot()
	if len(snapshot.Events) != 2 || snapshot.Events[0].Outcome != hatSql.SQLSinkDeliveryCommitted || snapshot.Events[1].Outcome != hatSql.SQLSinkDeliveryDuplicate {
		t.Fatalf("audit events = %#v, want committed and duplicate", snapshot.Events)
	}
	if len(snapshot.Events[0].Progress) != 2 || snapshot.Events[0].Progress[0].Partition != "0" {
		t.Fatalf("audit progress = %#v, want deterministic partition order", snapshot.Events[0].Progress)
	}

	payload, err := audit.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	restoredSnapshot, err := hatSql.UnmarshalSQLSinkDeliveryAudit(payload)
	if err != nil {
		t.Fatalf("UnmarshalSQLSinkDeliveryAudit() error = %v", err)
	}
	restored := hatSql.NewSQLSinkDeliveryAudit(hatSql.SQLSinkDeliveryAuditOptions{Capacity: 8})
	if err := restored.Restore(restoredSnapshot); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if got := restored.Snapshot(); len(got.Events) != 2 || got.Events[1].Sequence != 2 || got.Dropped != 0 {
		t.Fatalf("restored audit = %#v, want two retained events", got)
	}
}

func TestSQLSinkDeliveryAuditBoundsRetentionAndRejectsCorruption(t *testing.T) {
	audit := hatSql.NewSQLSinkDeliveryAudit(hatSql.SQLSinkDeliveryAuditOptions{Capacity: 2})
	progress := []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 1}}
	for index, outcome := range []hatSql.SQLSinkDeliveryOutcome{
		hatSql.SQLSinkDeliveryCommitted,
		hatSql.SQLSinkDeliveryFailed,
		hatSql.SQLSinkDeliveryConflict,
	} {
		if err := audit.Record(hatSql.SQLSinkDeliveryEvent{
			Sink:          "warehouse",
			TransactionID: string(rune('a' + index)),
			Outcome:       outcome,
			Progress:      progress,
		}); err != nil {
			t.Fatalf("Record(%s) error = %v", outcome, err)
		}
	}
	snapshot := audit.Snapshot()
	if snapshot.Dropped != 1 || len(snapshot.Events) != 2 || snapshot.Events[0].Sequence != 2 || snapshot.Events[1].Sequence != 3 {
		t.Fatalf("bounded snapshot = %#v, want sequences 2 and 3 with one drop", snapshot)
	}
	if stats := audit.Stats(); stats.Capacity != 2 || stats.Retained != 2 || stats.Dropped != 1 || stats.NextSequence != 4 {
		t.Fatalf("audit stats = %#v", stats)
	}

	payload, err := audit.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary() error = %v", err)
	}
	payload[0] = 'X'
	if _, err := hatSql.UnmarshalSQLSinkDeliveryAudit(payload); !errors.Is(err, hatSql.ErrSQLSinkDeliveryAuditInvalid) {
		t.Fatalf("corrupt UnmarshalSQLSinkDeliveryAudit() error = %v, want invalid", err)
	}
	if err := audit.Record(hatSql.SQLSinkDeliveryEvent{Sink: "warehouse", TransactionID: "bad", Outcome: hatSql.SQLSinkDeliveryCommitted}); !errors.Is(err, hatSql.ErrSQLSinkDeliveryAuditInvalid) {
		t.Fatalf("invalid Record() error = %v, want invalid", err)
	}
}

func TestSQLSinkCommitCoordinatorAuditsFailureAndConflict(t *testing.T) {
	audit := hatSql.NewSQLSinkDeliveryAudit(hatSql.SQLSinkDeliveryAuditOptions{Capacity: 8})
	coordinator := hatSql.NewSQLSinkCommitCoordinatorWithOptions(hatSql.SQLSinkCommitCoordinatorOptions{Audit: audit})
	failed := hatSql.SQLSinkCommit{
		Sink:          "warehouse",
		TransactionID: "failed",
		Progress:      []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 1}},
	}
	if _, err := coordinator.Commit(failed, func() error { return errors.New("sink unavailable") }); err == nil {
		t.Fatal("failed Commit() returned nil error")
	}
	committed := failed
	committed.TransactionID = "committed"
	if ok, err := coordinator.Commit(committed, func() error { return nil }); err != nil || !ok {
		t.Fatalf("successful Commit() = %t/%v", ok, err)
	}
	conflict := committed
	conflict.Progress = []hatSql.SQLSinkProgress{{Sink: "warehouse", Partition: "0", Frontier: 2}}
	if _, err := coordinator.Commit(conflict, func() error { return nil }); !errors.Is(err, hatSql.ErrSQLSinkCommitConflict) {
		t.Fatalf("conflicting Commit() error = %v, want conflict", err)
	}
	events := audit.Snapshot().Events
	if len(events) != 3 || events[0].Outcome != hatSql.SQLSinkDeliveryFailed || events[1].Outcome != hatSql.SQLSinkDeliveryCommitted || events[2].Outcome != hatSql.SQLSinkDeliveryConflict {
		t.Fatalf("coordinator audit events = %#v", events)
	}
}
