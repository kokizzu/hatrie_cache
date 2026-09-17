package hatSql_test

import (
	"errors"
	"reflect"
	"sync/atomic"
	"testing"

	hatSql "hatrie_cache/hat/hatSql"
)

func TestSQLSourceTransactionEnvelopeGroupsRelationsAndRestores(t *testing.T) {
	envelope := hatSql.SQLSourceTransactionEnvelope{
		Source: " orders-cdc ",
		Transaction: hatSql.SQLSourceTransaction{
			ID: " txn-1 ",
			Offsets: []hatSql.SQLSourceOffset{
				{Source: "orders-cdc", Partition: "1", Offset: 20},
				{Source: "orders-cdc", Partition: "0", Offset: 10},
			},
		},
		Relations: []string{" customers ", "orders"},
	}
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	var calls atomic.Int32
	ingested, err := coordinator.IngestEnvelope(envelope, func() error {
		calls.Add(1)
		return nil
	})
	if err != nil || !ingested {
		t.Fatalf("first IngestEnvelope() = %t/%v, want true/nil", ingested, err)
	}
	want := hatSql.SQLSourceTransactionEnvelope{
		Source: "orders-cdc",
		Transaction: hatSql.SQLSourceTransaction{
			ID: "txn-1",
			Offsets: []hatSql.SQLSourceOffset{
				{Source: "orders-cdc", Partition: "0", Offset: 10},
				{Source: "orders-cdc", Partition: "1", Offset: 20},
			},
		},
		Relations: []string{"customers", "orders"},
	}
	if ingested, err := coordinator.IngestEnvelope(envelope, func() error {
		calls.Add(1)
		return nil
	}); err != nil || ingested {
		t.Fatalf("duplicate IngestEnvelope() = %t/%v, want false/nil", ingested, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("callback calls = %d, want 1", calls.Load())
	}

	snapshot := coordinator.SnapshotEnvelopes()
	if !reflect.DeepEqual(snapshot, []hatSql.SQLSourceTransactionEnvelope{want}) {
		t.Fatalf("SnapshotEnvelopes() = %#v, want %#v", snapshot, []hatSql.SQLSourceTransactionEnvelope{want})
	}
	restored := hatSql.NewSQLSourceIngestionCoordinator()
	if err := restored.RestoreEnvelopes(snapshot); err != nil {
		t.Fatalf("RestoreEnvelopes() error = %v", err)
	}
	if ingested, err := restored.IngestEnvelope(want, func() error {
		calls.Add(1)
		return nil
	}); err != nil || ingested {
		t.Fatalf("restored duplicate IngestEnvelope() = %t/%v, want false/nil", ingested, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("restored callback calls = %d, want 1", calls.Load())
	}
}

func TestSQLSourceTransactionEnvelopeRejectsConflictsAndRetries(t *testing.T) {
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	envelope := newSQLSourceTransactionEnvelopeTestInput()
	boom := errors.New("source unavailable")
	if ingested, err := coordinator.IngestEnvelope(envelope, func() error {
		return boom
	}); !errors.Is(err, boom) || ingested {
		t.Fatalf("failed IngestEnvelope() = %t/%v, want false/boom", ingested, err)
	}
	if ingested, err := coordinator.IngestEnvelope(envelope, func() error {
		return nil
	}); err != nil || !ingested {
		t.Fatalf("retry IngestEnvelope() = %t/%v, want true/nil", ingested, err)
	}
	conflict := envelope
	conflict.Relations = []string{"orders", "payments"}
	if _, err := coordinator.IngestEnvelope(conflict, func() error { return nil }); !errors.Is(err, hatSql.ErrSQLSourceIngestionConflict) {
		t.Fatalf("conflicting relation set error = %v, want conflict", err)
	}
	conflict = envelope
	conflict.Transaction.Offsets = []hatSql.SQLSourceOffset{{Source: "events", Partition: "0", Offset: 99}}
	if _, err := coordinator.IngestEnvelope(conflict, func() error { return nil }); !errors.Is(err, hatSql.ErrSQLSourceIngestionConflict) {
		t.Fatalf("conflicting offsets error = %v, want conflict", err)
	}
}

func TestSQLSourceTransactionEnvelopeRejectsMalformedInputAtomically(t *testing.T) {
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	base := newSQLSourceTransactionEnvelopeTestInput()
	invalid := []hatSql.SQLSourceTransactionEnvelope{
		{Source: base.Source, Transaction: base.Transaction},
		{Source: base.Source, Transaction: base.Transaction, Relations: []string{"orders", "orders"}},
		{Source: base.Source, Transaction: base.Transaction, Relations: []string{""}},
		{Source: base.Source, Transaction: base.Transaction, Relations: []string{"orders\x00private"}},
	}
	for index, envelope := range invalid {
		if _, err := coordinator.IngestEnvelope(envelope, func() error { return nil }); !errors.Is(err, hatSql.ErrSQLSourceTransactionEnvelopeInvalid) {
			t.Fatalf("invalid envelope %d error = %v, want envelope validation error", index, err)
		}
	}
	if got := coordinator.SnapshotEnvelopes(); len(got) != 0 {
		t.Fatalf("invalid envelopes changed state: %#v", got)
	}

	if err := coordinator.RestoreEnvelopes([]hatSql.SQLSourceTransactionEnvelope{base, base}); !errors.Is(err, hatSql.ErrSQLSourceTransactionEnvelopeInvalid) {
		t.Fatalf("duplicate restore error = %v, want envelope validation error", err)
	}
	if got := coordinator.SnapshotEnvelopes(); len(got) != 0 {
		t.Fatalf("invalid restore changed state: %#v", got)
	}
}

func TestSQLSourceTransactionEnvelopeCopiesInputIntoCommitMarker(t *testing.T) {
	coordinator := hatSql.NewSQLSourceIngestionCoordinator()
	envelope := newSQLSourceTransactionEnvelopeTestInput()
	if _, err := coordinator.IngestEnvelope(envelope, func() error { return nil }); err != nil {
		t.Fatalf("IngestEnvelope() error = %v", err)
	}
	envelope.Relations[0] = "changed"
	envelope.Transaction.Offsets[0].Offset = 999
	want := newSQLSourceTransactionEnvelopeTestInput()
	want.Relations = []string{"orders", "users"}
	want.Transaction.Offsets = []hatSql.SQLSourceOffset{
		{Source: "events", Partition: "0", Offset: 10},
		{Source: "events", Partition: "1", Offset: 20},
	}
	if got := coordinator.SnapshotEnvelopes(); !reflect.DeepEqual(got, []hatSql.SQLSourceTransactionEnvelope{want}) {
		t.Fatalf("commit marker changed through callback: got %#v, want %#v", got, []hatSql.SQLSourceTransactionEnvelope{want})
	}
	snapshot := coordinator.SnapshotEnvelopes()
	snapshot[0].Relations[0] = "mutated"
	snapshot[0].Transaction.Offsets[0].Offset = 1234
	if got := coordinator.SnapshotEnvelopes(); !reflect.DeepEqual(got, []hatSql.SQLSourceTransactionEnvelope{want}) {
		t.Fatalf("snapshot mutation changed commit marker: got %#v, want %#v", got, []hatSql.SQLSourceTransactionEnvelope{want})
	}
}

func TestSQLSourceTransactionEnvelopeSingleFlightsConcurrentCommit(t *testing.T) {
	coordinator := hatSql.NewSQLSourceTransactionEnvelopeCoordinator()
	envelope := newSQLSourceTransactionEnvelopeTestInput()
	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	apply := func() error {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return nil
	}
	results := make(chan bool, 2)
	errCh := make(chan error, 2)
	for index := 0; index < 2; index++ {
		go func() {
			committed, err := coordinator.IngestEnvelope(envelope, apply)
			results <- committed
			errCh <- err
		}()
	}
	<-started
	close(release)
	firstCommitted, firstErr := <-results, <-errCh
	secondCommitted, secondErr := <-results, <-errCh
	if firstErr != nil || secondErr != nil {
		t.Fatalf("concurrent IngestEnvelope() errors = %v/%v", firstErr, secondErr)
	}
	if firstCommitted == secondCommitted {
		t.Fatalf("concurrent IngestEnvelope() results = %t/%t, want one true and one false", firstCommitted, secondCommitted)
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent callback calls = %d, want 1", calls.Load())
	}
}

func newSQLSourceTransactionEnvelopeTestInput() hatSql.SQLSourceTransactionEnvelope {
	return hatSql.SQLSourceTransactionEnvelope{
		Source: "events",
		Transaction: hatSql.SQLSourceTransaction{
			ID: "txn-1",
			Offsets: []hatSql.SQLSourceOffset{
				{Source: "events", Partition: "1", Offset: 20},
				{Source: "events", Partition: "0", Offset: 10},
			},
		},
		Relations: []string{"users", "orders"},
	}
}
