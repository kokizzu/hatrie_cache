package hatSql

import (
	"bytes"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func mu022TestEnvelope(id string, offset uint64) SQLSourceTransactionEnvelope {
	return SQLSourceTransactionEnvelope{
		Source: "orders",
		Transaction: SQLSourceTransaction{
			ID:      id,
			Offsets: []SQLSourceOffset{{Source: "orders", Partition: "0", Offset: offset}},
		},
		Relations: []string{"orders", "order_items"},
	}
}

func TestSQLConnectorTransactionRetryJournalLifecycle(t *testing.T) {
	journal, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	envelope := mu022TestEnvelope("tx-1", 10)
	record, result, err := journal.Begin(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if result != SQLConnectorTransactionStarted || record.State != SQLConnectorTransactionPending || record.Attempts != 1 {
		t.Fatalf("begin result = %#v, %q, want first pending attempt", record, result)
	}
	if pending, result, err := journal.Begin(envelope); err != nil || result != SQLConnectorTransactionInProgress || pending.Attempts != 1 {
		t.Fatalf("duplicate pending begin = %#v, %q, %v", pending, result, err)
	}

	record, err = journal.Complete(SQLConnectorTransactionCompletion{
		Source:        "orders",
		TransactionID: "tx-1",
		Attempt:       1,
		Outcome:       SQLConnectorTransactionRetryable,
		ErrorCode:     "timeout",
	})
	if err != nil {
		t.Fatal(err)
	}
	if record.State != SQLConnectorTransactionRetryable || record.Attempts != 1 || record.LastErrorCode != "timeout" {
		t.Fatalf("retry outcome = %#v", record)
	}
	if _, err := journal.Complete(SQLConnectorTransactionCompletion{Source: "orders", TransactionID: "tx-1", Attempt: 1, Outcome: SQLConnectorTransactionCommitted}); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalState) {
		t.Fatalf("retryable state completion error = %v", err)
	}

	record, result, err = journal.Begin(envelope)
	if err != nil {
		t.Fatal(err)
	}
	if result != SQLConnectorTransactionRetryStarted || record.State != SQLConnectorTransactionPending || record.Attempts != 2 {
		t.Fatalf("retry begin = %#v, %q", record, result)
	}
	record, err = journal.Complete(SQLConnectorTransactionCompletion{
		Source:        "orders",
		TransactionID: "tx-1",
		Attempt:       2,
		Outcome:       SQLConnectorTransactionCommitted,
	})
	if err != nil || record.State != SQLConnectorTransactionCommitted || record.LastErrorCode != "" {
		t.Fatalf("commit outcome = %#v, %v", record, err)
	}
	if duplicate, result, err := journal.Begin(envelope); err != nil || result != SQLConnectorTransactionAlreadyCommitted || duplicate.State != SQLConnectorTransactionCommitted {
		t.Fatalf("duplicate committed begin = %#v, %q, %v", duplicate, result, err)
	}
	if duplicate, err := journal.Complete(SQLConnectorTransactionCompletion{Source: "orders", TransactionID: "tx-1", Attempt: 2, Outcome: SQLConnectorTransactionCommitted}); err != nil || duplicate.State != SQLConnectorTransactionCommitted {
		t.Fatalf("idempotent committed completion = %#v, %v", duplicate, err)
	}
	if _, err := journal.Complete(SQLConnectorTransactionCompletion{Source: "orders", TransactionID: "tx-1", Attempt: 1, Outcome: SQLConnectorTransactionCommitted}); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalConflict) {
		t.Fatalf("stale attempt error = %v", err)
	}
	if _, result, err := journal.Begin(mu022TestEnvelope("tx-1", 11)); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalConflict) || result != SQLConnectorTransactionBeginInvalid {
		t.Fatalf("metadata conflict = %q, %v", result, err)
	}
}

func TestSQLConnectorTransactionRetryJournalRetentionAndCapacity(t *testing.T) {
	journal, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{Capacity: 2})
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := journal.Begin(mu022TestEnvelope("tx-1", 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.Begin(mu022TestEnvelope("tx-2", 2)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := journal.Begin(mu022TestEnvelope("tx-3", 3)); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalCapacity) {
		t.Fatalf("full pending journal error = %v", err)
	}
	if _, err := journal.Complete(SQLConnectorTransactionCompletion{Source: "orders", TransactionID: "tx-1", Attempt: first.Attempts, Outcome: SQLConnectorTransactionAborted, ErrorCode: "bad_payload"}); err != nil {
		t.Fatal(err)
	}
	if _, result, err := journal.Begin(mu022TestEnvelope("tx-3", 3)); err != nil || result != SQLConnectorTransactionStarted {
		t.Fatalf("terminal eviction begin = %q, %v", result, err)
	}
	stats := journal.Stats()
	if stats.Retained != 2 || stats.Dropped != 1 {
		t.Fatalf("retention stats = %#v", stats)
	}
	if _, found := journal.Lookup("orders", "tx-1"); found {
		t.Fatal("oldest terminal record should have been evicted")
	}

	retryJournal, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	retryRecord, _, err := retryJournal.Begin(mu022TestEnvelope("retry", 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := retryJournal.Complete(SQLConnectorTransactionCompletion{Source: "orders", TransactionID: "retry", Attempt: retryRecord.Attempts, Outcome: SQLConnectorTransactionRetryable, ErrorCode: "unavailable"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := retryJournal.Begin(mu022TestEnvelope("other", 2)); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalCapacity) {
		t.Fatalf("retryable record must not be evicted = %v", err)
	}
}

func TestSQLConnectorTransactionRetryJournalSnapshotAndBinaryRoundTrip(t *testing.T) {
	journal, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{Capacity: 8})
	if err != nil {
		t.Fatal(err)
	}
	first, _, err := journal.Begin(mu022TestEnvelope("tx-1", 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Complete(SQLConnectorTransactionCompletion{Source: "orders", TransactionID: "tx-1", Attempt: first.Attempts, Outcome: SQLConnectorTransactionRetryable, ErrorCode: "timeout"}); err != nil {
		t.Fatal(err)
	}
	second, _, err := journal.Begin(mu022TestEnvelope("tx-2", 2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Complete(SQLConnectorTransactionCompletion{Source: "orders", TransactionID: "tx-2", Attempt: second.Attempts, Outcome: SQLConnectorTransactionCommitted}); err != nil {
		t.Fatal(err)
	}
	original := journal.Snapshot()
	if len(original.Records) != 2 || original.Records[0].State != SQLConnectorTransactionRetryable || original.Records[1].State != SQLConnectorTransactionCommitted {
		t.Fatalf("snapshot = %#v", original)
	}
	original.Records[0].Envelope.Transaction.Offsets[0].Offset = 999
	if current, found := journal.Lookup("orders", "tx-1"); !found || current.Envelope.Transaction.Offsets[0].Offset != 1 {
		t.Fatal("snapshot must be independently owned")
	}

	payload, err := journal.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalSQLConnectorTransactionRetryJournal(payload)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := NewSQLConnectorTransactionRetryJournalFromSnapshot(decoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(journal.Snapshot(), restored.Snapshot()) {
		t.Fatalf("round-trip differs:\noriginal=%#v\nrestored=%#v", journal.Snapshot(), restored.Snapshot())
	}
	corrupt := append([]byte(nil), payload...)
	corrupt[len(corrupt)-1]++
	if _, err := UnmarshalSQLConnectorTransactionRetryJournal(corrupt); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalInvalid) {
		t.Fatalf("corrupt payload error = %v", err)
	}
	if bytes.Contains(payload, []byte("password")) {
		t.Fatal("journal payload must not contain raw error text")
	}

	before := restored.Snapshot()
	bad := restored.Snapshot()
	bad.Records[0].State = SQLConnectorTransactionPending
	if err := restored.Restore(bad); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalInvalid) {
		t.Fatalf("invalid snapshot error = %v", err)
	}
	if !reflect.DeepEqual(before, restored.Snapshot()) {
		t.Fatal("failed restore must not change journal")
	}
}

func TestSQLConnectorTransactionRetryJournalRejectsUnsafeInput(t *testing.T) {
	if _, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{Capacity: -1}); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalInvalid) {
		t.Fatalf("negative capacity error = %v", err)
	}
	if _, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{Capacity: MaxSQLConnectorTransactionRetryJournalCapacity + 1}); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalInvalid) {
		t.Fatalf("oversized capacity error = %v", err)
	}
	journal, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{})
	if err != nil {
		t.Fatal(err)
	}
	unsafe := mu022TestEnvelope("tx\n", 1)
	if _, _, err := journal.Begin(unsafe); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalInvalid) {
		t.Fatalf("unsafe envelope error = %v", err)
	}
	valid, _, err := journal.Begin(mu022TestEnvelope("tx", 1))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := journal.Complete(SQLConnectorTransactionCompletion{Source: "orders", TransactionID: "tx", Attempt: valid.Attempts, Outcome: SQLConnectorTransactionRetryable, ErrorCode: "password=secret"}); !errors.Is(err, ErrSQLConnectorTransactionRetryJournalInvalid) {
		t.Fatalf("unsafe error code error = %v", err)
	}
}

func TestSQLConnectorTransactionRetryJournalConcurrentLookupAndSnapshot(t *testing.T) {
	journal, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{Capacity: 256})
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	group.Add(2)
	go func() {
		defer group.Done()
		for index := 0; index < 1000; index++ {
			_, _, _ = journal.Begin(mu022TestEnvelope("tx", uint64(index+1)))
		}
	}()
	go func() {
		defer group.Done()
		for index := 0; index < 1000; index++ {
			_, _ = journal.Lookup("orders", "tx")
			_ = journal.Snapshot()
		}
	}()
	group.Wait()
}
