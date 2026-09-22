package hatDataStructure

import (
	"errors"
	"testing"
)

func TestT231AfterReplaceProvidesOrderedTransactionIdentity(t *testing.T) {
	var order []string
	type audit struct {
		transactionID uint64
		event         MemtxReplaceEvent[int]
	}
	var audits []audit
	reject := errors.New("reject")
	table, err := NewMemtxTableWithHooks[int](MemtxTableOptions{Capacity: 1}, MemtxTableHooks[int]{
		BeforeReplace: func(event MemtxReplaceEvent[int]) (int, error) {
			if event.New == 99 {
				return 0, reject
			}
			return event.New, nil
		},
		OnReplace: func(MemtxReplaceEvent[int]) {
			order = append(order, "changefeed")
		},
		AfterReplace: func(transactionID uint64, event MemtxReplaceEvent[int]) {
			order = append(order, "audit")
			audits = append(audits, audit{transactionID: transactionID, event: event})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := table.Insert(4, 10); err != nil {
		t.Fatalf("Insert() error = %v", err)
	}
	if _, err := table.Upsert(4, 99); !errors.Is(err, reject) {
		t.Fatalf("rejected Upsert() error = %v; want %v", err, reject)
	}
	if _, err := table.Upsert(4, 20); err != nil {
		t.Fatalf("accepted Upsert() error = %v", err)
	}

	if len(audits) != 2 {
		t.Fatalf("audit calls = %d; want 2", len(audits))
	}
	if audits[0].transactionID != 1 || audits[0].event.ID != 4 || audits[0].event.Exists || audits[0].event.Old != 0 || audits[0].event.New != 10 || audits[0].event.Operation != MemtxReplaceInsert {
		t.Fatalf("first audit = %#v", audits[0])
	}
	if audits[1].transactionID != 2 || audits[1].event.ID != 4 || !audits[1].event.Exists || audits[1].event.Old != 10 || audits[1].event.New != 20 || audits[1].event.Operation != MemtxReplaceUpdate {
		t.Fatalf("second audit = %#v", audits[1])
	}
	if len(order) != 4 || order[0] != "changefeed" || order[1] != "audit" || order[2] != "changefeed" || order[3] != "audit" {
		t.Fatalf("hook order = %#v; want changefeed,audit twice", order)
	}
}

var t231AfterReplaceSink uint64

func t231RecordAfterReplace(transactionID uint64, _ MemtxReplaceEvent[int]) {
	t231AfterReplaceSink = transactionID
}

func BenchmarkT231MemtxUpsertAfterReplace(b *testing.B) {
	table, err := NewMemtxTableWithHooks[int](MemtxTableOptions{Capacity: 1}, MemtxTableHooks[int]{
		AfterReplace: t231RecordAfterReplace,
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < b.N; index++ {
		if _, err := table.Upsert(1, index); err != nil {
			b.Fatal(err)
		}
	}
	if t231AfterReplaceSink == 0 {
		b.Fatal("after-replace hook did not receive a transaction identity")
	}
}
