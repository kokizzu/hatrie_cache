package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

func TestUpsertBatchConsolidatesLastValueAndRetainsDeletes(t *testing.T) {
	batch := NewUpsertBatch[string](4)
	if err := batch.Upsert("a", "one"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Upsert("b", "two"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Upsert("b", "last"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Delete("c"); err != nil {
		t.Fatal(err)
	}
	if err := batch.Upsert("a", "restored"); err != nil {
		t.Fatal(err)
	}
	if batch.Len() != 3 {
		t.Fatalf("Len() = %d, want 3", batch.Len())
	}

	var got []UpsertRecord[string]
	batch.ForEach(func(record UpsertRecord[string]) {
		got = append(got, record)
	})
	want := []UpsertRecord[string]{
		{Key: "a", Value: "restored"},
		{Key: "b", Value: "last"},
		{Key: "c", Deleted: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ForEach() = %#v, want %#v", got, want)
	}
	if record, ok := batch.Lookup("c"); !ok || !record.Deleted || record.Value != "" {
		t.Fatalf("Lookup(c) = %#v/%t, want deleted empty record", record, ok)
	}
}

func TestUpsertBatchSupportsZeroValueResetAndNilReceiver(t *testing.T) {
	var batch UpsertBatch[int64]
	if err := batch.Upsert("counter", 3); err != nil {
		t.Fatalf("zero-value Upsert() error = %v", err)
	}
	if err := batch.Delete("counter"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if record, ok := batch.Lookup("counter"); !ok || !record.Deleted {
		t.Fatalf("Lookup(counter) = %#v/%t, want tombstone", record, ok)
	}
	batch.Reset()
	if batch.Len() != 0 {
		t.Fatalf("Len() after Reset() = %d, want 0", batch.Len())
	}
	if err := batch.Upsert("counter", 4); err != nil {
		t.Fatalf("Upsert() after Reset() error = %v", err)
	}
	if record, ok := batch.Lookup("counter"); !ok || record.Deleted || record.Value != 4 {
		t.Fatalf("Lookup(counter) after Reset() = %#v/%t, want value 4", record, ok)
	}

	var nilBatch *UpsertBatch[int64]
	if err := nilBatch.Upsert("key", 1); !errors.Is(err, ErrUpsertBatchInvalid) {
		t.Fatalf("nil Upsert() error = %v, want ErrUpsertBatchInvalid", err)
	}
	if err := nilBatch.Delete("key"); !errors.Is(err, ErrUpsertBatchInvalid) {
		t.Fatalf("nil Delete() error = %v, want ErrUpsertBatchInvalid", err)
	}
	if nilBatch.Len() != 0 {
		t.Fatalf("nil Len() = %d, want 0", nilBatch.Len())
	}
	if _, ok := nilBatch.Lookup("key"); ok {
		t.Fatal("nil Lookup() found a record")
	}
}

func TestUpsertBatchRejectsEmptyKeysAndIgnoresNilVisitor(t *testing.T) {
	batch := NewUpsertBatch[string](0)
	if err := batch.Upsert("", "value"); !errors.Is(err, ErrUpsertBatchKeyInvalid) {
		t.Fatalf("empty Upsert() error = %v, want ErrUpsertBatchKeyInvalid", err)
	}
	if err := batch.Delete(""); !errors.Is(err, ErrUpsertBatchKeyInvalid) {
		t.Fatalf("empty Delete() error = %v, want ErrUpsertBatchKeyInvalid", err)
	}
	if batch.Len() != 0 {
		t.Fatalf("Len() after invalid keys = %d, want 0", batch.Len())
	}
	if err := batch.Upsert("key", "value"); err != nil {
		t.Fatal(err)
	}
	batch.ForEach(nil)
	if record, ok := batch.Lookup("missing"); ok || record.Key != "" {
		t.Fatalf("Lookup(missing) = %#v/%t, want empty false", record, ok)
	}
}
