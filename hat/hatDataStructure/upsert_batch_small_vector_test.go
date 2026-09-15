package hatDataStructure

import "testing"

const upsertBatchExpectedPromotionThreshold = 16

func TestUpsertBatchSmallBatchSemantics(t *testing.T) {
	batch := NewUpsertBatch[int](0)
	if err := batch.Upsert("alpha", 1); err != nil {
		t.Fatalf("initial Upsert() error = %v", err)
	}
	if err := batch.Upsert("alpha", 2); err != nil {
		t.Fatalf("replacement Upsert() error = %v", err)
	}
	if err := batch.Delete("beta"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if err := batch.Upsert("beta", 3); err != nil {
		t.Fatalf("tombstone replacement Upsert() error = %v", err)
	}
	if batch.Len() != 2 {
		t.Fatalf("Len() = %d, want 2", batch.Len())
	}
	alpha, ok := batch.Lookup("alpha")
	if !ok || alpha.Value != 2 || alpha.Deleted {
		t.Fatalf("alpha = %#v, %v, want value 2", alpha, ok)
	}
	beta, ok := batch.Lookup("beta")
	if !ok || beta.Value != 3 || beta.Deleted {
		t.Fatalf("beta = %#v, %v, want value 3", beta, ok)
	}
}

func TestUpsertBatchSmallBatchResetRetainsLogicalEmptyState(t *testing.T) {
	batch := NewUpsertBatch[int](4)
	if err := batch.Upsert("alpha", 1); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	batch.Reset()
	if batch.Len() != 0 {
		t.Fatalf("Len() after Reset() = %d, want 0", batch.Len())
	}
	if _, ok := batch.Lookup("alpha"); ok {
		t.Fatal("Lookup() found a reset key")
	}
	if err := batch.Delete("beta"); err != nil {
		t.Fatalf("Delete() after Reset() error = %v", err)
	}
	record, ok := batch.Lookup("beta")
	if !ok || !record.Deleted {
		t.Fatalf("beta = %#v, %v, want tombstone", record, ok)
	}
}

func TestUpsertBatchSmallVectorPromotesAfterThreshold(t *testing.T) {
	batch := NewUpsertBatch[int](0)
	for index := 0; index < upsertBatchExpectedPromotionThreshold+1; index++ {
		if err := batch.Upsert("key-"+string(rune('a'+index)), index); err != nil {
			t.Fatalf("Upsert(%d) error = %v", index, err)
		}
	}
	if batch.indexes == nil {
		t.Fatal("indexes is nil after crossing the linear threshold")
	}
	if batch.Len() != upsertBatchExpectedPromotionThreshold+1 {
		t.Fatalf("Len() = %d, want %d", batch.Len(), upsertBatchExpectedPromotionThreshold+1)
	}
	for index := 0; index < upsertBatchExpectedPromotionThreshold+1; index++ {
		record, ok := batch.Lookup("key-" + string(rune('a'+index)))
		if !ok || record.Value != index {
			t.Fatalf("record %d = %#v, %v, want value %d", index, record, ok, index)
		}
	}
}
