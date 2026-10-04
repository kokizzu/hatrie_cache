package hatDataStructure

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

type tu22Record struct {
	Email      string
	ExternalID string
	Name       string
}

func tu22UniqueConstraints() []CrossIndexUniqueConstraint[tu22Record] {
	return []CrossIndexUniqueConstraint[tu22Record]{
		{
			Name: "email",
			Key: func(record tu22Record) (string, bool, error) {
				return record.Email, record.Email != "", nil
			},
		},
		{
			Name: "external_id",
			Key: func(record tu22Record) (string, bool, error) {
				return record.ExternalID, record.ExternalID != "", nil
			},
		},
	}
}

func TestCrossIndexUniqueSetRejectsConflictWithoutPartialMutation(t *testing.T) {
	set, err := NewCrossIndexUniqueSet(tu22UniqueConstraints(), 4)
	if err != nil {
		t.Fatalf("NewCrossIndexUniqueSet() error = %v", err)
	}
	first := tu22Record{Email: "ada@example.test", ExternalID: "ext-1", Name: "Ada"}
	if err := set.Upsert(1, first); err != nil {
		t.Fatalf("Upsert(first) error = %v", err)
	}
	conflict := tu22Record{Email: "grace@example.test", ExternalID: "ext-1", Name: "Grace"}
	var conflictErr *CrossIndexUniqueConflictError
	if err := set.Upsert(2, conflict); !errors.As(err, &conflictErr) || conflictErr.Constraint != "external_id" || conflictErr.OwnerID != 1 {
		t.Fatalf("Upsert(conflict) error = %v, want external_id owned by 1", err)
	}
	if got, ok := set.Get(1); !ok || !reflect.DeepEqual(got, first) {
		t.Fatalf("first row after rejected update = %#v/%v, want %#v/true", got, ok, first)
	}
	if _, ok := set.Get(2); ok {
		t.Fatal("rejected row became visible")
	}
	if owner, ok := set.LookupID("email", "grace@example.test"); ok || owner != 0 {
		t.Fatalf("rejected email lookup = %d/%v", owner, ok)
	}
}

func TestCrossIndexUniqueSetMovesAllKeysAndAllowsAbsentValues(t *testing.T) {
	set, err := NewCrossIndexUniqueSet(tu22UniqueConstraints(), 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Upsert(7, tu22Record{Email: "old@example.test", ExternalID: "old", Name: "before"}); err != nil {
		t.Fatal(err)
	}
	updated := tu22Record{Email: "new@example.test", ExternalID: "new", Name: "after"}
	if err := set.Upsert(7, updated); err != nil {
		t.Fatalf("Upsert(updated) error = %v", err)
	}
	for _, lookup := range []struct {
		constraint string
		key        string
	}{
		{constraint: "email", key: "old@example.test"},
		{constraint: "external_id", key: "old"},
	} {
		if _, ok := set.LookupID(lookup.constraint, lookup.key); ok {
			t.Fatalf("stale %s key %q remains indexed", lookup.constraint, lookup.key)
		}
	}
	if owner, ok := set.LookupID("email", updated.Email); !ok || owner != 7 {
		t.Fatalf("new email lookup = %d/%v, want 7/true", owner, ok)
	}
	if err := set.Upsert(8, tu22Record{Name: "null-like"}); err != nil {
		t.Fatalf("Upsert(absent keys) error = %v", err)
	}
	if err := set.Upsert(9, tu22Record{Name: "another null-like"}); err != nil {
		t.Fatalf("Upsert(second absent keys) error = %v", err)
	}
}

func TestCrossIndexUniqueSetApplyBatchIsAtomic(t *testing.T) {
	set, err := NewCrossIndexUniqueSet(tu22UniqueConstraints(), 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Upsert(1, tu22Record{Email: "one@example.test", ExternalID: "one"}); err != nil {
		t.Fatal(err)
	}
	if err := set.ApplyBatch([]CrossIndexUniqueMutation[tu22Record]{
		{ID: 2, Value: tu22Record{Email: "two@example.test", ExternalID: "two"}},
		{ID: 3, Value: tu22Record{Email: "three@example.test", ExternalID: "one"}},
	}); err == nil {
		t.Fatal("ApplyBatch() succeeded with a cross-index conflict")
	}
	if set.Len() != 1 {
		t.Fatalf("Len() after rejected batch = %d, want 1", set.Len())
	}
	if _, ok := set.Get(2); ok {
		t.Fatal("partially applied batch row 2")
	}
	if _, ok := set.Get(3); ok {
		t.Fatal("partially applied batch row 3")
	}
	if err := set.ApplyBatch([]CrossIndexUniqueMutation[tu22Record]{
		{ID: 2, Value: tu22Record{Email: "two@example.test", ExternalID: "two"}},
		{ID: 1, Delete: true},
	}); err != nil {
		t.Fatalf("ApplyBatch(valid) error = %v", err)
	}
	if set.Len() != 1 {
		t.Fatalf("Len() after valid batch = %d, want 1", set.Len())
	}
	if _, ok := set.Get(1); ok {
		t.Fatal("deleted row 1 remains visible")
	}
}

func TestCrossIndexUniqueSetValidatesDefinitionAndLookup(t *testing.T) {
	var zero CrossIndexUniqueSet[tu22Record]
	if err := zero.Upsert(1, tu22Record{}); !errors.Is(err, ErrCrossIndexUniqueConstraintInvalid) {
		t.Fatalf("zero-value Upsert() error = %v, want invalid constraint", err)
	}
	if _, err := NewCrossIndexUniqueSet[tu22Record](nil, 0); !errors.Is(err, ErrCrossIndexUniqueConstraintInvalid) {
		t.Fatalf("nil constraints error = %v", err)
	}
	if _, err := NewCrossIndexUniqueSet([]CrossIndexUniqueConstraint[tu22Record]{
		{Name: "same", Key: func(tu22Record) (string, bool, error) { return "", false, nil }},
		{Name: "same", Key: func(tu22Record) (string, bool, error) { return "", false, nil }},
	}, 0); !errors.Is(err, ErrCrossIndexUniqueConstraintInvalid) {
		t.Fatalf("duplicate constraint error = %v", err)
	}
	set, err := NewCrossIndexUniqueSet(tu22UniqueConstraints(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := set.ConstraintNames(); !reflect.DeepEqual(got, []string{"email", "external_id"}) {
		t.Fatalf("ConstraintNames() = %v", got)
	}
	if _, ok := set.LookupID("missing", "value"); ok {
		t.Fatal("unknown constraint unexpectedly returned an owner")
	}
	if set.Delete(99) {
		t.Fatal("Delete(missing) unexpectedly reported success")
	}
	set.Clear()
	if set.Len() != 0 {
		t.Fatalf("Len() after Clear() = %d, want 0", set.Len())
	}
	if err := set.Upsert(10, tu22Record{Email: "after-clear@example.test", ExternalID: "after-clear"}); err != nil {
		t.Fatalf("Upsert() after Clear() error = %v", err)
	}
}

func TestCrossIndexUniqueSetConcurrentUse(t *testing.T) {
	set, err := NewCrossIndexUniqueSet(tu22UniqueConstraints(), 1)
	if err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for iteration := 0; iteration < 8; iteration++ {
		group.Add(1)
		go func(iteration int) {
			defer group.Done()
			for update := 0; update < 64; update++ {
				if err := set.Upsert(1, tu22Record{
					Email:      "worker-" + string(rune('a'+iteration)) + "@example.test",
					ExternalID: "worker-" + string(rune('a'+iteration)),
				}); err != nil {
					t.Errorf("concurrent Upsert() error = %v", err)
				}
			}
		}(iteration)
	}
	group.Wait()
	if set.Len() != 1 {
		t.Fatalf("Len() after concurrent replacement = %d, want 1", set.Len())
	}
}
