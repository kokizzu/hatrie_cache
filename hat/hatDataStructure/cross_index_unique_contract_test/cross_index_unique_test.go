package cross_index_unique_contract_test

import (
	"errors"
	"sync"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

func newCrossIndexUniqueSet(t *testing.T) *hatDataStructure.UniqueConstraintSet {
	t.Helper()
	set, err := hatDataStructure.NewUniqueConstraintSet(hatDataStructure.UniqueConstraintSetOptions{
		ConstraintNames: []string{"email", "external_code"},
		Capacity:        16,
	})
	if err != nil {
		t.Fatal(err)
	}
	return set
}

func TestCrossIndexUniqueConstructorValidation(t *testing.T) {
	t.Parallel()
	tests := []hatDataStructure.UniqueConstraintSetOptions{
		{},
		{ConstraintNames: []string{""}},
		{ConstraintNames: []string{"email", "email"}},
		{ConstraintNames: []string{"email"}, Capacity: -1},
		{ConstraintNames: []string{"email"}, MaxKeyBytes: -1},
	}
	for _, options := range tests {
		if _, err := hatDataStructure.NewUniqueConstraintSet(options); !errors.Is(err, hatDataStructure.ErrUniqueConstraintInvalidOptions) {
			t.Fatalf("options=%#v error=%v", options, err)
		}
	}
}

func TestCrossIndexUniqueAtomicConflictsAndReplacement(t *testing.T) {
	set := newCrossIndexUniqueSet(t)
	if err := set.Upsert(1, []string{"one@example.test", "code-1"}); err != nil {
		t.Fatal(err)
	}

	err := set.Upsert(2, []string{"two@example.test", "code-1"})
	if !errors.Is(err, hatDataStructure.ErrUniqueConstraintConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	var conflict *hatDataStructure.UniqueConstraintConflictError
	if !errors.As(err, &conflict) || conflict.Constraint != "external_code" || conflict.Owner != 1 {
		t.Fatalf("unexpected conflict error: %#v", err)
	}
	if owner, ok := set.Owner("email", "two@example.test"); ok || owner != 0 {
		t.Fatalf("failed write reserved email: owner=%d ok=%v", owner, ok)
	}
	if set.Len() != 1 {
		t.Fatalf("failed write changed length: %d", set.Len())
	}

	if err := set.Upsert(1, []string{"updated@example.test", "code-1"}); err != nil {
		t.Fatal(err)
	}
	if _, ok := set.Owner("email", "one@example.test"); ok {
		t.Fatal("old email remained reserved")
	}
	if owner, ok := set.Owner("email", "updated@example.test"); !ok || owner != 1 {
		t.Fatalf("updated email owner=%d ok=%v", owner, ok)
	}
	if err := set.Upsert(2, []string{"two@example.test", "code-2"}); err != nil {
		t.Fatal(err)
	}
	err = set.Upsert(1, []string{"two@example.test", "code-1-new"})
	if !errors.Is(err, hatDataStructure.ErrUniqueConstraintConflict) {
		t.Fatalf("expected replacement conflict, got %v", err)
	}
	if owner, ok := set.Owner("email", "updated@example.test"); !ok || owner != 1 {
		t.Fatalf("failed replacement removed old email: owner=%d ok=%v", owner, ok)
	}
	if owner, ok := set.Owner("external_code", "code-1"); !ok || owner != 1 {
		t.Fatalf("failed replacement removed old code: owner=%d ok=%v", owner, ok)
	}
	if _, ok := set.Owner("external_code", "code-1-new"); ok {
		t.Fatal("failed replacement reserved new code")
	}
}

func TestCrossIndexUniqueLookupsAndDelete(t *testing.T) {
	set := newCrossIndexUniqueSet(t)
	if err := set.Upsert(7, []string{"seven@example.test", "code-7"}); err != nil {
		t.Fatal(err)
	}
	if owner, ok := set.OwnerAt(0, "seven@example.test"); !ok || owner != 7 {
		t.Fatalf("email lookup owner=%d ok=%v", owner, ok)
	}
	if owner, ok := set.OwnerAt(1, "code-7"); !ok || owner != 7 {
		t.Fatalf("code lookup owner=%d ok=%v", owner, ok)
	}
	names := set.ConstraintNames()
	if len(names) != 2 || names[0] != "email" || names[1] != "external_code" {
		t.Fatalf("constraint names=%v", names)
	}
	names[0] = "changed"
	if names = set.ConstraintNames(); names[0] != "email" {
		t.Fatal("constraint names were not copied")
	}
	if !set.Delete(7) || set.Delete(7) {
		t.Fatal("delete result was not idempotent")
	}
	if set.Len() != 0 {
		t.Fatalf("length after delete=%d", set.Len())
	}
	if _, ok := set.Owner("email", "seven@example.test"); ok {
		t.Fatal("deleted email remained reserved")
	}
	if _, ok := set.OwnerAt(-1, "anything"); ok {
		t.Fatal("invalid index unexpectedly matched")
	}
}

func TestCrossIndexUniqueRejectsInvalidWrites(t *testing.T) {
	set, err := hatDataStructure.NewUniqueConstraintSet(hatDataStructure.UniqueConstraintSetOptions{
		ConstraintNames: []string{"email", "code"},
		MaxKeyBytes:     4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Upsert(1, []string{"only-one"}); !errors.Is(err, hatDataStructure.ErrUniqueConstraintArity) {
		t.Fatalf("arity error=%v", err)
	}
	if err := set.Upsert(1, []string{"12345", "ok"}); !errors.Is(err, hatDataStructure.ErrUniqueConstraintKeyTooLarge) {
		t.Fatalf("size error=%v", err)
	}
	if set.Len() != 0 {
		t.Fatal("invalid write changed length")
	}
}

func TestCrossIndexUniqueConcurrentUse(t *testing.T) {
	set := newCrossIndexUniqueSet(t)
	var group sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		group.Add(1)
		go func() {
			defer group.Done()
			id := uint64(worker + 1)
			for iteration := 0; iteration < 100; iteration++ {
				keys := []string{uintString(worker) + "@example.test", "code-" + uintString(worker)}
				if err := set.Upsert(id, keys); err != nil {
					t.Errorf("upsert: %v", err)
					return
				}
				if _, ok := set.OwnerAt(0, keys[0]); !ok {
					t.Errorf("missing owner for %q", keys[0])
					return
				}
			}
		}()
	}
	group.Wait()
	if set.Len() != 8 {
		t.Fatalf("concurrent length=%d", set.Len())
	}
}

func BenchmarkTU22CandidateUpsert(b *testing.B) {
	const size = 10_000
	rows := tu22BenchmarkRows(size)
	set, err := hatDataStructure.NewUniqueConstraintSet(hatDataStructure.UniqueConstraintSetOptions{
		ConstraintNames: []string{"email", "external_code"},
		Capacity:        size,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row := rows[i%size]
		keys := [2]string{row.Email, row.Code}
		if err := set.Upsert(uint64(i%size+1), keys[:]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTU22CandidateBuild10000(b *testing.B) {
	const size = 10_000
	rows := tu22BenchmarkRows(size)
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		set, err := hatDataStructure.NewUniqueConstraintSet(hatDataStructure.UniqueConstraintSetOptions{
			ConstraintNames: []string{"email", "external_code"},
			Capacity:        size,
		})
		if err != nil {
			b.Fatal(err)
		}
		for i, row := range rows {
			keys := [2]string{row.Email, row.Code}
			if err := set.Upsert(uint64(i+1), keys[:]); err != nil {
				b.Fatal(err)
			}
		}
	}
}
