package hatDataStructure

import (
	"errors"
	"sync"
	"testing"
)

type tu22User struct {
	Email  string
	Handle string
}

func TestTU22UniqueConstraintSetLifecycleAndAtomicConflict(t *testing.T) {
	set, err := NewUniqueConstraintSet[tu22User, string]([]UniqueConstraint[tu22User, string]{
		{Name: "email", Key: func(user tu22User) (string, bool) { return user.Email, user.Email != "" }},
		{Name: "handle", Key: func(user tu22User) (string, bool) { return user.Handle, user.Handle != "" }},
	})
	if err != nil {
		t.Fatalf("NewUniqueConstraintSet() error = %v", err)
	}
	if err := set.Insert(1, tu22User{Email: "ada@example.com", Handle: "ada"}); err != nil {
		t.Fatalf("Insert(1) error = %v", err)
	}
	if err := set.Insert(2, tu22User{Email: "ada@example.com", Handle: "grace"}); !errors.Is(err, ErrUniqueConstraintViolation) {
		t.Fatalf("duplicate email error = %v, want %v", err, ErrUniqueConstraintViolation)
	}
	var conflict *UniqueConstraintViolationError
	if !errors.As(set.Insert(2, tu22User{Email: "grace@example.com", Handle: "ada"}), &conflict) || conflict.Constraint != "handle" {
		t.Fatalf("duplicate handle conflict = %#v, want handle", conflict)
	}
	if err := set.Insert(2, tu22User{Email: "grace@example.com", Handle: "grace"}); err != nil {
		t.Fatalf("Insert(2) error = %v", err)
	}

	if err := set.Upsert(2, tu22User{Email: "ada@example.com", Handle: "new-grace"}); !errors.Is(err, ErrUniqueConstraintViolation) {
		t.Fatalf("conflicting Upsert() error = %v, want %v", err, ErrUniqueConstraintViolation)
	}
	if err := set.Insert(3, tu22User{Email: "grace@example.com", Handle: "grace"}); !errors.Is(err, ErrUniqueConstraintViolation) {
		t.Fatalf("conflicting Upsert() released old keys: %v", err)
	}
	if err := set.Upsert(2, tu22User{Email: "new@example.com", Handle: "new-grace"}); err != nil {
		t.Fatalf("non-conflicting Upsert() error = %v", err)
	}
	if err := set.Insert(3, tu22User{Email: "grace@example.com", Handle: "grace"}); err != nil {
		t.Fatalf("old keys were not released after Upsert(): %v", err)
	}
	if !set.Delete(3) || set.Delete(3) {
		t.Fatal("Delete() did not report exactly one removal")
	}
	set.Reset()
	if set.Len() != 0 {
		t.Fatalf("Len after Reset = %d, want 0", set.Len())
	}
}

func TestTU22UniqueConstraintSetValidationAndNil(t *testing.T) {
	if _, err := NewUniqueConstraintSet[tu22User, string](nil); !errors.Is(err, ErrUniqueConstraintDefinitionInvalid) {
		t.Fatalf("empty definitions error = %v, want %v", err, ErrUniqueConstraintDefinitionInvalid)
	}
	if _, err := NewUniqueConstraintSet[tu22User, string]([]UniqueConstraint[tu22User, string]{{Name: "email"}}); !errors.Is(err, ErrUniqueConstraintDefinitionInvalid) {
		t.Fatalf("nil key error = %v, want %v", err, ErrUniqueConstraintDefinitionInvalid)
	}
	var nilSet *UniqueConstraintSet[tu22User, string]
	if err := nilSet.Insert(1, tu22User{}); !errors.Is(err, ErrUniqueConstraintSetNil) {
		t.Fatalf("nil Insert error = %v, want %v", err, ErrUniqueConstraintSetNil)
	}
	if err := nilSet.Upsert(1, tu22User{}); !errors.Is(err, ErrUniqueConstraintSetNil) {
		t.Fatalf("nil Upsert error = %v, want %v", err, ErrUniqueConstraintSetNil)
	}
	if nilSet.Contains(1) || nilSet.Delete(1) || nilSet.Len() != 0 {
		t.Fatal("nil set reported a live row")
	}
}

func TestTU22UniqueConstraintSetConcurrentInsert(t *testing.T) {
	set, err := NewUniqueConstraintSet[tu22User, string]([]UniqueConstraint[tu22User, string]{
		{Name: "email", Key: func(user tu22User) (string, bool) { return user.Email, true }},
	})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for id := uint64(1); id <= 32; id++ {
		id := id
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := set.Insert(id, tu22User{Email: tu22Email(id)}); err != nil {
				t.Errorf("Insert(%d): %v", id, err)
			}
		}()
	}
	wait.Wait()
	if set.Len() != 32 {
		t.Fatalf("Len after concurrent inserts = %d, want 32", set.Len())
	}
}
