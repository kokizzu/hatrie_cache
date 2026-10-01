package hatDataStructure_test

import (
	"errors"
	"strconv"
	"sync"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

type tU22User struct {
	Email    string
	Username string
}

func tU22Constraints() []hatDataStructure.UniqueConstraint[tU22User] {
	return []hatDataStructure.UniqueConstraint[tU22User]{
		{Name: "email", Extract: func(user tU22User) string { return user.Email }},
		{Name: "username", Extract: func(user tU22User) string { return user.Username }},
	}
}

func TestUniqueConstraintSetValidatesAllKeysBeforeMutation(t *testing.T) {
	set, err := hatDataStructure.NewUniqueConstraintSet(tU22Constraints(), hatDataStructure.UniqueConstraintSetOptions{Capacity: 4})
	if err != nil {
		t.Fatal(err)
	}
	first := tU22User{Email: "a@example.test", Username: "ada"}
	second := tU22User{Email: "b@example.test", Username: "lin"}
	if err := set.Upsert(1, first); err != nil {
		t.Fatal(err)
	}
	if err := set.Upsert(2, second); err != nil {
		t.Fatal(err)
	}

	conflict := tU22User{Email: "new@example.test", Username: "ada"}
	err = set.Upsert(2, conflict)
	if !errors.Is(err, hatDataStructure.ErrUniqueConstraintDuplicateKey) {
		t.Fatalf("conflicting Upsert() error = %v, want duplicate error", err)
	}
	if got, ok := set.LookupByID(2); !ok || got.Value != second {
		t.Fatalf("conflicting Upsert() changed row 2: %#v/%v", got, ok)
	}
	if _, ok := set.Lookup("email", "new@example.test"); ok {
		t.Fatal("conflicting Upsert() published email before all constraints passed")
	}

	updated := tU22User{Email: "new@example.test", Username: "lin-new"}
	if err := set.Upsert(2, updated); err != nil {
		t.Fatal(err)
	}
	if _, ok := set.Lookup("email", second.Email); ok {
		t.Fatal("old email remained after update")
	}
	if _, ok := set.Lookup("username", second.Username); ok {
		t.Fatal("old username remained after update")
	}
	if got, ok := set.Lookup("email", updated.Email); !ok || got.ID != 2 || got.Value != updated {
		t.Fatalf("updated email lookup = %#v/%v", got, ok)
	}
	if got := set.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestUniqueConstraintSetRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name        string
		constraints []hatDataStructure.UniqueConstraint[tU22User]
	}{
		{name: "empty", constraints: nil},
		{name: "missing name", constraints: []hatDataStructure.UniqueConstraint[tU22User]{{Extract: func(tU22User) string { return "x" }}}},
		{name: "missing extractor", constraints: []hatDataStructure.UniqueConstraint[tU22User]{{Name: "email"}}},
		{name: "duplicate name", constraints: []hatDataStructure.UniqueConstraint[tU22User]{
			{Name: "email", Extract: func(tU22User) string { return "x" }},
			{Name: "email", Extract: func(tU22User) string { return "y" }},
		}},
		{name: "capacity too large", constraints: tU22Constraints()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := hatDataStructure.UniqueConstraintSetOptions{}
			if test.name == "capacity too large" {
				options.Capacity = hatDataStructure.MaxUniqueConstraintSetCapacity + 1
			}
			if _, err := hatDataStructure.NewUniqueConstraintSet(test.constraints, options); !errors.Is(err, hatDataStructure.ErrUniqueConstraintDefinitionInvalid) {
				t.Fatalf("NewUniqueConstraintSet() error = %v, want definition error", err)
			}
		})
	}
}

func TestUniqueConstraintSetDeleteAndClear(t *testing.T) {
	set, err := hatDataStructure.NewUniqueConstraintSet(tU22Constraints(), hatDataStructure.UniqueConstraintSetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := set.Upsert(1, tU22User{Email: "a@example.test", Username: "ada"}); err != nil {
		t.Fatal(err)
	}
	if !set.Delete(1) || set.Delete(1) {
		t.Fatal("Delete() presence result is incorrect")
	}
	if set.Len() != 0 || set.Contains("email", "a@example.test") {
		t.Fatal("Delete() retained constraint ownership")
	}
	if err := set.Upsert(2, tU22User{Email: "b@example.test", Username: "lin"}); err != nil {
		t.Fatal(err)
	}
	set.Clear()
	if set.Len() != 0 || set.Contains("username", "lin") {
		t.Fatal("Clear() retained constraint ownership")
	}
}

func TestUniqueConstraintSetSupportsConcurrentReadersAndWriters(t *testing.T) {
	set, err := hatDataStructure.NewUniqueConstraintSet(tU22Constraints(), hatDataStructure.UniqueConstraintSetOptions{Capacity: 128})
	if err != nil {
		t.Fatal(err)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			for offset := 0; offset < 32; offset++ {
				id := uint64(worker*1000 + offset)
				suffix := strconv.Itoa(worker) + "-" + strconv.Itoa(offset)
				user := tU22User{Email: suffix + "@example.test", Username: "user-" + suffix}
				if err := set.Upsert(id, user); err != nil {
					t.Errorf("Upsert(%d) error = %v", id, err)
					return
				}
				if got, ok := set.LookupByID(id); !ok || got.Value != user {
					t.Errorf("LookupByID(%d) = %#v/%v", id, got, ok)
					return
				}
				if !set.Delete(id) {
					t.Errorf("Delete(%d) = false", id)
					return
				}
			}
		}()
	}
	wait.Wait()
	if got := set.Len(); got != 0 {
		t.Fatalf("Len() after concurrent insert/delete = %d, want 0", got)
	}
}
