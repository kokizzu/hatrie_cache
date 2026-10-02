package hatDataStructure

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type crossIndexUniqueUser struct {
	Email    string
	Tenant   string
	Username string
}

func crossIndexUniqueUserConstraints() []CrossIndexUniqueConstraint[crossIndexUniqueUser] {
	return []CrossIndexUniqueConstraint[crossIndexUniqueUser]{
		{
			Name: "email",
			Key: func(user crossIndexUniqueUser) (string, error) {
				return strings.ToLower(strings.TrimSpace(user.Email)), nil
			},
		},
		{
			Name: "tenant_username",
			Key: func(user crossIndexUniqueUser) (string, error) {
				return user.Tenant + "\x00" + user.Username, nil
			},
		},
	}
}

func TestCrossIndexUniqueUpsertIsAtomicAcrossConstraints(t *testing.T) {
	registry, err := NewCrossIndexUnique(crossIndexUniqueUserConstraints(), 4)
	if err != nil {
		t.Fatalf("NewCrossIndexUnique() error = %v", err)
	}
	first := crossIndexUniqueUser{Email: "Alice@Example.com", Tenant: "acme", Username: "alice"}
	if err := registry.Upsert(1, first); err != nil {
		t.Fatalf("first Upsert() error = %v", err)
	}

	conflict := crossIndexUniqueUser{Email: "new@example.com", Tenant: "acme", Username: "alice"}
	if err := registry.Upsert(2, conflict); !errors.Is(err, ErrCrossIndexUniqueDuplicate) {
		t.Fatalf("cross-constraint conflict = %v, want duplicate", err)
	}
	if registry.Len() != 1 {
		t.Fatalf("Len() after rejected cross-constraint update = %d, want 1", registry.Len())
	}
	if _, ok, err := registry.Lookup("email", "new@example.com"); err != nil || ok {
		t.Fatalf("partially applied email key = %v/%t, want absent", err, ok)
	}
	if value, ok, err := registry.Lookup("email", "alice@example.com"); err != nil || !ok || value != first {
		t.Fatalf("original email key = %#v/%t/%v, want original user", value, ok, err)
	}

	if err := registry.Upsert(2, crossIndexUniqueUser{Email: "new@example.com", Tenant: "acme", Username: "bob"}); err != nil {
		t.Fatalf("non-conflicting Upsert() error = %v", err)
	}
	if registry.Len() != 2 {
		t.Fatalf("Len() after accepted update = %d, want 2", registry.Len())
	}
	if err := registry.Upsert(1, crossIndexUniqueUser{Email: "new@example.com", Tenant: "acme", Username: "ann"}); !errors.Is(err, ErrCrossIndexUniqueDuplicate) {
		t.Fatalf("existing-ID conflict = %v, want duplicate", err)
	}
	if value, ok, err := registry.Lookup("tenant_username", "acme\x00alice"); err != nil || !ok || value != first {
		t.Fatalf("existing value after rejected update = %#v/%t/%v, want original user", value, ok, err)
	}
}

func TestCrossIndexUniqueUpdatesAndDeletesAllIndexes(t *testing.T) {
	registry, err := NewCrossIndexUnique(crossIndexUniqueUserConstraints(), 0)
	if err != nil {
		t.Fatalf("NewCrossIndexUnique() error = %v", err)
	}
	old := crossIndexUniqueUser{Email: "old@example.com", Tenant: "acme", Username: "alice"}
	updated := crossIndexUniqueUser{Email: "new@example.com", Tenant: "acme", Username: "ann"}
	if err := registry.Upsert(7, old); err != nil {
		t.Fatalf("old Upsert() error = %v", err)
	}
	if err := registry.Upsert(7, updated); err != nil {
		t.Fatalf("updated Upsert() error = %v", err)
	}
	if _, ok, err := registry.Lookup("email", "old@example.com"); err != nil || ok {
		t.Fatalf("old email lookup = %v/%t, want absent", err, ok)
	}
	if value, ok, err := registry.Lookup("tenant_username", "acme\x00ann"); err != nil || !ok || value != updated {
		t.Fatalf("updated composite lookup = %#v/%t/%v, want updated user", value, ok, err)
	}
	if !registry.Delete(7) || registry.Delete(7) {
		t.Fatal("Delete() presence result is incorrect")
	}
	if registry.Len() != 0 {
		t.Fatalf("Len() after Delete() = %d, want 0", registry.Len())
	}
}

func TestCrossIndexUniqueRejectsInvalidDefinitionsAndKeyErrors(t *testing.T) {
	cases := []struct {
		name        string
		constraints []CrossIndexUniqueConstraint[crossIndexUniqueUser]
	}{
		{name: "empty", constraints: nil},
		{name: "empty-name", constraints: []CrossIndexUniqueConstraint[crossIndexUniqueUser]{{Key: func(crossIndexUniqueUser) (string, error) { return "", nil }}}},
		{name: "nil-key", constraints: []CrossIndexUniqueConstraint[crossIndexUniqueUser]{{Name: "email"}}},
		{name: "duplicate-name", constraints: []CrossIndexUniqueConstraint[crossIndexUniqueUser]{{Name: "email", Key: func(crossIndexUniqueUser) (string, error) { return "a", nil }}, {Name: "email", Key: func(crossIndexUniqueUser) (string, error) { return "b", nil }}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewCrossIndexUnique(test.constraints, 0); !errors.Is(err, ErrCrossIndexUniqueInvalid) {
				t.Fatalf("NewCrossIndexUnique() error = %v, want invalid definition", err)
			}
		})
	}

	keyErr := errors.New("invalid user")
	registry, err := NewCrossIndexUnique([]CrossIndexUniqueConstraint[crossIndexUniqueUser]{
		{Name: "email", Key: func(crossIndexUniqueUser) (string, error) { return "", keyErr }},
	}, 0)
	if err != nil {
		t.Fatalf("key-error NewCrossIndexUnique() error = %v", err)
	}
	if err := registry.Upsert(1, crossIndexUniqueUser{}); !errors.Is(err, keyErr) {
		t.Fatalf("key extraction error = %v, want %v", err, keyErr)
	}
}

func TestCrossIndexUniqueLookupAndClearValidateNames(t *testing.T) {
	var zero CrossIndexUnique[crossIndexUniqueUser]
	if err := zero.Upsert(1, crossIndexUniqueUser{}); !errors.Is(err, ErrCrossIndexUniqueInvalid) {
		t.Fatalf("zero-value Upsert() error = %v, want invalid registry", err)
	}

	registry, err := NewCrossIndexUnique(crossIndexUniqueUserConstraints(), 0)
	if err != nil {
		t.Fatalf("NewCrossIndexUnique() error = %v", err)
	}
	if _, _, err := registry.Lookup("missing", "key"); !errors.Is(err, ErrCrossIndexUniqueConstraint) {
		t.Fatalf("missing constraint error = %v, want constraint error", err)
	}
	if names := registry.ConstraintNames(); len(names) != 2 || names[0] != "email" || names[1] != "tenant_username" {
		t.Fatalf("ConstraintNames() = %#v, want ordered names", names)
	}
	if err := registry.Upsert(1, crossIndexUniqueUser{Email: "a@example.com", Tenant: "acme", Username: "a"}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	registry.Clear()
	if registry.Len() != 0 {
		t.Fatalf("Len() after Clear() = %d, want 0", registry.Len())
	}
}

func TestCrossIndexUniqueKeyErrorDoesNotMutateExistingValue(t *testing.T) {
	keyErr := errors.New("invalid user")
	registry, err := NewCrossIndexUnique([]CrossIndexUniqueConstraint[crossIndexUniqueUser]{
		{Name: "email", Key: func(user crossIndexUniqueUser) (string, error) {
			if user.Email == "bad" {
				return "", keyErr
			}
			return user.Email, nil
		}},
	}, 0)
	if err != nil {
		t.Fatalf("NewCrossIndexUnique() error = %v", err)
	}
	good := crossIndexUniqueUser{Email: "good@example.com"}
	if err := registry.Upsert(1, good); err != nil {
		t.Fatalf("good Upsert() error = %v", err)
	}
	if err := registry.Upsert(1, crossIndexUniqueUser{Email: "bad"}); !errors.Is(err, keyErr) {
		t.Fatalf("key extraction error = %v, want %v", err, keyErr)
	}
	if value, ok, err := registry.Lookup("email", good.Email); err != nil || !ok || value != good {
		t.Fatalf("value after key error = %#v/%t/%v, want original user", value, ok, err)
	}
}

func TestCrossIndexUniqueSupportsMoreThanInlineConstraints(t *testing.T) {
	constraints := make([]CrossIndexUniqueConstraint[crossIndexUniqueUser], 9)
	for index := range constraints {
		field := index
		constraints[index] = CrossIndexUniqueConstraint[crossIndexUniqueUser]{
			Name: "constraint-" + strconv.Itoa(index),
			Key: func(user crossIndexUniqueUser) (string, error) {
				return user.Email + "#" + strconv.Itoa(field), nil
			},
		}
	}
	registry, err := NewCrossIndexUnique(constraints, 1)
	if err != nil {
		t.Fatalf("NewCrossIndexUnique() error = %v", err)
	}
	if err := registry.Upsert(1, crossIndexUniqueUser{Email: "first@example.com"}); err != nil {
		t.Fatalf("first Upsert() error = %v", err)
	}
	if err := registry.Upsert(1, crossIndexUniqueUser{Email: "second@example.com"}); err != nil {
		t.Fatalf("changed Upsert() error = %v", err)
	}
	if _, ok, err := registry.Lookup("constraint-8", "first@example.com#8"); err != nil || ok {
		t.Fatalf("old dynamic key lookup = %v/%t, want absent", err, ok)
	}
}

func TestCrossIndexUniqueConcurrentUpsertsAndLookups(t *testing.T) {
	registry, err := NewCrossIndexUnique(crossIndexUniqueUserConstraints(), 8)
	if err != nil {
		t.Fatalf("NewCrossIndexUnique() error = %v", err)
	}
	const workers = 8
	var wait sync.WaitGroup
	errorsCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			user := crossIndexUniqueUser{
				Email:    "worker-" + strconv.Itoa(worker) + "@example.com",
				Tenant:   "tenant-" + strconv.Itoa(worker),
				Username: "user-" + strconv.Itoa(worker),
			}
			id := uint64(worker + 1)
			for iteration := 0; iteration < 100; iteration++ {
				if err := registry.Upsert(id, user); err != nil {
					errorsCh <- err
					return
				}
				if _, ok, err := registry.Lookup("email", user.Email); err != nil || !ok {
					if err == nil {
						err = ErrCrossIndexUniqueInvalid
					}
					errorsCh <- err
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("concurrent operation error = %v", err)
	}
	if registry.Len() != workers {
		t.Fatalf("Len() after concurrent upserts = %d, want %d", registry.Len(), workers)
	}
}
