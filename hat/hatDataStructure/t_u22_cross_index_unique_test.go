package hatDataStructure

import (
	"errors"
	"reflect"
	"testing"
)

type tu22Record struct {
	Email    string
	Username string
	Group    string
}

func tu22Definitions() []CrossIndexUniqueDefinition[tu22Record] {
	return []CrossIndexUniqueDefinition[tu22Record]{
		{Name: "email", Key: func(record tu22Record) string { return record.Email }},
		{Name: "username", Key: func(record tu22Record) string { return record.Username }},
	}
}

func TestTU22CrossIndexUniqueSetRejectsConflictsAtomically(t *testing.T) {
	set, err := NewCrossIndexUniqueSet(tu22Definitions(), CrossIndexUniqueOptions{MaxEntries: 4})
	if err != nil {
		t.Fatal(err)
	}
	first := tu22Record{Email: "ada@example.test", Username: "ada", Group: "users"}
	second := tu22Record{Email: "grace@example.test", Username: "grace", Group: "users"}
	if err := set.Upsert(1, first); err != nil {
		t.Fatal(err)
	}
	if err := set.Upsert(2, second); err != nil {
		t.Fatal(err)
	}

	conflict := tu22Record{Email: first.Email, Username: "new-grace", Group: "changed"}
	if err := set.Upsert(2, conflict); !errors.Is(err, ErrCrossIndexUniqueConflict) {
		t.Fatalf("conflicting update error = %v, want ErrCrossIndexUniqueConflict", err)
	}
	for _, check := range []struct {
		index string
		key   string
		id    uint64
	}{
		{index: "email", key: second.Email, id: 2},
		{index: "username", key: second.Username, id: 2},
	} {
		if got, ok := set.LookupOwner(check.index, check.key); !ok || got != check.id {
			t.Fatalf("LookupOwner(%q, %q) = %d, %v, want %d, true", check.index, check.key, got, ok, check.id)
		}
	}
	if _, ok := set.LookupOwner("username", conflict.Username); ok {
		t.Fatal("conflicting update partially inserted the second index key")
	}

	updated := tu22Record{Email: "updated@example.test", Username: "updated-grace", Group: "admins"}
	if err := set.Upsert(2, updated); err != nil {
		t.Fatal(err)
	}
	if _, ok := set.LookupOwner("email", second.Email); ok {
		t.Fatal("old email key still owned after update")
	}
	if _, ok := set.LookupOwner("username", second.Username); ok {
		t.Fatal("old username key still owned after update")
	}
	if got, ok := set.LookupOwner("email", updated.Email); !ok || got != 2 {
		t.Fatalf("updated email owner = %d, %v", got, ok)
	}
	if got, ok := set.LookupOwner("username", updated.Username); !ok || got != 2 {
		t.Fatalf("updated username owner = %d, %v", got, ok)
	}
	if got := set.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
}

func TestTU22CrossIndexUniqueSetKeepsOwnersWhenKeysDoNotChange(t *testing.T) {
	set, err := NewCrossIndexUniqueSet(tu22Definitions(), CrossIndexUniqueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initial := tu22Record{Email: "ada@example.test", Username: "ada", Group: "users"}
	if err := set.Upsert(1, initial); err != nil {
		t.Fatal(err)
	}
	updated := initial
	updated.Group = "admins"
	if err := set.Upsert(1, updated); err != nil {
		t.Fatal(err)
	}
	for index, key := range map[string]string{"email": initial.Email, "username": initial.Username} {
		if owner, ok := set.LookupOwner(index, key); !ok || owner != 1 {
			t.Fatalf("LookupOwner(%q, %q) = %d, %v, want 1, true", index, key, owner, ok)
		}
	}
}

func TestTU22CrossIndexUniqueSetDeleteAndCapacity(t *testing.T) {
	set, err := NewCrossIndexUniqueSet(tu22Definitions(), CrossIndexUniqueOptions{MaxEntries: 1})
	if err != nil {
		t.Fatal(err)
	}
	value := tu22Record{Email: "ada@example.test", Username: "ada"}
	if err := set.Upsert(1, value); err != nil {
		t.Fatal(err)
	}
	if err := set.Upsert(2, tu22Record{Email: "grace@example.test", Username: "grace"}); !errors.Is(err, ErrCrossIndexUniqueLimit) {
		t.Fatalf("capacity error = %v, want ErrCrossIndexUniqueLimit", err)
	}
	if !set.Delete(1) || set.Delete(1) {
		t.Fatal("Delete() presence result is incorrect")
	}
	for index, key := range map[string]string{"email": value.Email, "username": value.Username} {
		if _, ok := set.LookupOwner(index, key); ok {
			t.Fatalf("deleted key remains in %s index", index)
		}
	}
	if got := set.Len(); got != 0 {
		t.Fatalf("Len() after delete = %d, want 0", got)
	}
}

func TestTU22CrossIndexUniqueSetRejectsInvalidDefinitions(t *testing.T) {
	if _, err := NewCrossIndexUniqueSet[tu22Record](nil, CrossIndexUniqueOptions{}); !errors.Is(err, ErrCrossIndexUniqueInvalid) {
		t.Fatalf("empty definition error = %v", err)
	}
	if _, err := NewCrossIndexUniqueSet([]CrossIndexUniqueDefinition[tu22Record]{
		{Name: "email", Key: func(tu22Record) string { return "" }},
		{Name: "email", Key: func(tu22Record) string { return "" }},
	}, CrossIndexUniqueOptions{}); !errors.Is(err, ErrCrossIndexUniqueInvalid) {
		t.Fatalf("duplicate definition error = %v", err)
	}
	set, err := NewCrossIndexUniqueSet(tu22Definitions(), CrossIndexUniqueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := set.LookupOwner("missing", "key"); ok {
		t.Fatal("unknown index lookup succeeded")
	}
	if _, ok := set.LookupOwner("email", "missing"); ok {
		t.Fatal("missing key lookup succeeded")
	}
	if !reflect.DeepEqual(set.IndexNames(), []string{"email", "username"}) {
		t.Fatalf("IndexNames() = %#v", set.IndexNames())
	}
}
