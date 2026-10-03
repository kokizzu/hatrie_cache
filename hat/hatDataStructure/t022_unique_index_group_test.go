package hatDataStructure

import (
	"errors"
	"sync"
	"testing"
)

type t022UniqueUser struct {
	Email string
	Phone string
}

func t022UniqueConstraints() []UniqueIndexConstraint[t022UniqueUser] {
	return []UniqueIndexConstraint[t022UniqueUser]{
		{Name: "email", Extract: func(user t022UniqueUser) (string, bool) { return user.Email, user.Email != "" }},
		{Name: "phone", Extract: func(user t022UniqueUser) (string, bool) { return user.Phone, user.Phone != "" }},
	}
}

func TestT022UniqueIndexGroupEnforcesMultipleIndexesAtomically(t *testing.T) {
	group, err := NewUniqueIndexGroup(t022UniqueConstraints(), 8)
	if err != nil {
		t.Fatalf("NewUniqueIndexGroup() error = %v", err)
	}
	if err := group.Upsert(1, t022UniqueUser{Email: "ada@example.test", Phone: "+1-1"}); err != nil {
		t.Fatalf("Upsert(1) error = %v", err)
	}
	if err := group.Upsert(2, t022UniqueUser{Email: "grace@example.test", Phone: "+1-2"}); err != nil {
		t.Fatalf("Upsert(2) error = %v", err)
	}
	if owner, ok, err := group.Lookup("email", "ada@example.test"); err != nil || !ok || owner != 1 {
		t.Fatalf("email lookup = %d/%t/%v, want 1/true/nil", owner, ok, err)
	}

	if err := group.Upsert(3, t022UniqueUser{Email: "ada@example.test", Phone: "+1-3"}); !errors.Is(err, ErrUniqueIndexGroupDuplicate) {
		t.Fatalf("duplicate email error = %v, want ErrUniqueIndexGroupDuplicate", err)
	}
	if _, ok, _ := group.Lookup("phone", "+1-3"); ok {
		t.Fatal("rejected insert published its second unique key")
	}

	if err := group.Upsert(1, t022UniqueUser{Email: "new@example.test", Phone: "+1-2"}); !errors.Is(err, ErrUniqueIndexGroupDuplicate) {
		t.Fatalf("cross-index update error = %v, want ErrUniqueIndexGroupDuplicate", err)
	}
	if owner, ok, err := group.Lookup("email", "ada@example.test"); err != nil || !ok || owner != 1 {
		t.Fatalf("email after rejected update = %d/%t/%v, want old owner", owner, ok, err)
	}
	if owner, ok, err := group.Lookup("phone", "+1-1"); err != nil || !ok || owner != 1 {
		t.Fatalf("phone after rejected update = %d/%t/%v, want old owner", owner, ok, err)
	}

	if err := group.Upsert(1, t022UniqueUser{Email: "new@example.test", Phone: ""}); err != nil {
		t.Fatalf("valid update with absent phone error = %v", err)
	}
	if _, ok, _ := group.Lookup("email", "ada@example.test"); ok {
		t.Fatal("old email key remained after update")
	}
	if _, ok, _ := group.Lookup("phone", "+1-1"); ok {
		t.Fatal("old phone key remained after update")
	}
	if group.Delete(2) != true || group.Delete(2) {
		t.Fatal("Delete() presence result was not idempotent")
	}
	if _, ok, _ := group.Lookup("email", "grace@example.test"); ok {
		t.Fatal("deleted email key remained")
	}
	if group.Len() != 1 {
		t.Fatalf("Len() = %d, want 1", group.Len())
	}
}

func TestT022UniqueIndexGroupValidatesAndSupportsConcurrentReads(t *testing.T) {
	if _, err := NewUniqueIndexGroup[t022UniqueUser](nil, 0); !errors.Is(err, ErrUniqueIndexGroupInvalid) {
		t.Fatalf("empty constraints error = %v, want ErrUniqueIndexGroupInvalid", err)
	}
	if _, err := NewUniqueIndexGroup([]UniqueIndexConstraint[t022UniqueUser]{{Name: "email"}}, 0); !errors.Is(err, ErrUniqueIndexGroupInvalid) {
		t.Fatalf("missing extractor error = %v, want ErrUniqueIndexGroupInvalid", err)
	}
	if _, err := NewUniqueIndexGroup([]UniqueIndexConstraint[t022UniqueUser]{
		{Name: "email", Extract: func(t022UniqueUser) (string, bool) { return "", true }},
		{Name: " email ", Extract: func(t022UniqueUser) (string, bool) { return "", true }},
	}, 0); !errors.Is(err, ErrUniqueIndexGroupInvalid) {
		t.Fatalf("duplicate constraint error = %v, want ErrUniqueIndexGroupInvalid", err)
	}

	group, err := NewUniqueIndexGroup(t022UniqueConstraints(), 64)
	if err != nil {
		t.Fatalf("NewUniqueIndexGroup() error = %v", err)
	}
	var wait sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		wait.Add(1)
		go func(worker int) {
			defer wait.Done()
			for index := 0; index < 64; index++ {
				id := uint64(worker*64 + index + 1)
				_ = group.Upsert(id, t022UniqueUser{Email: "user-" + string(rune(id)), Phone: "phone-" + string(rune(id))})
				_, _, _ = group.Lookup("email", "user-"+string(rune(id)))
			}
		}(worker)
	}
	wait.Wait()
	if group.Len() == 0 {
		t.Fatal("concurrent Upsert() produced no rows")
	}
	if _, _, err := group.Lookup("missing", "value"); !errors.Is(err, ErrUniqueIndexGroupConstraintUnknown) {
		t.Fatalf("unknown constraint error = %v, want ErrUniqueIndexGroupConstraintUnknown", err)
	}
}

func BenchmarkT022UniqueIndexGroupUpsert(b *testing.B) {
	users := make([]t022UniqueUser, 1024)
	for index := range users {
		users[index] = t022UniqueUser{Email: "user-" + string(rune(index+1)), Phone: "phone-" + string(rune(index+1))}
	}
	b.Run("plain_maps", func(b *testing.B) { benchmarkT022PlainMaps(b, users) })
	b.Run("atomic_group", func(b *testing.B) { benchmarkT022AtomicGroup(b, users) })
}

func benchmarkT022PlainMaps(b *testing.B, users []t022UniqueUser) {
	emailOwners := make(map[string]uint64, len(users))
	phoneOwners := make(map[string]uint64, len(users))
	for index, user := range users {
		id := uint64(index + 1)
		emailOwners[user.Email] = id
		phoneOwners[user.Phone] = id
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		id := uint64(index&1023) + 1
		user := users[index&1023]
		if emailOwners[user.Email] != id || phoneOwners[user.Phone] != id {
			b.Fatal("plain map owner changed")
		}
	}
}

func benchmarkT022AtomicGroup(b *testing.B, users []t022UniqueUser) {
	group, err := NewUniqueIndexGroup(t022UniqueConstraints(), len(users))
	if err != nil {
		b.Fatal(err)
	}
	for index, user := range users {
		if err := group.Upsert(uint64(index+1), user); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := group.Upsert(uint64(index&1023)+1, users[index&1023]); err != nil {
			b.Fatal(err)
		}
	}
}
