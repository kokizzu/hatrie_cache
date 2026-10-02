package hatDataStructure

import (
	"strconv"
	"testing"
)

type crossIndexUniqueBenchmarkUser struct {
	Email    string
	Tenant   string
	Username string
}

func crossIndexUniqueBenchmarkUsers() []crossIndexUniqueBenchmarkUser {
	users := make([]crossIndexUniqueBenchmarkUser, 64)
	for index := range users {
		users[index] = crossIndexUniqueBenchmarkUser{
			Email:    "user-" + strconv.Itoa(index) + "@example.com",
			Tenant:   "tenant-" + strconv.Itoa(index%8),
			Username: "name-" + strconv.Itoa(index),
		}
	}
	return users
}

func crossIndexUniqueBenchmarkIndex(t *testing.B) *CrossIndexUnique[crossIndexUniqueBenchmarkUser] {
	t.Helper()
	index, err := NewCrossIndexUnique([]CrossIndexUniqueConstraint[crossIndexUniqueBenchmarkUser]{
		{Name: "email", Key: func(user crossIndexUniqueBenchmarkUser) (string, error) { return user.Email, nil }},
		{Name: "tenant_username", Key: func(user crossIndexUniqueBenchmarkUser) (string, error) {
			return user.Tenant + "\x00" + user.Username, nil
		}},
	}, 64)
	if err != nil {
		t.Fatal(err)
	}
	return index
}

func BenchmarkCrossIndexUniqueManualUpsert(b *testing.B) {
	users := crossIndexUniqueBenchmarkUsers()
	byEmail := make(map[string]uint64, len(users))
	byTenantUsername := make(map[string]uint64, len(users))
	previous := users[0]
	for index := range users {
		id := uint64(index + 1)
		byEmail[users[index].Email] = id
		byTenantUsername[users[index].Tenant+"\x00"+users[index].Username] = id
	}
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		id := uint64((index % len(users)) + 1)
		user := users[index%len(users)]
		emailKey := user.Email
		compositeKey := user.Tenant + "\x00" + user.Username
		if owner, ok := byEmail[emailKey]; ok && owner != id {
			b.Fatal("unexpected email conflict")
		}
		if owner, ok := byTenantUsername[compositeKey]; ok && owner != id {
			b.Fatal("unexpected composite conflict")
		}
		oldEmail := previous.Email
		oldComposite := previous.Tenant + "\x00" + previous.Username
		delete(byEmail, oldEmail)
		delete(byTenantUsername, oldComposite)
		byEmail[emailKey] = id
		byTenantUsername[compositeKey] = id
		previous = user
	}
}

func BenchmarkCrossIndexUniqueUpsert(b *testing.B) {
	users := crossIndexUniqueBenchmarkUsers()
	index := crossIndexUniqueBenchmarkIndex(b)
	for id, user := range users {
		if err := index.Upsert(uint64(id+1), user); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id := uint64((iteration % len(users)) + 1)
		if err := index.Upsert(id, users[iteration%len(users)]); err != nil {
			b.Fatal(err)
		}
	}
}

func crossIndexUniqueBenchmarkAlternates() []crossIndexUniqueBenchmarkUser {
	users := crossIndexUniqueBenchmarkUsers()
	for index := range users {
		users[index].Email += "-alternate"
		users[index].Username += "-alternate"
	}
	return users
}

func BenchmarkCrossIndexUniqueChangedUpsert(b *testing.B) {
	users := crossIndexUniqueBenchmarkUsers()
	alternates := crossIndexUniqueBenchmarkAlternates()
	index := crossIndexUniqueBenchmarkIndex(b)
	for id, user := range users {
		if err := index.Upsert(uint64(id+1), user); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id := iteration % len(users)
		user := alternates[id]
		if iteration/len(users)%2 != 0 {
			user = users[id]
		}
		if err := index.Upsert(uint64(id+1), user); err != nil {
			b.Fatal(err)
		}
	}
}
