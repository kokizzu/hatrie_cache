package hatDataStructure_test

import (
	"strconv"
	"testing"

	"hatrie_cache/hat/hatDataStructure"
)

type tU22BenchmarkUser struct {
	Email    string
	Username string
}

var tU22BenchmarkSink uint64

func BenchmarkTU22SeparateUniqueIndexes(b *testing.B) {
	users := make([]tU22BenchmarkUser, 256)
	for i := range users {
		users[i] = tU22BenchmarkUser{Email: "user-" + strconv.Itoa(i) + "@example.test", Username: "user-" + strconv.Itoa(i)}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		email, err := hatDataStructure.NewHashIndex(func(user tU22BenchmarkUser) string { return user.Email }, hatDataStructure.HashIndexOptions{Unique: true, Capacity: len(users)})
		if err != nil {
			b.Fatal(err)
		}
		username, err := hatDataStructure.NewHashIndex(func(user tU22BenchmarkUser) string { return user.Username }, hatDataStructure.HashIndexOptions{Unique: true, Capacity: len(users)})
		if err != nil {
			b.Fatal(err)
		}
		for id, user := range users {
			if err := email.Upsert(uint64(id), user); err != nil {
				b.Fatal(err)
			}
			if err := username.Upsert(uint64(id), user); err != nil {
				b.Fatal(err)
			}
		}
		tU22BenchmarkSink += uint64(email.Len() + username.Len())
	}
}

func BenchmarkTU22UniqueConstraintSet(b *testing.B) {
	users := make([]tU22BenchmarkUser, 256)
	for i := range users {
		users[i] = tU22BenchmarkUser{Email: "user-" + strconv.Itoa(i) + "@example.test", Username: "user-" + strconv.Itoa(i)}
	}
	constraints := []hatDataStructure.UniqueConstraint[tU22BenchmarkUser]{
		{Name: "email", Extract: func(user tU22BenchmarkUser) string { return user.Email }},
		{Name: "username", Extract: func(user tU22BenchmarkUser) string { return user.Username }},
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		set, err := hatDataStructure.NewUniqueConstraintSet(constraints, hatDataStructure.UniqueConstraintSetOptions{Capacity: len(users)})
		if err != nil {
			b.Fatal(err)
		}
		for id, user := range users {
			if err := set.Upsert(uint64(id), user); err != nil {
				b.Fatal(err)
			}
		}
		tU22BenchmarkSink += uint64(set.Len())
	}
}
