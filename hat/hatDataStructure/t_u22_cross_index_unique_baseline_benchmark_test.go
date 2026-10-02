package hatDataStructure

import (
	"strconv"
	"testing"
)

type tu22BaselineRecord struct {
	Email    string
	Username string
}

func BenchmarkTU22BaselineSequentialUniqueIndexUpsert(b *testing.B) {
	email, err := NewHashIndex(
		func(record tu22BaselineRecord) string { return record.Email },
		HashIndexOptions{Unique: true, Capacity: 1},
	)
	if err != nil {
		b.Fatal(err)
	}
	username, err := NewHashIndex(
		func(record tu22BaselineRecord) string { return record.Username },
		HashIndexOptions{Unique: true, Capacity: 1},
	)
	if err != nil {
		b.Fatal(err)
	}
	values := make([]tu22BaselineRecord, 1024)
	for index := range values {
		suffix := strconv.Itoa(index)
		values[index] = tu22BaselineRecord{Email: "user-" + suffix + "@example.test", Username: "user-" + suffix}
	}
	if err := email.Upsert(1, values[0]); err != nil {
		b.Fatal(err)
	}
	if err := username.Upsert(1, values[0]); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		value := values[index%len(values)]
		if err := email.Upsert(1, value); err != nil {
			b.Fatal(err)
		}
		if err := username.Upsert(1, value); err != nil {
			b.Fatal(err)
		}
	}
}
