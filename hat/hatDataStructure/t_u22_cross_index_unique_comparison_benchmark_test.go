package hatDataStructure

import (
	"strconv"
	"testing"
)

type tu22SequentialRecord struct {
	email    string
	username string
}

type tu22SequentialUniqueSet struct {
	emailOwners    map[string]uint64
	usernameOwners map[string]uint64
	records        map[uint64]tu22SequentialRecord
}

func newTU22SequentialUniqueSet(capacity int) *tu22SequentialUniqueSet {
	return &tu22SequentialUniqueSet{
		emailOwners:    make(map[string]uint64, capacity),
		usernameOwners: make(map[string]uint64, capacity),
		records:        make(map[uint64]tu22SequentialRecord, capacity),
	}
}

func (set *tu22SequentialUniqueSet) upsert(id uint64, value tu22SequentialRecord) bool {
	if owner, ok := set.emailOwners[value.email]; ok && owner != id {
		return false
	}
	if owner, ok := set.usernameOwners[value.username]; ok && owner != id {
		return false
	}
	old, exists := set.records[id]
	if exists {
		delete(set.emailOwners, old.email)
		delete(set.usernameOwners, old.username)
	}
	set.emailOwners[value.email] = id
	set.usernameOwners[value.username] = id
	set.records[id] = value
	return true
}

func BenchmarkTU22BaselineSequentialTwoIndexUpsert(b *testing.B) {
	set := newTU22SequentialUniqueSet(1024)
	values := make([]tu22SequentialRecord, 1024)
	for i := range values {
		key := "user-" + strconv.Itoa(i)
		values[i] = tu22SequentialRecord{email: key + "@example.test", username: key}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !set.upsert(uint64(i%len(values)), values[i%len(values)]) {
			b.Fatal("baseline upsert failed")
		}
	}
}

func BenchmarkTU22BaselineSequentialTwoIndexChangingKeys(b *testing.B) {
	set := newTU22SequentialUniqueSet(1024)
	values := [2][]tu22SequentialRecord{make([]tu22SequentialRecord, 1024), make([]tu22SequentialRecord, 1024)}
	for version := range values {
		for i := range values[version] {
			key := "user-" + strconv.Itoa(i) + "-" + strconv.Itoa(version)
			values[version][i] = tu22SequentialRecord{email: key + "@example.test", username: key}
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !set.upsert(uint64(i%len(values[0])), values[i%2][i%len(values[0])]) {
			b.Fatal("baseline upsert failed")
		}
	}
}
