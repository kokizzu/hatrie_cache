package hatDataStructure

import (
	"strconv"
	"sync"
	"testing"
)

type tu22BaselineRecord struct {
	Email      string
	ExternalID string
}

// BenchmarkCrossIndexUniqueBaseline measures the map bookkeeping that a
// caller must write when several independent unique indexes are updated with
// rollback-safe ordering.
func BenchmarkCrossIndexUniqueBaseline(b *testing.B) {
	rows := make(map[uint64]tu22BaselineRecord, 1024)
	emailOwners := make(map[string]uint64, 1024)
	externalOwners := make(map[string]uint64, 1024)
	var mutex sync.Mutex
	values := make([]tu22BaselineRecord, 1024)
	for index := range values {
		values[index] = tu22BaselineRecord{
			Email:      "user-" + strconv.Itoa(index) + "@example.test",
			ExternalID: "ext-" + strconv.Itoa(index),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		mutex.Lock()
		id := uint64(iteration % len(values))
		value := values[id]
		if current, ok := rows[id]; ok {
			delete(emailOwners, current.Email)
			delete(externalOwners, current.ExternalID)
		}
		if owner, ok := emailOwners[value.Email]; ok && owner != id {
			b.Fatalf("email owner %d conflicts with id %d", owner, id)
		}
		if owner, ok := externalOwners[value.ExternalID]; ok && owner != id {
			b.Fatalf("external owner %d conflicts with id %d", owner, id)
		}
		rows[id] = value
		emailOwners[value.Email] = id
		externalOwners[value.ExternalID] = id
		mutex.Unlock()
	}
}

func BenchmarkCrossIndexUniqueSet(b *testing.B) {
	set, err := NewCrossIndexUniqueSet([]CrossIndexUniqueConstraint[tu22BaselineRecord]{
		{Name: "email", Key: func(value tu22BaselineRecord) (string, bool, error) { return value.Email, true, nil }},
		{Name: "external_id", Key: func(value tu22BaselineRecord) (string, bool, error) { return value.ExternalID, true, nil }},
	}, 1024)
	if err != nil {
		b.Fatal(err)
	}
	values := make([]tu22BaselineRecord, 1024)
	for index := range values {
		values[index] = tu22BaselineRecord{
			Email:      "user-" + strconv.Itoa(index) + "@example.test",
			ExternalID: "ext-" + strconv.Itoa(index),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		id := uint64(iteration % len(values))
		if err := set.Upsert(id, values[id]); err != nil {
			b.Fatal(err)
		}
	}
}
