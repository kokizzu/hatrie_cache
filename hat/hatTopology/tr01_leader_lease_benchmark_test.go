package hatTopology

import (
	"testing"
	"time"
)

func BenchmarkTR01InlineLeaseValidation(b *testing.B) {
	now := time.Unix(4000, 0)
	holder := "node-a"
	token := uint64(1)
	expiresAt := now.Add(time.Hour)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if holder != "node-a" || token != 1 || !now.Before(expiresAt) {
			b.Fatal("inline lease validation failed")
		}
	}
}

func BenchmarkTR01LeaderLeaseValidation(b *testing.B) {
	now := time.Unix(4000, 0)
	store, err := NewLeaderLeaseStore(LeaderLeaseOptions{Now: func() time.Time { return now }})
	if err != nil {
		b.Fatal(err)
	}
	lease, err := store.Acquire("shard-0", "node-a", DefaultLeaderLeaseMaxTTL)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if err := store.Validate(lease.Name, lease.Holder, lease.Token); err != nil {
			b.Fatal(err)
		}
	}
}
