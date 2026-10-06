package hatTopology

import (
	"sync"
	"testing"
)

type tu13BaselineMembership struct {
	mu      sync.RWMutex
	members map[string]string
}

func (membership *tu13BaselineMembership) lookup(name string) (string, bool) {
	membership.mu.RLock()
	address, ok := membership.members[name]
	membership.mu.RUnlock()
	return address, ok
}

func BenchmarkTU13BaselineMembershipLookup(b *testing.B) {
	membership := tu13BaselineMembership{members: map[string]string{"node-a": "127.0.0.1:9001"}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = membership.lookup("node-a")
	}
}

func BenchmarkTU13BaselineMembershipLookupParallel(b *testing.B) {
	membership := tu13BaselineMembership{members: map[string]string{"node-a": "127.0.0.1:9001"}}
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = membership.lookup("node-a")
		}
	})
}
