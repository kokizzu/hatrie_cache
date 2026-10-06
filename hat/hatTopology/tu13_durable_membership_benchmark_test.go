package hatTopology

import "testing"

func tu13DurableMembershipRegistry(b *testing.B) *DurableMembershipRegistry {
	b.Helper()
	registry, err := NewDurableMembershipRegistry(&memoryMembershipStore{}, DurableMembershipOptions{})
	if err != nil {
		b.Fatal(err)
	}
	if _, err := registry.Join("node-a", "127.0.0.1:9001"); err != nil {
		b.Fatal(err)
	}
	return registry
}

func BenchmarkTU13DurableMembershipLookup(b *testing.B) {
	registry := tu13DurableMembershipRegistry(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = registry.Member("node-a")
	}
}

func BenchmarkTU13DurableMembershipLookupParallel(b *testing.B) {
	registry := tu13DurableMembershipRegistry(b)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = registry.Member("node-a")
		}
	})
}

func BenchmarkTU13DurableMembershipEncode(b *testing.B) {
	snapshot := MembershipSnapshot{Revision: 16}
	for index := 0; index < 16; index++ {
		snapshot.Members = append(snapshot.Members, ClusterMember{
			Name:       "node-" + string(rune('a'+index)),
			Address:    "127.0.0.1:9" + string(rune('0'+index)),
			Generation: 1,
			State:      MembershipStateActive,
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		payload, err := EncodeMembershipSnapshot(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		tu13MembershipEncodeSink = payload
	}
}

var tu13MembershipEncodeSink []byte
