package hatReplication

import "testing"

var tu038ConflictIntrospectionSink ConflictVersion

func BenchmarkTU038ConflictResolutionBaseline(b *testing.B) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		winner, err := registry.Resolve("orders", left, right)
		if err != nil {
			b.Fatal(err)
		}
		tu038ConflictIntrospectionSink = winner
	}
}

func BenchmarkTU038ConflictResolutionWithIntrospection(b *testing.B) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	log, err := NewConflictIntrospectionLog(ConflictIntrospectionOptions{
		MaxEvents:     4096,
		KeyHashSecret: []byte("tu038-benchmark-secret"),
	})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 1, NodeID: "node-a", Sequence: 1}
	right := ConflictVersion{Timestamp: 2, NodeID: "node-b", Sequence: 1}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		winner, err := registry.ResolveWithKey(log, "orders", "order-1", left, right)
		if err != nil {
			b.Fatal(err)
		}
		tu038ConflictIntrospectionSink = winner
	}
}
