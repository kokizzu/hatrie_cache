package hatReplication

import "testing"

var t210ConflictVersionSink ConflictVersion
var t210ConflictEventSink ConflictResolutionEvent

func BenchmarkT210ResolveWithoutHook(b *testing.B) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "region-a", Sequence: 7}
	right := ConflictVersion{Timestamp: 11, NodeID: "region-b", Sequence: 9}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		winner, err := registry.Resolve("orders", left, right)
		if err != nil {
			b.Fatal(err)
		}
		t210ConflictVersionSink = winner
	}
}

func BenchmarkT210ResolveWithHook(b *testing.B) {
	registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
	if err != nil {
		b.Fatal(err)
	}
	left := ConflictVersion{Timestamp: 10, NodeID: "region-a", Sequence: 7}
	right := ConflictVersion{Timestamp: 11, NodeID: "region-b", Sequence: 9}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		winner, err := registry.ResolveWithHook("orders", left, right, func(event ConflictResolutionEvent) {
			t210ConflictEventSink = event
		})
		if err != nil {
			b.Fatal(err)
		}
		t210ConflictVersionSink = winner
	}
}
