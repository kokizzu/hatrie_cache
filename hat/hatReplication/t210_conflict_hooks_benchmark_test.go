package hatReplication

import "testing"

var t210ConflictHookVersionSink ConflictVersion

func BenchmarkT210ConflictHook(b *testing.B) {
	local := ConflictVersion{Timestamp: 100, NodeID: "region-a", Sequence: 7}
	remote := ConflictVersion{Timestamp: 101, NodeID: "region-b", Sequence: 44}
	context := ConflictHookContext{Space: "orders", Local: local, Remote: remote}

	b.Run("default-fastpath", func(b *testing.B) {
		registry, err := NewConflictPolicyRegistry(ConflictPolicy{})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			winner, err := registry.ResolveWithContext(context)
			if err != nil {
				b.Fatal(err)
			}
			t210ConflictHookVersionSink = winner
		}
	})

	b.Run("hook-use-policy", func(b *testing.B) {
		registry, err := NewConflictPolicyRegistry(ConflictPolicy{
			Hook: func(ConflictHookContext) (ConflictHookDecision, error) {
				return ConflictHookUsePolicy, nil
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			winner, err := registry.ResolveWithContext(context)
			if err != nil {
				b.Fatal(err)
			}
			t210ConflictHookVersionSink = winner
		}
	})

	b.Run("hook-accept-remote", func(b *testing.B) {
		registry, err := NewConflictPolicyRegistry(ConflictPolicy{
			Hook: func(ConflictHookContext) (ConflictHookDecision, error) {
				return ConflictHookAcceptRemote, nil
			},
		})
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			winner, err := registry.ResolveWithContext(context)
			if err != nil {
				b.Fatal(err)
			}
			t210ConflictHookVersionSink = winner
		}
	})
}
