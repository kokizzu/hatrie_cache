package hatTopology_test

import (
	"testing"

	"hatrie_cache/hat/hatTopology"
)

func BenchmarkT202LeaderForKey(b *testing.B) {
	provider := staticTopologyProvider{topology: hatTopology.ClusterTopology{
		Version: 1,
		Mode:    hatTopology.TopologyModeSharded,
		Nodes: []hatTopology.TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []hatTopology.TopologyShard{{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}}},
	}}

	for _, test := range []struct {
		name             string
		requireHeartbeat bool
	}{
		{name: "LegacyAssumedOnline"},
		{name: "HeartbeatRequired", requireHeartbeat: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			store := hatTopology.NewElectionStore(provider, hatTopology.ElectionOptions{
				RequireHeartbeat: test.requireHeartbeat,
			})
			if test.requireHeartbeat {
				if err := store.Heartbeat("node-a"); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := store.LeaderForKey("account:42"); !ok {
					b.Fatal("LeaderForKey() returned no route")
				}
			}
		})
	}
}
