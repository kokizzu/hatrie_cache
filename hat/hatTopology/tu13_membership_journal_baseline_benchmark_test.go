//go:build tu13baseline

package hatTopology

import "testing"

func BenchmarkTU13BaselineTopologyClone(b *testing.B) {
	topology := ClusterTopology{
		Version: Version,
		Mode:    TopologyModeFullReplica,
		Self:    "node-a",
		Nodes:   []TopologyNode{{ID: "node-a", Address: "127.0.0.1:8000", Role: "primary"}},
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		clone := Clone(topology)
		if len(clone.Nodes) != 1 {
			b.Fatal("topology clone lost node")
		}
	}
}
