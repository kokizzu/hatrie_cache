package hatTopology

import "testing"

func BenchmarkTU13BaselineInMemoryMembershipUpdate(b *testing.B) {
	members := make(map[string]TopologyNode, 64)
	nodes := make([]TopologyNode, 64)
	for index := range nodes {
		nodes[index] = TopologyNode{ID: "node", Address: "127.0.0.1:9000", Role: "replica"}
		nodes[index].ID += string(rune('a' + index))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		node := nodes[index%len(nodes)]
		members[node.ID] = node
	}
}
