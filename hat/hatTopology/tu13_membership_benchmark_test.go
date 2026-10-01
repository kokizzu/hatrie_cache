package hatTopology_test

import (
	"testing"

	"hatrie_cache/hat/hatTopology"
)

var tu13MembershipBenchmarkSink any

func BenchmarkTU13BaselineTopologyJoin(b *testing.B) {
	initial := hatTopology.SingleNodeTopology("node-a", "http://a")
	node := hatTopology.TopologyNode{ID: "node-b", Address: "http://b", Role: "replica"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		candidate := hatTopology.Clone(initial)
		candidate.Nodes = append(candidate.Nodes, node)
		candidate.FencingToken++
		normalized, err := hatTopology.Normalize(candidate)
		if err != nil {
			b.Fatal(err)
		}
		tu13MembershipBenchmarkSink = normalized
	}
}

func BenchmarkTU13MembershipJoinCommit(b *testing.B) {
	initial := hatTopology.SingleNodeTopology("node-a", "http://a")
	node := hatTopology.TopologyNode{ID: "node-b", Address: "http://b"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		log, err := hatTopology.NewMembershipLog(initial, hatTopology.MembershipLogOptions{})
		if err != nil {
			b.Fatal(err)
		}
		proposal, err := log.ProposeJoin(node)
		if err != nil {
			b.Fatal(err)
		}
		if err := log.Commit(proposal, tu13Decision(proposal, "node-a")); err != nil {
			b.Fatal(err)
		}
		tu13MembershipBenchmarkSink = log.Generation()
	}
}

func BenchmarkTU13MembershipSnapshotMarshal(b *testing.B) {
	log, err := hatTopology.NewMembershipLog(hatTopology.SingleNodeTopology("node-a", "http://a"), hatTopology.MembershipLogOptions{MaxRecords: 128})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 32; index++ {
		nodeID := "node-" + string(rune('b'+index))
		proposal, proposalErr := log.ProposeJoin(hatTopology.TopologyNode{ID: nodeID})
		if proposalErr != nil {
			b.Fatal(proposalErr)
		}
		if commitErr := log.Commit(proposal, tu13Decision(proposal, "node-a")); commitErr != nil {
			b.Fatal(commitErr)
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		encoded, err := log.MarshalSnapshot()
		if err != nil {
			b.Fatal(err)
		}
		tu13MembershipBenchmarkSink = len(encoded)
	}
}
