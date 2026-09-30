package hatReplication

import (
	"context"
	"testing"
)

var t208WriteQuorumResult WriteQuorumResult
var t208ReadQuorumResult ReadQuorumResult

func BenchmarkT208LegacyWriteQuorum(b *testing.B) {
	nodes := []string{"voter-a", "voter-b", "voter-c"}
	write := func(context.Context, string) error { return nil }
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteWriteQuorum(context.Background(), nodes, 2, write)
		if err != nil {
			b.Fatal(err)
		}
		t208WriteQuorumResult = result
	}
}

func BenchmarkT208TargetWriteQuorumAllVoters(b *testing.B) {
	targets := []QuorumTarget{{Node: "voter-a"}, {Node: "voter-b"}, {Node: "voter-c"}}
	write := func(context.Context, string) error { return nil }
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteWriteQuorumTargets(context.Background(), targets, 2, write)
		if err != nil {
			b.Fatal(err)
		}
		t208WriteQuorumResult = result
	}
}

func BenchmarkT208TargetWriteQuorumWithAnonymous(b *testing.B) {
	targets := []QuorumTarget{{Node: "voter-a"}, {Node: "voter-b"}, {Node: "anonymous-a", Anonymous: true}}
	write := func(context.Context, string) error { return nil }
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteWriteQuorumTargets(context.Background(), targets, 2, write)
		if err != nil {
			b.Fatal(err)
		}
		t208WriteQuorumResult = result
	}
}

func BenchmarkT208LegacyReadQuorum(b *testing.B) {
	nodes := []string{"voter-a", "voter-b", "voter-c"}
	read := func(context.Context, string) (any, error) { return "value", nil }
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteReadQuorum(context.Background(), nodes, 2, read, nil)
		if err != nil {
			b.Fatal(err)
		}
		t208ReadQuorumResult = result
	}
}

func BenchmarkT208TargetReadQuorumAllVoters(b *testing.B) {
	targets := []QuorumTarget{{Node: "voter-a"}, {Node: "voter-b"}, {Node: "voter-c"}}
	read := func(context.Context, string) (any, error) { return "value", nil }
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteReadQuorumTargets(context.Background(), targets, 2, read, nil)
		if err != nil {
			b.Fatal(err)
		}
		t208ReadQuorumResult = result
	}
}

func BenchmarkT208TargetReadQuorumWithAnonymous(b *testing.B) {
	targets := []QuorumTarget{{Node: "voter-a"}, {Node: "voter-b"}, {Node: "anonymous-a", Anonymous: true}}
	read := func(context.Context, string) (any, error) { return "value", nil }
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteReadQuorumTargets(context.Background(), targets, 2, read, nil)
		if err != nil {
			b.Fatal(err)
		}
		t208ReadQuorumResult = result
	}
}
