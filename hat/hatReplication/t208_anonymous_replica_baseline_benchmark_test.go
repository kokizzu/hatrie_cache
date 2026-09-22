//go:build t208baseline
// +build t208baseline

package hatReplication

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkT208BaselineWriteQuorum(b *testing.B) {
	nodes := []string{"voter-a", "voter-b", "voter-c"}
	write := func(context.Context, string) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteWriteQuorum(context.Background(), nodes, 2, write); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT208BaselineMembershipSnapshot(b *testing.B) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{
		MaxMembers:    256,
		MaxCandidates: 4,
		MaxEvictions:  4,
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 128; index++ {
		nodeID := fmt.Sprintf("node-%03d", index)
		admission.members[nodeID] = ReplicaJoinMember{
			NodeID:            nodeID,
			Address:           fmt.Sprintf("127.0.0.1:%d", 9100+index),
			StorageGeneration: 7,
			Generation:        uint64(index + 1),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = admission.Snapshot()
	}
}
