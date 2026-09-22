//go:build t207
// +build t207

package hatReplication

import (
	"fmt"
	"testing"
)

func BenchmarkT207RejoinPrepareRetry(b *testing.B) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{
		MaxMembers:    1024,
		MaxCandidates: 4,
		MaxEvictions:  1024,
	})
	if err != nil {
		b.Fatal(err)
	}
	admission.members["node-a"] = ReplicaJoinMember{
		NodeID:            "node-a",
		Address:           "127.0.0.1:9001",
		StorageGeneration: 7,
		Generation:        1,
	}
	admission.generation = 1
	eviction, err := admission.Evict("node-a", 1, "benchmark")
	if err != nil {
		b.Fatal(err)
	}
	request := t207RejoinRequest("node-a", "127.0.0.1:9001", eviction.EvictionEpoch)
	if _, err := admission.PrepareRejoin(request); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := admission.PrepareRejoin(request); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT207RecoverySnapshot(b *testing.B) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{
		MaxMembers:    256,
		MaxCandidates: 4,
		MaxEvictions:  128,
	})
	if err != nil {
		b.Fatal(err)
	}
	for index := 0; index < 128; index++ {
		nodeID := fmt.Sprintf("node-%03d", index)
		admission.members[nodeID] = ReplicaJoinMember{
			NodeID:            nodeID,
			Address:           fmt.Sprintf("127.0.0.1:%d", 9001+index),
			StorageGeneration: 7,
			Generation:        uint64(index + 1),
		}
	}
	admission.generation = 128
	for index := 0; index < 128; index++ {
		nodeID := fmt.Sprintf("node-%03d", index)
		if _, err := admission.Evict(nodeID, uint64(index+1), "benchmark"); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = admission.RecoverySnapshot()
	}
}
