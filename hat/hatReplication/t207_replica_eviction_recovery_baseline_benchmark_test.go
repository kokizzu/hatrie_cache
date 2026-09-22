//go:build t207baseline
// +build t207baseline

package hatReplication

import (
	"fmt"
	"testing"
)

func BenchmarkT207BaselineJoinRetry(b *testing.B) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{
		MaxMembers:    1024,
		MaxCandidates: 4,
	})
	if err != nil {
		b.Fatal(err)
	}
	request := ReplicaJoinRequest{
		JoinerID:              "node-a",
		Address:               "127.0.0.1:9001",
		SnapshotID:            "snapshot-1",
		StorageGeneration:     7,
		TargetJournalSequence: 120,
		FencingToken:          9,
		Candidates: []ReplicaJoinCandidate{{
			NodeID:                  "source-a",
			Address:                 "127.0.0.1:8001",
			Healthy:                 true,
			StorageGeneration:       7,
			SnapshotJournalSequence: 100,
			AppliedJournalSequence:  120,
			AvailableThrough:        120,
		}},
	}
	if _, err := admission.Prepare(request); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := admission.Prepare(request); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT207BaselineMembershipSnapshot(b *testing.B) {
	admission, err := NewReplicaJoinAdmission(ReplicaJoinAdmissionOptions{
		MaxMembers:    1024,
		MaxCandidates: 4,
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
			Generation:        1,
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = admission.Snapshot()
	}
}
