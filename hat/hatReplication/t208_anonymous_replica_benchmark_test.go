//go:build t208
// +build t208

package hatReplication

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkT208VoterWriteQuorum(b *testing.B) {
	roster := ReplicaRoleRoster{
		Voters:    []string{"voter-a", "voter-b", "voter-c"},
		Anonymous: []string{"reader-a"},
	}
	nodes := []string{"voter-a", "voter-b", "voter-c"}
	write := func(context.Context, string) error { return nil }
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ExecuteVoterWriteQuorum(context.Background(), roster, nodes, 2, write); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT208RoleRoster(b *testing.B) {
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
		role := ReplicaRoleVoter
		if index%4 == 0 {
			role = ReplicaRoleAnonymous
		}
		admission.members[nodeID] = ReplicaJoinMember{
			NodeID:            nodeID,
			Address:           fmt.Sprintf("127.0.0.1:%d", 9100+index),
			StorageGeneration: 7,
			Role:              role,
			Generation:        uint64(index + 1),
		}
	}
	admission.generation = 128
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		_ = admission.RoleRoster()
	}
}
