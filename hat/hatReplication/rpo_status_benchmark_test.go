package hatReplication

import "testing"

func BenchmarkBuildReplicaRPOStatuses64(b *testing.B) {
	replicas := make([]ReplicaRPOInput, 64)
	for index := range replicas {
		replicas[index] = ReplicaRPOInput{Node: "replica", AppliedSequence: uint64(index)}
	}
	b.ReportAllocs()
	for range b.N {
		statuses, err := BuildReplicaRPOStatuses(100, replicas, 4)
		if err != nil || len(statuses) != len(replicas) {
			b.Fatal(err)
		}
	}
}
