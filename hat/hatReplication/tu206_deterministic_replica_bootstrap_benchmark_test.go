package hatReplication

import (
	"fmt"
	"testing"
)

var tu206PlanSink ReplicaBootstrapPlan
var tu206SourceSink ReplicaBootstrapSource

func BenchmarkTU206BaselineFirstEligibleSource(b *testing.B) {
	sources := tu206BenchmarkSources()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		for _, source := range sources {
			if source.Ready && source.StorageGeneration == 7 && source.CurrentJournalSequence >= 100 && source.CurrentJournalSequence >= source.SnapshotJournalSequence && source.CurrentJournalSequence-source.SnapshotJournalSequence <= 20 {
				tu206SourceSink = source
				break
			}
		}
	}
}

func BenchmarkTU206PlanReplicaBootstrap(b *testing.B) {
	sources := tu206BenchmarkSources()
	request := ReplicaBootstrapRequest{
		JoinerID:                  "node-z",
		RequiredStorageGeneration: 7,
		MinimumJournalSequence:    100,
		PreferredRegions:          []string{"sg"},
		MaxWALGap:                 20,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		plan, err := PlanReplicaBootstrap(request, sources)
		if err != nil {
			b.Fatal(err)
		}
		tu206PlanSink = plan
	}
}

func tu206BenchmarkSources() []ReplicaBootstrapSource {
	sources := make([]ReplicaBootstrapSource, 64)
	for index := range sources {
		sources[index] = ReplicaBootstrapSource{
			NodeID:                  fmt.Sprintf("node-%02d", index),
			Address:                 "10.0.0.1:9000",
			Region:                  "us",
			Ready:                   true,
			SnapshotID:              "snapshot-1",
			StorageGeneration:       7,
			SnapshotJournalSequence: 100,
			CurrentJournalSequence:  110,
			FencingToken:            uint64(index + 1),
		}
	}
	sources[0].Region = "sg"
	sources[0].SnapshotJournalSequence = 102
	sources[1].Region = "sg"
	sources[1].SnapshotJournalSequence = 101
	return sources
}
