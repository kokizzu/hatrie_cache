package hatCache

import "testing"

var commandJournalReplicaRetentionBenchmarkSink uint64

func BenchmarkCommandJournalReplicaRetention(b *testing.B) {
	for _, test := range []struct {
		name     string
		replicas int
	}{
		{name: "Disabled"},
		{name: "OneReplica", replicas: 1},
		{name: "FourReplicas", replicas: 4},
	} {
		b.Run(test.name, func(b *testing.B) {
			journal := &CommandJournal{}
			if test.replicas > 0 {
				journal.replicaWatermarks = make(map[string]uint64, test.replicas)
				for index := 0; index < test.replicas; index++ {
					journal.replicaWatermarks[string(rune('a'+index))] = uint64(index)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				journal.mu.Lock()
				through, protected := journal.replicaRetentionThroughLocked()
				journal.mu.Unlock()
				if protected {
					commandJournalReplicaRetentionBenchmarkSink += through
				}
			}
		})
	}
}
