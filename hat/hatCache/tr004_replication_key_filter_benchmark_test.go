package hatCache

import "testing"

func BenchmarkTR004ReplicationKeyPrefixMatcher(b *testing.B) {
	for _, benchmark := range []struct {
		name     string
		prefixes []string
		key      string
	}{
		{name: "Disabled", key: "region:eu:orders"},
		{name: "MatchFirst", prefixes: []string{"region:eu:"}, key: "region:eu:orders"},
		{name: "MatchLast", prefixes: []string{"region:us:", "region:ap:", "region:sa:", "region:eu:"}, key: "region:eu:orders"},
		{name: "Miss", prefixes: []string{"region:us:", "region:ap:", "region:sa:", "region:eu:"}, key: "region:af:orders"},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			replicator := &HTTPReplicator{keyPrefixes: normalizeReplicationKeyPrefixes(benchmark.prefixes)}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				benchmarkTR004MatchSink = replicator.replicationKeyAllowed(benchmark.key)
			}
		})
	}
}

var benchmarkTR004MatchSink bool
