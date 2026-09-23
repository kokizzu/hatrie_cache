package hatCache

import "testing"

func BenchmarkT203LeaderWriteFencing(b *testing.B) {
	topology := SingleNodeTopology("node-a", "http://node-a")
	topology.FencingToken = 7
	store, err := NewTopologyStore(topology)
	if err != nil {
		b.Fatal(err)
	}
	election := NewElectionStore(store, ElectionOptions{})
	for _, test := range []struct {
		name           string
		enforceFencing bool
		request        CacheCommandRequest
	}{
		{
			name: "LegacyLeaderCheck",
			request: CacheCommandRequest{
				Command: "SETSTR",
				Key:     "benchmark:key",
				Value:   "value",
			},
		},
		{
			name:           "StrictLeaderCheck",
			enforceFencing: true,
			request: CacheCommandRequest{
				Command: "SETSTR",
				Key:     "benchmark:key",
				Value:   "value",
				Pairs:   Map{replicationMetaFencingToken: "7"},
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				response, rejected := rejectNonLeaderWrite(test.request, "node-a", store, election, true, test.enforceFencing)
				if rejected || response.OK {
					b.Fatal("leader write was rejected")
				}
			}
		})
	}
}
