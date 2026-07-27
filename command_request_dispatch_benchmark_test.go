package hatriecache

import "testing"

func BenchmarkExecuteCommandRequestPassingControl(b *testing.B) {
	for _, benchmark := range []struct {
		name    string
		request CacheCommandRequest
		setup   func(*HatTrie)
	}{
		{name: "ExactGet", request: CacheCommandRequest{Command: "GET", Key: "key"}, setup: func(ht *HatTrie) { ht.UpsertString("key", "value") }},
		{name: "GenericGet", request: CacheCommandRequest{Command: "get", Key: "key"}, setup: func(ht *HatTrie) { ht.UpsertString("key", "value") }},
		{name: "ExactSet", request: CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "value"}},
		{name: "GenericSet", request: CacheCommandRequest{Command: "setstr", Key: "key", Value: "value"}},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			b.Cleanup(ht.Destroy)
			if benchmark.setup != nil {
				benchmark.setup(ht)
			}
			b.ReportAllocs()
			for b.Loop() {
				response := ht.ExecuteCommand(benchmark.request)
				if !response.OK {
					b.Fatal("ExecuteCommand() failed")
				}
				benchmarkCommandResponseSink = response
			}
		})
	}
}
