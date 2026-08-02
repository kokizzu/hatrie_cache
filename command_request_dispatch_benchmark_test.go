package hatriecache

import "testing"

func BenchmarkExecuteCommandRequestPassingControl(b *testing.B) {
	ttlSeconds := int64(3600)
	for _, benchmark := range []struct {
		name       string
		request    CacheCommandRequest
		setup      func(*HatTrie)
		beforeEach func(*HatTrie)
	}{
		{name: "ExactGet", request: CacheCommandRequest{Command: "GET", Key: "key"}, setup: func(ht *HatTrie) { ht.UpsertString("key", "value") }},
		{name: "GenericGet", request: CacheCommandRequest{Command: "get", Key: "key"}, setup: func(ht *HatTrie) { ht.UpsertString("key", "value") }},
		{name: "ExactSet", request: CacheCommandRequest{Command: "SETSTR", Key: "key", Value: "value"}},
		{name: "GenericSet", request: CacheCommandRequest{Command: "setstr", Key: "key", Value: "value"}},
		{name: "ExactIncrement", request: CacheCommandRequest{Command: "INC", Key: "key", Value: "1"}, setup: func(ht *HatTrie) { ht.UpsertCounter("key", 0) }},
		{name: "ExactExpire", request: CacheCommandRequest{Command: "EXPIRE", Key: "key", TTLSeconds: &ttlSeconds}, setup: func(ht *HatTrie) { ht.UpsertString("key", "value") }},
		{name: "ExactExists", request: CacheCommandRequest{Command: "EXISTS", Key: "key"}, setup: func(ht *HatTrie) { ht.UpsertString("key", "value") }},
		{name: "ExactDeleteMissing", request: CacheCommandRequest{Command: "DEL", Key: "key"}},
		{name: "ExactDeleteReinsert", request: CacheCommandRequest{Command: "DEL", Key: "key"}, beforeEach: func(ht *HatTrie) { ht.UpsertString("key", "value") }},
		{name: "ExactMapPeek", request: CacheCommandRequest{Command: "PEEKMAP", Key: "key", Subkey: "field"}, setup: func(ht *HatTrie) { ht.PutMap("key", "field", "value") }},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			ht := CreateHatTrie()
			b.Cleanup(ht.Destroy)
			if benchmark.setup != nil {
				benchmark.setup(ht)
			}
			b.ReportAllocs()
			for b.Loop() {
				if benchmark.beforeEach != nil {
					benchmark.beforeEach(ht)
				}
				response := ht.ExecuteCommand(benchmark.request)
				if !response.OK {
					b.Fatal("ExecuteCommand() failed")
				}
				benchmarkCommandResponseSink = response
			}
		})
	}
}
