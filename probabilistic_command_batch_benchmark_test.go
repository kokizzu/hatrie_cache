package hatriecache

import "testing"

func BenchmarkProbabilisticCommandBatch64Baseline(b *testing.B) {
	values := benchmarkXorCommandValueSlice()
	for _, workload := range []struct {
		name    string
		command string
		key     string
		reset   func(*HatTrie) error
	}{
		{
			name:    "Bloom",
			command: "ADDBF",
			key:     "bloom:batch",
			reset: func(trie *HatTrie) error {
				return trie.UpsertBloomFilter("bloom:batch", 4096, 0.001)
			},
		},
		{
			name:    "Cuckoo",
			command: "ADDCF",
			key:     "cuckoo:batch",
			reset: func(trie *HatTrie) error {
				return trie.UpsertCuckooFilter("cuckoo:batch", 4096, 0.001)
			},
		},
		{
			name:    "CountMin",
			command: "INCRCMS",
			key:     "count-min:batch",
			reset: func(trie *HatTrie) error {
				return trie.UpsertCountMinSketch("count-min:batch", 2048, 4)
			},
		},
		{
			name:    "HyperLogLog",
			command: "ADDHLL",
			key:     "hll:batch",
			reset: func(trie *HatTrie) error {
				return trie.UpsertHyperLogLog("hll:batch", 14)
			},
		},
		{
			name:    "TopK",
			command: "ADDTOPK",
			key:     "top-k:batch",
			reset: func(trie *HatTrie) error {
				return trie.UpsertTopK("top-k:batch", 128)
			},
		},
		{
			name:    "Reservoir",
			command: "ADDRS",
			key:     "reservoir:batch",
			reset: func(trie *HatTrie) error {
				return trie.UpsertReservoirSample("reservoir:batch", 128)
			},
		},
	} {
		for _, dispatch := range []struct {
			name   string
			prefix string
		}{
			{name: "Exact"},
			{name: "Generic", prefix: " "},
		} {
			b.Run(workload.name+"/"+dispatch.name, func(b *testing.B) {
				trie := CreateHatTrie()
				defer trie.Destroy()
				request := CacheCommandRequest{
					Command: dispatch.prefix + workload.command,
					Key:     workload.key,
					Values:  values,
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if err := workload.reset(trie); err != nil {
						b.Fatal(err)
					}
					benchmarkExecuteCommand(b, trie, request)
				}
			})
		}
	}
}
