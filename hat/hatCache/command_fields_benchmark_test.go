package hatCache

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func BenchmarkCommandPairFieldsAlternating(b *testing.B) {
	const extractions = 1 << 12
	request := CacheCommandRequest{Pairs: commandFieldsBenchmarkPairs(64)}
	for _, benchmark := range []struct {
		name string
	}{
		{name: "Map"},
		{name: "Radix"},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			var borrowedDuration, copiedDuration time.Duration
			for iteration := 0; iteration < b.N; iteration++ {
				borrowedFirst := iteration&1 != 0
				for pass := 0; pass < 2; pass++ {
					started := time.Now()
					if borrowedFirst == (pass == 0) {
						for extraction := 0; extraction < extractions; extraction++ {
							fields, ok := commandPairFieldsBorrowed(request)
							if !ok {
								b.Fatal("borrowed pair extraction failed")
							}
							commandFieldsTestSink = fields
						}
						borrowedDuration += time.Since(started)
					} else {
						for extraction := 0; extraction < extractions; extraction++ {
							fields, ok := commandPairFieldsCopied(request)
							if !ok {
								b.Fatal("copied pair extraction failed")
							}
							commandFieldsTestSink = fields
						}
						copiedDuration += time.Since(started)
					}
				}
			}
			operations := float64(b.N * extractions)
			b.ReportMetric(float64(borrowedDuration.Nanoseconds())/operations, "borrowed-ns/extract")
			b.ReportMetric(float64(copiedDuration.Nanoseconds())/operations, "copied-ns/extract")
		})
	}
}

func BenchmarkCommandPairBulkReplacement(b *testing.B) {
	for _, command := range []string{"PUTMAP", "PUTRT"} {
		for _, size := range []int{64, 4096} {
			b.Run(fmt.Sprintf("%s%d", command, size), func(b *testing.B) {
				trie := CreateHatTrie()
				b.Cleanup(trie.Destroy)
				request := CacheCommandRequest{
					Command: command,
					Key:     "bulk",
					Pairs:   commandFieldsBenchmarkPairs(size),
				}
				if response := trie.ExecuteCommand(request); !response.OK {
					b.Fatalf("setup %s failed: %#v", command, response)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					if response := trie.ExecuteCommand(request); !response.OK {
						b.Fatalf("%s failed: %#v", command, response)
					}
				}
			})
		}
	}
}

func commandPairFieldsBorrowed(request CacheCommandRequest) (Map, bool) {
	if len(request.Pairs) == 0 || strings.TrimSpace(request.Subkey) != "" {
		return nil, false
	}
	for subkey := range request.Pairs {
		if strings.TrimSpace(subkey) == "" {
			return nil, false
		}
	}
	return request.Pairs, true
}

func commandPairFieldsCopied(request CacheCommandRequest) (Map, bool) {
	fields := Map{}
	for subkey, value := range request.Pairs {
		if strings.TrimSpace(subkey) == "" {
			return nil, false
		}
		fields[subkey] = value
	}
	subkey := strings.TrimSpace(request.Subkey)
	if subkey != "" {
		fields[subkey] = request.Value
	}
	if len(fields) == 0 {
		return nil, false
	}
	return fields, true
}

func commandFieldsBenchmarkPairs(count int) Map {
	pairs := make(Map, count)
	for index := 0; index < count; index++ {
		pairs[fmt.Sprintf("field:%06d", index)] = fmt.Sprintf("value:%06d", index)
	}
	return pairs
}
