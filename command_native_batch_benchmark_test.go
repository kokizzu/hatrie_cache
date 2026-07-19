package hatriecache

import (
	"fmt"
	"testing"
)

func BenchmarkNativeCScalarBatch4096(b *testing.B) {
	const commands = 4096
	for _, family := range []string{"SetInt", "Get"} {
		batch := make([]CacheCommandRequest, commands)
		for index := range batch {
			command := "SETINT"
			value := "1"
			if family == "Get" {
				command = "GET"
				value = ""
			}
			batch[index] = CacheCommandRequest{Command: command, Key: fmt.Sprintf("key:%04d", index), Value: value}
		}
		request := CacheCommandRequest{Command: "BATCH", Batch: batch}
		for _, mode := range []string{"GoLoop", "NativeC"} {
			b.Run(family+"/"+mode, func(b *testing.B) {
				trie := CreateHatTrie()
				defer trie.Destroy()
				if family == "Get" {
					for index := range batch {
						trie.UpsertCounter(batch[index].Key, int32(index))
					}
				}
				if mode == "NativeC" {
					if response := trie.ExecuteCommand(request); !response.OK {
						b.Fatalf("native warmup response = %#v", response)
					}
				} else if response, _ := trie.executePublicScalarBatchCommandWithExecutor(request, nil); !response.OK {
					b.Fatalf("Go warmup response = %#v", response)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for iteration := 0; iteration < b.N; iteration++ {
					var response CacheCommandResponse
					if mode == "GoLoop" {
						response, _ = trie.executePublicScalarBatchCommandWithExecutor(request, nil)
					} else {
						response = trie.ExecuteCommand(request)
					}
					if !response.OK || len(response.Responses) != commands {
						b.Fatalf("batch response = %#v", response)
					}
				}
				b.ReportMetric(float64(commands), "commands/op")
			})
		}
	}
}
