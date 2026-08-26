package hatCache

import (
	"context"
	"fmt"
	"testing"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

var batchTelemetryCommandResponseSink CacheCommandResponse
var batchTelemetryScalarResponseSink *hatriecachev1.ScalarBatchResponse

func BenchmarkBatchTelemetry(b *testing.B) {
	b.Run("NativeRead4096", benchmarkNativeReadBatchTelemetry)
	for _, size := range []int{16, 256} {
		b.Run(fmt.Sprintf("ScalarDirectRead%d", size), func(b *testing.B) {
			benchmarkScalarDirectReadTelemetry(b, size)
		})
	}
}

func benchmarkNativeReadBatchTelemetry(b *testing.B) {
	const commands = 4096
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	batch := make([]CacheCommandRequest, commands)
	for index := range batch {
		key := fmt.Sprintf("telemetry:native:%04d", index)
		trie.UpsertString(key, "value")
		batch[index] = CacheCommandRequest{Command: "GET", Key: key}
	}
	request := CacheCommandRequest{Command: "BATCH", Batch: batch}
	if response := trie.ExecuteCommand(request); !response.OK {
		b.Fatalf("warmup response = %#v", response)
	}
	b.ReportAllocs()
	b.ReportMetric(commands, "commands/op")
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		response := trie.ExecuteCommand(request)
		if !response.OK || len(response.Responses) != commands {
			b.Fatalf("batch response = %#v", response)
		}
		batchTelemetryCommandResponseSink = response
	}
}

func benchmarkScalarDirectReadTelemetry(b *testing.B, commands int) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    1,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	for index := range request.Operations {
		key := fmt.Sprintf("telemetry:scalar:%04d", index)
		trie.UpsertString(key, "value")
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		request.Keys[index] = key
	}
	if response := trie.executeScalarBatchDirect(context.Background(), request); !response.GetOk() {
		b.Fatalf("warmup response = %#v", response)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(commands), "commands/op")
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		response := trie.executeScalarBatchDirect(context.Background(), request)
		if !response.GetOk() || len(response.GetStatuses()) != commands {
			b.Fatalf("scalar response = %#v", response)
		}
		batchTelemetryScalarResponseSink = response
	}
}
