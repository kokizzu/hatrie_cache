package hatriecache

import (
	"context"
	"fmt"
	"testing"
	"unsafe"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

var scalarNativeBatchResponseSink *hatriecachev1.ScalarBatchResponse

func BenchmarkScalarNativeBatch(b *testing.B) {
	for _, size := range []int{2, 4, 8, 16, 32, 64, 256} {
		b.Run(fmt.Sprintf("Read%d", size), func(b *testing.B) {
			benchmarkScalarNativeReadBatch(b, size)
		})
		b.Run(fmt.Sprintf("ReadSame%d", size), func(b *testing.B) {
			benchmarkScalarNativeSameReadBatch(b, size)
		})
		b.Run(fmt.Sprintf("Mixed%d", size), func(b *testing.B) {
			benchmarkScalarNativeMixedBatch(b, size)
		})
	}
}

func benchmarkScalarNativeSameReadBatch(b *testing.B, commands int) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	trie.UpsertString("native:read:same", "value")
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    3,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	for index := range request.Operations {
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		request.Keys[index] = "native:read:same"
	}
	benchmarkScalarNativeRequest(b, trie, request)
}

func benchmarkScalarNativeReadBatch(b *testing.B, commands int) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    1,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	for index := range request.Operations {
		key := fmt.Sprintf("native:read:%04d", index)
		trie.UpsertString(key, "value")
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		request.Keys[index] = key
	}
	benchmarkScalarNativeRequest(b, trie, request)
}

func benchmarkScalarNativeMixedBatch(b *testing.B, commands int) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    2,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	for index := range request.Operations {
		key := fmt.Sprintf("native:mixed:%04d", index/6)
		request.Keys[index] = key
		switch index % 6 {
		case 0:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING
			request.StringValues = append(request.StringValues, []byte("value"))
		case 1:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		case 2:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER
			request.IntegerValues = append(request.IntegerValues, 7)
		case 3:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT
			request.IntegerValues = append(request.IntegerValues, 2)
		case 4:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS
		case 5:
			request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE
		}
	}
	benchmarkScalarNativeRequest(b, trie, request)
}

func benchmarkScalarNativeRequest(b *testing.B, trie *HatTrie, request *hatriecachev1.ScalarBatchRequest) {
	if response := trie.executeScalarBatchDirect(context.Background(), request); !response.GetOk() {
		b.Fatalf("warmup response = %#v", response)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		response := trie.executeScalarBatchDirect(context.Background(), request)
		if !response.GetOk() || len(response.GetStatuses()) != len(request.Operations) {
			b.Fatalf("scalar response = %#v", response)
		}
		scalarNativeBatchResponseSink = response
	}
	b.StopTimer()
	b.ReportMetric(float64(len(request.Operations)), "commands/op")
	b.ReportMetric(float64(nativeScalarBatchScratchBytes(trie)), "scratch_B")
}

func nativeScalarBatchScratchBytes(trie *HatTrie) uintptr {
	scratch := &trie.nativeCommandBatchScratch
	bytes := uintptr(cap(scratch.items)) * unsafe.Sizeof(nativeCommandBatchItem{})
	bytes += uintptr(cap(scratch.keys))
	if cap(scratch.operations) != 0 {
		bytes += uintptr(cap(scratch.operations)) * unsafe.Sizeof(scratch.operations[:1][0])
	}
	if cap(scratch.results) != 0 {
		bytes += uintptr(cap(scratch.results)) * unsafe.Sizeof(scratch.results[:1][0])
	}
	return bytes
}
