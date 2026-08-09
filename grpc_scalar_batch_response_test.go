package hatriecache

import (
	"strings"
	"testing"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestScalarBatchResponseFromCommandPreallocatesAllGetColumns(t *testing.T) {
	const commands = nativeScalarDirectBatchChunkSize
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    91,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	result := CacheCommandResponse{OK: true, Responses: make([]CacheCommandResponse, commands)}
	var want strings.Builder
	for index := range request.Operations {
		value := "value:" + string(rune('a'+index%26))
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		request.Keys[index] = "get:" + string(rune('a'+index%26))
		result.Responses[index] = CacheCommandResponse{OK: true, Value: value}
		want.WriteString(value)
	}

	allocations := testing.AllocsPerRun(100, func() {
		response := scalarBatchResponseFromCommand(request, result)
		if !response.GetOk() || response.GetBatchId() != request.GetBatchId() || len(response.GetStatuses()) != commands || len(response.GetValueEnds()) != commands || string(response.GetValues()) != want.String() {
			t.Fatalf("scalarBatchResponseFromCommand() = %#v, want %d ordered values", response, commands)
		}
		if cap(response.GetValues()) != len(response.GetValues()) {
			t.Fatalf("compatibility GET values capacity = %d, want exact %d", cap(response.GetValues()), len(response.GetValues()))
		}
		if cap(response.GetValueEnds()) != len(response.GetValueEnds()) {
			t.Fatalf("compatibility GET value ends capacity = %d, want exact %d", cap(response.GetValueEnds()), len(response.GetValueEnds()))
		}
	})
	if allocations != 5 {
		t.Fatalf("compatibility GET allocations = %.0f, want 5 with exact output capacity", allocations)
	}
}

func TestScalarBatchResponseFromCommandPreallocatesOnlyEmittedGetValues(t *testing.T) {
	const commands = minNativeCommandBatchSize
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    93,
		Operations: repeatedScalarBatchOperation(hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET, commands),
		Keys:       make([]string, commands),
	}
	results := repeatedScalarBatchCommandResponse(CacheCommandResponse{OK: true}, commands)
	results[0] = CacheCommandResponse{OK: true, Value: "first"}
	results[1] = CacheCommandResponse{OK: true, Message: "key not found"}
	results[2] = CacheCommandResponse{OK: false, Message: "read failed"}

	response := scalarBatchResponseFromCommand(request, CacheCommandResponse{OK: false, Responses: results})
	if response.GetStatuses()[0] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK || response.GetStatuses()[1] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND || response.GetStatuses()[2] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR {
		t.Fatalf("compatibility GET statuses = %v, want OK/NOT_FOUND/INTERNAL_ERROR", response.GetStatuses()[:3])
	}
	if string(response.GetValues()) != "first" || len(response.GetValueEnds()) != commands-2 || response.GetValueEnds()[0] != uint32(len("first")) {
		t.Fatalf("compatibility GET values = %q/%v, want first with %d ends", response.GetValues(), response.GetValueEnds(), commands-2)
	}
	if cap(response.GetValues()) != len(response.GetValues()) || cap(response.GetValueEnds()) != len(response.GetValueEnds()) {
		t.Fatalf("compatibility emitted GET capacities = %d/%d, want exact %d/%d", cap(response.GetValues()), cap(response.GetValueEnds()), len(response.GetValues()), len(response.GetValueEnds()))
	}
	if len(response.GetErrorIndexes()) != 1 || response.GetErrorIndexes()[0] != 2 || len(response.GetErrors()) != 1 || response.GetErrors()[0] != "read failed" {
		t.Fatalf("compatibility GET errors = %v/%v, want index 2/read failed", response.GetErrorIndexes(), response.GetErrors())
	}
}

func BenchmarkScalarBatchCompatibilityResponse(b *testing.B) {
	for _, benchmark := range []struct {
		name       string
		operations []hatriecachev1.ScalarCommand
		results    []CacheCommandResponse
	}{
		{
			name:       "Get64",
			operations: repeatedScalarBatchOperation(hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET, nativeScalarDirectBatchChunkSize),
			results:    repeatedScalarBatchCommandResponse(CacheCommandResponse{OK: true, Value: "compatibility:value"}, nativeScalarDirectBatchChunkSize),
		},
		{
			name: "Mixed64",
			operations: func() []hatriecachev1.ScalarCommand {
				operations := make([]hatriecachev1.ScalarCommand, nativeScalarDirectBatchChunkSize)
				for index := range operations {
					switch index % 3 {
					case 0:
						operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING
					case 1:
						operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
					default:
						operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT
					}
				}
				return operations
			}(),
			results: func() []CacheCommandResponse {
				results := make([]CacheCommandResponse, nativeScalarDirectBatchChunkSize)
				for index := range results {
					switch index % 3 {
					case 1:
						results[index] = CacheCommandResponse{OK: true, Value: "compatibility:value"}
					case 2:
						results[index] = CacheCommandResponse{OK: true, Value: "7"}
					default:
						results[index] = CacheCommandResponse{OK: true}
					}
				}
				return results
			}(),
		},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			request := &hatriecachev1.ScalarBatchRequest{
				BatchId:    92,
				Operations: benchmark.operations,
				Keys:       make([]string, len(benchmark.operations)),
			}
			result := CacheCommandResponse{OK: true, Responses: benchmark.results}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				response := scalarBatchResponseFromCommand(request, result)
				if !response.GetOk() || len(response.GetStatuses()) != len(request.GetOperations()) {
					b.Fatalf("scalarBatchResponseFromCommand() = %#v", response)
				}
				scalarNativeBatchResponseSink = response
			}
		})
	}
}

func repeatedScalarBatchOperation(operation hatriecachev1.ScalarCommand, count int) []hatriecachev1.ScalarCommand {
	operations := make([]hatriecachev1.ScalarCommand, count)
	for index := range operations {
		operations[index] = operation
	}
	return operations
}

func repeatedScalarBatchCommandResponse(response CacheCommandResponse, count int) []CacheCommandResponse {
	responses := make([]CacheCommandResponse, count)
	for index := range responses {
		responses[index] = response
	}
	return responses
}
