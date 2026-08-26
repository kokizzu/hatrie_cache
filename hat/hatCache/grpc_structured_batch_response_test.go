package hatCache

import (
	"strings"
	"testing"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestStructuredBatchResponseFromCommandPreallocatesPeekMapColumns(t *testing.T) {
	const commands = nativeScalarDirectBatchChunkSize
	request := &hatriecachev1.StructuredBatchRequest{
		BatchId:    94,
		Operations: make([]hatriecachev1.StructuredCommand, commands),
		Keys:       make([]string, commands),
		Subkeys:    make([]string, commands),
	}
	result := CacheCommandResponse{OK: true, Responses: make([]CacheCommandResponse, commands)}
	var want strings.Builder
	for index := range request.Operations {
		value := "value:" + string(rune('a'+index%26))
		request.Operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP
		request.Keys[index] = "map:" + string(rune('a'+index%26))
		request.Subkeys[index] = "field"
		result.Responses[index] = CacheCommandResponse{OK: true, Value: value}
		want.WriteString(value)
	}

	allocations := testing.AllocsPerRun(100, func() {
		response := structuredBatchResponseFromCommand(request, result)
		if !response.GetOk() || response.GetBatchId() != request.GetBatchId() || len(response.GetStatuses()) != commands || len(response.GetValueEnds()) != commands || string(response.GetValues()) != want.String() {
			t.Fatalf("structuredBatchResponseFromCommand() = %#v, want %d ordered values", response, commands)
		}
		if cap(response.GetValues()) != len(response.GetValues()) {
			t.Fatalf("compatibility PEEK_MAP values capacity = %d, want exact %d", cap(response.GetValues()), len(response.GetValues()))
		}
		if cap(response.GetValueEnds()) != len(response.GetValueEnds()) {
			t.Fatalf("compatibility PEEK_MAP value ends capacity = %d, want exact %d", cap(response.GetValueEnds()), len(response.GetValueEnds()))
		}
	})
	if allocations != 5 {
		t.Fatalf("compatibility PEEK_MAP allocations = %.0f, want 5 with exact output capacity", allocations)
	}
}

func TestStructuredBatchResponseFromCommandPreallocatesOnlyEmittedPeekMapValues(t *testing.T) {
	const commands = minNativeCommandBatchSize
	request := &hatriecachev1.StructuredBatchRequest{
		BatchId:    96,
		Operations: repeatedStructuredBatchOperation(hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP, commands),
		Keys:       make([]string, commands),
		Subkeys:    make([]string, commands),
	}
	results := repeatedStructuredBatchCommandResponse(CacheCommandResponse{OK: true}, commands)
	results[0] = CacheCommandResponse{OK: true, Value: "first"}
	results[1] = CacheCommandResponse{OK: true, Message: "value not found"}
	results[2] = CacheCommandResponse{OK: false, Message: "read failed"}

	response := structuredBatchResponseFromCommand(request, CacheCommandResponse{OK: false, Responses: results})
	if response.GetStatuses()[0] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK || response.GetStatuses()[1] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND || response.GetStatuses()[2] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR {
		t.Fatalf("compatibility PEEK_MAP statuses = %v, want OK/NOT_FOUND/INTERNAL_ERROR", response.GetStatuses()[:3])
	}
	if string(response.GetValues()) != "first" || len(response.GetValueEnds()) != commands-2 || response.GetValueEnds()[0] != uint32(len("first")) {
		t.Fatalf("compatibility PEEK_MAP values = %q/%v, want first with %d ends", response.GetValues(), response.GetValueEnds(), commands-2)
	}
	if cap(response.GetValues()) != len(response.GetValues()) || cap(response.GetValueEnds()) != len(response.GetValueEnds()) {
		t.Fatalf("compatibility emitted PEEK_MAP capacities = %d/%d, want exact %d/%d", cap(response.GetValues()), cap(response.GetValueEnds()), len(response.GetValues()), len(response.GetValueEnds()))
	}
	if len(response.GetErrorIndexes()) != 1 || response.GetErrorIndexes()[0] != 2 || len(response.GetErrors()) != 1 || response.GetErrors()[0] != "read failed" {
		t.Fatalf("compatibility PEEK_MAP errors = %v/%v, want index 2/read failed", response.GetErrorIndexes(), response.GetErrors())
	}
}

func TestStructuredBatchResponseFromCommandPreallocatesHasSetIntegerColumn(t *testing.T) {
	const commands = nativeScalarDirectBatchChunkSize
	request := &hatriecachev1.StructuredBatchRequest{
		BatchId:    97,
		Operations: repeatedStructuredBatchOperation(hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET, commands),
		Keys:       make([]string, commands),
		Values:     make([][]byte, commands),
	}
	results := repeatedStructuredBatchCommandResponse(CacheCommandResponse{OK: true, Value: "1"}, commands)

	allocations := testing.AllocsPerRun(100, func() {
		response := structuredBatchResponseFromCommand(request, CacheCommandResponse{OK: true, Responses: results})
		if !response.GetOk() || len(response.GetIntegerValues()) != commands || cap(response.GetIntegerValues()) != commands {
			t.Fatalf("compatibility HAS_SET integer values = %d/%d, want exact %d", len(response.GetIntegerValues()), cap(response.GetIntegerValues()), commands)
		}
	})
	if allocations != 4 {
		t.Fatalf("compatibility HAS_SET allocations = %.0f, want 4 with exact integer output capacity", allocations)
	}
}

func TestStructuredBatchResponseFromCommandPreallocatesOnlyEmittedHasSetIntegers(t *testing.T) {
	const commands = minNativeCommandBatchSize
	request := &hatriecachev1.StructuredBatchRequest{
		BatchId:    98,
		Operations: repeatedStructuredBatchOperation(hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET, commands),
		Keys:       make([]string, commands),
		Values:     make([][]byte, commands),
	}
	results := repeatedStructuredBatchCommandResponse(CacheCommandResponse{OK: true, Value: "1"}, commands)
	results[0] = CacheCommandResponse{OK: true, Value: "1"}
	results[1] = CacheCommandResponse{OK: true, Value: "0"}
	results[2] = CacheCommandResponse{OK: true, Message: "key not found"}
	results[3] = CacheCommandResponse{OK: false, Message: "read failed"}

	response := structuredBatchResponseFromCommand(request, CacheCommandResponse{OK: false, Responses: results})
	if got := response.GetIntegerValues(); len(got) != commands-2 || got[0] != 1 || got[1] != 0 {
		t.Fatalf("compatibility HAS_SET integer values = %v, want 1, 0, and %d emitted results", got, commands-2)
	}
	if cap(response.GetIntegerValues()) != len(response.GetIntegerValues()) {
		t.Fatalf("compatibility HAS_SET integer capacity = %d, want exact %d", cap(response.GetIntegerValues()), len(response.GetIntegerValues()))
	}
	if response.GetStatuses()[2] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND || response.GetStatuses()[3] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR {
		t.Fatalf("compatibility HAS_SET statuses = %v, want missing/internal error", response.GetStatuses()[:4])
	}
	if len(response.GetErrorIndexes()) != 1 || response.GetErrorIndexes()[0] != 3 || len(response.GetErrors()) != 1 || response.GetErrors()[0] != "read failed" {
		t.Fatalf("compatibility HAS_SET errors = %v/%v, want index 3/read failed", response.GetErrorIndexes(), response.GetErrors())
	}
}

func BenchmarkStructuredBatchCompatibilityResponse(b *testing.B) {
	for _, benchmark := range []struct {
		name       string
		operations []hatriecachev1.StructuredCommand
		results    []CacheCommandResponse
	}{
		{
			name:       "PeekMap64",
			operations: repeatedStructuredBatchOperation(hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP, nativeScalarDirectBatchChunkSize),
			results:    repeatedStructuredBatchCommandResponse(CacheCommandResponse{OK: true, Value: "compatibility:value"}, nativeScalarDirectBatchChunkSize),
		},
		{
			name:       "HasSet64",
			operations: repeatedStructuredBatchOperation(hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET, nativeScalarDirectBatchChunkSize),
			results:    repeatedStructuredBatchCommandResponse(CacheCommandResponse{OK: true, Value: "1"}, nativeScalarDirectBatchChunkSize),
		},
		{
			name: "Mixed64",
			operations: func() []hatriecachev1.StructuredCommand {
				operations := make([]hatriecachev1.StructuredCommand, nativeScalarDirectBatchChunkSize)
				for index := range operations {
					switch index % 3 {
					case 0:
						operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP
					case 1:
						operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP
					default:
						operations[index] = hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET
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
			request := &hatriecachev1.StructuredBatchRequest{
				BatchId:    95,
				Operations: benchmark.operations,
				Keys:       make([]string, len(benchmark.operations)),
			}
			result := CacheCommandResponse{OK: true, Responses: benchmark.results}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				response := structuredBatchResponseFromCommand(request, result)
				if !response.GetOk() || len(response.GetStatuses()) != len(request.GetOperations()) {
					b.Fatalf("structuredBatchResponseFromCommand() = %#v", response)
				}
				structuredDirectBatchResponseSink = response
			}
		})
	}
}

func repeatedStructuredBatchOperation(operation hatriecachev1.StructuredCommand, count int) []hatriecachev1.StructuredCommand {
	operations := make([]hatriecachev1.StructuredCommand, count)
	for index := range operations {
		operations[index] = operation
	}
	return operations
}

func repeatedStructuredBatchCommandResponse(response CacheCommandResponse, count int) []CacheCommandResponse {
	responses := make([]CacheCommandResponse, count)
	for index := range responses {
		responses[index] = response
	}
	return responses
}
