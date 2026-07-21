package hatriecache

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func (server *CacheGRPCServer) StructuredBatchStream(stream hatriecachev1.CacheService_StructuredBatchStreamServer) error {
	ctx, err := server.requestContext(stream.Context())
	if err != nil {
		return err
	}
	if err := server.requireTrie(); err != nil {
		return err
	}
	for {
		request, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		mutates := structuredBatchMutates(request.GetOperations())
		if mutates {
			if err := server.rejectDangerousGRPC("command", AuditEvent{
				Command: "STRUCTURED_BATCH",
				Method:  "/hatriecache.v1.CacheService/StructuredBatchStream",
			}); err != nil {
				return err
			}
		}
		response := server.executeStructuredBatch(ctx, request)
		if mutates {
			server.auditGRPC(AuditEvent{
				Action:  "command",
				Command: "STRUCTURED_BATCH",
				OK:      response.GetOk(),
				Method:  "/hatriecache.v1.CacheService/StructuredBatchStream",
				Message: response.GetError(),
			})
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func structuredBatchMutates(operations []hatriecachev1.StructuredCommand) bool {
	for _, operation := range operations {
		switch operation {
		case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_SHIFT_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_REMOVE_SET,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_PRIORITY:
			return true
		}
	}
	return false
}

func (server *CacheGRPCServer) executeStructuredBatch(ctx context.Context, request *hatriecachev1.StructuredBatchRequest) *hatriecachev1.StructuredBatchResponse {
	response := &hatriecachev1.StructuredBatchResponse{BatchId: request.GetBatchId()}
	if err := validateStructuredBatchColumns(request); err != nil {
		response.Error = err.Error()
		return response
	}
	if err := ctx.Err(); err != nil {
		response.Error = err.Error()
		return response
	}
	if server.scalarBatchRequiresCompatibilityPath() {
		return server.executeStructuredBatchCompatibility(ctx, request)
	}
	return server.trie.executeStructuredBatchDirect(ctx, request)
}

func validateStructuredBatchColumns(request *hatriecachev1.StructuredBatchRequest) error {
	operations := request.GetOperations()
	if len(operations) == 0 {
		return errors.New("structured batch requires operations")
	}
	if len(operations) > maxPublicCommandBatchSize {
		return errors.New("structured batch exceeds maximum size")
	}
	if len(request.GetKeys()) != len(operations) {
		return errors.New("structured batch keys must match operations")
	}
	subkeysNeeded := 0
	valuesNeeded := 0
	prioritiesNeeded := 0
	for _, operation := range operations {
		switch operation {
		case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP:
			subkeysNeeded++
			valuesNeeded++
		case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP:
			subkeysNeeded++
		case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_REMOVE_SET,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET:
			valuesNeeded++
		case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY:
			valuesNeeded++
			prioritiesNeeded++
		case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_SHIFT_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HEAD_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAIL_SLICE,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_SET,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_PRIORITY,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_PRIORITY,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_PRIORITY:
		default:
			return errors.New("structured batch contains an unsupported operation")
		}
	}
	if len(request.GetSubkeys()) != subkeysNeeded {
		return errors.New("structured batch subkeys do not match map operations")
	}
	if len(request.GetValues()) != valuesNeeded {
		return errors.New("structured batch values do not match value operations")
	}
	if len(request.GetPriorities()) != prioritiesNeeded {
		return errors.New("structured batch priorities do not match PUSH_PRIORITY operations")
	}
	return nil
}

type structuredBatchCursor struct {
	subkey   int
	value    int
	priority int
}

func (cursor *structuredBatchCursor) command(request *hatriecachev1.StructuredBatchRequest, index int) CacheCommandRequest {
	item := CacheCommandRequest{Key: request.Keys[index]}
	switch request.Operations[index] {
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP:
		item.Command = "PUTMAP"
		item.Subkey = request.Subkeys[cursor.subkey]
		item.Value = string(request.Values[cursor.value])
		cursor.subkey++
		cursor.value++
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP:
		item.Command = "PEEKMAP"
		item.Subkey = request.Subkeys[cursor.subkey]
		cursor.subkey++
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP:
		item.Command = "TAKEMAP"
		item.Subkey = request.Subkeys[cursor.subkey]
		cursor.subkey++
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE:
		item.Command = "PUSHSLICE"
		item.Value = string(request.Values[cursor.value])
		cursor.value++
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_SLICE:
		item.Command = "POPSLICE"
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_SHIFT_SLICE:
		item.Command = "SHIFTSLICE"
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HEAD_SLICE:
		item.Command = "HEADSLICE"
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAIL_SLICE:
		item.Command = "TAILSLICE"
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET:
		item.Command = "ADDSET"
		item.Value = string(request.Values[cursor.value])
		cursor.value++
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_REMOVE_SET:
		item.Command = "REMSET"
		item.Value = string(request.Values[cursor.value])
		cursor.value++
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET:
		item.Command = "HASSET"
		item.Value = string(request.Values[cursor.value])
		cursor.value++
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_SET:
		item.Command = "GETSET"
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY:
		item.Command = "PUSHPQ"
		item.Value = string(request.Values[cursor.value])
		item.Priority = &request.Priorities[cursor.priority]
		cursor.value++
		cursor.priority++
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_PRIORITY:
		item.Command = "PEEKPQ"
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_PRIORITY:
		item.Command = "POPPQ"
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_PRIORITY:
		item.Command = "GETPQ"
	}
	return item
}

func (ht *HatTrie) executeStructuredBatchDirect(ctx context.Context, request *hatriecachev1.StructuredBatchRequest) *hatriecachev1.StructuredBatchResponse {
	response := newStructuredBatchResponse(request.GetBatchId(), len(request.GetOperations()))
	cursor := structuredBatchCursor{}
	for index, operation := range request.GetOperations() {
		if index&63 == 0 {
			if err := ctx.Err(); err != nil {
				response.Ok = false
				response.Error = err.Error()
				response.Statuses = response.Statuses[:index]
				response.ValueKinds = response.ValueKinds[:index]
				return response
			}
		}
		item := cursor.command(request, index)
		result := ht.ExecuteCommand(item)
		appendStructuredBatchResult(response, index, operation, result)
	}
	return response
}

func (server *CacheGRPCServer) executeStructuredBatchCompatibility(ctx context.Context, request *hatriecachev1.StructuredBatchRequest) *hatriecachev1.StructuredBatchResponse {
	command := structuredBatchCacheCommand(request)
	result, _ := executeCacheCommand(ctx, server.trie, command, commandExecutionOptions{
		NodeName:            server.options.NodeName,
		Journal:             server.options.Journal,
		DirtyTracker:        server.options.DirtyTracker,
		Topology:            server.options.Topology,
		Election:            server.options.Election,
		Replicator:          server.options.Replicator,
		ReplicationSafety:   server.options.ReplicationSafety,
		EnforceLeaderWrites: server.options.EnforceLeaderWrites,
	})
	return structuredBatchResponseFromCommand(request, result)
}

func structuredBatchCacheCommand(request *hatriecachev1.StructuredBatchRequest) CacheCommandRequest {
	batch := make([]CacheCommandRequest, len(request.GetOperations()))
	cursor := structuredBatchCursor{}
	for index := range batch {
		batch[index] = cursor.command(request, index)
	}
	return CacheCommandRequest{Command: "BATCH", Batch: batch}
}

func structuredBatchResponseFromCommand(request *hatriecachev1.StructuredBatchRequest, result CacheCommandResponse) *hatriecachev1.StructuredBatchResponse {
	response := newStructuredBatchResponse(request.GetBatchId(), len(request.GetOperations()))
	if len(result.Responses) != len(request.GetOperations()) {
		response.Ok = false
		response.Error = result.Message
		response.Statuses = nil
		response.ValueKinds = nil
		return response
	}
	for index, item := range result.Responses {
		appendStructuredBatchResult(response, index, request.Operations[index], item)
	}
	return response
}

func newStructuredBatchResponse(batchID uint64, count int) *hatriecachev1.StructuredBatchResponse {
	return &hatriecachev1.StructuredBatchResponse{
		BatchId:    batchID,
		Ok:         true,
		Statuses:   make([]hatriecachev1.ScalarResultStatus, count),
		ValueKinds: make([]hatriecachev1.ScalarValueKind, count),
	}
}

func appendStructuredBatchResult(response *hatriecachev1.StructuredBatchResponse, index int, operation hatriecachev1.StructuredCommand, result CacheCommandResponse) {
	if !result.OK {
		status := scalarStatusForCommandError(result.Message)
		response.Statuses[index] = status
		if status == hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR {
			response.ErrorIndexes = append(response.ErrorIndexes, uint32(index))
			response.Errors = append(response.Errors, result.Message)
		}
		return
	}
	if result.Message == "value not found" || result.Message == "key not found" {
		response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND
		return
	}
	response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
	switch operation {
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_SLICE,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_SHIFT_SLICE,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HEAD_SLICE,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAIL_SLICE,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_SET,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_PRIORITY,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_PRIORITY,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_PRIORITY:
		response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_BYTES
		response.Values = append(response.Values, result.Value...)
		response.ValueEnds = append(response.ValueEnds, uint32(len(response.Values)))
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_REMOVE_SET,
		hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY:
		value, err := strconv.ParseInt(result.Value, 10, 64)
		if err != nil {
			addStructuredBatchError(response, index, err)
			return
		}
		response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_INTEGER
		response.IntegerValues = append(response.IntegerValues, value)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET:
		response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_BOOLEAN
		if strings.TrimSpace(result.Value) == "1" {
			response.IntegerValues = append(response.IntegerValues, 1)
		} else {
			response.IntegerValues = append(response.IntegerValues, 0)
		}
	}
}

func addStructuredBatchError(response *hatriecachev1.StructuredBatchResponse, index int, err error) {
	response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR
	response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_NONE
	response.ErrorIndexes = append(response.ErrorIndexes, uint32(index))
	response.Errors = append(response.Errors, err.Error())
}
