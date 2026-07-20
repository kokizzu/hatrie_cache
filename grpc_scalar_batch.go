package hatriecache

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func (server *CacheGRPCServer) ScalarBatchStream(stream hatriecachev1.CacheService_ScalarBatchStreamServer) error {
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
		if scalarBatchMutates(request.GetOperations()) {
			if err := server.rejectDangerousGRPC("command", AuditEvent{
				Command: "SCALAR_BATCH",
				Method:  "/hatriecache.v1.CacheService/ScalarBatchStream",
			}); err != nil {
				return err
			}
		}
		response := server.executeScalarBatch(ctx, request)
		if scalarBatchMutates(request.GetOperations()) {
			server.auditGRPC(AuditEvent{
				Action:  "command",
				Command: "SCALAR_BATCH",
				OK:      response.GetOk(),
				Method:  "/hatriecache.v1.CacheService/ScalarBatchStream",
				Message: response.GetError(),
			})
		}
		if err := stream.Send(response); err != nil {
			return err
		}
	}
}

func scalarBatchMutates(operations []hatriecachev1.ScalarCommand) bool {
	for _, operation := range operations {
		switch operation {
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE:
			return true
		}
	}
	return false
}

func (server *CacheGRPCServer) executeScalarBatch(ctx context.Context, request *hatriecachev1.ScalarBatchRequest) *hatriecachev1.ScalarBatchResponse {
	response := &hatriecachev1.ScalarBatchResponse{BatchId: request.GetBatchId()}
	if err := validateScalarBatchColumns(request); err != nil {
		response.Error = err.Error()
		return response
	}
	if err := ctx.Err(); err != nil {
		response.Error = err.Error()
		return response
	}
	if server.scalarBatchRequiresCompatibilityPath() {
		return server.executeScalarBatchCompatibility(ctx, request)
	}
	return server.trie.executeScalarBatchDirect(ctx, request)
}

func (server *CacheGRPCServer) scalarBatchRequiresCompatibilityPath() bool {
	return server.options.Journal != nil ||
		server.options.DirtyTracker != nil ||
		server.options.Replicator != nil ||
		server.options.EnforceLeaderWrites
}

func validateScalarBatchColumns(request *hatriecachev1.ScalarBatchRequest) error {
	operations := request.GetOperations()
	if len(operations) == 0 {
		return errors.New("scalar batch requires operations")
	}
	if len(operations) > maxPublicCommandBatchSize {
		return errors.New("scalar batch exceeds maximum size")
	}
	if len(request.GetKeys()) != len(operations) {
		return errors.New("scalar batch keys must match operations")
	}
	stringsNeeded := 0
	integersNeeded := 0
	for _, operation := range operations {
		switch operation {
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE:
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING:
			stringsNeeded++
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
			integersNeeded++
		default:
			return errors.New("scalar batch contains an unsupported operation")
		}
	}
	if len(request.GetStringValues()) != stringsNeeded {
		return errors.New("scalar batch string values do not match SET_STRING operations")
	}
	if len(request.GetIntegerValues()) != integersNeeded {
		return errors.New("scalar batch integer values do not match counter operations")
	}
	return nil
}

func (ht *HatTrie) executeScalarBatchDirect(ctx context.Context, request *hatriecachev1.ScalarBatchRequest) *hatriecachev1.ScalarBatchResponse {
	if ht.localPartitionSet() != nil {
		if err := ctx.Err(); err != nil {
			return &hatriecachev1.ScalarBatchResponse{BatchId: request.GetBatchId(), Error: err.Error()}
		}
		result := ht.executePartitionedPublicBatchCommand(scalarBatchCacheCommand(request))
		return scalarBatchResponseFromCommand(request, result)
	}
	operations := request.GetOperations()
	response := newScalarBatchResponse(request.GetBatchId(), len(operations))
	stringIndex := 0
	integerIndex := 0

	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.ensureOpen()
	for index, operation := range operations {
		if index&63 == 0 {
			if err := ctx.Err(); err != nil {
				response.Ok = false
				response.Error = err.Error()
				response.Statuses = response.Statuses[:index]
				response.ValueKinds = response.ValueKinds[:index]
				return response
			}
		}
		key := request.Keys[index]
		var stringValue []byte
		var integerValue int64
		switch operation {
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING:
			stringValue = request.StringValues[stringIndex]
			stringIndex++
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
			integerValue = request.IntegerValues[integerIndex]
			integerIndex++
		}
		if key == "" {
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_KEY
			continue
		}
		if err := validateKey(key); err != nil {
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_KEY
			continue
		}
		switch operation {
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET:
			ht.scalarBatchGetLocked(response, index, key)
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS:
			hval := ht.peekLocked(key)
			hit := !hval.Empty()
			ht.recordReadLocked(hit, key)
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
			response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_BOOLEAN
			if hit {
				response.IntegerValues = append(response.IntegerValues, 1)
			} else {
				response.IntegerValues = append(response.IntegerValues, 0)
			}
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING:
			if err := ht.upsertStringLocked(key, string(stringValue)); err != nil {
				addScalarBatchError(response, index, err)
				continue
			}
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER:
			if integerValue < minCommandInt32 || integerValue > maxCommandInt32 {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_ARGUMENT
				continue
			}
			if err := ht.upsertCounterLocked(key, int32(integerValue)); err != nil {
				addScalarBatchError(response, index, err)
				continue
			}
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
			if integerValue < minCommandInt32 || integerValue > maxCommandInt32 {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_ARGUMENT
				continue
			}
			value, updated, err := ht.incrementCounterLocked(key, int32(integerValue), true)
			if err != nil {
				addScalarBatchError(response, index, err)
				continue
			}
			if !updated {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_COUNTER_OVERFLOW
				continue
			}
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
			response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_INTEGER
			response.IntegerValues = append(response.IntegerValues, int64(value))
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE:
			if ht.deleteAndRecordLocked(key) {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
			} else {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND
			}
		}
	}
	return response
}

func (ht *HatTrie) scalarBatchGetLocked(response *hatriecachev1.ScalarBatchResponse, index int, key string) {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		addScalarBatchError(response, index, err)
		return
	}
	if hval.Empty() {
		ht.recordReadLocked(false, key)
		response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND
		return
	}
	response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
	response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_BYTES
	if hval.IsCounter() {
		response.Values = strconv.AppendInt(response.Values, int64(hval.Index), 10)
	} else {
		value, valueErr := ht.commandValueLocked(hval)
		if valueErr != nil {
			ht.recordReadLocked(false, key)
			addScalarBatchError(response, index, valueErr)
			response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_NONE
			return
		}
		response.Values = append(response.Values, value...)
	}
	response.ValueEnds = append(response.ValueEnds, uint32(len(response.Values)))
	ht.recordReadLocked(true, key)
}

func newScalarBatchResponse(batchID uint64, count int) *hatriecachev1.ScalarBatchResponse {
	return &hatriecachev1.ScalarBatchResponse{
		BatchId:    batchID,
		Ok:         true,
		Statuses:   make([]hatriecachev1.ScalarResultStatus, count),
		ValueKinds: make([]hatriecachev1.ScalarValueKind, count),
	}
}

func addScalarBatchError(response *hatriecachev1.ScalarBatchResponse, index int, err error) {
	response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR
	response.ErrorIndexes = append(response.ErrorIndexes, uint32(index))
	response.Errors = append(response.Errors, err.Error())
}

func (server *CacheGRPCServer) executeScalarBatchCompatibility(ctx context.Context, request *hatriecachev1.ScalarBatchRequest) *hatriecachev1.ScalarBatchResponse {
	command := scalarBatchCacheCommand(request)
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
	return scalarBatchResponseFromCommand(request, result)
}

func scalarBatchResponseFromCommand(request *hatriecachev1.ScalarBatchRequest, result CacheCommandResponse) *hatriecachev1.ScalarBatchResponse {
	response := newScalarBatchResponse(request.GetBatchId(), len(request.GetOperations()))
	if len(result.Responses) != len(request.GetOperations()) {
		response.Ok = false
		response.Error = result.Message
		response.Statuses = nil
		response.ValueKinds = nil
		return response
	}
	for index, item := range result.Responses {
		operation := request.Operations[index]
		if !item.OK {
			response.Statuses[index] = scalarStatusForCommandError(item.Message)
			if response.Statuses[index] == hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR {
				response.ErrorIndexes = append(response.ErrorIndexes, uint32(index))
				response.Errors = append(response.Errors, item.Message)
			}
			continue
		}
		if item.Message == "key not found" || item.Message == "key not found or no ttl" {
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND
			continue
		}
		response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
		switch operation {
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET:
			response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_BYTES
			response.Values = append(response.Values, item.Value...)
			response.ValueEnds = append(response.ValueEnds, uint32(len(response.Values)))
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS:
			response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_BOOLEAN
			if item.Value == "1" {
				response.IntegerValues = append(response.IntegerValues, 1)
			} else {
				response.IntegerValues = append(response.IntegerValues, 0)
			}
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
			value, err := strconv.ParseInt(item.Value, 10, 64)
			if err != nil {
				addScalarBatchError(response, index, err)
				continue
			}
			response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_INTEGER
			response.IntegerValues = append(response.IntegerValues, value)
		}
	}
	return response
}

func scalarBatchCacheCommand(request *hatriecachev1.ScalarBatchRequest) CacheCommandRequest {
	batch := make([]CacheCommandRequest, len(request.GetOperations()))
	stringIndex := 0
	integerIndex := 0
	for index, operation := range request.GetOperations() {
		item := CacheCommandRequest{Key: request.Keys[index]}
		switch operation {
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET:
			item.Command = "GET"
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS:
			item.Command = "EXISTS"
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING:
			item.Command = "SETSTR"
			item.Value = string(request.StringValues[stringIndex])
			stringIndex++
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER:
			item.Command = "SETINT"
			item.Value = strconv.FormatInt(request.IntegerValues[integerIndex], 10)
			integerIndex++
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
			item.Command = "INC"
			item.Value = strconv.FormatInt(request.IntegerValues[integerIndex], 10)
			integerIndex++
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE:
			item.Command = "DEL"
		}
		batch[index] = item
	}
	return CacheCommandRequest{Command: "BATCH", Batch: batch}
}

func scalarStatusForCommandError(message string) hatriecachev1.ScalarResultStatus {
	switch {
	case strings.Contains(message, "key is required"), strings.Contains(message, "key must"):
		return hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_KEY
	case strings.Contains(message, "32-bit integer"):
		return hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_ARGUMENT
	case strings.Contains(message, "counter overflow"):
		return hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_COUNTER_OVERFLOW
	default:
		return hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INTERNAL_ERROR
	}
}
