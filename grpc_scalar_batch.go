package hatriecache

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

const scalarBatchSharedKeyStackLimit = 256

func (server *CacheGRPCServer) ScalarBatchStream(stream hatriecachev1.CacheService_ScalarBatchStreamServer) error {
	ctx, err := server.requestContext(stream.Context())
	if err != nil {
		return err
	}
	if err := server.requireTrie(); err != nil {
		return err
	}
	request := new(hatriecachev1.ScalarBatchRequest)
	for {
		*request = hatriecachev1.ScalarBatchRequest{}
		err := stream.RecvMsg(request)
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
	if len(request.GetKeys()) != len(operations) && len(request.GetKeys()) != 1 {
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
		result := ht.executePartitionedPublicBatchCommand(scalarBatchCacheCommand(materializeScalarBatchSharedKey(request)))
		return scalarBatchResponseFromCommand(request, result)
	}
	operations := request.GetOperations()
	if len(request.Keys) == 1 && len(operations) > 1 {
		if len(operations) <= scalarBatchSharedKeyStackLimit {
			return ht.executeScalarBatchDirectSharedKeyStack(ctx, request)
		}
		request = materializeScalarBatchSharedKey(request)
	}
	response := newScalarBatchResponse(request.GetBatchId(), len(operations))
	stringIndex := 0
	integerIndex := 0

	ht.mu.Lock()
	defer ht.mu.Unlock()
	telemetry := batchTelemetry{}
	defer ht.flushBatchTelemetryLocked(&telemetry)
	ht.ensureOpen()
	if err := ctx.Err(); err != nil {
		response.Ok = false
		response.Error = err.Error()
		response.Statuses = response.Statuses[:0]
		response.ValueKinds = response.ValueKinds[:0]
		return response
	}
	if ht.executeScalarBatchRepeatedReadLocked(request, response, &telemetry) {
		return response
	}
	if ht.executeScalarBatchNativeLocked(ctx, request, response, &telemetry) {
		return response
	}
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
			ht.scalarBatchGetLocked(response, index, key, &telemetry)
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS:
			hval := ht.peekLocked(key)
			hit := !hval.Empty()
			ht.recordReadBatchLocked(&telemetry, hit, key)
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
			response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_BOOLEAN
			if hit {
				response.IntegerValues = append(response.IntegerValues, 1)
			} else {
				response.IntegerValues = append(response.IntegerValues, 0)
			}
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING:
			if err := ht.upsertStringValueLocked(key, string(stringValue)); err != nil {
				addScalarBatchError(response, index, err)
				continue
			}
			ht.recordWriteBatchLocked(&telemetry, key)
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER:
			if integerValue < minCommandInt32 || integerValue > maxCommandInt32 {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_ARGUMENT
				continue
			}
			if err := ht.upsertCounterValueLocked(key, int32(integerValue)); err != nil {
				addScalarBatchError(response, index, err)
				continue
			}
			ht.recordWriteBatchLocked(&telemetry, key)
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
			if integerValue < minCommandInt32 || integerValue > maxCommandInt32 {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_INVALID_ARGUMENT
				continue
			}
			value, updated, err := ht.incrementCounterValueLocked(key, int32(integerValue), true)
			if err != nil {
				addScalarBatchError(response, index, err)
				continue
			}
			if !updated {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_COUNTER_OVERFLOW
				continue
			}
			ht.recordWriteBatchLocked(&telemetry, key)
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
			response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_INTEGER
			response.IntegerValues = append(response.IntegerValues, int64(value))
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE:
			if ht.deleteAndRecordBatchLocked(&telemetry, key) {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
			} else {
				response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND
			}
		}
	}
	return response
}

func (ht *HatTrie) executeScalarBatchDirectSharedKeyStack(ctx context.Context, request *hatriecachev1.ScalarBatchRequest) *hatriecachev1.ScalarBatchResponse {
	var keys [scalarBatchSharedKeyStackLimit]string
	for index := range request.Operations {
		keys[index] = request.Keys[0]
	}
	prepared := &hatriecachev1.ScalarBatchRequest{
		BatchId:       request.BatchId,
		Operations:    request.Operations,
		Keys:          keys[:len(request.Operations)],
		StringValues:  request.StringValues,
		IntegerValues: request.IntegerValues,
	}
	return ht.executeScalarBatchDirect(ctx, prepared)
}

func (ht *HatTrie) executeScalarBatchRepeatedReadLocked(request *hatriecachev1.ScalarBatchRequest, response *hatriecachev1.ScalarBatchResponse, telemetry *batchTelemetry) bool {
	operations := request.GetOperations()
	if len(operations) < 2 || operations[0] != hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET || operations[1] != hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET {
		return false
	}
	key := request.Keys[0]
	sharedKey := len(request.Keys) == 1
	if key == "" || (!sharedKey && request.Keys[1] != key) || validateKey(key) != nil {
		return false
	}
	for index := 2; index < len(operations); index++ {
		if operations[index] != hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET || (!sharedKey && request.Keys[index] != key) {
			return false
		}
	}

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchCountLocked(telemetry, false, uint64(len(operations)), key)
		for index := range operations {
			addScalarBatchError(response, index, err)
		}
		return true
	}
	if hval.Empty() {
		ht.recordReadBatchCountLocked(telemetry, false, uint64(len(operations)), key)
		for index := range operations {
			response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND
		}
		return true
	}

	var value string
	if hval.IsCounter() {
		value = strconv.FormatInt(int64(hval.Index), 10)
	} else {
		value, err = ht.commandValueLocked(hval)
		if err != nil {
			ht.recordReadBatchCountLocked(telemetry, false, uint64(len(operations)), key)
			for index := range operations {
				addScalarBatchError(response, index, err)
			}
			return true
		}
	}
	if len(value) != 0 && len(operations) <= int(^uint(0)>>1)/len(value) {
		response.Values = make([]byte, 0, len(value)*len(operations))
	}
	response.ValueEnds = make([]uint32, 0, len(operations))
	for index := range operations {
		response.Statuses[index] = hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK
		response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_BYTES
		response.Values = append(response.Values, value...)
		response.ValueEnds = append(response.ValueEnds, uint32(len(response.Values)))
	}
	ht.recordReadBatchCountLocked(telemetry, true, uint64(len(operations)), key)
	return true
}

func (ht *HatTrie) executeScalarBatchNativeLocked(ctx context.Context, request *hatriecachev1.ScalarBatchRequest, response *hatriecachev1.ScalarBatchResponse, telemetry *batchTelemetry) bool {
	operations := request.GetOperations()
	if len(operations) < minNativeScalarDirectBatchSize || len(ht.dbrefs.array) != 0 || ctx.Err() != nil {
		return false
	}
	if operations[0] == hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS && scalarBatchAllExists(operations) {
		// EXISTS always emits one boolean, so this exact capacity cannot over-reserve.
		response.IntegerValues = make([]int64, 0, len(operations))
	}
	integerIndex := 0
	chunkKeyBytes := 0
	maxChunkKeyBytes := 0
	for index, operation := range operations {
		if index&63 == 0 && ctx.Err() != nil {
			return false
		}
		key := request.Keys[index]
		if key == "" || validateKey(key) != nil {
			return false
		}
		if _, expiring := ht.expires[key]; expiring {
			return false
		}
		chunkKeyBytes += len(key)
		if index&63 == 63 {
			if chunkKeyBytes > maxChunkKeyBytes {
				maxChunkKeyBytes = chunkKeyBytes
			}
			chunkKeyBytes = 0
		}
		switch operation {
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
			value := request.IntegerValues[integerIndex]
			integerIndex++
			if value < minCommandInt32 || value > maxCommandInt32 {
				return false
			}
		}
	}
	if chunkKeyBytes > maxChunkKeyBytes {
		maxChunkKeyBytes = chunkKeyBytes
	}
	defer func() {
		if cap(ht.nativeCommandBatchScratch.keys) > maxNativeScalarDirectRetainedKeyBytes {
			ht.nativeCommandBatchScratch.keys = nil
		}
	}()
	stringIndex := 0
	integerIndex = 0
	if len(operations) <= nativeScalarDirectBatchChunkSize {
		if err := ctx.Err(); err != nil {
			response.Ok = false
			response.Error = err.Error()
			response.Statuses = response.Statuses[:0]
			response.ValueKinds = response.ValueKinds[:0]
			return true
		}
		results, _, _ := ht.runNativeScalarRequestChunkLocked(request, 0, len(operations), maxChunkKeyBytes, stringIndex, integerIndex)
		ht.reserveNativeScalarRawStringReadResponseLocked(request, response, results)
		for index, result := range results {
			item := nativeCommandBatchItem{
				key:     request.Keys[index],
				command: scalarNativePublicCommand(operations[index]),
			}
			ht.applyNativeScalarBatchResultLocked(response, index, item, result, telemetry)
		}
		return true
	}
	for start := 0; start < len(operations); start += nativeScalarDirectBatchChunkSize {
		if err := ctx.Err(); err != nil {
			response.Ok = false
			response.Error = err.Error()
			response.Statuses = response.Statuses[:start]
			response.ValueKinds = response.ValueKinds[:start]
			return true
		}
		end := start + nativeScalarDirectBatchChunkSize
		if end > len(operations) {
			end = len(operations)
		}
		results, nextStringIndex, nextIntegerIndex := ht.runNativeScalarRequestChunkLocked(request, start, end, maxChunkKeyBytes, stringIndex, integerIndex)
		stringIndex = nextStringIndex
		integerIndex = nextIntegerIndex
		for index, result := range results {
			responseIndex := start + index
			item := nativeCommandBatchItem{
				key:     request.Keys[responseIndex],
				command: scalarNativePublicCommand(operations[responseIndex]),
			}
			ht.applyNativeScalarBatchResultLocked(response, responseIndex, item, result, telemetry)
		}
	}
	return true
}

func scalarBatchAllExists(operations []hatriecachev1.ScalarCommand) bool {
	for _, operation := range operations {
		if operation != hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS {
			return false
		}
	}
	return true
}

func scalarNativePublicCommand(operation hatriecachev1.ScalarCommand) publicScalarBatchCommand {
	switch operation {
	case hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET:
		return publicScalarBatchGet
	case hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS:
		return publicScalarBatchExists
	case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING:
		return publicScalarBatchSetString
	case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER:
		return publicScalarBatchSetCounter
	case hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
		return publicScalarBatchIncrement
	case hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE:
		return publicScalarBatchDelete
	default:
		return publicScalarBatchInvalid
	}
}

func (ht *HatTrie) scalarBatchGetLocked(response *hatriecachev1.ScalarBatchResponse, index int, key string, telemetry *batchTelemetry) {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		addScalarBatchError(response, index, err)
		return
	}
	if hval.Empty() {
		ht.recordReadBatchLocked(telemetry, false, key)
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
			ht.recordReadBatchLocked(telemetry, false, key)
			addScalarBatchError(response, index, valueErr)
			response.ValueKinds[index] = hatriecachev1.ScalarValueKind_SCALAR_VALUE_KIND_NONE
			return
		}
		response.Values = append(response.Values, value...)
	}
	response.ValueEnds = append(response.ValueEnds, uint32(len(response.Values)))
	ht.recordReadBatchLocked(telemetry, true, key)
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
	request = materializeScalarBatchSharedKey(request)
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

func materializeScalarBatchSharedKey(request *hatriecachev1.ScalarBatchRequest) *hatriecachev1.ScalarBatchRequest {
	if len(request.Keys) != 1 || len(request.Operations) == 1 {
		return request
	}
	prepared := &hatriecachev1.ScalarBatchRequest{
		BatchId:       request.BatchId,
		Operations:    request.Operations,
		Keys:          make([]string, len(request.Operations)),
		StringValues:  request.StringValues,
		IntegerValues: request.IntegerValues,
	}
	for index := range prepared.Keys {
		prepared.Keys[index] = request.Keys[0]
	}
	return prepared
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
