package hatriecache

import (
	"context"
	"strconv"
	"time"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

const structuredBatchDirectChunkSize = 4

func (ht *HatTrie) structuredBatchRequiresCommandLoop(request *hatriecachev1.StructuredBatchRequest) bool {
	if ht == nil || ht.localPartitionSet() != nil {
		return true
	}

	ht.mu.RLock()
	detailedKeyStats := ht.keyStatsMode != KeyStatsModeOff
	ht.mu.RUnlock()
	if detailedKeyStats {
		return true
	}

	subkeyIndex := 0
	for index, operation := range request.GetOperations() {
		key := request.Keys[index]
		if !commandFastPathField(key) || !validKey(key) {
			return true
		}
		switch operation {
		case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
			hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP:
			if !commandFastPathField(request.Subkeys[subkeyIndex]) {
				return true
			}
			subkeyIndex++
		}
	}
	return false
}

func (ht *HatTrie) executeStructuredBatchBounded(ctx context.Context, request *hatriecachev1.StructuredBatchRequest) *hatriecachev1.StructuredBatchResponse {
	return ht.executeStructuredBatchBoundedPrepared(ctx, request, "", false)
}

func (ht *HatTrie) executeStructuredBatchBoundedPrepared(ctx context.Context, request *hatriecachev1.StructuredBatchRequest, sharedValue string, hasSharedValue bool) *hatriecachev1.StructuredBatchResponse {
	return ht.executeStructuredBatchBoundedWithChunkSizePrepared(ctx, request, structuredBatchDirectChunkSize, sharedValue, hasSharedValue)
}

func (ht *HatTrie) executeStructuredBatchBoundedWithChunkSize(ctx context.Context, request *hatriecachev1.StructuredBatchRequest, chunkSize int) *hatriecachev1.StructuredBatchResponse {
	return ht.executeStructuredBatchBoundedWithChunkSizePrepared(ctx, request, chunkSize, "", false)
}

func (ht *HatTrie) executeStructuredBatchBoundedWithChunkSizePrepared(ctx context.Context, request *hatriecachev1.StructuredBatchRequest, chunkSize int, sharedValue string, hasSharedValue bool) *hatriecachev1.StructuredBatchResponse {
	response := newStructuredBatchResponse(request.GetBatchId(), len(request.GetOperations()))
	telemetry := batchTelemetry{}
	telemetryNow := time.Time{}

	cursor := structuredBatchCursor{sharedValue: sharedValue, hasSharedValue: hasSharedValue}
	operations := request.GetOperations()
	for start := 0; start < len(operations); start += chunkSize {
		if err := ctx.Err(); err != nil {
			response.Ok = false
			response.Error = err.Error()
			response.Statuses = response.Statuses[:start]
			response.ValueKinds = response.ValueKinds[:start]
			return response
		}
		end := start + chunkSize
		if end > len(operations) {
			end = len(operations)
		}

		ht.mu.Lock()
		for index := start; index < end; index++ {
			operation := operations[index]
			key := request.Keys[index]
			var subkey string
			var value string
			var priority int64
			switch operation {
			case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP:
				subkey = request.Subkeys[cursor.subkey]
				value = cursor.nextValue(request)
				cursor.subkey++
			case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP,
				hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP:
				subkey = request.Subkeys[cursor.subkey]
				cursor.subkey++
			case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE,
				hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET,
				hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_REMOVE_SET,
				hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET:
				value = cursor.nextValue(request)
			case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY:
				value = cursor.nextValue(request)
				priority = request.Priorities[cursor.priority]
				cursor.priority++
			}
			result := ht.executeStructuredBatchItemLocked(operation, key, subkey, value, priority, &telemetry)
			appendStructuredBatchResult(response, index, operation, result)
		}
		if telemetry.hits|telemetry.misses|telemetry.writes|telemetry.deletes != 0 {
			if telemetryNow.IsZero() {
				telemetryNow = ht.currentTime()
			}
			ht.flushBatchTelemetryAtLocked(&telemetry, telemetryNow)
			telemetry = batchTelemetry{}
		}
		ht.mu.Unlock()
	}
	return response
}

func (ht *HatTrie) executeStructuredBatchItemLocked(
	operation hatriecachev1.StructuredCommand,
	key string,
	subkey string,
	value string,
	priority int64,
	telemetry *batchTelemetry,
) CacheCommandResponse {
	switch operation {
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUT_MAP:
		return ht.structuredPutMapLocked(key, subkey, value, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_MAP:
		return ht.structuredPeekMapLocked(key, subkey, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAKE_MAP:
		return ht.structuredTakeMapLocked(key, subkey, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_SLICE:
		return ht.structuredPushSliceLocked(key, value, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_SLICE:
		return ht.structuredPopSliceLocked(key, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_SHIFT_SLICE:
		return ht.structuredShiftSliceLocked(key, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HEAD_SLICE:
		return ht.structuredHeadSliceLocked(key, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_TAIL_SLICE:
		return ht.structuredTailSliceLocked(key, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_ADD_SET:
		return ht.structuredAddSetLocked(key, value, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_REMOVE_SET:
		return ht.structuredRemoveSetLocked(key, value, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_HAS_SET:
		return ht.structuredHasSetLocked(key, value, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_SET:
		return ht.structuredGetSetLocked(key, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PUSH_PRIORITY:
		return ht.structuredPushPriorityLocked(key, priority, value, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_PEEK_PRIORITY:
		return ht.structuredPeekPriorityLocked(key, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_POP_PRIORITY:
		return ht.structuredPopPriorityLocked(key, telemetry)
	case hatriecachev1.StructuredCommand_STRUCTURED_COMMAND_GET_PRIORITY:
		return ht.structuredGetPriorityLocked(key, telemetry)
	default:
		return commandError("unsupported command")
	}
}

func (ht *HatTrie) structuredPutMapLocked(key string, subkey string, value string, telemetry *batchTelemetry) CacheCommandResponse {
	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error())
	}
	if hval.IsMap() {
		hval.Index = ht.maps.putEntryAdaptive(hval.Index, subkey, value)
		*rawPtr = hval.toValue()
	} else {
		if rawPtr == nil {
			rawPtr = ht.upsertLocation(key)
		}
		ht.returnStorage(hval)
		ht.clearExpirationLocked(key)
		hval = HatValue{Index: ht.maps.addEntryAdaptive(subkey, value), Flags: DATAVALUE_TYPE_MAP}
		*rawPtr = hval.toValue()
	}
	ht.recordWriteBatchLocked(telemetry, key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "stored map fields"}
}

func (ht *HatTrie) structuredPeekMapLocked(key string, subkey string, telemetry *batchTelemetry) CacheCommandResponse {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsMap() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return structuredValueNotFound()
	}
	value, ok := ht.maps.peek(hval.Index, subkey)
	ht.recordReadBatchLocked(telemetry, ok, key)
	if !ok {
		return structuredValueNotFound()
	}
	return commandValueResponse("ok", value)
}

func (ht *HatTrie) structuredTakeMapLocked(key string, subkey string, telemetry *batchTelemetry) CacheCommandResponse {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsMap() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return structuredValueNotFound()
	}
	value, ok := ht.maps.TakeEntry(hval.Index, subkey)
	ht.recordReadBatchLocked(telemetry, ok, key)
	if !ok {
		return structuredValueNotFound()
	}
	ht.recordWriteBatchLocked(telemetry, key)
	return commandValueResponse("removed", value)
}

func (ht *HatTrie) structuredPushSliceLocked(key string, value string, telemetry *batchTelemetry) CacheCommandResponse {
	if value == "" {
		return commandError("value or values is required")
	}
	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error())
	}
	if hval.IsSlice() {
		hval.Index, err = ht.slices.pushOneChecked(hval.Index, value)
		if err != nil {
			return commandError(err.Error())
		}
		*rawPtr = hval.toValue()
	} else {
		idx, addErr := ht.slices.addValuesAdaptive(value)
		if addErr != nil {
			return commandError(addErr.Error())
		}
		if rawPtr == nil {
			rawPtr = ht.upsertLocation(key)
		}
		ht.returnStorage(hval)
		ht.clearExpirationLocked(key)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}
		*rawPtr = hval.toValue()
	}
	ht.recordWriteBatchLocked(telemetry, key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "pushed slice values"}
}

func (ht *HatTrie) structuredPopSliceLocked(key string, telemetry *batchTelemetry) CacheCommandResponse {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsSlice() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return structuredValueNotFound()
	}
	value, ok := ht.slices.pop(hval.Index, true)
	ht.recordReadBatchLocked(telemetry, ok, key)
	if !ok {
		ht.cacheValueLocked(key, hval)
		return structuredValueNotFound()
	}
	ht.recordWriteBatchLocked(telemetry, key)
	ht.cacheValueLocked(key, hval)
	return commandValueResponse("removed", value)
}

func (ht *HatTrie) structuredShiftSliceLocked(key string, telemetry *batchTelemetry) CacheCommandResponse {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsSlice() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return structuredValueNotFound()
	}
	value, ok := ht.slices.shift(hval.Index)
	ht.recordReadBatchLocked(telemetry, ok, key)
	if !ok {
		return structuredValueNotFound()
	}
	ht.recordWriteBatchLocked(telemetry, key)
	return commandValueResponse("removed", value)
}

func (ht *HatTrie) structuredHeadSliceLocked(key string, telemetry *batchTelemetry) CacheCommandResponse {
	return ht.structuredSliceEdgeLocked(key, true, telemetry)
}

func (ht *HatTrie) structuredTailSliceLocked(key string, telemetry *batchTelemetry) CacheCommandResponse {
	return ht.structuredSliceEdgeLocked(key, false, telemetry)
}

func (ht *HatTrie) structuredSliceEdgeLocked(key string, head bool, telemetry *batchTelemetry) CacheCommandResponse {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsSlice() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return structuredValueNotFound()
	}
	var value interface{}
	var ok bool
	if head {
		value, ok = ht.slices.head(hval.Index)
	} else {
		value, ok = ht.slices.tail(hval.Index)
	}
	ht.recordReadBatchLocked(telemetry, ok, key)
	if !ok {
		return structuredValueNotFound()
	}
	return commandValueResponse("ok", value)
}

func (ht *HatTrie) structuredAddSetLocked(key string, value string, telemetry *batchTelemetry) CacheCommandResponse {
	if value == "" {
		return commandError("value or values is required")
	}
	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error())
	}
	added := 1
	if hval.IsSet() {
		hval.Index, added = ht.sets.addPlainString(hval.Index, value)
		*rawPtr = hval.toValue()
	} else {
		if rawPtr == nil {
			rawPtr = ht.upsertLocation(key)
		}
		ht.returnStorage(hval)
		ht.clearExpirationLocked(key)
		packed := packedStringSetValue{values: [2]interface{}{value}, length: 1}
		hval = HatValue{Index: ht.sets.addPacked(packed), Flags: DATAVALUE_TYPE_SET}
		*rawPtr = hval.toValue()
	}
	if added > 0 {
		ht.recordWriteBatchLocked(telemetry, key)
		ht.cacheValueLocked(key, hval)
	}
	return CacheCommandResponse{OK: true, Message: "added set values", Value: strconv.Itoa(added)}
}

func (ht *HatTrie) structuredRemoveSetLocked(key string, value string, telemetry *batchTelemetry) CacheCommandResponse {
	if value == "" {
		return commandError("value or values is required")
	}
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsSet() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return CacheCommandResponse{OK: true, Message: "removed set values", Value: "0"}
	}
	removed := ht.sets.removePlainString(hval.Index, value)
	ht.recordReadBatchLocked(telemetry, removed > 0, key)
	if removed > 0 {
		ht.recordWriteBatchLocked(telemetry, key)
	}
	return CacheCommandResponse{OK: true, Message: "removed set values", Value: strconv.Itoa(removed)}
}

func (ht *HatTrie) structuredHasSetLocked(key string, value string, telemetry *batchTelemetry) CacheCommandResponse {
	if value == "" {
		return commandError("value or values is required")
	}
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsSet() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandBool01Response(false)
	}
	hit := ht.sets.hasPlainString(hval.Index, value)
	ht.recordReadBatchLocked(telemetry, hit, key)
	return commandBool01Response(hit)
}

func (ht *HatTrie) structuredGetSetLocked(key string, telemetry *batchTelemetry) CacheCommandResponse {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsSet() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return structuredValueNotFound()
	}
	ht.recordReadBatchLocked(telemetry, true, key)
	return commandValueResponse("ok", ht.sets.values(hval.Index))
}

func (ht *HatTrie) structuredPushPriorityLocked(key string, priority int64, value string, telemetry *batchTelemetry) CacheCommandResponse {
	if value == "" {
		return commandError("value or values is required")
	}
	rawPtr, hval, err := ht.freshLocationCheckedLocked(key)
	if err != nil {
		return commandError(err.Error())
	}
	if hval.IsPriorityQueue() {
		err = ht.priorityQueues.array[hval.Index].PushStringChecked(priority, value)
		if err != nil {
			return commandError(err.Error())
		}
		*rawPtr = hval.toValue()
	} else {
		if rawPtr == nil {
			rawPtr = ht.upsertLocation(key)
		}
		ht.returnStorage(hval)
		ht.clearExpirationLocked(key)
		idx := ht.priorityQueues.Add(nil)
		if err = ht.priorityQueues.array[idx].PushStringChecked(priority, value); err != nil {
			ht.priorityQueues.Del(idx)
			return commandError(err.Error())
		}
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_PRIORITY_QUEUE}
		*rawPtr = hval.toValue()
	}
	ht.recordWriteBatchLocked(telemetry, key)
	ht.cacheValueLocked(key, hval)
	return CacheCommandResponse{OK: true, Message: "pushed priority queue values", Value: "1"}
}

func (ht *HatTrie) structuredPeekPriorityLocked(key string, telemetry *batchTelemetry) CacheCommandResponse {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsPriorityQueue() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return structuredValueNotFound()
	}
	item, ok := ht.priorityQueues.array[hval.Index].Peek()
	ht.recordReadBatchLocked(telemetry, ok, key)
	if !ok {
		return structuredValueNotFound()
	}
	return commandValueResponse("ok", item)
}

func (ht *HatTrie) structuredPopPriorityLocked(key string, telemetry *batchTelemetry) CacheCommandResponse {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsPriorityQueue() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return structuredValueNotFound()
	}
	item, ok := ht.priorityQueues.array[hval.Index].popItemRetain()
	ht.recordReadBatchLocked(telemetry, ok, key)
	if !ok {
		ht.cacheValueLocked(key, hval)
		return structuredValueNotFound()
	}
	ht.recordWriteBatchLocked(telemetry, key)
	ht.cacheValueLocked(key, hval)
	if text, ok := item.value().(string); ok {
		if payload, encoded := commandFastPriorityQueueItemJSON(item.Priority, text); encoded {
			return CacheCommandResponse{OK: true, Message: "removed", Value: payload}
		}
	}
	return commandValueResponse("removed", item.PriorityItem())
}

func (ht *HatTrie) structuredGetPriorityLocked(key string, telemetry *batchTelemetry) CacheCommandResponse {
	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadBatchLocked(telemetry, false, key)
		return commandError(err.Error())
	}
	if !hval.IsPriorityQueue() {
		ht.recordReadBatchLocked(telemetry, false, key)
		return structuredValueNotFound()
	}
	ht.recordReadBatchLocked(telemetry, true, key)
	return commandValueResponse("ok", ht.priorityQueues.array[hval.Index].Items())
}

func structuredValueNotFound() CacheCommandResponse {
	return CacheCommandResponse{OK: true, Message: "value not found"}
}
