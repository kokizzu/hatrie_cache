package hatriecache

/*
#cgo CFLAGS: -std=c99 -Wall -Wextra -I${SRCDIR}/luikore__hat-trie/src
#include "native_command_batch.h"
*/
import "C"

import (
	"runtime"
	"strconv"
	"strings"
	"unsafe"
)

const minNativeCommandBatchSize = 32

type nativeCommandBatchFamily uint8

const (
	nativeCommandBatchUnsupported nativeCommandBatchFamily = iota
	nativeCommandBatchRead
	nativeCommandBatchSetString
	nativeCommandBatchSetCounter
	nativeCommandBatchIncrement
	nativeCommandBatchDelete
)

type nativeCommandBatchItem struct {
	responseIndex int
	key           string
	command       publicScalarBatchCommand
	input         C.value_t
}

type nativeCommandBatchScratch struct {
	items      []nativeCommandBatchItem
	keys       []byte
	operations []C.hc_batch_operation_t
	results    []C.hc_batch_result_t
}

func (ht *HatTrie) executePublicNativeScalarBatchCommand(request CacheCommandRequest) (CacheCommandResponse, bool) {
	payloads, err := publicCommandBatchRequests(request)
	if err != nil {
		return commandError(err.Error()), true
	}
	if len(payloads) < minNativeCommandBatchSize {
		return CacheCommandResponse{}, false
	}
	family := nativePublicCommandBatchFamily(payloads)
	if family == nativeCommandBatchUnsupported {
		return CacheCommandResponse{}, false
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	if !ht.nativeCommandBatchStateSupportedLocked(payloads, family) {
		return CacheCommandResponse{}, false
	}

	responses := make([]CacheCommandResponse, len(payloads))
	items := ht.nativeCommandBatchScratch.items[:0]
	if cap(items) < len(payloads) {
		items = make([]nativeCommandBatchItem, 0, len(payloads))
	}
	defer func() {
		ht.nativeCommandBatchScratch.items = items[:0]
	}()
	allOK := true
	for index, payload := range payloads {
		key := strings.TrimSpace(payload.Key)
		if key == "" {
			responses[index] = commandError("key is required")
			allOK = false
			continue
		}
		if err := validateKey(key); err != nil {
			responses[index] = commandError(err.Error())
			allOK = false
			continue
		}
		command, _, _ := publicScalarBatchCommandCode(payload.Command)
		item := nativeCommandBatchItem{responseIndex: index, key: key, command: command}
		switch family {
		case nativeCommandBatchSetString:
			valueIndex := ht.raws.addStringOwned(payload.Value)
			item.input = HatValue{Index: valueIndex, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
		case nativeCommandBatchSetCounter:
			value, ok := parseCommandInt32(payload.Value)
			if !ok {
				responses[index] = commandError("value must be a 32-bit integer")
				allOK = false
				continue
			}
			item.input = HatValue{Index: value, Flags: DATAVALUE_TYPE_COUNTER}.toValue()
		case nativeCommandBatchIncrement:
			value, ok := parseCommandIncrement(payload.Value)
			if !ok {
				responses[index] = commandError("value must be a 32-bit integer")
				allOK = false
				continue
			}
			item.input = C.value_t(uint64(uint32(value)))
		}
		items = append(items, item)
	}

	results := ht.runNativeCommandBatchLocked(items, family)
	for index, item := range items {
		result := results[index]
		response := ht.applyNativeCommandBatchResultLocked(item, family, result)
		if !response.OK {
			allOK = false
		}
		responses[item.responseIndex] = response
	}
	return publicCommandBatchResponse(responses, allOK), true
}

func nativePublicCommandBatchFamily(payloads []CacheCommandRequest) nativeCommandBatchFamily {
	family := nativeCommandBatchUnsupported
	for _, payload := range payloads {
		candidate := nativePublicCommandBatchRequestFamily(payload)
		if candidate == nativeCommandBatchUnsupported || (family != nativeCommandBatchUnsupported && candidate != family) {
			return nativeCommandBatchUnsupported
		}
		family = candidate
	}
	return family
}

func nativePublicCommandBatchRequestFamily(payload CacheCommandRequest) nativeCommandBatchFamily {
	command, validationMessage, supported := publicScalarBatchCommandCode(payload.Command)
	if !supported || validationMessage != "" {
		return nativeCommandBatchUnsupported
	}
	switch command {
	case publicScalarBatchGet, publicScalarBatchExists:
		return nativeCommandBatchRead
	case publicScalarBatchSetString:
		if payload.TTLSeconds == nil && payload.UnixSeconds == nil {
			return nativeCommandBatchSetString
		}
	case publicScalarBatchSetCounter:
		if payload.TTLSeconds == nil && payload.UnixSeconds == nil {
			return nativeCommandBatchSetCounter
		}
	case publicScalarBatchIncrement:
		return nativeCommandBatchIncrement
	case publicScalarBatchDelete:
		return nativeCommandBatchDelete
	}
	return nativeCommandBatchUnsupported
}

func journalScalarCommandBatchFamily(request CacheCommandRequest) nativeCommandBatchFamily {
	request.Command = normalizedCommand(request.Command)
	family := nativePublicCommandBatchRequestFamily(request)
	switch family {
	case nativeCommandBatchSetString, nativeCommandBatchSetCounter:
		return family
	default:
		return nativeCommandBatchUnsupported
	}
}

func journalScalarCommandBatchRun(records []CommandJournalRecord, start int) (int, bool) {
	if start < 0 || start >= len(records) {
		return start, false
	}
	family := journalScalarCommandBatchFamily(records[start].Request)
	if family == nativeCommandBatchUnsupported {
		return start, false
	}
	end := start + 1
	for end < len(records) && journalScalarCommandBatchFamily(records[end].Request) == family {
		end++
	}
	return end, true
}

func (ht *HatTrie) executeJournalScalarBatch(records []CommandJournalRecord) (int, CacheCommandResponse, bool) {
	if ht == nil || len(records) < minNativeCommandBatchSize || ht.localPartitionSet() != nil {
		return 0, CacheCommandResponse{}, false
	}
	family := journalScalarCommandBatchFamily(records[0].Request)
	if family == nativeCommandBatchUnsupported {
		return 0, CacheCommandResponse{}, false
	}
	for _, record := range records[1:] {
		if journalScalarCommandBatchFamily(record.Request) != family {
			return 0, CacheCommandResponse{}, false
		}
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.journalScalarBatchCalls++
	applied := 0
	for index, record := range records {
		request := record.Request
		key := strings.TrimSpace(request.Key)
		if key == "" {
			ht.recordJournalScalarBatchWritesLocked(records[:applied])
			return index, commandError("key is required"), true
		}
		if err := validateKey(key); err != nil {
			ht.recordJournalScalarBatchWritesLocked(records[:applied])
			return index, commandError(err.Error()), true
		}
		switch family {
		case nativeCommandBatchSetString:
			if err := ht.upsertStringValueLocked(key, request.Value); err != nil {
				ht.recordJournalScalarBatchWritesLocked(records[:applied])
				return index, commandError(err.Error()), true
			}
		case nativeCommandBatchSetCounter:
			value, ok := parseCommandInt32(request.Value)
			if !ok {
				ht.recordJournalScalarBatchWritesLocked(records[:applied])
				return index, commandError("value must be a 32-bit integer"), true
			}
			if err := ht.upsertCounterValueLocked(key, value); err != nil {
				ht.recordJournalScalarBatchWritesLocked(records[:applied])
				return index, commandError(err.Error()), true
			}
		}
		applied++
	}
	ht.recordJournalScalarBatchWritesLocked(records)
	return len(records), CacheCommandResponse{OK: true}, true
}

func (ht *HatTrie) executeCompactJournalSetBatch(records []compactCommandJournalRecord) (int, CacheCommandResponse) {
	if len(records) < minNativeCommandBatchSize || ht.localPartitionSet() != nil {
		for index, record := range records {
			response := ht.ExecuteCommand(record.request())
			if !response.OK {
				return index, response
			}
		}
		return len(records), CacheCommandResponse{OK: true}
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.journalScalarBatchCalls++
	applied := 0
	for index, record := range records {
		key := strings.TrimSpace(record.Key)
		if key == "" {
			ht.recordCompactJournalBatchWritesLocked(records[:applied])
			return index, commandError("key is required")
		}
		if err := validateKey(key); err != nil {
			ht.recordCompactJournalBatchWritesLocked(records[:applied])
			return index, commandError(err.Error())
		}
		switch record.Command {
		case compactCommandJournalSetString, compactCommandJournalSetStringAlias:
			if err := ht.upsertStringValueLocked(key, record.Value); err != nil {
				ht.recordCompactJournalBatchWritesLocked(records[:applied])
				return index, commandError(err.Error())
			}
		case compactCommandJournalSetCounter:
			value, ok := parseCommandInt32(record.Value)
			if !ok {
				ht.recordCompactJournalBatchWritesLocked(records[:applied])
				return index, commandError("value must be a 32-bit integer")
			}
			if err := ht.upsertCounterValueLocked(key, value); err != nil {
				ht.recordCompactJournalBatchWritesLocked(records[:applied])
				return index, commandError(err.Error())
			}
		default:
			ht.recordCompactJournalBatchWritesLocked(records[:applied])
			return index, commandError("unsupported compact journal command")
		}
		applied++
	}
	ht.recordCompactJournalBatchWritesLocked(records)
	return len(records), CacheCommandResponse{OK: true}
}

func (ht *HatTrie) nativeCommandBatchStateSupportedLocked(payloads []CacheCommandRequest, family nativeCommandBatchFamily) bool {
	if family == nativeCommandBatchIncrement && (len(ht.expires) != 0 || len(ht.dbrefs.array) != 0) {
		return false
	}
	if family == nativeCommandBatchRead || family == nativeCommandBatchSetString || family == nativeCommandBatchSetCounter {
		for _, payload := range payloads {
			if _, ok := ht.expires[strings.TrimSpace(payload.Key)]; ok {
				return false
			}
		}
	}
	return true
}

func (ht *HatTrie) runNativeCommandBatchLocked(items []nativeCommandBatchItem, family nativeCommandBatchFamily) []C.hc_batch_result_t {
	if len(items) == 0 {
		return nil
	}
	keyBytes := 0
	for _, item := range items {
		keyBytes += len(item.key)
	}
	keys := ht.nativeCommandBatchScratch.keys[:0]
	if cap(keys) < keyBytes {
		keys = make([]byte, 0, keyBytes)
	}
	operations := ht.nativeCommandBatchScratch.operations
	if cap(operations) < len(items) {
		operations = make([]C.hc_batch_operation_t, len(items))
	} else {
		operations = operations[:len(items)]
	}
	results := ht.nativeCommandBatchScratch.results
	if cap(results) < len(items) {
		results = make([]C.hc_batch_result_t, len(items))
	} else {
		results = results[:len(items)]
	}
	operationCode := C.uint8_t(C.HC_BATCH_LOOKUP)
	switch family {
	case nativeCommandBatchSetString, nativeCommandBatchSetCounter:
		operationCode = C.uint8_t(C.HC_BATCH_SET)
	case nativeCommandBatchIncrement:
		operationCode = C.uint8_t(C.HC_BATCH_INCREMENT)
	case nativeCommandBatchDelete:
		operationCode = C.uint8_t(C.HC_BATCH_DELETE)
	}
	for index, item := range items {
		offset := len(keys)
		keys = append(keys, item.key...)
		operations[index].key_offset = C.uint32_t(offset)
		operations[index].key_length = C.uint32_t(len(item.key))
		operations[index].operation = operationCode
		operations[index].input = item.input
	}
	C.hc_hattrie_command_batch(
		ht.root,
		(*C.char)(unsafe.Pointer(unsafe.SliceData(keys))),
		(*C.hc_batch_operation_t)(unsafe.Pointer(unsafe.SliceData(operations))),
		(*C.hc_batch_result_t)(unsafe.Pointer(unsafe.SliceData(results))),
		C.size_t(len(items)),
	)
	runtime.KeepAlive(keys)
	runtime.KeepAlive(operations)
	ht.nativeCommandBatchScratch.keys = keys[:0]
	ht.nativeCommandBatchScratch.operations = operations[:0]
	ht.nativeCommandBatchScratch.results = results[:0]
	ht.nativeCommandBatchCalls++
	return results
}

func (ht *HatTrie) applyNativeCommandBatchResultLocked(item nativeCommandBatchItem, family nativeCommandBatchFamily, result C.hc_batch_result_t) CacheCommandResponse {
	previous := HatValue{}
	previous.fromValue(result.previous)
	current := HatValue{}
	current.fromValue(result.value)
	status := uint8(result.status)

	switch family {
	case nativeCommandBatchRead:
		if status != uint8(C.HC_BATCH_OK) || current.Empty() {
			ht.recordReadLocked(false, item.key)
			if item.command == publicScalarBatchExists {
				return CacheCommandResponse{OK: true, Message: "ok", Value: "0"}
			}
			return CacheCommandResponse{OK: true, Message: "key not found"}
		}
		if item.command == publicScalarBatchExists {
			ht.recordReadLocked(true, item.key)
			return CacheCommandResponse{OK: true, Message: "ok", Value: "1"}
		}
		if current.IsLevelDBReference() {
			var err error
			current, err = ht.hydrateLevelDBReferenceLocked(item.key, current)
			if err != nil {
				ht.recordReadLocked(false, item.key)
				return commandError(err.Error())
			}
			if current.Empty() {
				ht.recordReadLocked(false, item.key)
				return CacheCommandResponse{OK: true, Message: "key not found"}
			}
		}
		value, err := ht.commandValueLocked(current)
		if err != nil {
			ht.recordReadLocked(false, item.key)
			return commandError(err.Error())
		}
		ht.recordReadLocked(true, item.key)
		return CacheCommandResponse{OK: true, Message: "ok", Value: value}
	case nativeCommandBatchSetString:
		ht.returnStorage(previous)
		ht.clearExpirationLocked(item.key)
		ht.recordWriteLocked(item.key)
		return CacheCommandResponse{OK: true, Message: "stored string"}
	case nativeCommandBatchSetCounter:
		if !previous.IsCounter() {
			ht.returnStorage(previous)
		}
		ht.clearExpirationLocked(item.key)
		ht.recordWriteLocked(item.key)
		return CacheCommandResponse{OK: true, Message: "stored counter"}
	case nativeCommandBatchIncrement:
		if status == uint8(C.HC_BATCH_OVERFLOW) {
			return commandError("counter overflow")
		}
		if !previous.IsCounter() {
			ht.returnStorage(previous)
			ht.clearExpirationLocked(item.key)
		}
		ht.recordWriteLocked(item.key)
		return CacheCommandResponse{OK: true, Message: "incremented", Value: strconv.FormatInt(int64(current.Index), 10)}
	case nativeCommandBatchDelete:
		if status != uint8(C.HC_BATCH_OK) {
			return CacheCommandResponse{OK: true, Message: "key not found"}
		}
		ht.clearHotKeyLocked(item.key)
		ht.clearExpirationLocked(item.key)
		ht.deleteLevelDBSpillCandidateLocked(item.key)
		ht.removeKeyStatsLocked(item.key)
		ht.returnStorage(previous)
		ht.recordDeleteLocked(item.key)
		return CacheCommandResponse{OK: true, Message: "deleted"}
	default:
		return commandError("unsupported command")
	}
}
