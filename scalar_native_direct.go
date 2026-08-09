package hatriecache

/*
#cgo CFLAGS: -std=c99 -Wall -Wextra -I${SRCDIR}/luikore__hat-trie/src
#include "native_command_batch.h"
*/
import "C"

import (
	"runtime"
	"unsafe"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func (ht *HatTrie) runNativeScalarRequestChunkLocked(request *hatriecachev1.ScalarBatchRequest, start int, end int, maxKeyBytes int, stringIndex int, integerIndex int) ([]C.hc_batch_result_t, int, int) {
	count := end - start
	keys := ht.nativeCommandBatchScratch.keys[:0]
	if cap(keys) < maxKeyBytes {
		keys = make([]byte, 0, maxKeyBytes)
	}
	operations := ht.nativeCommandBatchScratch.operations
	if cap(operations) < count {
		operations = make([]C.hc_batch_operation_t, count)
	} else {
		operations = operations[:count]
	}
	results := ht.nativeCommandBatchScratch.results
	if cap(results) < count {
		results = make([]C.hc_batch_result_t, count)
	} else {
		results = results[:count]
	}

	for responseIndex := start; responseIndex < end; responseIndex++ {
		index := responseIndex - start
		key := request.Keys[responseIndex]
		offset := len(keys)
		keys = append(keys, key...)
		operation := request.Operations[responseIndex]
		operations[index].key_offset = C.uint32_t(offset)
		operations[index].key_length = C.uint32_t(len(key))
		operations[index].operation = nativeScalarBatchOperationCode(operation)
		operations[index].input = 0
		switch operation {
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING:
			valueIndex := ht.strings.Add(string(request.StringValues[stringIndex]))
			stringIndex++
			operations[index].input = HatValue{Index: valueIndex, Flags: DATAVALUE_TYPE_RAW_STRING}.toValue()
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER:
			value := int32(request.IntegerValues[integerIndex])
			integerIndex++
			operations[index].input = HatValue{Index: value, Flags: DATAVALUE_TYPE_COUNTER}.toValue()
		case hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
			value := int32(request.IntegerValues[integerIndex])
			integerIndex++
			operations[index].input = nativeCommandBatchIncrementInput(value)
		}
	}

	C.hc_hattrie_command_batch(
		ht.root,
		(*C.char)(unsafe.Pointer(unsafe.SliceData(keys))),
		C.size_t(len(keys)),
		(*C.hc_batch_operation_t)(unsafe.Pointer(unsafe.SliceData(operations))),
		(*C.hc_batch_result_t)(unsafe.Pointer(unsafe.SliceData(results))),
		C.size_t(count),
	)
	runtime.KeepAlive(keys)
	runtime.KeepAlive(operations)
	ht.nativeCommandBatchScratch.keys = keys[:0]
	ht.nativeCommandBatchScratch.operations = operations[:0]
	ht.nativeCommandBatchScratch.results = results[:0]
	ht.nativeCommandBatchCalls++
	return results, stringIndex, integerIndex
}

func (ht *HatTrie) reserveNativeScalarRawStringReadResponseLocked(request *hatriecachev1.ScalarBatchRequest, response *hatriecachev1.ScalarBatchResponse, results []C.hc_batch_result_t) {
	operations := request.GetOperations()
	if len(operations) == 0 || len(operations) > nativeScalarDirectBatchChunkSize || len(results) != len(operations) || len(response.Values) != 0 || len(response.ValueEnds) != 0 {
		return
	}
	if operations[0] != hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET || uint8(results[0].status) != uint8(C.HC_BATCH_OK) {
		return
	}
	first := HatValue{}
	first.fromValue(results[0].value)
	if !first.IsStringAtRaws() {
		return
	}

	valueBytes := 0
	valueCount := 0
	max := int(^uint(0) >> 1)
	for index, result := range results {
		if operations[index] != hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET {
			return
		}
		if uint8(result.status) != uint8(C.HC_BATCH_OK) {
			continue
		}
		current := HatValue{}
		current.fromValue(result.value)
		if !current.IsStringAtRaws() {
			continue
		}
		value := ht.strings.Get(current.Index)
		if len(value) > max-valueBytes {
			return
		}
		valueBytes += len(value)
		valueCount++
	}
	if valueCount == 0 {
		return
	}
	if valueBytes != 0 {
		response.Values = make([]byte, 0, valueBytes)
	}
	response.ValueEnds = make([]uint32, 0, valueCount)
}

func nativeScalarBatchOperationCode(operation hatriecachev1.ScalarCommand) C.uint8_t {
	switch operation {
	case hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING,
		hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER:
		return C.uint8_t(C.HC_BATCH_SET)
	case hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT:
		return C.uint8_t(C.HC_BATCH_INCREMENT)
	case hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE:
		return C.uint8_t(C.HC_BATCH_DELETE)
	default:
		return C.uint8_t(C.HC_BATCH_LOOKUP)
	}
}
