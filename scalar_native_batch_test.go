package hatriecache

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestScalarBatchDirectUsesOneNativeCallAndPreservesMixedOrder(t *testing.T) {
	trie := newTestTrie(t)
	operations := []hatriecachev1.ScalarCommand{
		hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING,
		hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
		hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
		hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT,
		hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS,
		hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE,
		hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
	}
	keys := []string{"same", "same", "same", "same", "same", "same", "same"}
	for len(operations) < minNativeCommandBatchSize {
		operations = append(operations, hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET)
		keys = append(keys, "same")
	}
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:       1,
		Operations:    operations,
		Keys:          keys,
		StringValues:  [][]byte{[]byte("first")},
		IntegerValues: []int64{7, 2},
	}

	beforeCalls := trie.nativeCommandBatchCalls
	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetStatuses()) != len(operations) {
		t.Fatalf("executeScalarBatchDirect() = %#v, want %d results", response, len(operations))
	}
	if got := trie.nativeCommandBatchCalls - beforeCalls; got != 1 {
		t.Fatalf("native scalar batch calls = %d, want 1", got)
	}
	for index := 0; index < 6; index++ {
		if response.Statuses[index] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
			t.Fatalf("status[%d] = %s, want OK", index, response.Statuses[index])
		}
	}
	for index := 6; index < len(response.Statuses); index++ {
		if response.Statuses[index] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND {
			t.Fatalf("status[%d] = %s, want NOT_FOUND", index, response.Statuses[index])
		}
	}
	if len(response.ValueEnds) != 1 || response.ValueEnds[0] != uint32(len("first")) || string(response.Values) != "first" {
		t.Fatalf("GET string columns = values %q ends %v, want first/[5]", response.Values, response.ValueEnds)
	}
	if len(response.IntegerValues) != 2 || response.IntegerValues[0] != 9 || response.IntegerValues[1] != 1 {
		t.Fatalf("integer columns = %v, want [9 1]", response.IntegerValues)
	}
	if trie.Exists("same") {
		t.Fatal("same key exists after ordered DELETE, want missing")
	}
}

func TestScalarBatchDirectDeleteHitThenMisses(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("delete", "value")
	operations := make([]hatriecachev1.ScalarCommand, minNativeCommandBatchSize)
	keys := make([]string, minNativeCommandBatchSize)
	for index := range operations {
		operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE
		keys[index] = "delete"
	}
	request := &hatriecachev1.ScalarBatchRequest{BatchId: 41, Operations: operations, Keys: keys}

	beforeCalls := trie.nativeCommandBatchCalls
	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetStatuses()) != len(operations) {
		t.Fatalf("executeScalarBatchDirect(delete) = %#v, want %d results", response, len(operations))
	}
	if got := trie.nativeCommandBatchCalls - beforeCalls; got != 1 {
		t.Fatalf("native scalar batch calls = %d, want 1", got)
	}
	if response.Statuses[0] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
		t.Fatalf("status[0] = %s, want OK", response.Statuses[0])
	}
	for index := 1; index < len(response.Statuses); index++ {
		if response.Statuses[index] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_NOT_FOUND {
			t.Fatalf("status[%d] = %s, want NOT_FOUND", index, response.Statuses[index])
		}
	}
	if trie.Exists("delete") {
		t.Fatal("delete key exists after native batch")
	}
}

func TestScalarBatchDirectFallsBackForTTLState(t *testing.T) {
	trie := newTestTrie(t)
	now := time.Unix(1700000000, 0)
	trie.now = func() time.Time { return now }
	trie.UpsertString("ttl", "value")
	if ok := trie.ExpireAt("ttl", now.Add(time.Hour)); !ok {
		t.Fatal("ExpireAt(ttl) = false")
	}
	operations := make([]hatriecachev1.ScalarCommand, minNativeCommandBatchSize)
	keys := make([]string, minNativeCommandBatchSize)
	for index := range operations {
		operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		keys[index] = "ttl"
	}
	request := &hatriecachev1.ScalarBatchRequest{BatchId: 2, Operations: operations, Keys: keys}

	beforeCalls := trie.nativeCommandBatchCalls
	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetValueEnds()) != len(operations) {
		t.Fatalf("executeScalarBatchDirect(TTL) = %#v, want all values", response)
	}
	if got := trie.nativeCommandBatchCalls - beforeCalls; got != 0 {
		t.Fatalf("TTL native scalar batch calls = %d, want fallback", got)
	}
	if ttl := trie.TTL("ttl"); ttl != time.Hour {
		t.Fatalf("TTL(ttl) = %s, want 1h", ttl)
	}
}

func TestScalarBatchDirectKeepsRepeatedPresentReadOnCachedPath(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("same", "value")
	operations := make([]hatriecachev1.ScalarCommand, minNativeCommandBatchSize)
	keys := make([]string, minNativeCommandBatchSize)
	for index := range operations {
		operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		keys[index] = "same"
	}
	request := &hatriecachev1.ScalarBatchRequest{BatchId: 3, Operations: operations, Keys: keys}

	beforeStats := trie.Stats()
	beforeCalls := trie.nativeCommandBatchCalls
	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetValueEnds()) != len(operations) {
		t.Fatalf("executeScalarBatchDirect(repeated read) = %#v, want all values", response)
	}
	if got := trie.nativeCommandBatchCalls - beforeCalls; got != 0 {
		t.Fatalf("repeated present read native calls = %d, want cached Go path", got)
	}
	if string(response.GetValues()) != strings.Repeat("value", len(operations)) {
		t.Fatalf("repeated read values = %q, want repeated value", response.GetValues())
	}
	afterStats := trie.Stats()
	if afterStats.Reads-beforeStats.Reads != uint64(len(operations)) || afterStats.Hits-beforeStats.Hits != uint64(len(operations)) || afterStats.Misses-beforeStats.Misses != 0 {
		t.Fatalf("repeated read stats delta = reads %d hits %d misses %d", afterStats.Reads-beforeStats.Reads, afterStats.Hits-beforeStats.Hits, afterStats.Misses-beforeStats.Misses)
	}
}

func TestScalarBatchDirectKeepsSharedKeyRepeatedReadOnCachedPath(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("same", "value")
	operations := make([]hatriecachev1.ScalarCommand, minNativeCommandBatchSize)
	for index := range operations {
		operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
	}
	request := &hatriecachev1.ScalarBatchRequest{BatchId: 33, Operations: operations, Keys: []string{"same"}}

	beforeStats := trie.Stats()
	beforeCalls := trie.nativeCommandBatchCalls
	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetValueEnds()) != len(operations) {
		t.Fatalf("executeScalarBatchDirect(shared repeated read) = %#v, want all values", response)
	}
	if got := trie.nativeCommandBatchCalls - beforeCalls; got != 0 {
		t.Fatalf("shared repeated read native calls = %d, want cached Go path", got)
	}
	if string(response.GetValues()) != strings.Repeat("value", len(operations)) {
		t.Fatalf("shared repeated read values = %q, want repeated value", response.GetValues())
	}
	afterStats := trie.Stats()
	if afterStats.Reads-beforeStats.Reads != uint64(len(operations)) || afterStats.Hits-beforeStats.Hits != uint64(len(operations)) || afterStats.Misses-beforeStats.Misses != 0 {
		t.Fatalf("shared repeated read stats delta = reads %d hits %d misses %d", afterStats.Reads-beforeStats.Reads, afterStats.Hits-beforeStats.Hits, afterStats.Misses-beforeStats.Misses)
	}
}

func TestScalarBatchDirectCoalescesRepeatedMiss(t *testing.T) {
	trie := newTestTrie(t)
	operations := make([]hatriecachev1.ScalarCommand, minNativeCommandBatchSize)
	keys := make([]string, minNativeCommandBatchSize)
	for index := range operations {
		operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		keys[index] = "missing"
	}
	request := &hatriecachev1.ScalarBatchRequest{BatchId: 4, Operations: operations, Keys: keys}

	beforeStats := trie.Stats()
	beforeCalls := trie.nativeCommandBatchCalls
	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetStatuses()) != len(operations) {
		t.Fatalf("executeScalarBatchDirect(repeated miss) = %#v, want statuses", response)
	}
	if got := trie.nativeCommandBatchCalls - beforeCalls; got != 0 {
		t.Fatalf("repeated missing read native calls = %d, want coalesced path", got)
	}
	afterStats := trie.Stats()
	if afterStats.Reads-beforeStats.Reads != uint64(len(operations)) || afterStats.Hits-beforeStats.Hits != 0 || afterStats.Misses-beforeStats.Misses != uint64(len(operations)) {
		t.Fatalf("repeated miss stats delta = reads %d hits %d misses %d", afterStats.Reads-beforeStats.Reads, afterStats.Hits-beforeStats.Hits, afterStats.Misses-beforeStats.Misses)
	}
}

func TestScalarBatchDirectNativeChunksPreserveOrder(t *testing.T) {
	trie := newTestTrie(t)
	const commands = nativeScalarDirectBatchChunkSize + 1
	operations := make([]hatriecachev1.ScalarCommand, commands)
	keys := make([]string, commands)
	for index := 0; index < nativeScalarDirectBatchChunkSize-1; index++ {
		operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		keys[index] = fmt.Sprintf("missing:%d", index)
	}
	operations[nativeScalarDirectBatchChunkSize-1] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING
	keys[nativeScalarDirectBatchChunkSize-1] = "boundary"
	operations[nativeScalarDirectBatchChunkSize] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
	keys[nativeScalarDirectBatchChunkSize] = "boundary"
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:      5,
		Operations:   operations,
		Keys:         keys,
		StringValues: [][]byte{[]byte("crossed")},
	}

	beforeCalls := trie.nativeCommandBatchCalls
	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || response.Statuses[nativeScalarDirectBatchChunkSize-1] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK || response.Statuses[nativeScalarDirectBatchChunkSize] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
		t.Fatalf("executeScalarBatchDirect(chunk boundary) = %#v", response)
	}
	if got := trie.nativeCommandBatchCalls - beforeCalls; got != 2 {
		t.Fatalf("native scalar chunk calls = %d, want 2", got)
	}
	if string(response.GetValues()) != "crossed" || len(response.GetValueEnds()) != 1 {
		t.Fatalf("boundary GET values = %q/%v, want crossed", response.GetValues(), response.GetValueEnds())
	}
	scratch := &trie.nativeCommandBatchScratch
	if cap(scratch.items) != 0 {
		t.Fatalf("native direct item scratch capacity = %d, want 0", cap(scratch.items))
	}
	if cap(scratch.operations) > nativeScalarDirectBatchChunkSize || cap(scratch.results) > nativeScalarDirectBatchChunkSize {
		t.Fatalf("native scratch capacities = operations %d results %d, want <= %d", cap(scratch.operations), cap(scratch.results), nativeScalarDirectBatchChunkSize)
	}
}

func TestScalarBatchDirectNativePreallocatesExistsOutputColumn(t *testing.T) {
	trie := newTestTrie(t)
	const commands = 255
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    7,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	for index := range request.Operations {
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS
		request.Keys[index] = fmt.Sprintf("exists:%d", index)
	}
	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetStatuses()) != commands || len(response.IntegerValues) != commands {
		t.Fatalf("executeScalarBatchDirect(exists) = %#v", response)
	}
	if cap(response.IntegerValues) != len(response.IntegerValues) {
		t.Fatalf("integer values capacity = %d, want exact %d", cap(response.IntegerValues), len(response.IntegerValues))
	}
}

func TestScalarBatchDirectNativePreallocatesIncrementOutputColumn(t *testing.T) {
	trie := newTestTrie(t)
	const commands = nativeScalarDirectBatchChunkSize
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:       11,
		Operations:    make([]hatriecachev1.ScalarCommand, commands),
		Keys:          make([]string, commands),
		IntegerValues: make([]int64, commands),
	}
	for index := range request.Operations {
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT
		request.Keys[index] = fmt.Sprintf("increment:%d", index)
		request.IntegerValues[index] = 1
	}

	beforeCalls := trie.nativeCommandBatchCalls
	allocations := testing.AllocsPerRun(100, func() {
		response := trie.executeScalarBatchDirect(context.Background(), request)
		if !response.GetOk() || len(response.GetStatuses()) != commands || len(response.GetIntegerValues()) != commands {
			t.Fatalf("executeScalarBatchDirect(increment) = %#v, want %d results", response, commands)
		}
		for index, value := range response.GetIntegerValues() {
			if value < 1 || response.GetStatuses()[index] != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_OK {
				t.Fatalf("increment result[%d] = %d/%s, want positive/OK", index, value, response.GetStatuses()[index])
			}
		}
		if cap(response.GetIntegerValues()) != len(response.GetIntegerValues()) {
			t.Fatalf("increment integer values capacity = %d, want exact %d", cap(response.GetIntegerValues()), len(response.GetIntegerValues()))
		}
	})
	if got := trie.nativeCommandBatchCalls - beforeCalls; got != 101 {
		t.Fatalf("native scalar increment calls = %d, want 101", got)
	}
	if allocations != 4 {
		t.Fatalf("increment scalar batch allocations = %.0f, want 4 with exact integer capacity", allocations)
	}
}

func TestScalarBatchDirectNativeIncrementOverflowLeavesIntegerColumnNil(t *testing.T) {
	trie := newTestTrie(t)
	const commands = nativeScalarDirectBatchChunkSize
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:       12,
		Operations:    make([]hatriecachev1.ScalarCommand, commands),
		Keys:          make([]string, commands),
		IntegerValues: make([]int64, commands),
	}
	for index := range request.Operations {
		key := fmt.Sprintf("increment-overflow:%d", index)
		trie.UpsertCounter(key, maxCommandInt32)
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT
		request.Keys[index] = key
		request.IntegerValues[index] = 1
	}

	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetStatuses()) != commands {
		t.Fatalf("executeScalarBatchDirect(increment overflow) = %#v, want %d results", response, commands)
	}
	for index, status := range response.GetStatuses() {
		if status != hatriecachev1.ScalarResultStatus_SCALAR_RESULT_STATUS_COUNTER_OVERFLOW {
			t.Fatalf("increment overflow status[%d] = %s, want COUNTER_OVERFLOW", index, status)
		}
	}
	if response.IntegerValues != nil {
		t.Fatalf("increment overflow integer values = %v, want nil", response.IntegerValues)
	}
}

func TestScalarBatchDirectNativePreallocatesRawStringReadColumns(t *testing.T) {
	trie := newTestTrie(t)
	const commands = nativeScalarDirectBatchChunkSize
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    8,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	var wantValues strings.Builder
	for index := range request.Operations {
		key := fmt.Sprintf("read:%d", index)
		value := fmt.Sprintf("value:%d", index)
		trie.UpsertString(key, value)
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		request.Keys[index] = key
		wantValues.WriteString(value)
	}

	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetStatuses()) != commands || len(response.GetValueEnds()) != commands || string(response.GetValues()) != wantValues.String() {
		t.Fatalf("executeScalarBatchDirect(raw reads) = %#v, want %d byte results", response, commands)
	}
	if cap(response.GetValues()) != len(response.GetValues()) {
		t.Fatalf("raw read values capacity = %d, want exact %d", cap(response.GetValues()), len(response.GetValues()))
	}
	if cap(response.GetValueEnds()) != len(response.GetValueEnds()) {
		t.Fatalf("raw read value ends capacity = %d, want exact %d", cap(response.GetValueEnds()), len(response.GetValueEnds()))
	}
}

func TestScalarBatchDirectNativeRawByteReadsAvoidTemporaryStrings(t *testing.T) {
	trie := newTestTrie(t)
	const commands = nativeScalarDirectBatchChunkSize
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    9,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	var wantValues bytes.Buffer
	for index := range request.Operations {
		key := fmt.Sprintf("bytes:%d", index)
		value := []byte{byte(index), 0, 0xff, byte(index >> 1)}
		trie.UpsertBytes(key, value)
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		request.Keys[index] = key
		wantValues.Write(value)
	}

	allocations := testing.AllocsPerRun(100, func() {
		response := trie.executeScalarBatchDirect(context.Background(), request)
		if !response.GetOk() || len(response.GetStatuses()) != commands || len(response.GetValueEnds()) != commands || !bytes.Equal(response.GetValues(), wantValues.Bytes()) {
			t.Fatalf("executeScalarBatchDirect(raw bytes) = %#v, want %d byte results", response, commands)
		}
		if cap(response.GetValues()) != len(response.GetValues()) {
			t.Fatalf("raw byte values capacity = %d, want exact %d", cap(response.GetValues()), len(response.GetValues()))
		}
		if cap(response.GetValueEnds()) != len(response.GetValueEnds()) {
			t.Fatalf("raw byte value ends capacity = %d, want exact %d", cap(response.GetValueEnds()), len(response.GetValueEnds()))
		}
	})
	if allocations != 5 {
		t.Fatalf("raw byte scalar batch allocations = %.0f, want 5 with exact response capacity", allocations)
	}
}

func TestScalarBatchDirectFallbackRawByteReadsAvoidTemporaryStrings(t *testing.T) {
	trie := newTestTrie(t)
	trie.now = func() time.Time { return time.Unix(1700000000, 0) }
	const commands = nativeScalarDirectBatchChunkSize
	request := &hatriecachev1.ScalarBatchRequest{
		BatchId:    10,
		Operations: make([]hatriecachev1.ScalarCommand, commands),
		Keys:       make([]string, commands),
	}
	var wantValues bytes.Buffer
	for index := range request.Operations {
		key := fmt.Sprintf("ttl-bytes:%d", index)
		value := []byte{byte(index), 0, 0xff, byte(index >> 1)}
		trie.UpsertBytes(key, value)
		if !trie.ExpireAt(key, trie.now().Add(time.Hour)) {
			t.Fatalf("ExpireAt(%q) = false", key)
		}
		request.Operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		request.Keys[index] = key
		wantValues.Write(value)
	}

	beforeCalls := trie.nativeCommandBatchCalls
	allocations := testing.AllocsPerRun(100, func() {
		response := trie.executeScalarBatchDirect(context.Background(), request)
		if !response.GetOk() || len(response.GetStatuses()) != commands || len(response.GetValueEnds()) != commands || !bytes.Equal(response.GetValues(), wantValues.Bytes()) {
			t.Fatalf("executeScalarBatchDirect(fallback raw bytes) = %#v, want %d byte results", response, commands)
		}
	})
	if trie.nativeCommandBatchCalls != beforeCalls {
		t.Fatal("TTL-backed raw byte reads used the native path")
	}
	if allocations != 15 {
		t.Fatalf("fallback raw byte scalar batch allocations = %.0f, want 15 without temporary strings", allocations)
	}
}

func TestScalarBatchDirectDoesNotRetainOversizedNativeKeys(t *testing.T) {
	trie := newTestTrie(t)
	operations := make([]hatriecachev1.ScalarCommand, minNativeScalarDirectBatchSize)
	keys := make([]string, minNativeScalarDirectBatchSize)
	keyPrefix := strings.Repeat("k", maxHATTrieKeyLength-1)
	for index := range operations {
		operations[index] = hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET
		keys[index] = keyPrefix + string(rune('a'+index))
	}
	request := &hatriecachev1.ScalarBatchRequest{BatchId: 6, Operations: operations, Keys: keys}

	beforeCalls := trie.nativeCommandBatchCalls
	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetStatuses()) != len(operations) {
		t.Fatalf("executeScalarBatchDirect(large keys) = %#v", response)
	}
	if got := trie.nativeCommandBatchCalls - beforeCalls; got != 1 {
		t.Fatalf("large-key native scalar calls = %d, want 1", got)
	}
	if capacity := cap(trie.nativeCommandBatchScratch.keys); capacity != 0 {
		t.Fatalf("retained native key scratch = %d bytes, want 0 after oversized batch", capacity)
	}
}
