package hatCache

import (
	"context"
	"testing"
	"time"

	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestNativeBatchTelemetryAggregatesDefaultMode(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("present", "value")

	batch := make([]CacheCommandRequest, minNativeCommandBatchSize)
	for index := range batch {
		key := "present"
		if index%2 != 0 {
			key = "missing"
		}
		batch[index] = CacheCommandRequest{Command: "GET", Key: key}
	}

	before := trie.Stats()
	now := time.Unix(1700000000, 123)
	clockCalls := 0
	trie.now = func() time.Time {
		clockCalls++
		return now
	}

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "BATCH", Batch: batch})
	if !response.OK || len(response.Responses) != len(batch) {
		t.Fatalf("ExecuteCommand(BATCH) = %#v, want %d successful responses", response, len(batch))
	}
	if clockCalls != 1 {
		t.Fatalf("batch clock calls = %d, want 1", clockCalls)
	}
	after := trie.Stats()
	wantHits := uint64(len(batch) / 2)
	wantMisses := uint64(len(batch) / 2)
	if after.Hits-before.Hits != wantHits || after.Misses-before.Misses != wantMisses || after.Reads-before.Reads != uint64(len(batch)) {
		t.Fatalf("batch stats delta = reads %d hits %d misses %d, want %d/%d/%d", after.Reads-before.Reads, after.Hits-before.Hits, after.Misses-before.Misses, len(batch), wantHits, wantMisses)
	}
	if !after.LastHit.Equal(now) || !after.LastMiss.Equal(now) {
		t.Fatalf("batch timestamps = hit %s miss %s, want %s", after.LastHit, after.LastMiss, now)
	}
}

func TestScalarBatchTelemetryAggregatesDefaultMode(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("present", "value")
	trie.UpsertString("delete", "value")

	request := &hatriecachev1.ScalarBatchRequest{
		BatchId: 1,
		Operations: []hatriecachev1.ScalarCommand{
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_GET,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_EXISTS,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_STRING,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_SET_COUNTER,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_INCREMENT,
			hatriecachev1.ScalarCommand_SCALAR_COMMAND_DELETE,
		},
		Keys:          []string{"present", "missing", "present", "string", "counter", "counter", "delete"},
		StringValues:  [][]byte{[]byte("stored")},
		IntegerValues: []int64{7, 2},
	}

	before := trie.Stats()
	now := time.Unix(2000000100, 456)
	clockCalls := 0
	trie.now = func() time.Time {
		clockCalls++
		return now
	}

	response := trie.executeScalarBatchDirect(context.Background(), request)
	if !response.GetOk() || len(response.GetStatuses()) != len(request.GetOperations()) {
		t.Fatalf("executeScalarBatchDirect() = %#v, want success", response)
	}
	if clockCalls != 1 {
		t.Fatalf("scalar batch clock calls = %d, want 1", clockCalls)
	}
	after := trie.Stats()
	if after.Reads-before.Reads != 3 || after.Hits-before.Hits != 2 || after.Misses-before.Misses != 1 {
		t.Fatalf("scalar read stats delta = reads %d hits %d misses %d, want 3/2/1", after.Reads-before.Reads, after.Hits-before.Hits, after.Misses-before.Misses)
	}
	if after.Writes-before.Writes != 4 || after.Deletes-before.Deletes != 1 {
		t.Fatalf("scalar write stats delta = writes %d deletes %d, want 4/1", after.Writes-before.Writes, after.Deletes-before.Deletes)
	}
	if !after.LastHit.Equal(now) || !after.LastMiss.Equal(now) || !after.LastWrite.Equal(now) {
		t.Fatalf("scalar timestamps = hit %s miss %s write %s, want %s", after.LastHit, after.LastMiss, after.LastWrite, now)
	}
}

func TestNativeBatchTelemetryKeepsDetailedKeyStats(t *testing.T) {
	trie := newTestTrie(t)
	if err := trie.ConfigureKeyStats(KeyStatsModeFull, 0); err != nil {
		t.Fatal(err)
	}
	trie.UpsertString("present", "value")

	batch := make([]CacheCommandRequest, minNativeCommandBatchSize)
	for index := range batch {
		batch[index] = CacheCommandRequest{Command: "GET", Key: "present"}
	}
	clockCalls := 0
	trie.now = func() time.Time {
		clockCalls++
		return time.Unix(1700000200+int64(clockCalls), 0)
	}

	response := trie.ExecuteCommand(CacheCommandRequest{Command: "BATCH", Batch: batch})
	if !response.OK {
		t.Fatalf("ExecuteCommand(BATCH) = %#v, want success", response)
	}
	if clockCalls != len(batch) {
		t.Fatalf("detailed batch clock calls = %d, want %d", clockCalls, len(batch))
	}
	stats, ok := trie.StatsForKey("present")
	if !ok || stats.Reads != uint64(len(batch)) || stats.Hits != uint64(len(batch)) || stats.Misses != 0 {
		t.Fatalf("StatsForKey(present) = %#v/%v, want %d hits", stats, ok, len(batch))
	}
}
