package hatriecache

import (
	"encoding/json"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestExecuteCommandStringCounterTTLAndDelete(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(1200, 0)
	ht.now = func() time.Time { return now }

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("SETSTR response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "name"}); !got.OK || got.Value != "ivi" {
		t.Fatalf("GET response = %#v, want ivi", got)
	}

	ttlSeconds := int64(30)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETINT", Key: "views", Value: "42", TTLSeconds: &ttlSeconds}); !got.OK {
		t.Fatalf("SETINT response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "views"}); !got.OK || got.Value != "42" {
		t.Fatalf("GETSTR counter response = %#v, want 42", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "TTL", Key: "views"}); !got.OK || got.Value != "30" {
		t.Fatalf("TTL response = %#v, want 30", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "DEL", Key: "name"}); !got.OK || got.Message != "deleted" {
		t.Fatalf("DEL response = %#v, want deleted", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "name"}); !got.OK || got.Value != "0" {
		t.Fatalf("EXISTS response = %#v, want 0", got)
	}
}

func TestExecuteCommandRejectsTooLongKey(t *testing.T) {
	for _, command := range []string{"SETSTR", "SETINT", "CREATERB", "CREATESB", "CREATERT"} {
		t.Run(command, func(t *testing.T) {
			ht := newTestTrie(t)
			got := ht.ExecuteCommand(CacheCommandRequest{
				Command: command,
				Key:     strings.Repeat("k", maxHATTrieKeyLength+1),
				Value:   "1",
			})
			if got.OK {
				t.Fatalf("%s with too-long key response = %#v, want error", command, got)
			}
			if ht.Size() != 0 {
				t.Fatalf("trie size after too-long command = %d, want 0", ht.Size())
			}
		})
	}
}

func TestExecuteCommandExtendedTTLCommands(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(1300, 0)
	ht.now = func() time.Time { return now }

	ttlSeconds := int64(5)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTRX", Key: "temp", Value: "value", TTLSeconds: &ttlSeconds}); !got.OK {
		t.Fatalf("SETSTRX response = %#v, want ok", got)
	}
	now = now.Add(6 * time.Second)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "temp"}); !got.OK || got.Value != "" {
		t.Fatalf("expired GET response = %#v, want not found", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETINTX", Key: "counter", Value: "7", TTLSeconds: &ttlSeconds}); !got.OK {
		t.Fatalf("SETINTX response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXPIRE", Key: "counter", TTLSeconds: &ttlSeconds}); !got.OK {
		t.Fatalf("EXPIRE response = %#v, want ok", got)
	}

	expiresAt := now.Add(10 * time.Second).Unix()
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXPIREAT", Key: "counter", UnixSeconds: &expiresAt}); !got.OK {
		t.Fatalf("EXPIREAT response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "TTL", Key: "counter"}); !got.OK || got.Value != "10" {
		t.Fatalf("TTL after EXPIREAT response = %#v, want 10", got)
	}
	now = now.Add(11 * time.Second)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "counter"}); !got.OK || got.Value != "" {
		t.Fatalf("GET expired EXPIREAT key = %#v, want not found", got)
	}

	absoluteExpiry := now.Add(5 * time.Second).Unix()
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "absolute", Value: "value", UnixSeconds: &absoluteExpiry}); !got.OK {
		t.Fatalf("SETSTR absolute expiration response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "TTL", Key: "absolute"}); !got.OK || got.Value != "5" {
		t.Fatalf("TTL after SETSTR absolute expiration = %#v, want 5", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "bad", Value: "value", TTLSeconds: &ttlSeconds, UnixSeconds: &absoluteExpiry}); got.OK {
		t.Fatalf("SETSTR with two expirations response = %#v, want error", got)
	}

	ht.UpsertString("past", "value")
	pastExpiry := now.Unix()
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXPIREAT", Key: "past", UnixSeconds: &pastExpiry}); !got.OK {
		t.Fatalf("EXPIREAT past response = %#v, want ok", got)
	}
	if ht.Exists("past") {
		t.Fatal("EXPIREAT in the past left key present")
	}
}

func TestExecuteCommandIncrementCounter(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INC", Key: "views"}); !got.OK || got.Value != "1" {
		t.Fatalf("INC default response = %#v, want 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INC", Key: "views", Value: "41"}); !got.OK || got.Value != "42" {
		t.Fatalf("INC 41 response = %#v, want 42", got)
	}

	ht.UpsertCounter("max", maxCommandInt32)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INC", Key: "max"}); got.OK {
		t.Fatalf("INC overflow response = %#v, want not ok", got)
	}
	if got := ht.GetCounter("max"); got != maxCommandInt32 {
		t.Fatalf("counter after overflow = %d, want unchanged max", got)
	}
}

func TestExecuteCommandMapOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs:   Map{"name": "ivi", "age": 32},
	}); !got.OK {
		t.Fatalf("PUTMAP pairs response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUTMAP", Key: "profile", Subkey: "city", Value: "Singapore"}); !got.OK {
		t.Fatalf("PUTMAP subkey response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "profile", Subkey: "name"}); !got.OK || got.Value != "ivi" {
		t.Fatalf("PEEKMAP name response = %#v, want ivi", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "profile", Subkey: "age"}); !got.OK || got.Value != "32" {
		t.Fatalf("PEEKMAP age response = %#v, want 32", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "TAKEMAP", Key: "profile", Subkey: "city"}); !got.OK || got.Value != "Singapore" {
		t.Fatalf("TAKEMAP city response = %#v, want Singapore", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PEEKMAP", Key: "profile", Subkey: "city"}); !got.OK || got.Value != "" {
		t.Fatalf("PEEKMAP removed city response = %#v, want value not found", got)
	}
}

func TestExecuteCommandPutMapClonesNestedValues(t *testing.T) {
	ht := newTestTrie(t)
	nested := Map{"field": "stored"}
	payload := []byte("bytes")
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs:   Map{"nested": nested, "payload": payload},
	}); !got.OK {
		t.Fatalf("PUTMAP nested response = %#v, want ok", got)
	}

	nested["field"] = "caller"
	payload[0] = 'X'
	got := ht.GetMap("profile")
	if got["nested"].(Map)["field"] != "stored" {
		t.Fatalf("PUTMAP nested value = %#v, want stored clone", got["nested"])
	}
	if string(got["payload"].([]byte)) != "bytes" {
		t.Fatalf("PUTMAP payload = %q, want bytes", got["payload"])
	}
}

func TestExecuteCommandPutMapRejectsUnsupportedValues(t *testing.T) {
	ht := newTestTrie(t)
	unsupported := func() {}

	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs:   Map{"bad": unsupported},
	}); got.OK {
		t.Fatalf("PUTMAP unsupported response = %#v, want error", got)
	}
	if hval := ht.Get("profile"); !hval.Empty() {
		t.Fatalf("PUTMAP unsupported created value %+v", hval)
	}

	ht.UpsertMap("profile", Map{"keep": "value"})
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "PUTMAP",
		Key:     "profile",
		Pairs:   Map{"bad": unsupported},
	}); got.OK {
		t.Fatalf("PUTMAP unsupported existing response = %#v, want error", got)
	}
	if got := ht.GetMap("profile"); !reflect.DeepEqual(got, Map{"keep": "value"}) {
		t.Fatalf("PUTMAP unsupported existing map = %#v, want unchanged map", got)
	}
}

func TestExecuteCommandSliceOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUSHSLICE", Key: "queue", Value: "first"}); !got.OK {
		t.Fatalf("PUSHSLICE value response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUSHSLICE", Key: "queue", Values: Slice{"second", 3}}); !got.OK {
		t.Fatalf("PUSHSLICE values response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HEADSLICE", Key: "queue"}); !got.OK || got.Value != "first" {
		t.Fatalf("HEADSLICE response = %#v, want first", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "TAILSLICE", Key: "queue"}); !got.OK || got.Value != "3" {
		t.Fatalf("TAILSLICE response = %#v, want 3", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "POPSLICE", Key: "queue"}); !got.OK || got.Value != "3" {
		t.Fatalf("POPSLICE response = %#v, want 3", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SHIFTSLICE", Key: "queue"}); !got.OK || got.Value != "first" {
		t.Fatalf("SHIFTSLICE response = %#v, want first", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HEADSLICE", Key: "queue"}); !got.OK || got.Value != "second" {
		t.Fatalf("HEADSLICE remaining response = %#v, want second", got)
	}
}

func TestExecuteCommandPushSliceRejectsUnsupportedValues(t *testing.T) {
	ht := newTestTrie(t)
	unsupported := func() {}

	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "PUSHSLICE",
		Key:     "queue",
		Values:  Slice{unsupported},
	}); got.OK {
		t.Fatalf("PUSHSLICE unsupported response = %#v, want error", got)
	}
	if hval := ht.Get("queue"); !hval.Empty() {
		t.Fatalf("PUSHSLICE unsupported created value %+v", hval)
	}

	ht.UpsertSlice("queue", Slice{"keep"})
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "PUSHSLICE",
		Key:     "queue",
		Values:  Slice{unsupported},
	}); got.OK {
		t.Fatalf("PUSHSLICE unsupported existing response = %#v, want error", got)
	}
	if got := ht.GetSlice("queue"); !reflect.DeepEqual(got, Slice{"keep"}) {
		t.Fatalf("PUSHSLICE unsupported existing slice = %#v, want unchanged slice", got)
	}
}

func TestExecuteCommandSetOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSET", Key: "tags", Value: "go"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDSET value response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSET", Key: "tags", Values: Slice{"cache", "go", 3}}); !got.OK || got.Value != "2" {
		t.Fatalf("ADDSET values response = %#v, want added 2", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASSET", Key: "tags", Value: "go"}); !got.OK || got.Value != "1" {
		t.Fatalf("HASSET go response = %#v, want 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASSET", Key: "tags", Value: "missing"}); !got.OK || got.Value != "0" {
		t.Fatalf("HASSET missing response = %#v, want 0", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETSET", Key: "tags"}); !got.OK || got.Value != `["cache","go",3]` {
		t.Fatalf("GETSET response = %#v, want sorted JSON array", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "REMSET", Key: "tags", Values: Slice{"go", "missing"}}); !got.OK || got.Value != "1" {
		t.Fatalf("REMSET response = %#v, want removed 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "tags"}); !got.OK || got.Value != `["cache",3]` {
		t.Fatalf("GET set response = %#v, want JSON array", got)
	}
}

func TestExecuteCommandSetRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	unsupported := func() {}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSET", Key: "tags", Value: "go"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDSET seed response = %#v, want added 1", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSET", Key: "tags", Values: Slice{"cache", unsupported}}); got.OK {
		t.Fatalf("ADDSET unsupported response = %#v, want error", got)
	}
	if got := ht.GetSet("tags"); !reflect.DeepEqual(got, Set{"go"}) {
		t.Fatalf("GetSet(after rejected ADDSET) = %#v, want original set", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "REMSET", Key: "tags", Values: Slice{"go", unsupported}}); got.OK {
		t.Fatalf("REMSET unsupported response = %#v, want error", got)
	}
	if !ht.HasSet("tags", "go") {
		t.Fatal("REMSET removed a value before returning an error")
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSET", Key: "missing", Values: Slice{unsupported}}); got.OK {
		t.Fatalf("ADDSET missing unsupported response = %#v, want error", got)
	}
	if hval := ht.Get("missing"); !hval.Empty() {
		t.Fatalf("ADDSET missing unsupported created value %+v", hval)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASSET", Key: "tags", Values: Slice{unsupported}}); got.OK {
		t.Fatalf("HASSET unsupported response = %#v, want error", got)
	}
}

func TestExecuteCommandPriorityQueueOperations(t *testing.T) {
	ht := newTestTrie(t)

	lowPriority := int64(10)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUSHPQ", Key: "jobs", Value: "slow", Priority: &lowPriority}); !got.OK || got.Value != "1" {
		t.Fatalf("PUSHPQ value response = %#v, want added 1", got)
	}
	highPriority := int64(1)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUSHPQ", Key: "jobs", Values: Slice{"urgent", json.Number("2")}, Priority: &highPriority}); !got.OK || got.Value != "2" {
		t.Fatalf("PUSHPQ values response = %#v, want added 2", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUSHPQ", Key: "jobs", Value: "medium", Subkey: "5"}); !got.OK || got.Value != "1" {
		t.Fatalf("PUSHPQ subkey priority response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PEEKPQ", Key: "jobs"}); !got.OK || got.Value != `{"priority":1,"value":"urgent"}` {
		t.Fatalf("PEEKPQ response = %#v, want urgent priority item", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "POPPQ", Key: "jobs"}); !got.OK || got.Value != `{"priority":1,"value":"urgent"}` {
		t.Fatalf("POPPQ response = %#v, want urgent priority item", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETPQ", Key: "jobs"}); !got.OK || got.Value != `[{"priority":1,"value":2},{"priority":5,"value":"medium"},{"priority":10,"value":"slow"}]` {
		t.Fatalf("GETPQ response = %#v, want ordered JSON array", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "jobs"}); !got.OK || got.Value != `[{"priority":1,"value":2},{"priority":5,"value":"medium"},{"priority":10,"value":"slow"}]` {
		t.Fatalf("GET priority queue response = %#v, want ordered JSON array", got)
	}
}

func TestExecuteCommandPriorityQueueRejectsUnsupportedValues(t *testing.T) {
	ht := newTestTrie(t)
	unsupported := func() {}
	priority := int64(1)

	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command:  "PUSHPQ",
		Key:      "jobs",
		Values:   Slice{unsupported},
		Priority: &priority,
	}); got.OK {
		t.Fatalf("PUSHPQ unsupported response = %#v, want error", got)
	}
	if hval := ht.Get("jobs"); !hval.Empty() {
		t.Fatalf("PUSHPQ unsupported created value %+v", hval)
	}

	ht.UpsertPriorityQueue("jobs", PriorityQueue{{Priority: 2, Value: "keep"}})
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command:  "PUSHPQ",
		Key:      "jobs",
		Values:   Slice{unsupported},
		Priority: &priority,
	}); got.OK {
		t.Fatalf("PUSHPQ unsupported existing response = %#v, want error", got)
	}
	if got := ht.GetPriorityQueue("jobs"); !reflect.DeepEqual(got, PriorityQueue{{Priority: 2, Value: "keep"}}) {
		t.Fatalf("PUSHPQ unsupported existing queue = %#v, want unchanged queue", got)
	}
}

func TestExecuteCommandBloomFilterOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATEBF", Key: "seen", Value: "1000", Subkey: "0.001"}); !got.OK {
		t.Fatalf("CREATEBF response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDBF", Key: "seen", Value: "alpha"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDBF value response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDBF", Key: "seen", Values: Slice{"beta", "alpha"}}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDBF values response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASBF", Key: "seen", Value: "alpha"}); !got.OK || got.Value != "1" {
		t.Fatalf("HASBF alpha response = %#v, want 1", got)
	}
	missing := bloomFilterMissingValue(t, ht, "seen")
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASBF", Key: "seen", Value: missing}); !got.OK || got.Value != "0" {
		t.Fatalf("HASBF missing response = %#v, want 0", got)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOBF", Key: "seen"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFOBF response = %#v, want JSON info", infoResp)
	}
	var info BloomFilterInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFOBF JSON error = %v", err)
	}
	if info.Insertions != 2 || info.HashCount == 0 || info.BitBytes == 0 {
		t.Fatalf("INFOBF = %#v, want populated filter info", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "seen"}); !got.OK || got.Value == "" {
		t.Fatalf("GET bloom filter response = %#v, want JSON info", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDBF", Key: "auto", Value: "value"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDBF auto response = %#v, want added 1", got)
	}
	if !ht.Get("auto").IsBloomFilter() {
		t.Fatal("ADDBF on missing key did not create a Bloom filter")
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "CREATEBF",
		Key:     "paired",
		Pairs:   Map{"expected_items": json.Number("128"), "false_positive_rate": json.Number("0.05")},
	}); !got.OK {
		t.Fatalf("CREATEBF pairs response = %#v, want ok", got)
	}
	info, ok := ht.BloomFilterInfo("paired")
	if !ok || info.BitCount == 0 || info.HashCount == 0 {
		t.Fatalf("paired BloomFilterInfo = %#v/%v, want configured filter", info, ok)
	}
}

func TestExecuteCommandBloomFilterRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDBF", Key: "seen", Value: "alpha"}); !got.OK {
		t.Fatalf("ADDBF alpha response = %#v, want ok", got)
	}

	got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDBF",
		Key:     "seen",
		Values:  Slice{"beta", func() {}},
	})
	if got.OK {
		t.Fatalf("ADDBF unsupported response = %#v, want error", got)
	}
	info, ok := ht.BloomFilterInfo("seen")
	if !ok || info.Insertions != 1 {
		t.Fatalf("BloomFilterInfo(after rejected command) = %#v/%v, want one insertion", info, ok)
	}
	if !ht.HasBloomFilter("seen", "alpha") {
		t.Fatal("rejected ADDBF batch removed existing value")
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDBF",
		Key:     "missing",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("ADDBF missing unsupported response = %#v, want error", got)
	}
	if value := ht.Get("missing"); !value.Empty() {
		t.Fatalf("rejected missing-key Bloom filter command left value %+v", value)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "HASBF",
		Key:     "seen",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("HASBF unsupported response = %#v, want error", got)
	}
}

func TestExecuteCommandCuckooFilterOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATECF", Key: "seen", Value: "128", Subkey: "0.001"}); !got.OK {
		t.Fatalf("CREATECF response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDCF", Key: "seen", Value: "alpha"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDCF value response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDCF", Key: "seen", Values: Slice{"beta", "alpha"}}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDCF values response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASCF", Key: "seen", Value: "alpha"}); !got.OK || got.Value != "1" {
		t.Fatalf("HASCF alpha response = %#v, want 1", got)
	}
	missing := cuckooFilterMissingValue(t, ht, "seen")
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASCF", Key: "seen", Value: missing}); !got.OK || got.Value != "0" {
		t.Fatalf("HASCF missing response = %#v, want 0", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "DELCF", Key: "seen", Value: "alpha"}); !got.OK || got.Value != "1" {
		t.Fatalf("DELCF alpha response = %#v, want removed 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASCF", Key: "seen", Value: "alpha"}); !got.OK || got.Value != "0" {
		t.Fatalf("HASCF deleted alpha response = %#v, want 0", got)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOCF", Key: "seen"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFOCF response = %#v, want JSON info", infoResp)
	}
	var info CuckooFilterInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFOCF JSON error = %v", err)
	}
	if info.Count != 1 || info.BucketSize != cuckooFilterBucketSize || info.FingerprintBytes == 0 {
		t.Fatalf("INFOCF = %#v, want populated filter info", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "seen"}); !got.OK || got.Value == "" {
		t.Fatalf("GET cuckoo filter response = %#v, want JSON info", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDCF", Key: "auto", Value: "value"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDCF auto response = %#v, want added 1", got)
	}
	if !ht.Get("auto").IsCuckooFilter() {
		t.Fatal("ADDCF on missing key did not create a Cuckoo filter")
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "CREATECF",
		Key:     "paired",
		Pairs:   Map{"capacity": json.Number("64"), "false_positive_rate": json.Number("0.02")},
	}); !got.OK {
		t.Fatalf("CREATECF pairs response = %#v, want ok", got)
	}
	pairedInfo, ok := ht.CuckooFilterInfo("paired")
	if !ok || pairedInfo.Capacity < 64 || pairedInfo.EstimatedFalsePositiveRate > 0.03 {
		t.Fatalf("paired CuckooFilterInfo = %#v/%v, want configured filter", pairedInfo, ok)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATECF", Key: "bad", Value: "0"}); got.OK {
		t.Fatalf("CREATECF invalid response = %#v, want error", got)
	}
}

func TestExecuteCommandCuckooFilterRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDCF", Key: "seen", Value: "alpha"}); !got.OK {
		t.Fatalf("ADDCF alpha response = %#v, want ok", got)
	}

	got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDCF",
		Key:     "seen",
		Values:  Slice{"beta", func() {}},
	})
	if got.OK {
		t.Fatalf("ADDCF unsupported response = %#v, want error", got)
	}
	info, ok := ht.CuckooFilterInfo("seen")
	if !ok || info.Count != 1 {
		t.Fatalf("CuckooFilterInfo(after rejected add command) = %#v/%v, want one item", info, ok)
	}
	if !ht.HasCuckooFilter("seen", "alpha") {
		t.Fatal("rejected ADDCF batch removed existing value")
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "DELCF",
		Key:     "seen",
		Values:  Slice{"alpha", func() {}},
	})
	if got.OK {
		t.Fatalf("DELCF unsupported response = %#v, want error", got)
	}
	info, ok = ht.CuckooFilterInfo("seen")
	if !ok || info.Count != 1 {
		t.Fatalf("CuckooFilterInfo(after rejected delete command) = %#v/%v, want one item", info, ok)
	}
	if !ht.HasCuckooFilter("seen", "alpha") {
		t.Fatal("rejected DELCF batch removed existing value")
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDCF",
		Key:     "missing",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("ADDCF missing unsupported response = %#v, want error", got)
	}
	if value := ht.Get("missing"); !value.Empty() {
		t.Fatalf("rejected missing-key Cuckoo filter command left value %+v", value)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "HASCF",
		Key:     "seen",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("HASCF unsupported response = %#v, want error", got)
	}
}

func TestExecuteCommandXorFilterOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATEXF", Key: "seen", Value: "8"}); !got.OK {
		t.Fatalf("CREATEXF response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDXF", Key: "seen", Value: "alpha"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDXF value response = %#v, want staged 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDXF", Key: "seen", Values: Slice{"beta", "alpha"}}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDXF values response = %#v, want staged 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASXF", Key: "seen", Value: "alpha"}); got.OK {
		t.Fatalf("HASXF before build response = %#v, want error", got)
	}
	buildResp := ht.ExecuteCommand(CacheCommandRequest{Command: "BUILDXF", Key: "seen"})
	if !buildResp.OK || buildResp.Value == "" {
		t.Fatalf("BUILDXF response = %#v, want JSON info", buildResp)
	}
	var buildInfo XorFilterInfo
	if err := json.Unmarshal([]byte(buildResp.Value), &buildInfo); err != nil {
		t.Fatalf("BUILDXF JSON error = %v", err)
	}
	if !buildInfo.Built || buildInfo.Items != 2 || buildInfo.FingerprintBytes == 0 {
		t.Fatalf("BUILDXF info = %#v, want compact built XOR filter", buildInfo)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASXF", Key: "seen", Value: "alpha"}); !got.OK || got.Value != "1" {
		t.Fatalf("HASXF alpha response = %#v, want hit", got)
	}
	missing := xorFilterMissingValue(t, ht.xorFilters.array[ht.Get("seen").Index])
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASXF", Key: "seen", Value: missing}); !got.OK || got.Value != "0" {
		t.Fatalf("HASXF missing response = %#v, want miss", got)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOXF", Key: "seen"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFOXF response = %#v, want JSON info", infoResp)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "seen"}); !got.OK || got.Value == "" {
		t.Fatalf("GET XOR filter response = %#v, want JSON info", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDXF", Key: "seen", Value: "late"}); got.OK {
		t.Fatalf("ADDXF after build response = %#v, want error", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDXF", Key: "auto", Value: "value"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDXF auto response = %#v, want staged 1", got)
	}
	if !ht.Get("auto").IsXorFilter() {
		t.Fatal("ADDXF on missing key did not create an XOR filter")
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "CREATEXF",
		Key:     "paired",
		Pairs:   Map{"expected_items": json.Number("16")},
	}); !got.OK {
		t.Fatalf("CREATEXF pairs response = %#v, want ok", got)
	}
	pairedInfo, ok := ht.XorFilterInfo("paired")
	if !ok || pairedInfo.ExpectedItems != 16 || pairedInfo.Built {
		t.Fatalf("paired XorFilterInfo = %#v/%v, want pending configured filter", pairedInfo, ok)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATEXF", Key: "bad", Value: "0"}); got.OK {
		t.Fatalf("CREATEXF invalid response = %#v, want error", got)
	}
}

func TestExecuteCommandXorFilterRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDXF", Key: "seen", Value: "alpha"}); !got.OK {
		t.Fatalf("ADDXF alpha response = %#v, want ok", got)
	}

	got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDXF",
		Key:     "seen",
		Values:  Slice{"beta", func() {}},
	})
	if got.OK {
		t.Fatalf("ADDXF unsupported response = %#v, want error", got)
	}
	info, ok := ht.XorFilterInfo("seen")
	if !ok || info.Staged != 1 || info.Items != 1 {
		t.Fatalf("XorFilterInfo(after rejected command) = %#v/%v, want one staged item", info, ok)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDXF",
		Key:     "missing",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("ADDXF missing unsupported response = %#v, want error", got)
	}
	if value := ht.Get("missing"); !value.Empty() {
		t.Fatalf("rejected missing-key XOR filter command left value %+v", value)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "HASXF",
		Key:     "seen",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("HASXF unsupported pending response = %#v, want error", got)
	}
	if build := ht.ExecuteCommand(CacheCommandRequest{Command: "BUILDXF", Key: "seen"}); !build.OK {
		t.Fatalf("BUILDXF response = %#v, want ok", build)
	}
	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "HASXF",
		Key:     "seen",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("HASXF unsupported built response = %#v, want error", got)
	}
}

func TestExecuteCommandRoaringBitmapOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATERB", Key: "ids"}); !got.OK {
		t.Fatalf("CREATERB response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "ids", Value: "1"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDRB value response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "ids", Values: Slice{json.Number("65543"), "1"}}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDRB values response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASRB", Key: "ids", Value: "65543"}); !got.OK || got.Value != "1" {
		t.Fatalf("HASRB present response = %#v, want 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASRB", Key: "ids", Value: "2"}); !got.OK || got.Value != "0" {
		t.Fatalf("HASRB missing response = %#v, want 0", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "REMRB", Key: "ids", Value: "1"}); !got.OK || got.Value != "1" {
		t.Fatalf("REMRB response = %#v, want removed 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "COUNTRB", Key: "ids"}); !got.OK || got.Value != "1" {
		t.Fatalf("COUNTRB response = %#v, want 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETRB", Key: "ids"}); !got.OK || got.Value != "[65543]" {
		t.Fatalf("GETRB response = %#v, want remaining sorted value", got)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFORB", Key: "ids"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFORB response = %#v, want JSON info", infoResp)
	}
	var info RoaringBitmapInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFORB JSON error = %v", err)
	}
	if info.Cardinality != 1 || info.Containers != 1 || info.EncodedBytes != 2 {
		t.Fatalf("INFORB = %#v, want one compact array value", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "ids"}); !got.OK || got.Value == "" {
		t.Fatalf("GET roaring bitmap response = %#v, want JSON info", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "auto", Value: "42"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDRB auto response = %#v, want added 1", got)
	}
	if !ht.Get("auto").IsRoaringBitmap() {
		t.Fatal("ADDRB on missing key did not create a Roaring bitmap")
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "bad", Value: "-1"}); got.OK {
		t.Fatalf("ADDRB invalid response = %#v, want error", got)
	}
}

func TestExecuteCommandRoaringBitmapRejectsInvalidBatchesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "ids", Value: "1"}); !got.OK {
		t.Fatalf("ADDRB initial response = %#v, want ok", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "ids", Values: Slice{json.Number("2"), "-1"}}); got.OK {
		t.Fatalf("ADDRB invalid batch response = %#v, want error", got)
	}
	if hit, err := ht.HasRoaringBitmapChecked("ids", 2); err != nil || hit {
		t.Fatalf("HasRoaringBitmapChecked(2 after rejected add) = %v/%v, want false/nil", hit, err)
	}
	if info, ok := ht.RoaringBitmapInfo("ids"); !ok || info.Cardinality != 1 {
		t.Fatalf("RoaringBitmapInfo(after rejected add) = %#v/%v, want one value", info, ok)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "REMRB", Key: "ids", Values: Slice{"1", "-1"}}); got.OK {
		t.Fatalf("REMRB invalid batch response = %#v, want error", got)
	}
	if hit, err := ht.HasRoaringBitmapChecked("ids", 1); err != nil || !hit {
		t.Fatalf("HasRoaringBitmapChecked(1 after rejected remove) = %v/%v, want true/nil", hit, err)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDRB", Key: "missing", Values: Slice{"-1"}}); got.OK {
		t.Fatalf("ADDRB missing invalid response = %#v, want error", got)
	}
	if hval := ht.Get("missing"); !hval.Empty() {
		t.Fatalf("ADDRB missing invalid created value %+v", hval)
	}
}

func TestExecuteCommandSparseBitsetOperations(t *testing.T) {
	ht := newTestTrie(t)
	maxID := strconv.FormatUint(^uint64(0), 10)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATESB", Key: "ids"}); !got.OK {
		t.Fatalf("CREATESB response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSB", Key: "ids", Value: "1"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDSB value response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSB", Key: "ids", Values: Slice{json.Number("4294967303"), maxID, "1"}}); !got.OK || got.Value != "2" {
		t.Fatalf("ADDSB values response = %#v, want added 2", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASSB", Key: "ids", Value: maxID}); !got.OK || got.Value != "1" {
		t.Fatalf("HASSB present response = %#v, want 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASSB", Key: "ids", Value: "2"}); !got.OK || got.Value != "0" {
		t.Fatalf("HASSB missing response = %#v, want 0", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "REMSB", Key: "ids", Value: "1"}); !got.OK || got.Value != "1" {
		t.Fatalf("REMSB response = %#v, want removed 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "COUNTSB", Key: "ids"}); !got.OK || got.Value != "2" {
		t.Fatalf("COUNTSB response = %#v, want 2", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETSB", Key: "ids"}); !got.OK || got.Value != "[4294967303,18446744073709551615]" {
		t.Fatalf("GETSB response = %#v, want remaining sorted values", got)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOSB", Key: "ids"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFOSB response = %#v, want JSON info", infoResp)
	}
	var info SparseBitsetInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFOSB JSON error = %v", err)
	}
	if info.Cardinality != 2 || info.Containers != 2 || info.EncodedBytes != 4 {
		t.Fatalf("INFOSB = %#v, want compact sparse values", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "ids"}); !got.OK || got.Value == "" {
		t.Fatalf("GET sparse bitset response = %#v, want JSON info", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSB", Key: "auto", Value: "42"}); !got.OK || got.Value != "1" {
		t.Fatalf("ADDSB auto response = %#v, want added 1", got)
	}
	if !ht.Get("auto").IsSparseBitset() {
		t.Fatal("ADDSB on missing key did not create a sparse bitset")
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSB", Key: "bad", Value: "-1"}); got.OK {
		t.Fatalf("ADDSB invalid response = %#v, want error", got)
	}
}

func TestExecuteCommandSparseBitsetRejectsInvalidBatchesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSB", Key: "ids", Value: "1"}); !got.OK {
		t.Fatalf("ADDSB initial response = %#v, want ok", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSB", Key: "ids", Values: Slice{json.Number("2"), "-1"}}); got.OK {
		t.Fatalf("ADDSB invalid batch response = %#v, want error", got)
	}
	if hit, err := ht.HasSparseBitsetChecked("ids", 2); err != nil || hit {
		t.Fatalf("HasSparseBitsetChecked(2 after rejected add) = %v/%v, want false/nil", hit, err)
	}
	if info, ok := ht.SparseBitsetInfo("ids"); !ok || info.Cardinality != 1 {
		t.Fatalf("SparseBitsetInfo(after rejected add) = %#v/%v, want one value", info, ok)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "REMSB", Key: "ids", Values: Slice{"1", "-1"}}); got.OK {
		t.Fatalf("REMSB invalid batch response = %#v, want error", got)
	}
	if hit, err := ht.HasSparseBitsetChecked("ids", 1); err != nil || !hit {
		t.Fatalf("HasSparseBitsetChecked(1 after rejected remove) = %v/%v, want true/nil", hit, err)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDSB", Key: "missing", Values: Slice{"-1"}}); got.OK {
		t.Fatalf("ADDSB missing invalid response = %#v, want error", got)
	}
	if hval := ht.Get("missing"); !hval.Empty() {
		t.Fatalf("ADDSB missing invalid created value %+v", hval)
	}
}

func TestExecuteCommandRadixTreeOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATERT", Key: "index"}); !got.OK {
		t.Fatalf("CREATERT response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUTRT", Key: "index", Subkey: "user:100", Value: "active"}); !got.OK || got.Value != "1" {
		t.Fatalf("PUTRT subkey response = %#v, want added 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "PUTRT",
		Key:     "index",
		Pairs:   Map{"asset:logo": "logo.png", "user:101": json.Number("42")},
	}); !got.OK || got.Value != "2" {
		t.Fatalf("PUTRT pairs response = %#v, want added 2", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETRT", Key: "index", Subkey: "user:100"}); !got.OK || got.Value != "active" {
		t.Fatalf("GETRT user:100 response = %#v, want active", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETRT", Key: "index", Subkey: "user:101"}); !got.OK || got.Value != "42" {
		t.Fatalf("GETRT user:101 response = %#v, want json.Number scalar", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASRT", Key: "index", Subkey: "asset:logo"}); !got.OK || got.Value != "1" {
		t.Fatalf("HASRT asset:logo response = %#v, want 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASRT", Key: "index", Subkey: "asset:missing"}); !got.OK || got.Value != "0" {
		t.Fatalf("HASRT missing response = %#v, want 0", got)
	}
	prefixResp := ht.ExecuteCommand(CacheCommandRequest{Command: "PREFIXRT", Key: "index", Subkey: "user:"})
	if !prefixResp.OK || prefixResp.Value == "" {
		t.Fatalf("PREFIXRT response = %#v, want JSON items", prefixResp)
	}
	var items []RadixTreeItem
	if err := json.Unmarshal([]byte(prefixResp.Value), &items); err != nil {
		t.Fatalf("PREFIXRT JSON error = %v", err)
	}
	if got, want := radixTreeItemKeys(items), []string{"user:100", "user:101"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("PREFIXRT keys = %#v, want %#v", got, want)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFORT", Key: "index"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFORT response = %#v, want JSON info", infoResp)
	}
	var info RadixTreeInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFORT JSON error = %v", err)
	}
	if info.Items != 3 || info.Nodes == 0 || info.EncodedBytes == 0 {
		t.Fatalf("INFORT = %#v, want populated radix tree info", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "index"}); !got.OK || got.Value == "" {
		t.Fatalf("GET radix tree response = %#v, want JSON info", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "DELRT", Key: "index", Subkey: "user:100"}); !got.OK || got.Value != "1" {
		t.Fatalf("DELRT response = %#v, want removed 1", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "HASRT", Key: "index", Subkey: "user:100"}); !got.OK || got.Value != "0" {
		t.Fatalf("HASRT removed response = %#v, want 0", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUTRT", Key: "auto", Subkey: "key", Value: "value"}); !got.OK || got.Value != "1" {
		t.Fatalf("PUTRT auto response = %#v, want added 1", got)
	}
	if !ht.Get("auto").IsRadixTree() {
		t.Fatal("PUTRT on missing key did not create a radix tree")
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETRT", Key: "index"}); got.OK {
		t.Fatalf("GETRT missing subkey response = %#v, want error", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUTRT", Key: "bad"}); got.OK {
		t.Fatalf("PUTRT missing fields response = %#v, want error", got)
	}
}

func TestExecuteCommandRadixTreeRejectsUnsupportedValues(t *testing.T) {
	ht := newTestTrie(t)
	unsupported := func() {}

	got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "PUTRT",
		Key:     "index",
		Pairs:   Map{"bad": unsupported},
	})
	if got.OK {
		t.Fatalf("PUTRT unsupported missing response = %#v, want error", got)
	}
	if hval := ht.Get("index"); !hval.Empty() {
		t.Fatalf("PUTRT unsupported missing stored value %+v", hval)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PUTRT", Key: "index", Subkey: "keep", Value: "value"}); !got.OK {
		t.Fatalf("PUTRT keep response = %#v, want ok", got)
	}
	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "PUTRT",
		Key:     "index",
		Pairs:   Map{"new": "value", "bad": unsupported},
	})
	if got.OK {
		t.Fatalf("PUTRT unsupported existing response = %#v, want error", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETRT", Key: "index", Subkey: "keep"}); !got.OK || got.Value != "value" {
		t.Fatalf("GETRT keep after rejected PUTRT = %#v, want value", got)
	}
	if hit, err := ht.HasRadixTreeChecked("index", "new"); err != nil || hit {
		t.Fatalf("HasRadixTreeChecked(new after rejected PUTRT) = %v/%v, want false/nil", hit, err)
	}
	if info, ok := ht.RadixTreeInfo("index"); !ok || info.Items != 1 {
		t.Fatalf("RadixTreeInfo(after rejected PUTRT) = %#v/%v, want one item", info, ok)
	}
}

func TestExecuteCommandCountMinSketchOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATECMS", Key: "freq", Value: "128", Subkey: "4"}); !got.OK {
		t.Fatalf("CREATECMS response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INCRCMS", Key: "freq", Value: "alpha", Subkey: "2"}); !got.OK || got.Value != "2" {
		t.Fatalf("INCRCMS alpha response = %#v, want estimate 2", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INCRCMS", Key: "freq", Values: Slice{"alpha", "beta"}}); !got.OK || got.Value != "2" {
		t.Fatalf("INCRCMS values response = %#v, want updated 2 values", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ESTCMS", Key: "freq", Value: "alpha"}); !got.OK || got.Value != "3" {
		t.Fatalf("ESTCMS alpha response = %#v, want 3", got)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOCMS", Key: "freq"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFOCMS response = %#v, want JSON info", infoResp)
	}
	var info CountMinSketchInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFOCMS JSON error = %v", err)
	}
	if info.Width != 128 || info.Depth != 4 || info.TotalCount != 4 || info.CounterBytes != 128*4*4 {
		t.Fatalf("INFOCMS = %#v, want populated sketch info", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "freq"}); !got.OK || got.Value == "" {
		t.Fatalf("GET count-min sketch response = %#v, want JSON info", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INCRCMS", Key: "auto", Value: "value"}); !got.OK || got.Value != "1" {
		t.Fatalf("INCRCMS auto response = %#v, want estimate 1", got)
	}
	if !ht.Get("auto").IsCountMinSketch() {
		t.Fatal("INCRCMS on missing key did not create a Count-Min Sketch")
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "CREATECMS",
		Key:     "paired",
		Pairs:   Map{"width": json.Number("64"), "depth": json.Number("3")},
	}); !got.OK {
		t.Fatalf("CREATECMS pairs response = %#v, want ok", got)
	}
	pairedInfo, ok := ht.CountMinSketchInfo("paired")
	if !ok || pairedInfo.Width != 64 || pairedInfo.Depth != 3 {
		t.Fatalf("paired CountMinSketchInfo = %#v/%v, want configured sketch", pairedInfo, ok)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATECMS", Key: "bad", Value: "0"}); got.OK {
		t.Fatalf("CREATECMS invalid response = %#v, want error", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INCRCMS", Key: "freq", Value: "alpha", Subkey: "0"}); got.OK {
		t.Fatalf("INCRCMS zero increment response = %#v, want error", got)
	}
}

func TestExecuteCommandCountMinSketchRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INCRCMS", Key: "freq", Value: "alpha", Subkey: "2"}); !got.OK {
		t.Fatalf("INCRCMS alpha response = %#v, want ok", got)
	}

	got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "INCRCMS",
		Key:     "freq",
		Values:  Slice{"beta", func() {}},
	})
	if got.OK {
		t.Fatalf("INCRCMS unsupported response = %#v, want error", got)
	}
	info, ok := ht.CountMinSketchInfo("freq")
	if !ok || info.TotalCount != 2 {
		t.Fatalf("CountMinSketchInfo(after rejected command) = %#v/%v, want total count 2", info, ok)
	}
	if estimate, ok := ht.EstimateCountMinSketch("freq", "alpha"); !ok || estimate < 2 {
		t.Fatalf("EstimateCountMinSketch(alpha after rejected command) = %d/%v, want retained alpha", estimate, ok)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "INCRCMS",
		Key:     "missing",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("INCRCMS missing unsupported response = %#v, want error", got)
	}
	if value := ht.Get("missing"); !value.Empty() {
		t.Fatalf("rejected missing-key Count-Min Sketch command left value %+v", value)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "ESTCMS",
		Key:     "freq",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("ESTCMS unsupported response = %#v, want error", got)
	}
}

func TestExecuteCommandHyperLogLogOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATEHLL", Key: "card", Value: "10"}); !got.OK {
		t.Fatalf("CREATEHLL response = %#v, want ok", got)
	}
	addResp := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDHLL", Key: "card", Values: Slice{"alpha", "beta", "alpha"}})
	if !addResp.OK {
		t.Fatalf("ADDHLL response = %#v, want ok", addResp)
	}
	addEstimate, err := strconv.ParseUint(addResp.Value, 10, 64)
	if err != nil || addEstimate < 2 {
		t.Fatalf("ADDHLL estimate = %q/%v, want at least 2", addResp.Value, err)
	}
	countResp := ht.ExecuteCommand(CacheCommandRequest{Command: "COUNTHLL", Key: "card"})
	if !countResp.OK || countResp.Value != addResp.Value {
		t.Fatalf("COUNTHLL response = %#v, want estimate %q", countResp, addResp.Value)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOHLL", Key: "card"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFOHLL response = %#v, want JSON info", infoResp)
	}
	var info HyperLogLogInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFOHLL JSON error = %v", err)
	}
	if info.Precision != 10 || info.RegisterBytes != 1<<10 || info.Observations != 3 || info.Estimate != addEstimate {
		t.Fatalf("INFOHLL = %#v, want populated HyperLogLog info", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "card"}); !got.OK || got.Value == "" {
		t.Fatalf("GET hyperloglog response = %#v, want JSON info", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDHLL", Key: "auto", Value: "value"}); !got.OK || got.Value == "0" {
		t.Fatalf("ADDHLL auto response = %#v, want non-zero estimate", got)
	}
	if !ht.Get("auto").IsHyperLogLog() {
		t.Fatal("ADDHLL on missing key did not create a HyperLogLog")
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "CREATEHLL",
		Key:     "paired",
		Pairs:   Map{"precision": json.Number("9")},
	}); !got.OK {
		t.Fatalf("CREATEHLL pairs response = %#v, want ok", got)
	}
	pairedInfo, ok := ht.HyperLogLogInfo("paired")
	if !ok || pairedInfo.Precision != 9 {
		t.Fatalf("paired HyperLogLogInfo = %#v/%v, want precision 9", pairedInfo, ok)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATEHLL", Key: "bad", Value: "3"}); got.OK {
		t.Fatalf("CREATEHLL invalid response = %#v, want error", got)
	}
}

func TestExecuteCommandHyperLogLogRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDHLL", Key: "card", Value: "alpha"}); !got.OK {
		t.Fatalf("ADDHLL alpha response = %#v, want ok", got)
	}

	got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDHLL",
		Key:     "card",
		Values:  Slice{"beta", func() {}},
	})
	if got.OK {
		t.Fatalf("ADDHLL unsupported response = %#v, want error", got)
	}
	info, ok := ht.HyperLogLogInfo("card")
	if !ok || info.Observations != 1 || info.Estimate < 1 {
		t.Fatalf("HyperLogLogInfo(after rejected command) = %#v/%v, want one observation", info, ok)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDHLL",
		Key:     "missing",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("ADDHLL missing unsupported response = %#v, want error", got)
	}
	if value := ht.Get("missing"); !value.Empty() {
		t.Fatalf("rejected missing-key HyperLogLog command left value %+v", value)
	}
}

func TestExecuteCommandTopKOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATETOPK", Key: "top", Value: "3"}); !got.OK {
		t.Fatalf("CREATETOPK response = %#v, want ok", got)
	}
	addResp := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDTOPK", Key: "top", Value: "alpha", Subkey: "5"})
	if !addResp.OK || addResp.Value == "" {
		t.Fatalf("ADDTOPK alpha response = %#v, want JSON estimate", addResp)
	}
	var estimate TopKEstimate
	if err := json.Unmarshal([]byte(addResp.Value), &estimate); err != nil {
		t.Fatalf("ADDTOPK estimate JSON error = %v", err)
	}
	if !estimate.Tracked || estimate.Count != 5 || estimate.Error != 0 {
		t.Fatalf("ADDTOPK estimate = %#v, want tracked count 5", estimate)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDTOPK", Key: "top", Values: Slice{"beta", "gamma"}}); !got.OK || got.Value != "2" {
		t.Fatalf("ADDTOPK values response = %#v, want 2 values", got)
	}
	estResp := ht.ExecuteCommand(CacheCommandRequest{Command: "ESTTOPK", Key: "top", Value: "alpha"})
	if !estResp.OK || estResp.Value == "" {
		t.Fatalf("ESTTOPK alpha response = %#v, want JSON estimate", estResp)
	}
	estimate = TopKEstimate{}
	if err := json.Unmarshal([]byte(estResp.Value), &estimate); err != nil {
		t.Fatalf("ESTTOPK estimate JSON error = %v", err)
	}
	if !estimate.Tracked || estimate.Count != 5 {
		t.Fatalf("ESTTOPK estimate = %#v, want alpha count 5", estimate)
	}
	getResp := ht.ExecuteCommand(CacheCommandRequest{Command: "GETTOPK", Key: "top"})
	if !getResp.OK || getResp.Value == "" {
		t.Fatalf("GETTOPK response = %#v, want JSON items", getResp)
	}
	var items []TopKItem
	if err := json.Unmarshal([]byte(getResp.Value), &items); err != nil {
		t.Fatalf("GETTOPK JSON error = %v", err)
	}
	if len(items) != 3 || items[0].Value != "alpha" || items[0].Count != 5 {
		t.Fatalf("GETTOPK items = %#v, want alpha first and three tracked values", items)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOTOPK", Key: "top"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFOTOPK response = %#v, want JSON info", infoResp)
	}
	var info TopKInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFOTOPK JSON error = %v", err)
	}
	if info.Capacity != 3 || info.Tracked != 3 || info.Total != 7 {
		t.Fatalf("INFOTOPK = %#v, want populated Top-K info", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "top"}); !got.OK || got.Value == "" {
		t.Fatalf("GET top-k response = %#v, want JSON items", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDTOPK", Key: "auto", Value: "value"}); !got.OK || got.Value == "" {
		t.Fatalf("ADDTOPK auto response = %#v, want JSON estimate", got)
	}
	if !ht.Get("auto").IsTopK() {
		t.Fatal("ADDTOPK on missing key did not create a Top-K sketch")
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "CREATETOPK",
		Key:     "paired",
		Pairs:   Map{"capacity": json.Number("4")},
	}); !got.OK {
		t.Fatalf("CREATETOPK pairs response = %#v, want ok", got)
	}
	if info, ok := ht.TopKInfo("paired"); !ok || info.Capacity != 4 {
		t.Fatalf("paired TopKInfo = %#v/%v, want capacity 4", info, ok)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATETOPK", Key: "bad", Value: "0"}); got.OK {
		t.Fatalf("CREATETOPK invalid response = %#v, want error", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDTOPK", Key: "top", Value: "alpha", Subkey: "0"}); got.OK {
		t.Fatalf("ADDTOPK zero count response = %#v, want error", got)
	}
}

func TestExecuteCommandTopKRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDTOPK", Key: "top", Value: "alpha", Subkey: "2"}); !got.OK {
		t.Fatalf("ADDTOPK alpha response = %#v, want ok", got)
	}

	got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDTOPK",
		Key:     "top",
		Values:  Slice{"beta", func() {}},
	})
	if got.OK {
		t.Fatalf("ADDTOPK unsupported response = %#v, want error", got)
	}
	info, ok := ht.TopKInfo("top")
	if !ok || info.Total != 2 || info.Tracked != 1 {
		t.Fatalf("TopKInfo(after rejected command) = %#v/%v, want unchanged sketch", info, ok)
	}
	items := ht.GetTopK("top")
	if len(items) != 1 || items[0].Value != "alpha" || items[0].Count != 2 {
		t.Fatalf("GetTopK(after rejected command) = %#v, want alpha count 2 only", items)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDTOPK",
		Key:     "missing",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("ADDTOPK missing unsupported response = %#v, want error", got)
	}
	if value := ht.Get("missing"); !value.Empty() {
		t.Fatalf("rejected missing-key Top-K command left value %+v", value)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "ESTTOPK",
		Key:     "top",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("ESTTOPK unsupported response = %#v, want error", got)
	}
}

func TestExecuteCommandReservoirSampleOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATERS", Key: "sample", Value: "3"}); !got.OK {
		t.Fatalf("CREATERS response = %#v, want ok", got)
	}
	addResp := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDRS",
		Key:     "sample",
		Values:  Slice{"alpha", "beta", "gamma", "delta"},
	})
	if !addResp.OK || addResp.Value == "" {
		t.Fatalf("ADDRS response = %#v, want JSON update", addResp)
	}
	var update ReservoirSampleUpdate
	if err := json.Unmarshal([]byte(addResp.Value), &update); err != nil {
		t.Fatalf("ADDRS update JSON error = %v", err)
	}
	if update.Seen != 4 || update.Tracked != 3 || update.Capacity != 3 {
		t.Fatalf("ADDRS update = %#v, want bounded stream sample", update)
	}
	getResp := ht.ExecuteCommand(CacheCommandRequest{Command: "GETRS", Key: "sample"})
	if !getResp.OK || getResp.Value == "" {
		t.Fatalf("GETRS response = %#v, want JSON sample items", getResp)
	}
	var items []ReservoirSampleItem
	if err := json.Unmarshal([]byte(getResp.Value), &items); err != nil {
		t.Fatalf("GETRS JSON error = %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("GETRS len = %d, want bounded sample capacity 3", len(items))
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFORS", Key: "sample"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFORS response = %#v, want JSON info", infoResp)
	}
	var info ReservoirSampleInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFORS JSON error = %v", err)
	}
	if info.Capacity != 3 || info.Tracked != 3 || info.Seen != 4 {
		t.Fatalf("INFORS = %#v, want populated reservoir sample info", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "sample"}); !got.OK || got.Value == "" {
		t.Fatalf("GET reservoir sample response = %#v, want JSON items", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDRS", Key: "auto", Value: "value"}); !got.OK || got.Value == "" {
		t.Fatalf("ADDRS auto response = %#v, want JSON update", got)
	}
	if !ht.Get("auto").IsReservoirSample() {
		t.Fatal("ADDRS on missing key did not create a reservoir sample")
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "CREATERS",
		Key:     "paired",
		Pairs:   Map{"capacity": json.Number("4")},
	}); !got.OK {
		t.Fatalf("CREATERS pairs response = %#v, want ok", got)
	}
	if info, ok := ht.ReservoirSampleInfo("paired"); !ok || info.Capacity != 4 {
		t.Fatalf("paired ReservoirSampleInfo = %#v/%v, want capacity 4", info, ok)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATERS", Key: "bad", Value: "0"}); got.OK {
		t.Fatalf("CREATERS invalid response = %#v, want error", got)
	}
}

func TestExecuteCommandReservoirSampleRejectsUnsupportedValuesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDRS", Key: "sample", Value: "alpha"}); !got.OK {
		t.Fatalf("ADDRS alpha response = %#v, want ok", got)
	}
	got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDRS",
		Key:     "sample",
		Values:  Slice{"beta", func() {}},
	})
	if got.OK {
		t.Fatalf("ADDRS unsupported response = %#v, want error", got)
	}
	info, ok := ht.ReservoirSampleInfo("sample")
	if !ok || info.Seen != 1 || info.Tracked != 1 {
		t.Fatalf("ReservoirSampleInfo(after rejected command) = %#v/%v, want unchanged sample", info, ok)
	}
	items := ht.GetReservoirSample("sample")
	if len(items) != 1 || items[0].Value != "alpha" {
		t.Fatalf("GetReservoirSample(after rejected command) = %#v, want alpha only", items)
	}

	got = ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDRS",
		Key:     "missing",
		Values:  Slice{func() {}},
	})
	if got.OK {
		t.Fatalf("ADDRS missing unsupported response = %#v, want error", got)
	}
	if value := ht.Get("missing"); !value.Empty() {
		t.Fatalf("rejected missing-key command left value %+v", value)
	}
}

func TestExecuteCommandQuantileSketchOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATEQ", Key: "latency", Value: "0.02"}); !got.OK {
		t.Fatalf("CREATEQ response = %#v, want ok", got)
	}
	addResp := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDQ",
		Key:     "latency",
		Values:  Slice{json.Number("1"), "50", float64(100)},
	})
	if !addResp.OK || addResp.Value == "" {
		t.Fatalf("ADDQ response = %#v, want JSON estimate", addResp)
	}
	var estimate QuantileEstimate
	if err := json.Unmarshal([]byte(addResp.Value), &estimate); err != nil {
		t.Fatalf("ADDQ estimate JSON error = %v", err)
	}
	if estimate.Count != 3 || estimate.Value < 1 || estimate.Value > 100 {
		t.Fatalf("ADDQ estimate = %#v, want populated quantile estimate", estimate)
	}
	estResp := ht.ExecuteCommand(CacheCommandRequest{Command: "ESTQ", Key: "latency", Value: "0.5"})
	if !estResp.OK || estResp.Value == "" {
		t.Fatalf("ESTQ response = %#v, want JSON estimate", estResp)
	}
	estimate = QuantileEstimate{}
	if err := json.Unmarshal([]byte(estResp.Value), &estimate); err != nil {
		t.Fatalf("ESTQ estimate JSON error = %v", err)
	}
	if estimate.Quantile != 0.5 || estimate.Count != 3 || estimate.Value < 1 || estimate.Value > 100 {
		t.Fatalf("ESTQ estimate = %#v, want median estimate", estimate)
	}
	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOQ", Key: "latency"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFOQ response = %#v, want JSON info", infoResp)
	}
	var info QuantileSketchInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFOQ JSON error = %v", err)
	}
	if info.Epsilon != 0.02 || info.Count != 3 || info.SummarySize == 0 {
		t.Fatalf("INFOQ = %#v, want populated quantile sketch info", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "latency"}); !got.OK || got.Value == "" {
		t.Fatalf("GET quantile sketch response = %#v, want JSON info", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDQ", Key: "auto", Value: "42"}); !got.OK || got.Value == "" {
		t.Fatalf("ADDQ auto response = %#v, want JSON estimate", got)
	}
	if !ht.Get("auto").IsQuantileSketch() {
		t.Fatal("ADDQ on missing key did not create a quantile sketch")
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "CREATEQ",
		Key:     "paired",
		Pairs:   Map{"epsilon": json.Number("0.05")},
	}); !got.OK {
		t.Fatalf("CREATEQ pairs response = %#v, want ok", got)
	}
	if info, ok := ht.QuantileSketchInfo("paired"); !ok || info.Epsilon != 0.05 {
		t.Fatalf("paired QuantileSketchInfo = %#v/%v, want epsilon 0.05", info, ok)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATEQ", Key: "bad", Value: "0"}); got.OK {
		t.Fatalf("CREATEQ invalid response = %#v, want error", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDQ", Key: "latency", Value: "NaN"}); got.OK {
		t.Fatalf("ADDQ NaN response = %#v, want error", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ESTQ", Key: "latency", Value: "1.5"}); got.OK {
		t.Fatalf("ESTQ invalid quantile response = %#v, want error", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDQ", Key: "zero", Value: "0"}); !got.OK {
		t.Fatalf("ADDQ zero response = %#v, want ok", got)
	}
	zeroInfoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOQ", Key: "zero"})
	if !zeroInfoResp.OK || zeroInfoResp.Value == "" {
		t.Fatalf("INFOQ zero response = %#v, want JSON info", zeroInfoResp)
	}
	var zeroInfoFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(zeroInfoResp.Value), &zeroInfoFields); err != nil {
		t.Fatalf("INFOQ zero JSON error = %v", err)
	}
	if string(zeroInfoFields["min"]) != "0" || string(zeroInfoFields["max"]) != "0" {
		t.Fatalf("INFOQ zero fields = %s, want explicit zero min and max in %s", zeroInfoResp.Value, zeroInfoResp.Value)
	}
}

func TestExecuteCommandQuantileSketchRejectsInvalidBatchesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDQ", Key: "latency", Values: Slice{"10", "20"}}); !got.OK {
		t.Fatalf("ADDQ initial response = %#v, want ok", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDQ", Key: "latency", Values: Slice{"30", "NaN"}}); got.OK {
		t.Fatalf("ADDQ invalid batch response = %#v, want error", got)
	}
	if info, ok := ht.QuantileSketchInfo("latency"); !ok || info.Count != 2 {
		t.Fatalf("QuantileSketchInfo(after rejected add) = %#v/%v, want two values", info, ok)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDQ", Key: "missing", Values: Slice{"NaN"}}); got.OK {
		t.Fatalf("ADDQ missing invalid response = %#v, want error", got)
	}
	if hval := ht.Get("missing"); !hval.Empty() {
		t.Fatalf("ADDQ missing invalid created value %+v", hval)
	}
}

func TestExecuteCommandFenwickTreeOperations(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATEFW", Key: "scores", Value: "8"}); !got.OK {
		t.Fatalf("CREATEFW response = %#v, want ok", got)
	}
	addResp := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDFW",
		Key:     "scores",
		Values:  Slice{json.Number("2"), json.Number("5")},
	})
	if !addResp.OK || addResp.Value == "" {
		t.Fatalf("ADDFW response = %#v, want JSON update", addResp)
	}
	var update FenwickTreeUpdate
	if err := json.Unmarshal([]byte(addResp.Value), &update); err != nil {
		t.Fatalf("ADDFW update JSON error = %v", err)
	}
	if update.Index != 2 || update.Delta != 5 || update.Value != 5 || update.PrefixSum != 5 || update.Total != 5 {
		t.Fatalf("ADDFW update = %#v, want populated Fenwick update", update)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDFW", Key: "scores", Value: "4", Subkey: "7"}); !got.OK {
		t.Fatalf("ADDFW value/subkey response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETFW", Key: "scores", Value: "4"}); !got.OK || got.Value != "7" {
		t.Fatalf("GETFW response = %#v, want 7", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SUMFW", Key: "scores", Value: "4"}); !got.OK || got.Value != "12" {
		t.Fatalf("SUMFW response = %#v, want 12", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "RANGEFW", Key: "scores", Value: "3", Subkey: "4"}); !got.OK || got.Value != "7" {
		t.Fatalf("RANGEFW response = %#v, want 7", got)
	}

	infoResp := ht.ExecuteCommand(CacheCommandRequest{Command: "INFOFW", Key: "scores"})
	if !infoResp.OK || infoResp.Value == "" {
		t.Fatalf("INFOFW response = %#v, want JSON info", infoResp)
	}
	var info FenwickTreeInfo
	if err := json.Unmarshal([]byte(infoResp.Value), &info); err != nil {
		t.Fatalf("INFOFW JSON error = %v", err)
	}
	if info.Size != 8 || info.Updates != 2 || info.Total != 12 || info.TreeBytes != 72 {
		t.Fatalf("INFOFW = %#v, want populated Fenwick tree info", info)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "scores"}); !got.OK || got.Value == "" {
		t.Fatalf("GET Fenwick tree response = %#v, want JSON info", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDFW", Key: "auto", Value: "1", Subkey: "3"}); !got.OK || got.Value == "" {
		t.Fatalf("ADDFW auto response = %#v, want JSON update", got)
	}
	if !ht.Get("auto").IsFenwickTree() {
		t.Fatal("ADDFW on missing key did not create a Fenwick tree")
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "CREATEFW",
		Key:     "paired",
		Pairs:   Map{"size": json.Number("4")},
	}); !got.OK {
		t.Fatalf("CREATEFW pairs response = %#v, want ok", got)
	}
	if info, ok := ht.FenwickTreeInfo("paired"); !ok || info.Size != 4 {
		t.Fatalf("paired FenwickTreeInfo = %#v/%v, want size 4", info, ok)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{
		Command: "ADDFW",
		Key:     "paired",
		Pairs:   Map{"index": json.Number("3"), "delta": json.Number("-2")},
	}); !got.OK {
		t.Fatalf("ADDFW pairs response = %#v, want ok", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PREFIXFW", Key: "paired", Pairs: Map{"end": json.Number("3")}}); !got.OK || got.Value != "-2" {
		t.Fatalf("PREFIXFW pairs response = %#v, want -2", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "CREATEFW", Key: "bad", Value: "0"}); got.OK {
		t.Fatalf("CREATEFW invalid response = %#v, want error", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDFW", Key: "scores", Value: "9", Subkey: "1"}); got.OK {
		t.Fatalf("ADDFW out-of-range response = %#v, want error", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDFW", Key: "scores", Value: "1", Subkey: "0"}); got.OK {
		t.Fatalf("ADDFW zero delta response = %#v, want error", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "RANGEFW", Key: "scores", Value: "4", Subkey: "3"}); got.OK {
		t.Fatalf("RANGEFW inverted range response = %#v, want error", got)
	}
}

func TestExecuteCommandFenwickTreeRejectsInvalidUpdatesWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDFW", Key: "scores", Value: "2", Subkey: "5"}); !got.OK {
		t.Fatalf("ADDFW initial response = %#v, want ok", got)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDFW", Key: "scores", Value: "2", Subkey: "0"}); got.OK {
		t.Fatalf("ADDFW zero-delta response = %#v, want error", got)
	}
	if got, ok := ht.GetFenwickTree("scores", 2); !ok || got != 5 {
		t.Fatalf("GetFenwickTree(2 after rejected zero-delta) = %d/%v, want 5/true", got, ok)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDFW", Key: "scores", Value: strconv.FormatUint(maxFenwickTreeSize, 10), Subkey: "1"}); got.OK {
		t.Fatalf("ADDFW out-of-range response = %#v, want error", got)
	}
	if info, ok := ht.FenwickTreeInfo("scores"); !ok || info.Updates != 1 || info.Total != 5 {
		t.Fatalf("FenwickTreeInfo(after rejected update) = %#v/%v, want one update total 5", info, ok)
	}

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "ADDFW", Key: "missing", Value: "1", Subkey: "0"}); got.OK {
		t.Fatalf("ADDFW missing invalid response = %#v, want error", got)
	}
	if hval := ht.Get("missing"); !hval.Empty() {
		t.Fatalf("ADDFW missing invalid created value %+v", hval)
	}
}

func TestExecuteCommandInternalReplicationCommands(t *testing.T) {
	source := newTestTrie(t)
	now := time.Unix(1400, 0)
	source.now = func() time.Time { return now }
	source.UpsertSet("tags", Set{"go", "cache", "go"})
	if !source.Expire("tags", time.Minute) {
		t.Fatal("Expire(tags) = false, want true")
	}

	dump := source.ExecuteCommand(CacheCommandRequest{Command: "DUMP", Key: "tags"})
	if !dump.OK || dump.Value == "" {
		t.Fatalf("DUMP response = %#v, want snapshot entry JSON", dump)
	}
	var dumped snapshotEntry
	if err := json.Unmarshal([]byte(dump.Value), &dumped); err != nil {
		t.Fatalf("DUMP value JSON error = %v", err)
	}
	if dumped.Key != "tags" || dumped.Type != "set" {
		t.Fatalf("dumped entry = %#v, want tags set", dumped)
	}
	if missing := source.ExecuteCommand(CacheCommandRequest{Command: "DUMP", Key: "missing"}); !missing.OK || missing.Value != "" {
		t.Fatalf("DUMP missing response = %#v, want key not found", missing)
	}

	target := newTestTrie(t)
	target.now = func() time.Time { return now }
	if got := target.ExecuteCommand(CacheCommandRequest{Command: "INTERNALSET", Key: "tags", Value: dump.Value}); !got.OK {
		t.Fatalf("INTERNALSET response = %#v, want ok", got)
	}
	if got := target.ExecuteCommand(CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "implicit-key",
		Value:   `{"type":"string","string":"value"}`,
	}); !got.OK {
		t.Fatalf("INTERNALSET without payload key response = %#v, want ok", got)
	}
	target.UpsertString("expired-copy", "old")
	expiredIdx := target.Get("expired-copy").Index
	expiredAt := now.Add(-time.Second)
	expiredPayload, err := json.Marshal(snapshotEntry{
		Key:       "expired-copy",
		Type:      "string",
		String:    "new",
		ExpiresAt: &expiredAt,
	})
	if err != nil {
		t.Fatalf("Marshal(expired snapshot entry) error = %v", err)
	}
	if got := target.ExecuteCommand(CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "expired-copy",
		Value:   string(expiredPayload),
	}); !got.OK {
		t.Fatalf("INTERNALSET expired payload response = %#v, want ok", got)
	}
	if got := target.GetSet("tags"); !reflect.DeepEqual(got, Set{"cache", "go"}) {
		t.Fatalf("replicated set = %#v, want cache/go", got)
	}
	if got := target.GetString("implicit-key"); got != "value" {
		t.Fatalf("implicit-key = %q, want value", got)
	}
	if target.Exists("expired-copy") {
		t.Fatal("expired INTERNALSET payload left key present")
	}
	if !rawIndexReleased(target, expiredIdx) {
		t.Fatalf("expired INTERNALSET did not release raw index %d", expiredIdx)
	}
	if got := target.TTL("tags"); got != time.Minute {
		t.Fatalf("replicated TTL = %s, want 1m", got)
	}
	if got := target.ExecuteCommand(CacheCommandRequest{Command: "INTERNALDEL", Key: "tags"}); !got.OK || got.Message != "internal value deleted" {
		t.Fatalf("INTERNALDEL response = %#v, want deleted", got)
	}
	if target.Exists("tags") {
		t.Fatal("tags exists after INTERNALDEL")
	}
}

func TestExecuteCommandInternalSetValidation(t *testing.T) {
	ht := newTestTrie(t)

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INTERNALSET", Key: "key"}); got.OK {
		t.Fatalf("INTERNALSET empty response = %#v, want not ok", got)
	}
	mismatch := `{"key":"other","type":"string","string":"value"}`
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INTERNALSET", Key: "key", Value: mismatch}); got.OK {
		t.Fatalf("INTERNALSET mismatched key response = %#v, want not ok", got)
	}
	emptyKey := `{"key":"","type":"string","string":"value"}`
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INTERNALSET", Key: "key", Value: emptyKey}); got.OK {
		t.Fatalf("INTERNALSET explicit empty key response = %#v, want not ok", got)
	}
	spaceKey := `{"key":" ","type":"string","string":"value"}`
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INTERNALSET", Key: "key", Value: spaceKey}); got.OK {
		t.Fatalf("INTERNALSET explicit space key response = %#v, want not ok", got)
	}
	nullKey := `{"key":null,"type":"string","string":"value"}`
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "INTERNALSET", Key: "key", Value: nullKey}); got.OK {
		t.Fatalf("INTERNALSET null key response = %#v, want not ok", got)
	}
	if got := ht.GetString("key"); got != "" {
		t.Fatalf("invalid INTERNALSET stored key = %q, want empty", got)
	}
}

func TestExecuteCommandValidation(t *testing.T) {
	ht := newTestTrie(t)

	for _, request := range []CacheCommandRequest{
		{Command: "", Key: "key"},
		{Command: "GET", Key: ""},
		{Command: "SETINT", Key: "counter", Value: "not-int"},
		{Command: "SETSTRX", Key: "key", Value: "value"},
		{Command: "EXPIRE", Key: "key"},
		{Command: "EXPIREAT", Key: "key"},
		{Command: "INC", Key: "key", Value: "not-int"},
		{Command: "PUTMAP", Key: "key"},
		{Command: "PUTMAP", Key: "key", Pairs: Map{"": "bad"}},
		{Command: "PEEKMAP", Key: "key"},
		{Command: "TAKEMAP", Key: "key"},
		{Command: "PUSHSLICE", Key: "key"},
		{Command: "ADDSET", Key: "key"},
		{Command: "REMSET", Key: "key"},
		{Command: "HASSET", Key: "key"},
		{Command: "PUSHPQ", Key: "key"},
		{Command: "PUSHPQ", Key: "key", Value: "job"},
		{Command: "PUSHPQ", Key: "key", Value: "job", Subkey: "bad"},
		{Command: "CREATEBF", Key: "key", Value: "0"},
		{Command: "CREATEBF", Key: "key", Pairs: Map{"expected_items": 1.5}},
		{Command: "CREATEBF", Key: "key", Pairs: Map{"false_positive_rate": "not-number"}},
		{Command: "ADDBF", Key: "key"},
		{Command: "HASBF", Key: "key"},
		{Command: "CREATEXF", Key: "key", Value: "0"},
		{Command: "ADDXF", Key: "key"},
		{Command: "HASXF", Key: "key"},
		{Command: "INTERNALSET", Key: "key"},
		{Command: "UNKNOWN", Key: "key"},
	} {
		if got := ht.ExecuteCommand(request); got.OK {
			t.Fatalf("ExecuteCommand(%#v) = %#v, want not ok", request, got)
		}
	}

	invalidTTL := int64(0)
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "bad-ttl", Value: "value", TTLSeconds: &invalidTTL}); got.OK {
		t.Fatalf("SETSTR invalid TTL response = %#v, want not ok", got)
	}
	if got := ht.Get("bad-ttl"); !got.Empty() {
		t.Fatalf("SETSTR with invalid TTL stored value: %+v", got)
	}
}

func TestCommandUint64ValueAcceptsNativeNumericTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  uint64
	}{
		{name: "json number", value: json.Number("42"), want: 42},
		{name: "string", value: " 43 ", want: 43},
		{name: "uint64", value: uint64(44), want: 44},
		{name: "uint", value: uint(45), want: 45},
		{name: "uint32", value: uint32(46), want: 46},
		{name: "int", value: int(47), want: 47},
		{name: "int64", value: int64(48), want: 48},
		{name: "int32", value: int32(49), want: 49},
		{name: "float64 integer", value: float64(50), want: 50},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := commandUint64Value(tt.value)
			if err != nil || got != tt.want {
				t.Fatalf("commandUint64Value(%#v) = %d/%v, want %d/nil", tt.value, got, err, tt.want)
			}
		})
	}
}

func TestCommandUint64ValueRejectsInvalidNativeNumericValues(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{name: "negative int", value: int(-1)},
		{name: "negative int64", value: int64(-1)},
		{name: "negative int32", value: int32(-1)},
		{name: "negative float", value: float64(-1)},
		{name: "fractional float", value: float64(1.5)},
		{name: "NaN", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "unsupported type", value: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := commandUint64Value(tt.value); err == nil {
				t.Fatalf("commandUint64Value(%#v) = %d/nil, want error", tt.value, got)
			}
		})
	}
}

func TestCommandInt64ValueAcceptsNativeNumericTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  int64
	}{
		{name: "json number", value: json.Number("-42"), want: -42},
		{name: "string", value: " 43 ", want: 43},
		{name: "int64 min", value: int64(minFenwickTreeInt64), want: minFenwickTreeInt64},
		{name: "int", value: int(-44), want: -44},
		{name: "int32", value: int32(-45), want: -45},
		{name: "uint64 max int64", value: uint64(maxFenwickTreeInt64), want: maxFenwickTreeInt64},
		{name: "uint", value: uint(46), want: 46},
		{name: "uint32", value: uint32(47), want: 47},
		{name: "float64 integer", value: float64(-48), want: -48},
		{name: "float64 min", value: float64(minFenwickTreeInt64), want: minFenwickTreeInt64},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := commandInt64Value(tt.value)
			if err != nil || got != tt.want {
				t.Fatalf("commandInt64Value(%#v) = %d/%v, want %d/nil", tt.value, got, err, tt.want)
			}
		})
	}
}

func TestCommandInt64ValueRejectsInvalidNativeNumericValues(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{name: "invalid json number", value: json.Number("nope")},
		{name: "overflow json number", value: json.Number("9223372036854775808")},
		{name: "invalid string", value: "not-int"},
		{name: "uint64 too large", value: uint64(maxFenwickTreeInt64) + 1},
		{name: "fractional float", value: float64(1.5)},
		{name: "float too large", value: float64(maxFenwickTreeInt64)},
		{name: "NaN", value: math.NaN()},
		{name: "positive infinity", value: math.Inf(1)},
		{name: "unsupported type", value: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got, err := commandInt64Value(tt.value); err == nil {
				t.Fatalf("commandInt64Value(%#v) = %d/nil, want error", tt.value, got)
			}
		})
	}
}

func TestCommandFloat64ValueAcceptsNativeNumericTypes(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
		want  float64
	}{
		{name: "json number", value: json.Number("0.25"), want: 0.25},
		{name: "string", value: " 0.5 ", want: 0.5},
		{name: "float64", value: float64(0.75), want: 0.75},
		{name: "float32", value: float32(1.25), want: 1.25},
		{name: "uint64", value: uint64(2), want: 2},
		{name: "uint", value: uint(3), want: 3},
		{name: "uint32", value: uint32(4), want: 4},
		{name: "int", value: int(5), want: 5},
		{name: "int64", value: int64(6), want: 6},
		{name: "int32", value: int32(7), want: 7},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := commandFloat64Value(tt.value)
			if err != nil || got != tt.want {
				t.Fatalf("commandFloat64Value(%#v) = %v/%v, want %v/nil", tt.value, got, err, tt.want)
			}
		})
	}
}

func TestCommandFloat64ValueRejectsUnsupportedValues(t *testing.T) {
	if got, err := commandFloat64Value(true); err == nil {
		t.Fatalf("commandFloat64Value(true) = %v/nil, want error", got)
	}
}

func TestExecuteCommandStructuredValues(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertMap("map", Map{"name": "ivi"})
	ht.UpsertSlice("slice", Slice{"a", "b"})
	ht.UpsertSet("set", Set{"b", "a"})

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "map"}); !got.OK || got.Value != `{"name":"ivi"}` {
		t.Fatalf("GET map response = %#v, want JSON object", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "slice"}); !got.OK || got.Value != `["a","b"]` {
		t.Fatalf("GET slice response = %#v, want JSON array", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "set"}); !got.OK || got.Value != `["a","b"]` {
		t.Fatalf("GET set response = %#v, want JSON array", got)
	}
}
