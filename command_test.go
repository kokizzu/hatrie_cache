package hatriecache

import (
	"encoding/json"
	"reflect"
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
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "PEEKPQ", Key: "jobs"}); !got.OK || got.Value != `{"priority":1,"value":"urgent"}` {
		t.Fatalf("PEEKPQ response = %#v, want urgent priority item", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "POPPQ", Key: "jobs"}); !got.OK || got.Value != `{"priority":1,"value":"urgent"}` {
		t.Fatalf("POPPQ response = %#v, want urgent priority item", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETPQ", Key: "jobs"}); !got.OK || got.Value != `[{"priority":1,"value":2},{"priority":10,"value":"slow"}]` {
		t.Fatalf("GETPQ response = %#v, want ordered JSON array", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "jobs"}); !got.OK || got.Value != `[{"priority":1,"value":2},{"priority":10,"value":"slow"}]` {
		t.Fatalf("GET priority queue response = %#v, want ordered JSON array", got)
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
	if got := target.GetSet("tags"); !reflect.DeepEqual(got, Set{"cache", "go"}) {
		t.Fatalf("replicated set = %#v, want cache/go", got)
	}
	if got := target.GetString("implicit-key"); got != "value" {
		t.Fatalf("implicit-key = %q, want value", got)
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
