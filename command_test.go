package hatriecache

import (
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
