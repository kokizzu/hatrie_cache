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
}

func TestExecuteCommandValidation(t *testing.T) {
	ht := newTestTrie(t)

	for _, request := range []CacheCommandRequest{
		{Command: "", Key: "key"},
		{Command: "GET", Key: ""},
		{Command: "SETINT", Key: "counter", Value: "not-int"},
		{Command: "SETSTRX", Key: "key", Value: "value"},
		{Command: "EXPIRE", Key: "key"},
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

	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "map"}); !got.OK || got.Value != `{"name":"ivi"}` {
		t.Fatalf("GET map response = %#v, want JSON object", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "slice"}); !got.OK || got.Value != `["a","b"]` {
		t.Fatalf("GET slice response = %#v, want JSON array", got)
	}
}
