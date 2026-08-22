package hatriecache

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// These flows are the executable counterpart to DATA_STRUCTURE.md. Keep the
// documented before/request/after examples aligned with these commands.
func TestDataStructureGuideExamples(t *testing.T) {
	ht := CreateHatTrie()
	t.Cleanup(ht.Destroy)

	run := func(request CacheCommandRequest) CacheCommandResponse {
		t.Helper()
		response := ht.ExecuteCommand(request)
		if !response.OK {
			t.Fatalf("%s response = %#v", request.Command, response)
		}
		return response
	}
	requireValue := func(request CacheCommandRequest, want string) {
		t.Helper()
		if got := run(request).Value; got != want {
			t.Fatalf("%s value = %q, want %q", request.Command, got, want)
		}
	}
	requireJSON := func(request CacheCommandRequest) string {
		t.Helper()
		value := run(request).Value
		if !json.Valid([]byte(value)) {
			t.Fatalf("%s value = %q, want JSON", request.Command, value)
		}
		return value
	}

	t.Run("common and scalar", func(t *testing.T) {
		if got := run(CacheCommandRequest{Command: "GET", Key: "name"}).Message; got != "key not found" {
			t.Fatalf("GET missing message = %q", got)
		}
		requireValue(CacheCommandRequest{Command: "EXISTS", Key: "name"}, "0")
		run(CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "Ivi"})
		requireValue(CacheCommandRequest{Command: "GETSTR", Key: "name"}, "Ivi")
		if value := requireJSON(CacheCommandRequest{Command: "DUMP", Key: "name"}); !strings.Contains(value, "Ivi") {
			t.Fatalf("DUMP value = %q, want stored string", value)
		}
		run(CacheCommandRequest{Command: "SETX", Key: "temporary", Value: "yes", TTLSeconds: int64Ptr(60)})
		if got := run(CacheCommandRequest{Command: "TTL", Key: "temporary"}).Value; got == "-1" || got == "0" {
			t.Fatalf("TTL value = %q, want live ttl", got)
		}
		run(CacheCommandRequest{Command: "EXPIRE", Key: "temporary", TTLSeconds: int64Ptr(120)})
		run(CacheCommandRequest{Command: "EXPIREAT", Key: "temporary", UnixSeconds: int64Ptr(time.Now().Add(time.Hour).Unix())})
		run(CacheCommandRequest{Command: "SETINT", Key: "views", Value: "41"})
		requireValue(CacheCommandRequest{Command: "INC", Key: "views", Value: "1"}, "42")
		run(CacheCommandRequest{Command: "SETINTX", Key: "limited", Value: "7", TTLSeconds: int64Ptr(60)})
		batch := run(CacheCommandRequest{Command: "BATCH", Batch: []CacheCommandRequest{
			{Command: "SET", Key: "batch:name", Value: "Ada"},
			{Command: "GET", Key: "batch:name"},
		}})
		if len(batch.Responses) != 2 || batch.Responses[1].Value != "Ada" {
			t.Fatalf("BATCH responses = %#v", batch.Responses)
		}
		run(CacheCommandRequest{Command: "DEL", Key: "name"})
		requireValue(CacheCommandRequest{Command: "EXISTS", Key: "name"}, "0")
	})

	t.Run("collections", func(t *testing.T) {
		run(CacheCommandRequest{Command: "PUTMAP", Key: "user", Pairs: Map{"name": "Ivi", "role": "admin"}})
		requireValue(CacheCommandRequest{Command: "PEEKMAP", Key: "user", Subkey: "role"}, "admin")
		requireValue(CacheCommandRequest{Command: "TAKEMAP", Key: "user", Subkey: "role"}, "admin")
		if got := run(CacheCommandRequest{Command: "PEEKMAP", Key: "user", Subkey: "role"}).Message; got != "value not found" {
			t.Fatalf("PEEKMAP after TAKEMAP message = %q", got)
		}

		run(CacheCommandRequest{Command: "PUSHSLICE", Key: "jobs", Values: Slice{"build", "verify", "deploy"}})
		requireValue(CacheCommandRequest{Command: "HEADSLICE", Key: "jobs"}, "build")
		requireValue(CacheCommandRequest{Command: "TAILSLICE", Key: "jobs"}, "deploy")
		requireValue(CacheCommandRequest{Command: "POPSLICE", Key: "jobs"}, "deploy")
		requireValue(CacheCommandRequest{Command: "SHIFTSLICE", Key: "jobs"}, "build")
		requireValue(CacheCommandRequest{Command: "HEADSLICE", Key: "jobs"}, "verify")

		requireValue(CacheCommandRequest{Command: "ADDSET", Key: "tags", Values: Slice{"go", "cache", "go"}}, "2")
		requireValue(CacheCommandRequest{Command: "HASSET", Key: "tags", Value: "go"}, "1")
		requireValue(CacheCommandRequest{Command: "REMSET", Key: "tags", Value: "go"}, "1")
		requireValue(CacheCommandRequest{Command: "HASSET", Key: "tags", Value: "go"}, "0")
		if value := requireJSON(CacheCommandRequest{Command: "GETSET", Key: "tags"}); value != `["cache"]` {
			t.Fatalf("GETSET value = %s, want remaining member", value)
		}

		requireValue(CacheCommandRequest{Command: "PUSHPQ", Key: "queue", Priority: int64Ptr(10), Value: "urgent"}, "1")
		if value := requireJSON(CacheCommandRequest{Command: "PEEKPQ", Key: "queue"}); value != `{"priority":10,"value":"urgent"}` {
			t.Fatalf("PEEKPQ value = %s, want priority item", value)
		}
		if value := requireJSON(CacheCommandRequest{Command: "GETPQ", Key: "queue"}); !strings.Contains(value, "urgent") {
			t.Fatalf("GETPQ value = %s, want queued value", value)
		}
		if value := requireJSON(CacheCommandRequest{Command: "POPPQ", Key: "queue"}); value != `{"priority":10,"value":"urgent"}` {
			t.Fatalf("POPPQ value = %s, want removed priority item", value)
		}
	})

	t.Run("filters and sketches", func(t *testing.T) {
		run(CacheCommandRequest{Command: "CREATEBF", Key: "bloom", Value: "1000", Subkey: "0.01"})
		requireValue(CacheCommandRequest{Command: "ADDBF", Key: "bloom", Value: "alice"}, "1")
		requireValue(CacheCommandRequest{Command: "HASBF", Key: "bloom", Value: "alice"}, "1")
		requireJSON(CacheCommandRequest{Command: "INFOBF", Key: "bloom"})

		run(CacheCommandRequest{Command: "CREATECF", Key: "cuckoo", Value: "1000", Subkey: "0.01"})
		requireValue(CacheCommandRequest{Command: "ADDCF", Key: "cuckoo", Value: "alice"}, "1")
		requireValue(CacheCommandRequest{Command: "HASCF", Key: "cuckoo", Value: "alice"}, "1")
		requireValue(CacheCommandRequest{Command: "DELCF", Key: "cuckoo", Value: "alice"}, "1")
		requireValue(CacheCommandRequest{Command: "HASCF", Key: "cuckoo", Value: "alice"}, "0")
		requireJSON(CacheCommandRequest{Command: "INFOCF", Key: "cuckoo"})

		run(CacheCommandRequest{Command: "CREATEXF", Key: "xor", Value: "1000"})
		requireValue(CacheCommandRequest{Command: "ADDXF", Key: "xor", Value: "alice"}, "1")
		if response := ht.ExecuteCommand(CacheCommandRequest{Command: "HASXF", Key: "xor", Value: "alice"}); response.OK {
			t.Fatalf("HASXF before BUILDXF response = %#v, want error", response)
		}
		requireJSON(CacheCommandRequest{Command: "BUILDXF", Key: "xor"})
		requireValue(CacheCommandRequest{Command: "HASXF", Key: "xor", Value: "alice"}, "1")
		requireJSON(CacheCommandRequest{Command: "INFOXF", Key: "xor"})

		run(CacheCommandRequest{Command: "CREATECMS", Key: "cms", Value: "256", Subkey: "4"})
		requireValue(CacheCommandRequest{Command: "INCRCMS", Key: "cms", Value: "path", Subkey: "3"}, "3")
		requireValue(CacheCommandRequest{Command: "ESTCMS", Key: "cms", Value: "path"}, "3")
		requireJSON(CacheCommandRequest{Command: "INFOCMS", Key: "cms"})

		run(CacheCommandRequest{Command: "CREATEHLL", Key: "hll", Value: "14"})
		if value := run(CacheCommandRequest{Command: "ADDHLL", Key: "hll", Values: Slice{"alice", "bob"}}).Value; value == "0" {
			t.Fatalf("ADDHLL value = %q, want positive cardinality", value)
		}
		if value := run(CacheCommandRequest{Command: "COUNTHLL", Key: "hll"}).Value; value == "0" {
			t.Fatalf("COUNTHLL value = %q, want positive cardinality", value)
		}
		requireJSON(CacheCommandRequest{Command: "INFOHLL", Key: "hll"})

		run(CacheCommandRequest{Command: "CREATETOPK", Key: "top", Value: "3"})
		if value := requireJSON(CacheCommandRequest{Command: "ADDTOPK", Key: "top", Value: "alpha", Subkey: "5"}); !strings.Contains(value, `"count":5`) {
			t.Fatalf("ADDTOPK value = %s, want tracked count", value)
		}
		if value := requireJSON(CacheCommandRequest{Command: "ESTTOPK", Key: "top", Value: "alpha"}); !strings.Contains(value, "5") {
			t.Fatalf("ESTTOPK value = %s, want count", value)
		}
		if value := requireJSON(CacheCommandRequest{Command: "GETTOPK", Key: "top"}); !strings.Contains(value, "alpha") {
			t.Fatalf("GETTOPK value = %s, want alpha", value)
		}
		requireJSON(CacheCommandRequest{Command: "INFOTOPK", Key: "top"})

		run(CacheCommandRequest{Command: "CREATERS", Key: "sample", Value: "3"})
		requireJSON(CacheCommandRequest{Command: "ADDRS", Key: "sample", Values: Slice{"alpha", "beta"}})
		if value := requireJSON(CacheCommandRequest{Command: "GETRS", Key: "sample"}); !strings.Contains(value, "alpha") || !strings.Contains(value, "beta") {
			t.Fatalf("GETRS value = %s, want initial sample", value)
		}
		requireJSON(CacheCommandRequest{Command: "INFORS", Key: "sample"})

		run(CacheCommandRequest{Command: "CREATEQ", Key: "latency", Value: "0.01"})
		requireJSON(CacheCommandRequest{Command: "ADDQ", Key: "latency", Values: Slice{"10", "20", "30"}})
		requireJSON(CacheCommandRequest{Command: "ESTQ", Key: "latency", Value: "0.5"})
		requireJSON(CacheCommandRequest{Command: "INFOQ", Key: "latency"})
	})

	t.Run("bitmaps and indexes", func(t *testing.T) {
		run(CacheCommandRequest{Command: "CREATERB", Key: "roaring"})
		requireValue(CacheCommandRequest{Command: "ADDRB", Key: "roaring", Values: Slice{"7", "42"}}, "2")
		requireValue(CacheCommandRequest{Command: "HASRB", Key: "roaring", Value: "42"}, "1")
		requireValue(CacheCommandRequest{Command: "COUNTRB", Key: "roaring"}, "2")
		requireJSON(CacheCommandRequest{Command: "GETRB", Key: "roaring"})
		requireValue(CacheCommandRequest{Command: "REMRB", Key: "roaring", Value: "7"}, "1")
		requireJSON(CacheCommandRequest{Command: "INFORB", Key: "roaring"})

		run(CacheCommandRequest{Command: "CREATESB", Key: "sparse"})
		requireValue(CacheCommandRequest{Command: "ADDSB", Key: "sparse", Values: Slice{"7", "18446744073709551615"}}, "2")
		requireValue(CacheCommandRequest{Command: "HASSB", Key: "sparse", Value: "7"}, "1")
		requireValue(CacheCommandRequest{Command: "COUNTSB", Key: "sparse"}, "2")
		requireJSON(CacheCommandRequest{Command: "GETSB", Key: "sparse"})
		requireValue(CacheCommandRequest{Command: "REMSB", Key: "sparse", Value: "7"}, "1")
		requireJSON(CacheCommandRequest{Command: "INFOSB", Key: "sparse"})

		run(CacheCommandRequest{Command: "CREATERT", Key: "radix"})
		requireValue(CacheCommandRequest{Command: "PUTRT", Key: "radix", Subkey: "user:7/profile", Value: "active"}, "1")
		requireValue(CacheCommandRequest{Command: "GETRT", Key: "radix", Subkey: "user:7/profile"}, "active")
		requireValue(CacheCommandRequest{Command: "HASRT", Key: "radix", Subkey: "user:7/profile"}, "1")
		requireJSON(CacheCommandRequest{Command: "PREFIXRT", Key: "radix", Subkey: "user:"})
		requireValue(CacheCommandRequest{Command: "DELRT", Key: "radix", Subkey: "user:7/profile"}, "1")
		requireJSON(CacheCommandRequest{Command: "INFORT", Key: "radix"})

		run(CacheCommandRequest{Command: "CREATEFW", Key: "fenwick", Value: "24"})
		requireJSON(CacheCommandRequest{Command: "ADDFW", Key: "fenwick", Value: "13", Subkey: "7"})
		requireValue(CacheCommandRequest{Command: "GETFW", Key: "fenwick", Value: "13"}, "7")
		requireValue(CacheCommandRequest{Command: "SUMFW", Key: "fenwick", Value: "13"}, "7")
		requireValue(CacheCommandRequest{Command: "RANGEFW", Key: "fenwick", Value: "8", Subkey: "13"}, "7")
		requireJSON(CacheCommandRequest{Command: "INFOFW", Key: "fenwick"})
	})
}

func int64Ptr(value int64) *int64 { return &value }
