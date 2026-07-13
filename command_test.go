package hatriecache

import (
	"encoding/json"
	"reflect"
	"strconv"
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
		{Command: "CREATEBF", Key: "key", Value: "0"},
		{Command: "CREATEBF", Key: "key", Pairs: Map{"expected_items": 1.5}},
		{Command: "CREATEBF", Key: "key", Pairs: Map{"false_positive_rate": "not-number"}},
		{Command: "ADDBF", Key: "key"},
		{Command: "HASBF", Key: "key"},
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
