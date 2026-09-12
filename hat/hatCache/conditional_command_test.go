package hatCache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestExecuteCommandCASString(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("cas:key", "old")

	response := ht.ExecuteCommand(CacheCommandRequest{
		Command:       "CAS",
		Key:           "cas:key",
		ExpectedValue: "old",
		Value:         "new",
	})
	if !response.OK || response.Value != "1" {
		t.Fatalf("successful CAS response = %#v, want ok/1", response)
	}
	if got := ht.GetString("cas:key"); got != "new" {
		t.Fatalf("successful CAS value = %q, want new", got)
	}

	response = ht.ExecuteCommand(CacheCommandRequest{
		Command:       "COMPARESET",
		Key:           "cas:key",
		ExpectedValue: "old",
		Value:         "should-not-write",
	})
	if !response.OK || response.Value != "0" {
		t.Fatalf("failed CAS response = %#v, want ok/0", response)
	}
	if got := ht.GetString("cas:key"); got != "new" {
		t.Fatalf("failed CAS changed value = %q, want new", got)
	}
}

func TestExecuteCommandCASStringHandlesEmptyAndMissingValues(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("cas:empty", "")

	response := ht.ExecuteCommand(CacheCommandRequest{
		Command:       "CAS",
		Key:           "cas:empty",
		ExpectedValue: "",
		Value:         "filled",
	})
	if !response.OK || response.Value != "1" {
		t.Fatalf("empty-string CAS response = %#v, want ok/1", response)
	}

	response = ht.ExecuteCommand(CacheCommandRequest{
		Command:       "CAS",
		Key:           "cas:missing",
		ExpectedValue: "",
		Value:         "created-by-mistake",
	})
	if !response.OK || response.Value != "0" {
		t.Fatalf("missing-key CAS response = %#v, want ok/0", response)
	}
	if ht.Exists("cas:missing") {
		t.Fatal("missing-key CAS created a key")
	}
}

func TestCompareAndSwapStringIsAtomic(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("cas:atomic", "old")

	const attempts = 32
	var start sync.WaitGroup
	var done sync.WaitGroup
	var successes atomic.Int32
	start.Add(1)
	done.Add(attempts)
	for range attempts {
		go func() {
			defer done.Done()
			start.Wait()
			swapped, err := ht.CompareAndSwapString("cas:atomic", "old", "new")
			if err != nil {
				t.Errorf("CompareAndSwapString() error = %v", err)
				return
			}
			if swapped {
				successes.Add(1)
			}
		}()
	}
	start.Done()
	done.Wait()

	if got := successes.Load(); got != 1 {
		t.Fatalf("successful CAS operations = %d, want 1", got)
	}
	if got := ht.GetString("cas:atomic"); got != "new" {
		t.Fatalf("atomic CAS value = %q, want new", got)
	}
}

func TestCompareAndSwapStringPreservesExpiration(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("cas:ttl", "old")
	if !ht.Expire("cas:ttl", time.Hour) {
		t.Fatal("Expire() failed")
	}

	swapped, err := ht.CompareAndSwapString("cas:ttl", "old", "new")
	if err != nil || !swapped {
		t.Fatalf("CompareAndSwapString() = %v/%v, want true/nil", swapped, err)
	}
	if ttl := ht.TTL("cas:ttl"); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("CAS expiration = %s, want positive value no greater than one hour", ttl)
	}
}

func TestCompareAndSwapStringDoesNotReplaceOtherTypes(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertCounter("cas:counter", 7)

	swapped, err := ht.CompareAndSwapString("cas:counter", "7", "8")
	if err != nil || swapped {
		t.Fatalf("counter CAS = %v/%v, want false/nil", swapped, err)
	}
	if got := ht.GetCounter("cas:counter"); got != 7 {
		t.Fatalf("counter after CAS = %d, want 7", got)
	}
}

func TestMonitoringCASJSONCommand(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("cas:http", "old")
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	request := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"CAS","key":"cas:http","expected_value":"old","value":"new"}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("CAS HTTP status = %d, want 200: %s", response.Code, response.Body.String())
	}
	var decoded CacheCommandResponse
	if err := json.NewDecoder(response.Body).Decode(&decoded); err != nil {
		t.Fatalf("decode CAS HTTP response: %v", err)
	}
	if !decoded.OK || decoded.Value != "1" || ht.GetString("cas:http") != "new" {
		t.Fatalf("CAS HTTP response/state = %#v/%q, want ok/1 and new", decoded, ht.GetString("cas:http"))
	}
}

func TestExecuteCommandCASParticipatesInAtomicBatchRollback(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("cas:batch", "old")

	response := ht.ExecuteCommand(CacheCommandRequest{
		Command: "BATCH",
		Atomic:  true,
		Batch: []CacheCommandRequest{
			{Command: "CAS", Key: "cas:batch", ExpectedValue: "old", Value: "new"},
			{Command: "SETINT", Key: "cas:invalid", Value: "not-an-int"},
		},
	})
	if response.OK {
		t.Fatalf("atomic CAS batch response = %#v, want failure", response)
	}
	if got := ht.GetString("cas:batch"); got != "old" {
		t.Fatalf("atomic CAS rollback value = %q, want old", got)
	}
}
