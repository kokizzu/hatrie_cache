package hatriecache

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestWriteJSONStatusWritesRequestedStatus(t *testing.T) {
	resp := httptest.NewRecorder()
	writeJSONStatus(resp, http.StatusCreated, commandError("created"))

	if resp.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusCreated)
	}
	if contentType := resp.Header().Get("Content-Type"); contentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", contentType)
	}
	var got CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatalf("response JSON error = %v", err)
	}
	if got.OK || got.Message != "created" {
		t.Fatalf("response = %#v, want error message", got)
	}
}

func TestWriteJSONStatusRejectsEncodeErrorBeforeHeader(t *testing.T) {
	resp := httptest.NewRecorder()
	writeJSONStatus(resp, http.StatusCreated, map[string]interface{}{"bad": make(chan int)})

	if resp.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusInternalServerError)
	}
	if resp.Body.Len() == 0 {
		t.Fatal("body is empty, want encoder error")
	}
}

func TestMonitoringHandlerRejectsNilTrieRoutes(t *testing.T) {
	handler := NewMonitoringHandler(nil, MonitoringOptions{}).Handler()
	tests := []struct {
		name   string
		method string
		path   string
		body   string
	}{
		{name: "health", method: http.MethodGet, path: "/api/health"},
		{name: "stats", method: http.MethodGet, path: "/api/stats"},
		{name: "entries", method: http.MethodGet, path: "/api/entries"},
		{name: "commands", method: http.MethodPost, path: "/api/commands", body: `{"command":"GET","key":"name"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp := httptest.NewRecorder()
			request := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			if tt.body != "" {
				request.Header.Set("Content-Type", "application/json")
			}
			handler.ServeHTTP(resp, request)
			if resp.Code != http.StatusServiceUnavailable {
				t.Fatalf("%s status = %d, want 503", tt.name, resp.Code)
			}
			var got CacheCommandResponse
			if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
				t.Fatalf("%s response JSON error = %v", tt.name, err)
			}
			if got.OK || got.Message != "trie is not configured" {
				t.Fatalf("%s response = %#v, want trie not configured error", tt.name, got)
			}
		})
	}
}

func TestMonitoringHandlerExposesHealthStatsAndEntries(t *testing.T) {
	ht := newTestTrie(t)
	now := time.Unix(1000, 0)
	ht.now = func() time.Time { return now }

	ht.UpsertString("session:1", "active user")
	ht.UpsertSet("session:tags", Set{"active", "paid"})
	ht.UpsertPriorityQueue("session:jobs", PriorityQueue{{Priority: 1, Value: "rebuild"}, {Priority: 5, Value: "compact"}})
	if err := ht.UpsertBloomFilter("session:seen", 1000, 0.001); err != nil {
		t.Fatalf("UpsertBloomFilter() error = %v", err)
	}
	ht.AddBloomFilter("session:seen", "email:1")
	if err := ht.UpsertCuckooFilter("session:cf", 128, 0.001); err != nil {
		t.Fatalf("UpsertCuckooFilter() error = %v", err)
	}
	ht.AddCuckooFilter("session:cf", "email:1")
	if err := ht.UpsertXorFilter("session:xor", 8); err != nil {
		t.Fatalf("UpsertXorFilter() error = %v", err)
	}
	if _, err := ht.AddXorFilter("session:xor", "email:1", "email:2"); err != nil {
		t.Fatalf("AddXorFilter() error = %v", err)
	}
	if _, ok, err := ht.BuildXorFilter("session:xor"); err != nil || !ok {
		t.Fatalf("BuildXorFilter() = %v/%v, want ok", err, ok)
	}
	if err := ht.UpsertCountMinSketch("session:freq", 128, 4); err != nil {
		t.Fatalf("UpsertCountMinSketch() error = %v", err)
	}
	ht.IncrementCountMinSketch("session:freq", "/api/users", 3)
	if err := ht.UpsertHyperLogLog("session:card", 10); err != nil {
		t.Fatalf("UpsertHyperLogLog() error = %v", err)
	}
	ht.AddHyperLogLog("session:card", "user:1", "user:2")
	if err := ht.UpsertTopK("session:top", 3); err != nil {
		t.Fatalf("UpsertTopK() error = %v", err)
	}
	ht.AddTopK("session:top", "/api/users", 7)
	ht.UpsertRoaringBitmap("session:bitmap")
	ht.AddRoaringBitmap("session:bitmap", 1, 1<<16+7)
	ht.UpsertSparseBitset("session:bitset")
	ht.AddSparseBitset("session:bitset", 1, 1<<32+7, ^uint64(0))
	if err := ht.UpsertQuantileSketch("session:zquantiles", 0.01); err != nil {
		t.Fatalf("UpsertQuantileSketch() error = %v", err)
	}
	ht.AddQuantileSketch("session:zquantiles", 10, 20, 30)
	if err := ht.UpsertFenwickTree("session:fenwick", 8); err != nil {
		t.Fatalf("UpsertFenwickTree() error = %v", err)
	}
	ht.AddFenwickTree("session:fenwick", 2, 5)
	ht.AddFenwickTree("session:fenwick", 6, 7)
	if err := ht.UpsertReservoirSample("session:zzsample", 3); err != nil {
		t.Fatalf("UpsertReservoirSample() error = %v", err)
	}
	ht.AddReservoirSample("session:zzsample", "/api/users", "/api/sessions", "/api/cache", "/api/health")
	ht.UpsertRadixTree("session:radix")
	ht.PutRadixTree("session:radix", "user:100/profile", "active")
	ht.PutRadixTree("session:radix", "user:101/profile", "idle")
	ht.UpsertCounter("counter:views", 42)
	if !ht.Expire("session:1", time.Minute) {
		t.Fatal("Expire(session:1) = false, want true")
	}
	bloomInfo, ok := ht.BloomFilterInfo("session:seen")
	if !ok {
		t.Fatal("BloomFilterInfo(session:seen) = false, want true")
	}
	cuckooInfo, ok := ht.CuckooFilterInfo("session:cf")
	if !ok {
		t.Fatal("CuckooFilterInfo(session:cf) = false, want true")
	}
	sketchInfo, ok := ht.CountMinSketchInfo("session:freq")
	if !ok {
		t.Fatal("CountMinSketchInfo(session:freq) = false, want true")
	}
	hllInfo, ok := ht.HyperLogLogInfo("session:card")
	if !ok {
		t.Fatal("HyperLogLogInfo(session:card) = false, want true")
	}
	topKInfo, ok := ht.TopKInfo("session:top")
	if !ok {
		t.Fatal("TopKInfo(session:top) = false, want true")
	}
	roaringInfo, ok := ht.RoaringBitmapInfo("session:bitmap")
	if !ok {
		t.Fatal("RoaringBitmapInfo(session:bitmap) = false, want true")
	}
	sparseInfo, ok := ht.SparseBitsetInfo("session:bitset")
	if !ok {
		t.Fatal("SparseBitsetInfo(session:bitset) = false, want true")
	}
	quantileInfo, ok := ht.QuantileSketchInfo("session:zquantiles")
	if !ok {
		t.Fatal("QuantileSketchInfo(session:zquantiles) = false, want true")
	}
	fenwickInfo, ok := ht.FenwickTreeInfo("session:fenwick")
	if !ok {
		t.Fatal("FenwickTreeInfo(session:fenwick) = false, want true")
	}
	reservoirInfo, ok := ht.ReservoirSampleInfo("session:zzsample")
	if !ok {
		t.Fatal("ReservoirSampleInfo(session:zzsample) = false, want true")
	}
	xorInfo, ok := ht.XorFilterInfo("session:xor")
	if !ok {
		t.Fatal("XorFilterInfo(session:xor) = false, want true")
	}
	radixInfo, ok := ht.RadixTreeInfo("session:radix")
	if !ok {
		t.Fatal("RadixTreeInfo(session:radix) = false, want true")
	}

	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName: "test-node",
		Version:  "v1.2.3-test",
		StartAt:  now.Add(-time.Hour),
	}).Handler()

	healthResp := httptest.NewRecorder()
	handler.ServeHTTP(healthResp, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	if healthResp.Code != http.StatusOK {
		t.Fatalf("health status = %d, want 200", healthResp.Code)
	}
	var health MonitoringHealth
	if err := json.Unmarshal(healthResp.Body.Bytes(), &health); err != nil {
		t.Fatalf("health JSON error = %v", err)
	}
	if health.Node != "test-node" || health.Status != "online" || health.APIVersion != MonitoringAPIVersion || health.Version != "v1.2.3-test" {
		t.Fatalf("health = %#v, want test-node online", health)
	}

	statsResp := httptest.NewRecorder()
	handler.ServeHTTP(statsResp, httptest.NewRequest(http.MethodGet, "/api/stats", nil))
	if statsResp.Code != http.StatusOK {
		t.Fatalf("stats status = %d, want 200", statsResp.Code)
	}
	var stats CacheStats
	if err := json.Unmarshal(statsResp.Body.Bytes(), &stats); err != nil {
		t.Fatalf("stats JSON error = %v", err)
	}
	if stats.Writes == 0 {
		t.Fatalf("stats writes = 0, want existing cache writes")
	}

	entriesResp := httptest.NewRecorder()
	handler.ServeHTTP(entriesResp, httptest.NewRequest(http.MethodGet, "/api/entries?prefix=session:", nil))
	if entriesResp.Code != http.StatusOK {
		t.Fatalf("entries status = %d, want 200", entriesResp.Code)
	}
	var entries MonitoringEntriesResponse
	if err := json.Unmarshal(entriesResp.Body.Bytes(), &entries); err != nil {
		t.Fatalf("entries JSON error = %v", err)
	}
	if len(entries.Entries) != 15 {
		t.Fatalf("entries len = %d, want 15: %#v", len(entries.Entries), entries.Entries)
	}
	entry := entries.Entries[0]
	if entry.Key != "session:1" || entry.Type != "string" || entry.ValuePreview != "active user" {
		t.Fatalf("entry = %#v, want session string preview", entry)
	}
	if entry.TTLMillis == nil || *entry.TTLMillis != int64(time.Minute/time.Millisecond) {
		t.Fatalf("entry TTL = %v, want 60000", entry.TTLMillis)
	}
	roaringEntry := entries.Entries[1]
	wantRoaringPreview := strconv.FormatUint(roaringInfo.Cardinality, 10) + " integers, " + strconv.FormatUint(roaringInfo.Containers, 10) + " containers"
	if roaringEntry.Key != "session:bitmap" || roaringEntry.Type != "roaring_bitmap" || roaringEntry.SizeBytes != int64(roaringInfo.EncodedBytes) || roaringEntry.ValuePreview != wantRoaringPreview {
		t.Fatalf("roaring bitmap entry = %#v, want compact integer-set preview", roaringEntry)
	}
	sparseEntry := entries.Entries[2]
	wantSparsePreview := strconv.FormatUint(sparseInfo.Cardinality, 10) + " integers, " + strconv.FormatUint(sparseInfo.Containers, 10) + " containers"
	if sparseEntry.Key != "session:bitset" || sparseEntry.Type != "sparse_bitset" || sparseEntry.SizeBytes != int64(sparseInfo.EncodedBytes) || sparseEntry.ValuePreview != wantSparsePreview {
		t.Fatalf("sparse bitset entry = %#v, want compact uint64-set preview", sparseEntry)
	}
	hllEntry := entries.Entries[3]
	wantHLLPreview := strconv.Itoa(int(hllInfo.Precision)) + " precision, " + strconv.FormatUint(hllInfo.Estimate, 10) + " estimated"
	if hllEntry.Key != "session:card" || hllEntry.Type != "hyperloglog" || hllEntry.SizeBytes != int64(hllInfo.RegisterBytes) || hllEntry.ValuePreview != wantHLLPreview {
		t.Fatalf("hyperloglog entry = %#v, want compact register preview", hllEntry)
	}
	cuckooEntry := entries.Entries[4]
	wantCuckooPreview := strconv.FormatUint(cuckooInfo.Count, 10) + "/" + strconv.FormatUint(cuckooInfo.Capacity, 10) + " slots, " + strconv.Itoa(int(cuckooInfo.FingerprintBits)) + "-bit fingerprints"
	if cuckooEntry.Key != "session:cf" || cuckooEntry.Type != "cuckoo_filter" || cuckooEntry.SizeBytes != int64(cuckooInfo.FingerprintBytes) || cuckooEntry.ValuePreview != wantCuckooPreview {
		t.Fatalf("cuckoo filter entry = %#v, want compact fingerprint preview", cuckooEntry)
	}
	fenwickEntry := entries.Entries[5]
	wantFenwickPreview := strconv.FormatUint(fenwickInfo.Size, 10) + " counters, " + strconv.FormatInt(fenwickInfo.Total, 10) + " total"
	if fenwickEntry.Key != "session:fenwick" || fenwickEntry.Type != "fenwick_tree" || fenwickEntry.SizeBytes != int64(fenwickInfo.TreeBytes) || fenwickEntry.ValuePreview != wantFenwickPreview {
		t.Fatalf("fenwick tree entry = %#v, want compact prefix-sum preview", fenwickEntry)
	}
	sketchEntry := entries.Entries[6]
	wantSketchPreview := strconv.FormatUint(sketchInfo.Width, 10) + "x" + strconv.Itoa(int(sketchInfo.Depth)) + " counters, " + strconv.FormatUint(sketchInfo.TotalCount, 10) + " total"
	if sketchEntry.Key != "session:freq" || sketchEntry.Type != "count_min_sketch" || sketchEntry.SizeBytes != int64(sketchInfo.CounterBytes) || sketchEntry.ValuePreview != wantSketchPreview {
		t.Fatalf("count-min sketch entry = %#v, want compact counter preview", sketchEntry)
	}
	queueEntry := entries.Entries[7]
	if queueEntry.Key != "session:jobs" || queueEntry.Type != "priority_queue" || queueEntry.SizeBytes != 2 || queueEntry.ValuePreview != "2 priority items" {
		t.Fatalf("priority queue entry = %#v, want priority queue item preview", queueEntry)
	}
	radixEntry := entries.Entries[8]
	wantRadixPreview := strconv.FormatUint(radixInfo.Items, 10) + " items, " + strconv.FormatUint(radixInfo.Nodes, 10) + " nodes"
	if radixEntry.Key != "session:radix" || radixEntry.Type != "radix_tree" || radixEntry.SizeBytes != int64(radixInfo.EncodedBytes) || radixEntry.ValuePreview != wantRadixPreview {
		t.Fatalf("radix tree entry = %#v, want compact prefix-tree preview", radixEntry)
	}
	bloomEntry := entries.Entries[9]
	wantBloomPreview := strconv.FormatUint(bloomInfo.BitCount, 10) + " bits, " + strconv.Itoa(int(bloomInfo.HashCount)) + " hashes"
	if bloomEntry.Key != "session:seen" || bloomEntry.Type != "bloom_filter" || bloomEntry.SizeBytes != int64(bloomInfo.BitBytes) || bloomEntry.ValuePreview != wantBloomPreview {
		t.Fatalf("bloom filter entry = %#v, want compact bitset preview", bloomEntry)
	}
	setEntry := entries.Entries[10]
	if setEntry.Key != "session:tags" || setEntry.Type != "set" || setEntry.SizeBytes != 2 || setEntry.ValuePreview != "2 members" {
		t.Fatalf("set entry = %#v, want set member preview", setEntry)
	}
	topKEntry := entries.Entries[11]
	wantTopKPreview := strconv.FormatUint(topKInfo.Tracked, 10) + "/" + strconv.FormatUint(topKInfo.Capacity, 10) + " tracked, " + strconv.FormatUint(topKInfo.Total, 10) + " total"
	if topKEntry.Key != "session:top" || topKEntry.Type != "top_k" || topKEntry.ValuePreview != wantTopKPreview || topKEntry.SizeBytes <= 0 {
		t.Fatalf("top-k entry = %#v, want compact heavy-hitter preview", topKEntry)
	}
	xorEntry := entries.Entries[12]
	wantXorPreview := strconv.FormatUint(xorInfo.Items, 10) + " items, " + strconv.FormatUint(xorInfo.FingerprintBytes, 10) + " fingerprint bytes"
	if xorEntry.Key != "session:xor" || xorEntry.Type != "xor_filter" || xorEntry.SizeBytes != int64(xorInfo.FingerprintBytes) || xorEntry.ValuePreview != wantXorPreview {
		t.Fatalf("xor filter entry = %#v, want compact static-membership preview", xorEntry)
	}
	quantileEntry := entries.Entries[13]
	wantQuantilePreview := strconv.FormatUint(quantileInfo.Count, 10) + " samples, " + strconv.FormatUint(quantileInfo.SummarySize, 10) + " summary points"
	if quantileEntry.Key != "session:zquantiles" || quantileEntry.Type != "quantile_sketch" || quantileEntry.SizeBytes != quantileInfo.EncodedBytes || quantileEntry.ValuePreview != wantQuantilePreview {
		t.Fatalf("quantile sketch entry = %#v, want compact quantile preview", quantileEntry)
	}
	reservoirEntry := entries.Entries[14]
	wantReservoirPreview := strconv.FormatUint(reservoirInfo.Tracked, 10) + "/" + strconv.FormatUint(reservoirInfo.Capacity, 10) + " sampled, " + strconv.FormatUint(reservoirInfo.Seen, 10) + " seen"
	if reservoirEntry.Key != "session:zzsample" || reservoirEntry.Type != "reservoir_sample" || reservoirEntry.SizeBytes != reservoirInfo.EncodedBytes || reservoirEntry.ValuePreview != wantReservoirPreview {
		t.Fatalf("reservoir sample entry = %#v, want compact bounded-sample preview", reservoirEntry)
	}
}

func TestMonitoringHandlerLimitsEntries(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("session:1", "one")
	ht.UpsertString("session:2", "two")
	ht.UpsertString("session:3", "three")
	ht.UpsertString("other:1", "ignored")
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/entries?prefix=session:&limit=2", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("limited entries status = %d, want 200", resp.Code)
	}
	var body MonitoringEntriesResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("limited entries JSON error = %v", err)
	}
	if body.Limit != 2 || !body.HasMore {
		t.Fatalf("limited entries metadata = limit %d has_more %v, want 2/true", body.Limit, body.HasMore)
	}
	if body.NextAfterKey != "session:2" {
		t.Fatalf("limited entries next_after_key = %q, want session:2", body.NextAfterKey)
	}
	if got := entryKeysFromMonitoring(body.Entries); !reflect.DeepEqual(got, []string{"session:1", "session:2"}) {
		t.Fatalf("limited entries keys = %#v, want first two sorted session keys", got)
	}

	nextResp := httptest.NewRecorder()
	handler.ServeHTTP(nextResp, httptest.NewRequest(http.MethodGet, "/api/entries?prefix=session:&limit=2&after_key=session:2", nil))
	if nextResp.Code != http.StatusOK {
		t.Fatalf("next entries status = %d, want 200", nextResp.Code)
	}
	var nextBody MonitoringEntriesResponse
	if err := json.Unmarshal(nextResp.Body.Bytes(), &nextBody); err != nil {
		t.Fatalf("next entries JSON error = %v", err)
	}
	if nextBody.AfterKey != "session:2" || nextBody.HasMore || nextBody.NextAfterKey != "" {
		t.Fatalf("next entries metadata = after %q has_more %v next %q, want session:2/false/empty", nextBody.AfterKey, nextBody.HasMore, nextBody.NextAfterKey)
	}
	if got := entryKeysFromMonitoring(nextBody.Entries); !reflect.DeepEqual(got, []string{"session:3"}) {
		t.Fatalf("next entries keys = %#v, want remaining session key", got)
	}

	exactResp := httptest.NewRecorder()
	handler.ServeHTTP(exactResp, httptest.NewRequest(http.MethodGet, "/api/entries?prefix=session:&limit=3", nil))
	if exactResp.Code != http.StatusOK {
		t.Fatalf("exact entries status = %d, want 200", exactResp.Code)
	}
	var exactBody MonitoringEntriesResponse
	if err := json.Unmarshal(exactResp.Body.Bytes(), &exactBody); err != nil {
		t.Fatalf("exact entries JSON error = %v", err)
	}
	if exactBody.Limit != 3 || exactBody.HasMore {
		t.Fatalf("exact entries metadata = limit %d has_more %v, want 3/false", exactBody.Limit, exactBody.HasMore)
	}
	if got := entryKeysFromMonitoring(exactBody.Entries); !reflect.DeepEqual(got, []string{"session:1", "session:2", "session:3"}) {
		t.Fatalf("exact entries keys = %#v, want all sorted session keys", got)
	}
}

func TestMonitoringHandlerPagesAfterEmptyKey(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("", "empty")
	ht.UpsertString("alpha", "one")
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/entries?limit=1", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("limited entries status = %d, want 200", resp.Code)
	}
	var body MonitoringEntriesResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("limited entries JSON error = %v", err)
	}
	if !body.HasMore || body.NextAfterKey != "" {
		t.Fatalf("limited entries metadata = has_more %v next %q, want true/empty", body.HasMore, body.NextAfterKey)
	}
	if !bytes.Contains(resp.Body.Bytes(), []byte(`"next_after_key":""`)) {
		t.Fatalf("limited entries JSON = %s, want present empty next_after_key", resp.Body.String())
	}
	if got := entryKeysFromMonitoring(body.Entries); !reflect.DeepEqual(got, []string{""}) {
		t.Fatalf("limited entries keys = %#v, want empty key", got)
	}

	nextResp := httptest.NewRecorder()
	handler.ServeHTTP(nextResp, httptest.NewRequest(http.MethodGet, "/api/entries?limit=1&after_key=", nil))
	if nextResp.Code != http.StatusOK {
		t.Fatalf("next entries status = %d, want 200", nextResp.Code)
	}
	var nextBody MonitoringEntriesResponse
	if err := json.Unmarshal(nextResp.Body.Bytes(), &nextBody); err != nil {
		t.Fatalf("next entries JSON error = %v", err)
	}
	if nextBody.AfterKey != "" || nextBody.HasMore || nextBody.NextAfterKey != "" {
		t.Fatalf("next entries metadata = after %q has_more %v next %q, want empty/false/empty", nextBody.AfterKey, nextBody.HasMore, nextBody.NextAfterKey)
	}
	if !bytes.Contains(nextResp.Body.Bytes(), []byte(`"after_key":""`)) {
		t.Fatalf("next entries JSON = %s, want present empty after_key", nextResp.Body.String())
	}
	if got := entryKeysFromMonitoring(nextBody.Entries); !reflect.DeepEqual(got, []string{"alpha"}) {
		t.Fatalf("next entries keys = %#v, want key after empty cursor", got)
	}
}

func TestMonitoringHandlerRejectsInvalidEntriesLimit(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("session:1", "one")
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	for _, target := range []string{"/api/entries?limit=0", "/api/entries?limit=bad", "/api/entries?limit=100001", "/api/entries?prefix=session:&after_key=other:1"} {
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, target, nil))
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", target, resp.Code)
		}
		var body CacheCommandResponse
		if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s JSON error = %v", target, err)
		}
		if body.OK || !(strings.Contains(body.Message, "limit") || strings.Contains(body.Message, "after_key")) {
			t.Fatalf("%s body = %#v, want entries validation error", target, body)
		}
	}
}

func TestMonitoringHandlerCompressesJSONWhenAccepted(t *testing.T) {
	ht := newTestTrie(t)
	for idx := 0; idx < 32; idx++ {
		ht.UpsertString("session:"+strconv.Itoa(idx), strings.Repeat("value", 16))
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/entries?prefix=session:", nil)
	request.Header.Set("Accept-Encoding", "br, gzip")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, request)
	if resp.Code != http.StatusOK {
		t.Fatalf("entries status = %d, want 200", resp.Code)
	}
	if got := resp.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	if got := resp.Header().Values("Vary"); !headerValuesContain(got, "Accept-Encoding") {
		t.Fatalf("Vary = %#v, want Accept-Encoding", got)
	}

	data := gunzipResponseBody(t, resp.Body.Bytes())
	var body MonitoringEntriesResponse
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatalf("compressed entries JSON error = %v", err)
	}
	if len(body.Entries) != 32 {
		t.Fatalf("compressed entries count = %d, want 32", len(body.Entries))
	}
}

func TestMonitoringHandlerSkipsGzipWhenNotAccepted(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("session:1", "active")
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/entries?prefix=session:", nil)
	request.Header.Set("Accept-Encoding", "br")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, request)
	if resp.Code != http.StatusOK {
		t.Fatalf("entries status = %d, want 200", resp.Code)
	}
	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty", got)
	}
	var body MonitoringEntriesResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &body); err != nil {
		t.Fatalf("plain entries JSON error = %v", err)
	}
	if len(body.Entries) != 1 {
		t.Fatalf("plain entries count = %d, want 1", len(body.Entries))
	}
}

func TestMonitoringHandlerHonorsGzipQualityZero(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("session:1", "active")
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodGet, "/api/entries?prefix=session:", nil)
	request.Header.Set("Accept-Encoding", "gzip;q=0, *;q=1")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, request)
	if resp.Code != http.StatusOK {
		t.Fatalf("entries status = %d, want 200", resp.Code)
	}
	if got := resp.Header().Get("Content-Encoding"); got != "" {
		t.Fatalf("Content-Encoding = %q, want empty when gzip q=0", got)
	}
}

func TestMonitoringHandlerAcceptsGzipCommandRequest(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	body := gzipBytes(t, []byte(`{"command":"SETSTR","key":"compressed","value":"ok"}`))
	request := httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewReader(body))
	request.Header.Set("Content-Encoding", "gzip")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, request)
	if resp.Code != http.StatusOK {
		t.Fatalf("compressed command status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	if got := ht.GetString("compressed"); got != "ok" {
		t.Fatalf("compressed command stored %q, want ok", got)
	}
}

func TestMonitoringHandlerRejectsOversizedGzipCommandRequest(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	body := gzipBytes(t, []byte(`{"command":"SETSTR","key":"compressed","value":"`+strings.Repeat("x", maxMonitoringJSONRequestBytes)+`"}`))
	request := httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewReader(body))
	request.Header.Set("Content-Encoding", "gzip")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, request)
	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized compressed command status = %d, want 413: %s", resp.Code, resp.Body.String())
	}
	if got := ht.GetString("compressed"); got != "" {
		t.Fatalf("oversized compressed command stored %q, want empty", got)
	}
}

func TestMonitoringHandlerRejectsOneByteOversizedJSONCommandRequest(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	prefix := `{"command":"SETSTR","key":"exact","value":"`
	suffix := `"}`
	valueLen := maxMonitoringJSONRequestBytes + 1 - len(prefix) - len(suffix)
	if valueLen <= 0 {
		t.Fatalf("test request framing length = %d, want room for value", valueLen)
	}
	body := prefix + strings.Repeat("x", valueLen) + suffix
	if len(body) != maxMonitoringJSONRequestBytes+1 {
		t.Fatalf("body length = %d, want %d", len(body), maxMonitoringJSONRequestBytes+1)
	}

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(body)))
	if resp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("one-byte oversized command status = %d, want 413: %s", resp.Code, resp.Body.String())
	}
	if got := ht.GetString("exact"); got != "" {
		t.Fatalf("one-byte oversized command stored %q, want empty", got)
	}
}

func TestMonitoringHandlerRejectsUnsupportedRequestEncoding(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"GET","key":"x"}`))
	request.Header.Set("Content-Encoding", "br")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, request)
	if resp.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("unsupported encoding status = %d, want 415", resp.Code)
	}
	if got := ht.GetString("x"); got != "" {
		t.Fatalf("unsupported encoding mutated trie: x=%q", got)
	}
}

func gunzipResponseBody(t *testing.T, data []byte) []byte {
	t.Helper()

	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("NewReader(gzip response) error = %v", err)
	}
	defer reader.Close()
	out, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll(gzip response) error = %v", err)
	}
	return out
}

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()

	var buffer bytes.Buffer
	writer := gzip.NewWriter(&buffer)
	if _, err := writer.Write(data); err != nil {
		t.Fatalf("gzip Write() error = %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("gzip Close() error = %v", err)
	}
	return buffer.Bytes()
}

func headerValuesContain(values []string, want string) bool {
	for _, value := range values {
		for _, token := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(token), want) {
				return true
			}
		}
	}
	return false
}

func TestMonitoringHandlerPreviewsCoreValueFamilies(t *testing.T) {
	ht := newTestTrie(t)
	memoryBytes := []byte{1, 2, 3, 4, 5}
	diskBytes := bytes.Repeat([]byte("x"), DiskBytesThreshold+1)
	ht.UpsertCounter("preview:counter", 7)
	ht.UpsertBytes("preview:bytes:disk", diskBytes)
	ht.UpsertBytes("preview:bytes:memory", memoryBytes)
	ht.UpsertMap("preview:map", Map{"first": "one", "second": 2})
	ht.UpsertSlice("preview:slice", Slice{"first", "second"})

	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/entries?prefix=preview:", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("entries status = %d, want 200", resp.Code)
	}
	var entries MonitoringEntriesResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &entries); err != nil {
		t.Fatalf("entries JSON error = %v", err)
	}
	if len(entries.Entries) != 5 {
		t.Fatalf("entries len = %d, want 5: %#v", len(entries.Entries), entries.Entries)
	}
	byKey := make(map[string]MonitoringEntry, len(entries.Entries))
	for _, entry := range entries.Entries {
		byKey[entry.Key] = entry
	}

	counter := byKey["preview:counter"]
	if counter.Type != "counter" || counter.SizeBytes != 4 || counter.ValuePreview != "7" || counter.OnDisk {
		t.Fatalf("counter entry = %#v, want counter preview", counter)
	}
	disk := byKey["preview:bytes:disk"]
	wantDiskPreview := strconv.Itoa(len(diskBytes)) + " bytes"
	if disk.Type != "bytes" || !disk.OnDisk || disk.SizeBytes != int64(len(diskBytes)) || disk.ValuePreview != wantDiskPreview {
		t.Fatalf("disk bytes entry = %#v, want on-disk byte preview", disk)
	}
	memory := byKey["preview:bytes:memory"]
	if memory.Type != "bytes" || memory.OnDisk || memory.SizeBytes != int64(len(memoryBytes)) || memory.ValuePreview != strconv.Itoa(len(memoryBytes))+" bytes" {
		t.Fatalf("memory bytes entry = %#v, want in-memory byte preview", memory)
	}
	mapEntry := byKey["preview:map"]
	if mapEntry.Type != "map" || mapEntry.SizeBytes != 2 || mapEntry.ValuePreview != "2 fields" || mapEntry.OnDisk {
		t.Fatalf("map entry = %#v, want field-count preview", mapEntry)
	}
	sliceEntry := byKey["preview:slice"]
	if sliceEntry.Type != "slice" || sliceEntry.SizeBytes != 2 || sliceEntry.ValuePreview != "2 items" || sliceEntry.OnDisk {
		t.Fatalf("slice entry = %#v, want item-count preview", sliceEntry)
	}
}

func TestMonitoringEntriesUseSingleExpirationClockRead(t *testing.T) {
	ht := newTestTrie(t)
	base := time.Unix(9100, 0)
	ht.now = func() time.Time { return base }

	ht.UpsertString("clock:a", "one")
	ht.UpsertString("clock:b", "two")
	if !ht.Expire("clock:a", time.Minute) {
		t.Fatal("Expire(clock:a) = false, want true")
	}
	if !ht.Expire("clock:b", time.Minute) {
		t.Fatal("Expire(clock:b) = false, want true")
	}

	clockReads := 0
	ht.now = func() time.Time {
		clockReads++
		return base.Add(30 * time.Second)
	}
	entries := ht.monitoringEntries("clock:")
	if len(entries) != 2 {
		t.Fatalf("monitoring entries len = %d, want 2: %#v", len(entries), entries)
	}
	for _, entry := range entries {
		if entry.TTLMillis == nil || *entry.TTLMillis <= 0 {
			t.Fatalf("entry TTL = %#v, want positive ttl", entry)
		}
	}
	if clockReads != 1 {
		t.Fatalf("monitoringEntries clock reads = %d, want 1", clockReads)
	}
}

func TestMonitoringEntriesRejectInvalidPrefixWithoutMutation(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("keep", "value")

	entries := ht.monitoringEntries(strings.Repeat("k", maxHATTrieKeyLength+1))
	if len(entries) != 0 {
		t.Fatalf("monitoringEntries(too-long prefix) = %#v, want empty", entries)
	}
	if got := ht.GetString("keep"); got != "value" {
		t.Fatalf("keep after rejected prefix = %q, want value", got)
	}
}

func TestMonitoringPreviewHandlesInvalidDiskByteIndex(t *testing.T) {
	ht := newTestTrie(t)
	for _, idx := range []int32{-1, 99} {
		size, preview := ht.monitoringPreviewLocked(HatValue{
			Index: idx,
			Flags: DATAVALUE_TYPE_RAW_BYTES | (1 << DATAVALUE_DISK_BIT_SHIFT),
		})
		if size != 0 || preview != "" {
			t.Fatalf("monitoringPreviewLocked(%d) = %d/%q, want empty preview", idx, size, preview)
		}
	}
}

func TestTruncatePreviewPreservesUTF8(t *testing.T) {
	value := strings.Repeat("a", 76) + "\u65e5" + strings.Repeat("b", 20)
	got := truncatePreview(value)
	if !utf8.ValidString(got) {
		t.Fatalf("truncatePreview produced invalid UTF-8: %q", got)
	}
	want := strings.Repeat("a", 76) + "..."
	if got != want {
		t.Fatalf("truncatePreview() = %q, want %q", got, want)
	}
}

func TestMonitoringHandlerServesStaticWebDir(t *testing.T) {
	ht := newTestTrie(t)
	dir := t.TempDir()
	indexPath := filepath.Join(dir, "index.html")
	if err := os.WriteFile(indexPath, []byte("monitoring ui"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	handler := NewMonitoringHandler(ht, MonitoringOptions{WebDir: dir}).Handler()
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("static status = %d, want 200", resp.Code)
	}
	if got := resp.Body.String(); got != "monitoring ui" {
		t.Fatalf("static body = %q, want monitoring ui", got)
	}
}

func TestMonitoringHandlerExecutesCommands(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	setResp := httptest.NewRecorder()
	handler.ServeHTTP(setResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"name","value":"ivi"}`)))
	if setResp.Code != http.StatusOK {
		t.Fatalf("SETSTR status = %d, want 200", setResp.Code)
	}
	var setResult CacheCommandResponse
	if err := json.Unmarshal(setResp.Body.Bytes(), &setResult); err != nil {
		t.Fatalf("SETSTR JSON error = %v", err)
	}
	if !setResult.OK {
		t.Fatalf("SETSTR response = %#v, want ok", setResult)
	}

	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"GET","key":"name"}`)))
	if getResp.Code != http.StatusOK {
		t.Fatalf("GET status = %d, want 200", getResp.Code)
	}
	var getResult CacheCommandResponse
	if err := json.Unmarshal(getResp.Body.Bytes(), &getResult); err != nil {
		t.Fatalf("GET JSON error = %v", err)
	}
	if !getResult.OK || getResult.Value != "ivi" {
		t.Fatalf("GET response = %#v, want ivi", getResult)
	}

	putMapResp := httptest.NewRecorder()
	handler.ServeHTTP(putMapResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"PUTMAP","key":"profile","pairs":{"age":32}}`)))
	if putMapResp.Code != http.StatusOK {
		t.Fatalf("PUTMAP status = %d, want 200", putMapResp.Code)
	}
	var putMapResult CacheCommandResponse
	if err := json.Unmarshal(putMapResp.Body.Bytes(), &putMapResult); err != nil {
		t.Fatalf("PUTMAP JSON error = %v", err)
	}
	if !putMapResult.OK {
		t.Fatalf("PUTMAP response = %#v, want ok", putMapResult)
	}
	if got := ht.PeekMap("profile", "age"); got != json.Number("32") {
		t.Fatalf("profile age = %#v, want json.Number(32)", got)
	}
}

func TestMonitoringHandlerExecutesPublicBatchCommand(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	body := `{"command":"BATCH","batch":[{"command":"SETSTR","key":"name","value":"ivi"},{"command":"GETSTR","key":"name"},{"command":"SETINT","key":"bad","value":"not-int"},{"command":"EXISTS","key":"name"}]}`
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(body)))
	if resp.Code != http.StatusOK {
		t.Fatalf("BATCH status = %d, want 200", resp.Code)
	}
	var result CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("BATCH JSON error = %v", err)
	}
	if result.OK {
		t.Fatalf("BATCH response = %#v, want aggregate failure", result)
	}
	if len(result.Responses) != 4 {
		t.Fatalf("BATCH responses len = %d, want 4", len(result.Responses))
	}
	if !result.Responses[1].OK || result.Responses[1].Value != "ivi" {
		t.Fatalf("BATCH GET response = %#v, want ivi", result.Responses[1])
	}
	if result.Responses[2].OK {
		t.Fatalf("BATCH invalid SETINT response = %#v, want failure", result.Responses[2])
	}
	if got := ht.GetString("name"); got != "ivi" {
		t.Fatalf("BATCH stored name = %q, want ivi", got)
	}
}

func TestExecutePublicCommandBatchFastPathReplicatesCapturedWritesInOneTargetBatch(t *testing.T) {
	var replicated []CacheCommandRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		replicated = append(replicated, mustDecodeReplicationTestCommand(t, w, r))
		writeJSON(w, CacheCommandResponse{OK: true, Message: "ok"})
	}))
	defer target.Close()

	ht := newTestTrie(t)
	topology := replicationTestTopology(t, target.URL)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Client:   target.Client(),
	})
	request := CacheCommandRequest{
		Command: "BATCH",
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: "session:batch", Value: "first"},
			{Command: "SETSTR", Key: "session:batch", Value: "second"},
			{Command: "GET", Key: "session:batch"},
		},
	}

	response, rejected := executeCacheCommand(context.Background(), ht, request, commandExecutionOptions{Replicator: replicator})
	if rejected || !response.OK || len(response.Responses) != 3 || response.Responses[2].Value != "second" {
		t.Fatalf("BATCH response = %#v rejected=%v, want ok with final GET second", response, rejected)
	}
	if len(replicated) != 1 {
		t.Fatalf("replicated requests len = %d, want one grouped target request", len(replicated))
	}
	payloads := mustDecodeReplicationBatchValues(t, replicated[0])
	if len(payloads) != 2 {
		t.Fatalf("replicated payloads len = %d, want two writes", len(payloads))
	}
	wantValues := []string{"first", "second"}
	for idx, payload := range payloads {
		if !isTypedReplicationSetPayload(payload, "session:batch") {
			t.Fatalf("replicated payload %d = %#v, want typed binary session:batch", idx, payload)
		}
		entry, err := decodeTypedReplicationSnapshot(payload.Key, payload.Command, payload.BinaryValue)
		if err != nil {
			t.Fatalf("replicated payload %d snapshot error = %v", idx, err)
		}
		if entry.String != wantValues[idx] {
			t.Fatalf("replicated payload %d string = %q, want %q", idx, entry.String, wantValues[idx])
		}
	}
}

func TestExecutePublicCommandBatchUsesProductionScalarFastPath(t *testing.T) {
	data, err := os.ReadFile("monitoring.go")
	if err != nil {
		t.Fatalf("ReadFile(monitoring.go) error = %v", err)
	}
	source := string(data)
	for _, token := range []string{
		"executePublicScalarBatchWithSideEffects",
		"executePublicScalarBatchCommandWithExecutor",
		"options.Replicator != nil",
	} {
		if !strings.Contains(source, token) {
			t.Fatalf("monitoring.go missing production scalar batch fast-path token %q", token)
		}
	}
}

func TestExecutePublicCommandBatchProductionFastPathPreservesJournalDirtyAndRollback(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertCounter("max", maxCommandInt32)
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatJSON)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()
	dirty := NewLevelDBDirtyTracker()
	request := CacheCommandRequest{
		Command: "BATCH",
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: "name", Value: "ivi"},
			{Command: "GETSTR", Key: "name"},
			{Command: "INC", Key: "max", Value: "1"},
			{Command: "SETSTR", Key: "city", Value: "Singapore"},
		},
	}

	response, rejected := executeCacheCommand(context.Background(), ht, request, commandExecutionOptions{
		Journal:           journal,
		DirtyTracker:      dirty,
		ReplicationSafety: NewReplicationSafetyStore(),
	})
	if rejected || response.OK {
		t.Fatalf("executeCacheCommand(batch) = %#v rejected=%v, want aggregate failure without rejection", response, rejected)
	}
	if len(response.Responses) != 4 {
		t.Fatalf("batch responses len = %d, want 4", len(response.Responses))
	}
	if !response.Responses[0].OK || !response.Responses[1].OK || response.Responses[1].Value != "ivi" {
		t.Fatalf("first batch responses = %#v, want SETSTR and GETSTR success", response.Responses[:2])
	}
	if response.Responses[2].OK || !strings.Contains(response.Responses[2].Message, "overflow") {
		t.Fatalf("overflow batch response = %#v, want counter overflow failure", response.Responses[2])
	}
	if !response.Responses[3].OK {
		t.Fatalf("final batch response = %#v, want success", response.Responses[3])
	}
	if got := ht.GetCounter("max"); got != maxCommandInt32 {
		t.Fatalf("counter after failed INC = %d, want max int32", got)
	}
	if snapshot := dirty.Snapshot(); !reflect.DeepEqual(snapshot.keys, []string{"city", "name"}) {
		t.Fatalf("dirty keys = %#v, want city and name", snapshot.keys)
	}
	entries, err := readCommandJournalEntries(journal.path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 || entries[0].Request.Command != "SETSTR" || entries[0].Request.Key != "name" || entries[1].Request.Key != "city" {
		t.Fatalf("journal entries = %#v, want only successful mutating commands", entries)
	}
}

func TestExecutePublicCommandBatchJournalSyncsOnce(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertCounter("max", maxCommandInt32)
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatBinary)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()
	syncs := 0
	journal.syncHook = func() error {
		syncs++
		return nil
	}

	response, rejected := executeCacheCommand(context.Background(), ht, CacheCommandRequest{
		Command: "BATCH",
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: "name", Value: "ivi"},
			{Command: "GETSTR", Key: "name"},
			{Command: "INC", Key: "max", Value: "1"},
			{Command: "SETSTR", Key: "city", Value: "Singapore"},
		},
	}, commandExecutionOptions{Journal: journal})
	if rejected || response.OK {
		t.Fatalf("executeCacheCommand(batch) = %#v rejected=%v, want aggregate command failure", response, rejected)
	}
	if syncs != 1 {
		t.Fatalf("journal syncs = %d, want one durable boundary for the public batch", syncs)
	}
	entries, err := readCommandJournalEntries(journal.path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 || entries[0].Request.Key != "name" || entries[1].Request.Key != "city" {
		t.Fatalf("journal entries = %#v, want only the two successful writes", entries)
	}
}

func TestExecutePublicStructuredCommandBatchJournalSyncsOnceAndReplays(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatBinary)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()
	syncs := 0
	journal.syncHook = func() error {
		syncs++
		return nil
	}

	response, rejected := executeCacheCommand(context.Background(), ht, CacheCommandRequest{
		Command: "BATCH",
		Batch: []CacheCommandRequest{
			{Command: "PUTMAP", Key: "profile", Pairs: Map{"city": "Singapore", "age": "32"}},
			{Command: "SETSTR", Key: "name", Value: "ivi"},
			{Command: "PEEKMAP", Key: "profile", Subkey: "city"},
		},
	}, commandExecutionOptions{Journal: journal})
	if rejected || !response.OK {
		t.Fatalf("executeCacheCommand(structured batch) = %#v rejected=%v, want success", response, rejected)
	}
	if syncs != 1 {
		t.Fatalf("structured batch journal syncs = %d, want one", syncs)
	}

	replayed := newTestTrie(t)
	journal.syncHook = nil
	if _, err := journal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetString("name"); got != "ivi" {
		t.Fatalf("replayed name = %q, want ivi", got)
	}
	profile := replayed.GetMap("profile")
	if profile["city"] != "Singapore" || profile["age"] != "32" {
		t.Fatalf("replayed profile = %#v, want structured values", profile)
	}
}

func TestExecutePublicCommandBatchSyncFailureRollsBackMemoryAndJournal(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("name", "before")
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatBinary)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()
	syncs := 0
	journal.syncHook = func() error {
		syncs++
		if syncs == 1 {
			return errors.New("injected batch sync failure")
		}
		return nil
	}

	response, rejected := executeCacheCommand(context.Background(), ht, CacheCommandRequest{
		Command: "BATCH",
		Batch: []CacheCommandRequest{
			{Command: "SETSTR", Key: "name", Value: "after"},
			{Command: "SETSTR", Key: "city", Value: "Singapore"},
		},
	}, commandExecutionOptions{Journal: journal})
	if rejected || response.OK || !strings.Contains(response.Message, "injected batch sync failure") {
		t.Fatalf("executeCacheCommand(batch) = %#v rejected=%v, want injected durability failure", response, rejected)
	}
	if got := ht.GetString("name"); got != "before" {
		t.Fatalf("name after failed sync = %q, want original value", got)
	}
	if got := ht.GetString("city"); got != "" {
		t.Fatalf("city after failed sync = %q, want rolled back creation", got)
	}
	entries, err := readCommandJournalEntries(journal.path)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("journal entries after failed sync = %#v, want none", entries)
	}
}

func TestMonitoringHandlerJournalsMutatingCommands(t *testing.T) {
	ht := newTestTrie(t)
	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{Journal: journal}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"name","value":"ivi"}`)))
	if resp.Code != http.StatusOK {
		t.Fatalf("SETSTR status = %d, want 200", resp.Code)
	}

	replayed := newTestTrie(t)
	replayJournal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.GetString("name"); got != "ivi" {
		t.Fatalf("replayed name = %q, want ivi", got)
	}
}

func TestMonitoringHandlerExposesJournalTail(t *testing.T) {
	ht := newTestTrie(t)
	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "INC", Key: "views", Value: "2"}); !got.OK {
		t.Fatalf("journaled INC response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "next", Value: "batch"}); !got.OK {
		t.Fatalf("journaled second SETSTR response = %#v, want ok", got)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{Journal: journal}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/journal?after_sequence=1&limit=1", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("journal status = %d, want 200", resp.Code)
	}
	var tail CommandJournalTail
	if err := json.Unmarshal(resp.Body.Bytes(), &tail); err != nil {
		t.Fatalf("journal JSON error = %v", err)
	}
	if tail.LastSequence != 3 || tail.Limit != 1 || !tail.HasMore || len(tail.Entries) != 1 {
		t.Fatalf("journal tail = %#v, want one limited entry after sequence 1 with has_more", tail)
	}
	if entry := tail.Entries[0]; entry.Sequence != 2 || entry.Request.Command != "INC" || entry.Request.Key != "views" {
		t.Fatalf("journal tail entry = %#v, want sequence 2 INC views", entry)
	}

	badSequenceResp := httptest.NewRecorder()
	handler.ServeHTTP(badSequenceResp, httptest.NewRequest(http.MethodGet, "/api/journal?after_sequence=bad", nil))
	if badSequenceResp.Code != http.StatusBadRequest {
		t.Fatalf("bad journal sequence status = %d, want 400", badSequenceResp.Code)
	}
	badLimitResp := httptest.NewRecorder()
	handler.ServeHTTP(badLimitResp, httptest.NewRequest(http.MethodGet, "/api/journal?limit=0", nil))
	if badLimitResp.Code != http.StatusBadRequest {
		t.Fatalf("bad journal limit status = %d, want 400", badLimitResp.Code)
	}

	unconfigured := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()
	unconfiguredResp := httptest.NewRecorder()
	unconfigured.ServeHTTP(unconfiguredResp, httptest.NewRequest(http.MethodGet, "/api/journal", nil))
	if unconfiguredResp.Code != http.StatusConflict {
		t.Fatalf("unconfigured journal status = %d, want 409", unconfiguredResp.Code)
	}
}

func TestMonitoringHandlerStreamsJournalSnapshotForReplication(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		t.Fatalf("SETSTR response = %#v, want ok", response)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		Journal:              journal,
		ReplicationAuthToken: "replica-secret",
	}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/journal/snapshot", nil))
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated snapshot status = %d, want 401", resp.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/journal/snapshot", nil)
	req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d body=%q, want 200", resp.Code, resp.Body.String())
	}
	if got := resp.Header().Get("Content-Type"); got != snapshotContentType {
		t.Fatalf("snapshot content type = %q, want %q", got, snapshotContentType)
	}

	path := filepath.Join(t.TempDir(), "snapshot.hc")
	if err := os.WriteFile(path, resp.Body.Bytes(), 0o600); err != nil {
		t.Fatalf("WriteFile(snapshot) error = %v", err)
	}
	restored := newTestTrie(t)
	metadata, err := restored.LoadSnapshotWithMetadata(path)
	if err != nil {
		t.Fatalf("LoadSnapshotWithMetadata() error = %v", err)
	}
	if metadata.JournalSequence != 1 || restored.GetString("name") != "ivi" {
		t.Fatalf("snapshot sequence/name = %d/%q, want 1/ivi", metadata.JournalSequence, restored.GetString("name"))
	}
	if tail, err := journal.Tail(0, 10); err != nil || len(tail.Entries) != 1 {
		t.Fatalf("journal tail after snapshot stream = %#v/%v, want uncompacted entry", tail, err)
	}
}

func TestMonitoringHandlerStreamsPebbleJournalCheckpoint(t *testing.T) {
	trie := newTestTrie(t)
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()
	if response := journal.ExecuteCommand(trie, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !response.OK {
		t.Fatalf("SETSTR response = %#v", response)
	}
	handler := NewMonitoringHandler(trie, MonitoringOptions{Journal: journal, LevelDBStore: store}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/journal/checkpoint", nil))
	if response.Code != http.StatusOK {
		t.Fatalf("checkpoint status = %d: %s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Content-Type"); got != checkpointContentType {
		t.Fatalf("checkpoint content type = %q", got)
	}
	path := filepath.Join(t.TempDir(), "checkpoint.tar.gz")
	if err := os.WriteFile(path, response.Body.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err := VerifyBackupBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	if report.RecoveredKeys != 1 || report.JournalSequence != 1 || report.LevelDB == nil {
		t.Fatalf("checkpoint report = %#v", report)
	}
}

func TestMonitoringHandlerPullsJournalTail(t *testing.T) {
	var gotSourcePath string
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSourcePath = r.URL.String()
		if r.Method != http.MethodGet {
			t.Fatalf("source method = %s, want GET", r.Method)
		}
		writeJSON(w, CommandJournalTail{
			LastSequence: 4,
			Limit:        2,
			HasMore:      true,
			Entries: []CommandJournalRecord{
				{Sequence: 2, Request: CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}},
				{Sequence: 3, Request: CacheCommandRequest{Command: "INC", Key: "views", Value: "2"}},
			},
		})
	}))
	defer source.Close()

	ht := newTestTrie(t)
	journalPath := filepath.Join(t.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{Journal: journal}).Handler()
	body, err := json.Marshal(map[string]interface{}{
		"source":         source.URL,
		"after_sequence": 1,
		"limit":          2,
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/journal", bytes.NewReader(body)))
	if resp.Code != http.StatusOK {
		t.Fatalf("journal pull status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	if gotSourcePath != "/api/journal?after_sequence=1&limit=2" {
		t.Fatalf("source path = %q, want /api/journal?after_sequence=1&limit=2", gotSourcePath)
	}
	var result CommandJournalPullResult
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("journal pull JSON error = %v", err)
	}
	if result.Applied != 2 || result.AppliedThrough != 3 || result.LastSequence != 4 || result.AfterSequence != 1 || result.Source != source.URL || !result.HasMore {
		t.Fatalf("journal pull result = %#v, want applied through sequence 3", result)
	}
	if got := ht.GetString("name"); got != "ivi" {
		t.Fatalf("pulled name = %q, want ivi", got)
	}
	if got := ht.GetCounter("views"); got != 2 {
		t.Fatalf("pulled views = %d, want 2", got)
	}
	entries, err := readCommandJournalEntries(journalPath)
	if err != nil {
		t.Fatalf("readCommandJournalEntries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("local journal entries = %d, want 2", len(entries))
	}

	badMaxBatchesBody, err := json.Marshal(map[string]interface{}{
		"source":      source.URL,
		"max_batches": 2,
	})
	if err != nil {
		t.Fatalf("Marshal(bad max batches) error = %v", err)
	}
	badMaxBatchesResp := httptest.NewRecorder()
	handler.ServeHTTP(badMaxBatchesResp, httptest.NewRequest(http.MethodPost, "/api/journal", bytes.NewReader(badMaxBatchesBody)))
	if badMaxBatchesResp.Code != http.StatusBadRequest {
		t.Fatalf("bad max_batches status = %d, want 400", badMaxBatchesResp.Code)
	}
}

func TestMonitoringHandlerPullsJournalTailUntilCurrent(t *testing.T) {
	var gotSourcePaths []string
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSourcePaths = append(gotSourcePaths, r.URL.String())
		after := r.URL.Query().Get("after_sequence")
		switch after {
		case "1":
			writeJSON(w, CommandJournalTail{
				LastSequence: 4,
				Limit:        1,
				HasMore:      true,
				Entries: []CommandJournalRecord{
					{Sequence: 2, Request: CacheCommandRequest{Command: "SETSTR", Key: "one", Value: "1"}},
				},
			})
		case "2":
			writeJSON(w, CommandJournalTail{
				LastSequence: 4,
				Limit:        1,
				HasMore:      true,
				Entries: []CommandJournalRecord{
					{Sequence: 3, Request: CacheCommandRequest{Command: "SETSTR", Key: "two", Value: "2"}},
				},
			})
		case "3":
			writeJSON(w, CommandJournalTail{
				LastSequence: 4,
				Limit:        1,
				Entries: []CommandJournalRecord{
					{Sequence: 4, Request: CacheCommandRequest{Command: "SETSTR", Key: "three", Value: "3"}},
				},
			})
		default:
			t.Fatalf("unexpected source after_sequence = %q", after)
		}
	}))
	defer source.Close()

	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{Journal: journal}).Handler()
	body, err := json.Marshal(map[string]interface{}{
		"source":         source.URL,
		"after_sequence": 1,
		"limit":          1,
		"until_current":  true,
		"max_batches":    5,
	})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/journal", bytes.NewReader(body)))
	if resp.Code != http.StatusOK {
		t.Fatalf("journal pull until current status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	wantPaths := []string{
		"/api/journal?after_sequence=1&limit=1",
		"/api/journal?after_sequence=2&limit=1",
		"/api/journal?after_sequence=3&limit=1",
	}
	if !reflect.DeepEqual(gotSourcePaths, wantPaths) {
		t.Fatalf("source paths = %#v, want %#v", gotSourcePaths, wantPaths)
	}
	var result CommandJournalPullResult
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("journal pull until current JSON error = %v", err)
	}
	if result.Applied != 3 || result.AppliedThrough != 4 || result.LastSequence != 4 || result.Batches != 3 || result.HasMore {
		t.Fatalf("journal pull until current result = %#v, want fully caught up through sequence 4", result)
	}
	if got := ht.GetString("one"); got != "1" {
		t.Fatalf("pulled one = %q, want 1", got)
	}
	if got := ht.GetString("two"); got != "2" {
		t.Fatalf("pulled two = %q, want 2", got)
	}
	if got := ht.GetString("three"); got != "3" {
		t.Fatalf("pulled three = %q, want 3", got)
	}
}

func TestMonitoringHandlerPullJournalPropagatesCompactedSource(t *testing.T) {
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSONStatus(w, http.StatusConflict, commandError("hatriecache: command journal entries are compacted"))
	}))
	defer source.Close()

	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{Journal: journal}).Handler()
	body, err := json.Marshal(map[string]interface{}{"source": source.URL})
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/journal", bytes.NewReader(body)))
	if resp.Code != http.StatusConflict {
		t.Fatalf("compacted source status = %d, want 409", resp.Code)
	}
}

func TestMonitoringHandlerRejectsInvalidCommandRequests(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	for _, method := range []string{http.MethodGet, http.MethodPut} {
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, httptest.NewRequest(method, "/api/commands", nil))
		if resp.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status = %d, want 405", method, resp.Code)
		}
	}

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"GET","key":"x"} trailing`)))
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("invalid JSON status = %d, want 400", resp.Code)
	}
}

func TestMonitoringHandlerSkipsCanceledMutationRequests(t *testing.T) {
	ht := newTestTrie(t)
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Self:    "node-a",
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := NewElectionStore(topology, ElectionOptions{})
	snapshotCalled := false
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		Snapshot: func() error {
			snapshotCalled = true
			return nil
		},
		Topology: topology,
		Election: election,
	}).Handler()

	commandResp := httptest.NewRecorder()
	handler.ServeHTTP(commandResp, canceledMonitoringRequest(http.MethodPost, "/api/commands", `{"command":"SETSTR","key":"name","value":"ivi"}`))
	if commandResp.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled command status = %d, want 408", commandResp.Code)
	}
	if ht.Exists("name") {
		t.Fatal("canceled command stored name")
	}

	snapshotResp := httptest.NewRecorder()
	handler.ServeHTTP(snapshotResp, canceledMonitoringRequest(http.MethodPost, "/api/snapshot", ""))
	if snapshotResp.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled snapshot status = %d, want 408", snapshotResp.Code)
	}
	if snapshotCalled {
		t.Fatal("canceled snapshot request called snapshot callback")
	}

	topologyUpdate := `{"version":1,"self":"node-b","nodes":[{"id":"node-b"}],"shards":[{"id":0,"primary":"node-b"}]}`
	topologyResp := httptest.NewRecorder()
	handler.ServeHTTP(topologyResp, canceledMonitoringRequest(http.MethodPut, "/api/topology", topologyUpdate))
	if topologyResp.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled topology status = %d, want 408", topologyResp.Code)
	}
	if got := topology.Get().Self; got != "node-a" {
		t.Fatalf("topology self = %q after canceled PUT, want node-a", got)
	}

	electionResp := httptest.NewRecorder()
	handler.ServeHTTP(electionResp, canceledMonitoringRequest(http.MethodPost, "/api/election", `{"node":"node-a","online":false}`))
	if electionResp.Code != http.StatusRequestTimeout {
		t.Fatalf("canceled election status = %d, want 408", electionResp.Code)
	}
	if got := election.Status().Leaders[0]; got.Leader != "node-a" {
		t.Fatalf("leader after canceled election update = %#v, want node-a", got)
	}
}

func TestMonitoringHandlerForcesSnapshotWhenConfigured(t *testing.T) {
	ht := newTestTrie(t)
	called := false
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		Snapshot: func() error {
			called = true
			return nil
		},
	}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/snapshot", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d, want 200", resp.Code)
	}
	if !called {
		t.Fatal("snapshot callback was not called")
	}
	var result CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("snapshot JSON error = %v", err)
	}
	if !result.OK {
		t.Fatalf("snapshot response = %#v, want ok", result)
	}
}

func TestMonitoringHandlerRejectsForcedSnapshotWhenUnconfigured(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/snapshot", nil))
	if resp.Code != http.StatusConflict {
		t.Fatalf("snapshot status = %d, want 409", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/snapshot", nil))
	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("snapshot GET status = %d, want 405", resp.Code)
	}
}

func TestMonitoringHandlerCompactsLevelDBStorage(t *testing.T) {
	ht := newTestTrie(t)
	store, err := OpenLevelDBStore(filepath.Join(t.TempDir(), "cache.leveldb"))
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	ht.UpsertString("alpha", "one")
	ht.UpsertString("omega", "two")
	if err := store.Save(ht); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{LevelDBStore: store}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/storage/compact", strings.NewReader(`{"start_key":"alpha","limit_key":"omega\u0000"}`)))
	if resp.Code != http.StatusOK {
		t.Fatalf("storage compact status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	var result LevelDBCompactionResult
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("storage compact JSON error = %v", err)
	}
	if result.Store != "leveldb" || result.StartKey != "alpha" || result.LimitKey != "omega\x00" {
		t.Fatalf("storage compact result = %#v, want range metadata", result)
	}

	loaded := newTestTrie(t)
	if count, err := store.Load(loaded); err != nil || count != 2 {
		t.Fatalf("Load() after storage compact = %d/%v, want 2 nil", count, err)
	}
}

func TestMonitoringHandlerReportsLevelDBStorageStatus(t *testing.T) {
	ht := newTestTrie(t)
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	store, err := OpenLevelDBStoreWithFormat(path, StorageFormatJSON)
	if err != nil {
		t.Fatalf("OpenLevelDBStoreWithFormat() error = %v", err)
	}
	defer store.Close()
	handler := NewMonitoringHandler(ht, MonitoringOptions{LevelDBStore: store})
	mux := handler.Handler()

	if !handler.beginStorageOperation("compact") {
		t.Fatal("beginStorageOperation(compact) = false, want true")
	}
	resp := httptest.NewRecorder()
	mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/storage", nil))
	handler.finishStorageOperation()
	if resp.Code != http.StatusOK {
		t.Fatalf("storage status while running status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	var status storageStatus
	if err := json.Unmarshal(resp.Body.Bytes(), &status); err != nil {
		t.Fatalf("storage status JSON error = %v", err)
	}
	if !status.Configured || !status.LevelDBConfigured || status.Store != "leveldb" || status.Path != path || status.Format != string(StorageFormatJSON) {
		t.Fatalf("storage status = %#v, want configured leveldb path and JSON format", status)
	}
	if !status.Operation.Running || status.Operation.Action != "compact" || status.Operation.StartedAt == nil || status.Operation.AgeMillis < 0 {
		t.Fatalf("storage running operation = %#v, want compact operation metadata", status.Operation)
	}

	ht.UpsertString("session:storage", "saved")
	flushResp := httptest.NewRecorder()
	mux.ServeHTTP(flushResp, httptest.NewRequest(http.MethodPost, "/api/storage/flush", strings.NewReader(`{}`)))
	if flushResp.Code != http.StatusOK {
		t.Fatalf("storage flush status = %d, want 200: %s", flushResp.Code, flushResp.Body.String())
	}
	compactResp := httptest.NewRecorder()
	mux.ServeHTTP(compactResp, httptest.NewRequest(http.MethodPost, "/api/storage/compact", strings.NewReader(`{}`)))
	if compactResp.Code != http.StatusOK {
		t.Fatalf("storage compact status = %d, want 200: %s", compactResp.Code, compactResp.Body.String())
	}
	handler.recordStorageSpill(LevelDBSpillResult{Store: "leveldb", KeysSpilled: 1, HotBytesBefore: 128, HotBytesAfter: 64, DurationMillis: 5})

	resp = httptest.NewRecorder()
	mux.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/storage", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("storage status status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	status = storageStatus{}
	if err := json.Unmarshal(resp.Body.Bytes(), &status); err != nil {
		t.Fatalf("storage status JSON error = %v", err)
	}
	if status.Operation.Running {
		t.Fatalf("storage operation after completion = %#v, want not running", status.Operation)
	}
	if status.SizeBytes <= 0 {
		t.Fatalf("storage size bytes = %d, want non-zero LevelDB directory size", status.SizeBytes)
	}
	if status.LastFlush == nil || status.LastFlush.Keys != 1 || status.LastFlush.Store != "leveldb" {
		t.Fatalf("storage last flush = %#v, want remembered flush result", status.LastFlush)
	}
	if status.LastCompact == nil || status.LastCompact.Store != "leveldb" || status.LastCompact.DurationMillis < 0 {
		t.Fatalf("storage last compact = %#v, want remembered compact result", status.LastCompact)
	}
	if status.LastSpill == nil || status.LastSpill.KeysSpilled != 1 || status.LastSpill.HotBytesAfter != 64 {
		t.Fatalf("storage last spill = %#v, want remembered spill result", status.LastSpill)
	}
}

func TestMonitoringHandlerReportsPebbleStorageStatus(t *testing.T) {
	ht := newTestTrie(t)
	path := filepath.Join(t.TempDir(), "cache")
	store, err := OpenPersistentStoreWithFormat(path, StorageBackendPebble, StorageFormatBinary)
	if err != nil {
		t.Fatalf("OpenPersistentStoreWithFormat() error = %v", err)
	}
	defer store.Close()
	ht.UpsertString("session", "value")
	if err := store.Save(ht); err != nil {
		t.Fatalf("Save() error = %v", err)
	}

	resp := httptest.NewRecorder()
	NewMonitoringHandler(ht, MonitoringOptions{LevelDBStore: store}).Handler().ServeHTTP(
		resp,
		httptest.NewRequest(http.MethodGet, "/api/storage", nil),
	)
	if resp.Code != http.StatusOK {
		t.Fatalf("storage status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	var status storageStatus
	if err := json.Unmarshal(resp.Body.Bytes(), &status); err != nil {
		t.Fatalf("storage status JSON error = %v", err)
	}
	if !status.Configured || !status.LevelDBConfigured || status.Store != "pebble" || status.Path != path || status.Format != string(StorageFormatBinary) || status.SizeBytes <= 0 || status.Properties.Stats == "" {
		t.Fatalf("Pebble storage status = %#v", status)
	}
}

func TestMonitoringHandlerFlushesLevelDBStorage(t *testing.T) {
	ht := newTestTrie(t)
	store, err := OpenLevelDBStore(filepath.Join(t.TempDir(), "cache.leveldb"))
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	handler := NewMonitoringHandler(ht, MonitoringOptions{LevelDBStore: store}).Handler()
	ht.UpsertString("session:flush", "saved")

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/storage/flush", strings.NewReader(`{}`)))
	if resp.Code != http.StatusOK {
		t.Fatalf("storage flush status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	var result LevelDBFlushResult
	if err := json.Unmarshal(resp.Body.Bytes(), &result); err != nil {
		t.Fatalf("storage flush JSON error = %v", err)
	}
	if result.Store != "leveldb" || result.Keys != 1 || result.DurationMillis < 0 || result.FinishedAt.Before(result.StartedAt) {
		t.Fatalf("storage flush result = %#v, want leveldb key count and timing", result)
	}

	loaded := newTestTrie(t)
	if count, err := store.Load(loaded); err != nil || count != 1 {
		t.Fatalf("Load() after storage flush = %d/%v, want 1 nil", count, err)
	}
	if got := loaded.GetString("session:flush"); got != "saved" {
		t.Fatalf("flushed value = %q, want saved", got)
	}
}

func TestMonitoringHandlerMarksAndClearsLevelDBDirtyKeys(t *testing.T) {
	ht := newTestTrie(t)
	store, err := OpenLevelDBStore(filepath.Join(t.TempDir(), "cache.leveldb"))
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	dirty := NewLevelDBDirtyTracker()
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		LevelDBStore:        store,
		LevelDBDirtyTracker: dirty,
	}).Handler()

	commandResp := httptest.NewRecorder()
	handler.ServeHTTP(commandResp, httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"session:dirty","value":"saved"}`)))
	if commandResp.Code != http.StatusOK {
		t.Fatalf("command status = %d, want 200: %s", commandResp.Code, commandResp.Body.String())
	}
	if dirty.Pending() != 1 {
		t.Fatalf("dirty pending after command = %d, want 1", dirty.Pending())
	}

	flushResp := httptest.NewRecorder()
	handler.ServeHTTP(flushResp, httptest.NewRequest(http.MethodPost, "/api/storage/flush", strings.NewReader(`{}`)))
	if flushResp.Code != http.StatusOK {
		t.Fatalf("storage flush status = %d, want 200: %s", flushResp.Code, flushResp.Body.String())
	}
	if dirty.Pending() != 0 {
		t.Fatalf("dirty pending after flush = %d, want 0", dirty.Pending())
	}
	if entry, ok, err := store.Entry("session:dirty"); err != nil || !ok || entry.String != "saved" {
		t.Fatalf("Entry(session:dirty) = %#v/%v/%v, want flushed value", entry, ok, err)
	}
}

func TestMonitoringHandlerRejectsLevelDBCompactionWhenUnconfigured(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/storage/compact", strings.NewReader(`{}`)))
	if resp.Code != http.StatusConflict {
		t.Fatalf("storage compact status = %d, want 409", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/storage/compact", nil))
	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("storage compact GET status = %d, want 405", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/storage/flush", strings.NewReader(`{}`)))
	if resp.Code != http.StatusConflict {
		t.Fatalf("storage flush status = %d, want 409", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/storage/flush", nil))
	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("storage flush GET status = %d, want 405", resp.Code)
	}
}

func TestMonitoringHandlerWritesBackupBundle(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatJSON)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "sg:name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		Journal:              journal,
		BackupSnapshotFormat: SnapshotFormatJSON,
	}).Handler()

	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	body := `{"path":` + strconv.Quote(bundlePath) + `,"partition":{"mode":"partitioned","partitions":["sg"],"node_id":"node-sg-a","topology_epoch":42,"topology_fingerprint":"topology-v1","key_prefixes":["sg:"]}}`
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/backup", strings.NewReader(body)))
	if resp.Code != http.StatusOK {
		t.Fatalf("backup status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	if _, err := os.Stat(bundlePath); err != nil {
		t.Fatalf("backup bundle missing: %v", err)
	}
	var manifest BackupBundleManifest
	if err := json.Unmarshal(resp.Body.Bytes(), &manifest); err != nil {
		t.Fatalf("backup manifest JSON error = %v", err)
	}
	if manifest.JournalSequence != 1 || manifest.Snapshot != backupBundleSnapshotPath || manifest.Journal != backupBundleJournalPath {
		t.Fatalf("backup manifest = %#v, want snapshot and journal checkpoint at sequence 1", manifest)
	}
	if manifest.Partition == nil || !reflect.DeepEqual(*manifest.Partition, BackupPartitionMetadata{
		Mode:                "partitioned",
		Partitions:          []string{"sg"},
		NodeID:              "node-sg-a",
		TopologyEpoch:       42,
		TopologyFingerprint: "topology-v1",
		KeyPrefixes:         []string{"sg:"},
	}) {
		t.Fatalf("backup manifest partition = %#v, want requested partition metadata", manifest.Partition)
	}

	verifyResp := httptest.NewRecorder()
	handler.ServeHTTP(verifyResp, httptest.NewRequest(http.MethodPost, "/api/backup/verify", strings.NewReader(`{"path":`+strconv.Quote(bundlePath)+`}`)))
	if verifyResp.Code != http.StatusOK {
		t.Fatalf("backup verify status = %d, want 200: %s", verifyResp.Code, verifyResp.Body.String())
	}
	var doctor BackupDoctorReport
	if err := json.Unmarshal(verifyResp.Body.Bytes(), &doctor); err != nil || !doctor.OK || doctor.RecoveredKeys != 1 {
		t.Fatalf("backup verify report/error = %#v/%v", doctor, err)
	}

	rehearseResp := httptest.NewRecorder()
	handler.ServeHTTP(rehearseResp, httptest.NewRequest(http.MethodPost, "/api/backup/rehearse", strings.NewReader(`{"path":`+strconv.Quote(bundlePath)+`}`)))
	if rehearseResp.Code != http.StatusOK {
		t.Fatalf("backup rehearse status = %d, want 200: %s", rehearseResp.Code, rehearseResp.Body.String())
	}
	var rehearsal RestoreRehearsalReport
	if err := json.Unmarshal(rehearseResp.Body.Bytes(), &rehearsal); err != nil || !rehearsal.OK || rehearsal.RecoveredKeys != 1 || rehearsal.WorkDirKept {
		t.Fatalf("backup rehearsal report/error = %#v/%v", rehearsal, err)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/backup", nil))
	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("backup GET status = %d, want 405", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/backup", strings.NewReader(`{"path":""}`)))
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("backup empty path status = %d, want 400", resp.Code)
	}
}

func TestMonitoringHandlerCreatesRequestedPebbleCheckpoint(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("name", "ivi")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	handler := NewMonitoringHandler(trie, MonitoringOptions{LevelDBStore: store}).Handler()
	bundlePath := filepath.Join(t.TempDir(), "backup.tar.gz")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/backup", strings.NewReader(`{"path":`+strconv.Quote(bundlePath)+`,"mode":"pebble-checkpoint"}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("backup status = %d: %s", response.Code, response.Body.String())
	}
	var manifest BackupBundleManifest
	if err := json.Unmarshal(response.Body.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Mode != BackupModePebbleCheckpoint || manifest.Store != backupBundleStorePath {
		t.Fatalf("backup manifest = %#v", manifest)
	}
}

func TestMonitoringHandlerCreatesIncrementalBackupRepository(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("name", "ivi")
	store, err := OpenPebbleStore(filepath.Join(t.TempDir(), "live.pebble"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	dirty := NewLevelDBDirtyTracker()
	dirty.Mark("name")
	handler := NewMonitoringHandler(trie, MonitoringOptions{LevelDBStore: store, LevelDBDirtyTracker: dirty}).Handler()
	repository := filepath.Join(t.TempDir(), "repository")
	response := httptest.NewRecorder()
	body := `{"path":` + strconv.Quote(repository) + `,"mode":"pebble-incremental","retain":2}`
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/backup", strings.NewReader(body)))
	if response.Code != http.StatusOK {
		t.Fatalf("backup status = %d: %s", response.Code, response.Body.String())
	}
	var manifest BackupBundleManifest
	if err := json.Unmarshal(response.Body.Bytes(), &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Mode != BackupModePebbleIncremental || manifest.BackupID == "" || manifest.Store != backupBundleStorePath {
		t.Fatalf("incremental backup manifest = %#v", manifest)
	}
	if _, err := VerifyBackupPath(repository); err != nil {
		t.Fatalf("VerifyBackupPath(repository) error = %v", err)
	}
}

func TestMonitoringAuthTokenProtectsAPI(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{AuthToken: "secret"}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/health", nil))
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated health status = %d, want 401", resp.Code)
	}
	if got := resp.Header().Get("WWW-Authenticate"); !strings.Contains(got, "Bearer") {
		t.Fatalf("WWW-Authenticate = %q, want Bearer challenge", got)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token health status = %d, want 401", resp.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("bearer token health status = %d, want 200", resp.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req.Header.Set("X-Hatrie-Auth-Token", "secret")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("header token health status = %d, want 200", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated metrics status = %d, want 401", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated config status = %d, want 401", resp.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("bearer token metrics status = %d, want 200", resp.Code)
	}
}

func TestMonitoringPreviousAuthTokensExpire(t *testing.T) {
	ht := newTestTrie(t)
	future := time.Now().Add(time.Hour)
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		AuthToken:                        "current-operator",
		AuthPreviousToken:                "previous-operator",
		AuthPreviousExpiresAt:            future,
		ReplicationAuthToken:             "current-replication",
		ReplicationAuthPreviousToken:     "previous-replication",
		ReplicationAuthPreviousExpiresAt: future,
	}).Handler()

	for _, token := range []string{"current-operator", "previous-operator"} {
		req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("operator token %q status = %d, want 200", token, resp.Code)
		}
	}
	for _, token := range []string{"current-replication", "previous-replication"} {
		req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"INTERNALDEL","key":"missing"}`))
		req.Header.Set("X-Hatrie-Replication-Token", token)
		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("replication token %q status = %d body %q, want 200", token, resp.Code, resp.Body.String())
		}
	}

	expired := NewMonitoringHandler(ht, MonitoringOptions{
		AuthToken:                        "current-operator",
		AuthPreviousToken:                "previous-operator",
		AuthPreviousExpiresAt:            time.Now().Add(-time.Hour),
		ReplicationAuthToken:             "current-replication",
		ReplicationAuthPreviousToken:     "previous-replication",
		ReplicationAuthPreviousExpiresAt: time.Now().Add(-time.Hour),
	}).Handler()
	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req.Header.Set("Authorization", "Bearer previous-operator")
	resp := httptest.NewRecorder()
	expired.ServeHTTP(resp, req)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expired operator token status = %d, want 401", resp.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"INTERNALDEL","key":"missing"}`))
	req.Header.Set("X-Hatrie-Replication-Token", "previous-replication")
	resp = httptest.NewRecorder()
	expired.ServeHTTP(resp, req)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("expired replication token status = %d, want 401", resp.Code)
	}
}

func TestMonitoringReplicationAuthTokenOnlyAllowsInternalCommands(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		AuthToken:            "operator-secret",
		ReplicationAuthToken: "replica-secret",
	}).Handler()

	req := httptest.NewRequest(http.MethodGet, "/api/health", nil)
	req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("replication token health status = %d, want 401", resp.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"name","value":"ivi"}`))
	req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("replication token client command status = %d, want 401", resp.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"INTERNALSET","key":"name","value":"{\"type\":\"string\",\"string\":\"replicated\"}"}`))
	req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("replication token internal command status = %d body %q, want 200", resp.Code, resp.Body.String())
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "name"}); !got.OK || got.Value != "replicated" {
		t.Fatalf("replicated GETSTR = %#v, want replicated value", got)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"name","value":"operator"}`))
	req.Header.Set("Authorization", "Bearer operator-secret")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("operator token command status = %d body %q, want 200", resp.Code, resp.Body.String())
	}

	openHandler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{ReplicationAuthToken: "replica-secret"}).Handler()
	resp = httptest.NewRecorder()
	openHandler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"INTERNALDEL","key":"name"}`)))
	if resp.Code != http.StatusUnauthorized {
		t.Fatalf("internal command without replication token status = %d, want 401", resp.Code)
	}
}

func TestMonitoringReplicationAuthTokenAllowsOnlyJournalReadAndSnapshot(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		ReplicationAuthToken: "replica-secret",
	}).Handler()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "journal read", method: http.MethodGet, path: "/api/journal"},
		{name: "journal snapshot", method: http.MethodGet, path: "/api/journal/snapshot"},
		{name: "journal checkpoint", method: http.MethodGet, path: "/api/journal/checkpoint"},
		{name: "journal recovery", method: http.MethodGet, path: "/api/journal/recovery"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resp := httptest.NewRecorder()
			handler.ServeHTTP(resp, httptest.NewRequest(test.method, test.path, strings.NewReader(`{}`)))
			if resp.Code != http.StatusUnauthorized {
				t.Fatalf("unauthenticated status = %d, want 401", resp.Code)
			}

			req := httptest.NewRequest(test.method, test.path, strings.NewReader(`{}`))
			req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
			resp = httptest.NewRecorder()
			handler.ServeHTTP(resp, req)
			if resp.Code != http.StatusConflict {
				t.Fatalf("replication-authenticated status = %d, want handler conflict 409", resp.Code)
			}
		})
	}

	operatorHandler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		AuthToken:            "operator-secret",
		ReplicationAuthToken: "replica-secret",
	}).Handler()
	for _, path := range []string{"/api/journal", "/api/replication"} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
		resp := httptest.NewRecorder()
		operatorHandler.ServeHTTP(resp, req)
		if resp.Code != http.StatusUnauthorized {
			t.Fatalf("replication token POST %s status = %d, want 401", path, resp.Code)
		}
	}
}

func TestMonitoringHandlerExecutesInternalReplicationBatch(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("old", "value")
	handler := NewMonitoringHandler(ht, MonitoringOptions{ReplicationAuthToken: "replica-secret"}).Handler()

	batchedSet := mustMarshalMonitoringTestCommand(t, CacheCommandRequest{
		Command: "INTERNALSET",
		Key:     "name",
		Value:   `{"type":"string","string":"batched"}`,
	})
	batchedDel := mustMarshalMonitoringTestCommand(t, CacheCommandRequest{Command: "INTERNALDEL", Key: "old"})
	body := mustMarshalMonitoringTestCommand(t, CacheCommandRequest{
		Command: "INTERNALBATCH",
		Values:  Slice{batchedSet, batchedDel},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(body))
	req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("batch status = %d body %q, want 200", resp.Code, resp.Body.String())
	}
	var response CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
		t.Fatalf("batch response JSON error = %v", err)
	}
	if !response.OK {
		t.Fatalf("batch response = %#v, want ok", response)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "name"}); !got.OK || got.Value != "batched" {
		t.Fatalf("batched GETSTR = %#v, want batched value", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "old"}); !got.OK || got.Value != "0" {
		t.Fatalf("batched old EXISTS = %#v, want deleted", got)
	}

	unsafeBody := mustMarshalMonitoringTestCommand(t, CacheCommandRequest{
		Command: "INTERNALBATCH",
		Values: Slice{mustMarshalMonitoringTestCommand(t, CacheCommandRequest{
			Command: "SETSTR",
			Key:     "unsafe",
			Value:   "blocked",
		})},
	})
	req = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(unsafeBody))
	req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("unsafe batch status = %d body %q, want 200 command error", resp.Code, resp.Body.String())
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
		t.Fatalf("unsafe batch response JSON error = %v", err)
	}
	if response.OK {
		t.Fatalf("unsafe batch response = %#v, want command error", response)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "unsafe"}); !got.OK || got.Value != "0" {
		t.Fatalf("unsafe key EXISTS = %#v, want not mutated", got)
	}
}

func TestInternalReplicationBatchPreservesJournalDirtyAndSafetySideEffects(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("old", "value")
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatJSON)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()
	dirty := NewLevelDBDirtyTracker()
	safety := NewReplicationSafetyStore()
	request := CacheCommandRequest{
		Command: "INTERNALBATCH",
		Batch: []CacheCommandRequest{
			{
				Command: "INTERNALSET",
				Key:     "name",
				Value:   `{"type":"string","string":"batched"}`,
				Pairs: Map{
					replicationMetaSourceNode: "node-a",
					replicationMetaSequence:   "1",
				},
			},
			{
				Command: "INTERNALDEL",
				Key:     "old",
				Pairs: Map{
					replicationMetaSourceNode: "node-a",
					replicationMetaSequence:   "2",
				},
			},
		},
	}
	options := commandExecutionOptions{
		Journal:           journal,
		DirtyTracker:      dirty,
		ReplicationSafety: safety,
	}
	response, rejected := executeCacheCommand(context.Background(), ht, request, options)
	if rejected || !response.OK {
		t.Fatalf("executeCacheCommand(batch) = %#v rejected=%v, want ok", response, rejected)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "name"}); !got.OK || got.Value != "batched" {
		t.Fatalf("batched GETSTR = %#v, want batched value", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "old"}); !got.OK || got.Value != "0" {
		t.Fatalf("batched old EXISTS = %#v, want deleted", got)
	}
	if snapshot := dirty.Snapshot(); !reflect.DeepEqual(snapshot.keys, []string{"name", "old"}) {
		t.Fatalf("dirty keys = %#v, want name and old", snapshot.keys)
	}
	tail, err := journal.Tail(0, 10)
	if err != nil {
		t.Fatalf("journal Tail() error = %v", err)
	}
	if len(tail.Entries) != 2 {
		t.Fatalf("journal entries len = %d, want 2", len(tail.Entries))
	}

	response, rejected = executeCacheCommand(context.Background(), ht, request, options)
	if rejected || !response.OK {
		t.Fatalf("executeCacheCommand(duplicate batch) = %#v rejected=%v, want ok duplicate skip", response, rejected)
	}
	tail, err = journal.Tail(0, 10)
	if err != nil {
		t.Fatalf("journal Tail(duplicate) error = %v", err)
	}
	if len(tail.Entries) != 2 {
		t.Fatalf("journal entries after duplicate len = %d, want unchanged 2", len(tail.Entries))
	}
}

func TestInternalReplicationBatchEnvelopeAppliesSafetyOnce(t *testing.T) {
	ht := newTestTrie(t)
	topology := replicationTestTopology(t, "http://node-b")
	safety := NewReplicationSafetyStore()
	request := CacheCommandRequest{
		Command: replicationBatchEnvelopeCommand,
		Pairs: Map{
			replicationMetaSourceNode:          "node-a",
			replicationMetaSequence:            "42",
			replicationMetaTopologyFingerprint: topology.Fingerprint(),
		},
		Batch: []CacheCommandRequest{
			{Command: "INTERNALSET", Key: "name", Value: `{"type":"string","string":"first"}`},
			{Command: "INTERNALSET", Key: "city", Value: `{"type":"string","string":"Singapore"}`},
		},
	}
	options := commandExecutionOptions{Topology: topology, ReplicationSafety: safety}
	response, rejected := executeCacheCommand(context.Background(), ht, request, options)
	if rejected || !response.OK {
		t.Fatalf("executeCacheCommand(envelope) = %#v rejected=%v, want success", response, rejected)
	}

	request.Batch[0].Value = `{"type":"string","string":"duplicate"}`
	response, rejected = executeCacheCommand(context.Background(), ht, request, options)
	if rejected || !response.OK || response.Message != "duplicate replication command" {
		t.Fatalf("executeCacheCommand(duplicate envelope) = %#v rejected=%v, want duplicate success", response, rejected)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "name"}); !got.OK || got.Value != "first" {
		t.Fatalf("GETSTR after duplicate envelope = %#v, want first", got)
	}

	request.Pairs[replicationMetaSequence] = "43"
	request.Pairs[replicationMetaTopologyFingerprint] = "wrong-topology"
	response, rejected = executeCacheCommand(context.Background(), ht, request, options)
	if !rejected || response.OK || response.Message != "replication topology fingerprint mismatch" {
		t.Fatalf("executeCacheCommand(mismatched envelope) = %#v rejected=%v, want topology rejection", response, rejected)
	}
}

func TestTypedBinaryInternalSetCanonicalizesJournalEntry(t *testing.T) {
	source := newTestTrie(t)
	source.UpsertString("session:1", "one")
	request, ok := replicationCommandPayload(source, "session:1", replicationPayloadSet)
	if !ok {
		t.Fatal("replicationCommandPayload() ok = false")
	}
	request.Pairs = Map{replicationMetaSourceNode: "node-a", replicationMetaSequence: "1"}

	target := newTestTrie(t)
	journal, err := OpenCommandJournalWithFormat(filepath.Join(t.TempDir(), "commands.journal"), CommandJournalFormatBinary)
	if err != nil {
		t.Fatalf("OpenCommandJournalWithFormat() error = %v", err)
	}
	defer journal.Close()
	response, rejected := executeCacheCommand(context.Background(), target, request, commandExecutionOptions{
		Journal:           journal,
		ReplicationSafety: NewReplicationSafetyStore(),
	})
	if rejected || !response.OK {
		t.Fatalf("executeCacheCommand(binary set) = %#v rejected=%v, want success", response, rejected)
	}
	if got := target.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "session:1"}); !got.OK || got.Value != "one" {
		t.Fatalf("target GETSTR = %#v, want one", got)
	}
	tail, err := journal.Tail(0, 10)
	if err != nil {
		t.Fatalf("journal Tail() error = %v", err)
	}
	if len(tail.Entries) != 1 || tail.Entries[0].Request.Command != "INTERNALSET" || tail.Entries[0].Request.Value == "" || len(tail.Entries[0].Request.BinaryValue) != 0 {
		t.Fatalf("journal entry = %#v, want canonical legacy internal set", tail.Entries)
	}
}

func TestMonitoringHandlerRejectsInvalidInternalReplicationBatchWithoutPartialMutation(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{ReplicationAuthToken: "replica-secret"}).Handler()
	body := mustMarshalMonitoringTestCommand(t, CacheCommandRequest{
		Command: "INTERNALBATCH",
		Batch: []CacheCommandRequest{
			{
				Command: "INTERNALSET",
				Key:     "safe",
				Value:   `{"type":"string","string":"should-not-apply"}`,
			},
			{
				Command: "INTERNALSET",
				Key:     "broken",
				Value:   `{"type":"string"`,
			},
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(body))
	req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("invalid batch status = %d body %q, want 200 command error", resp.Code, resp.Body.String())
	}
	var response CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
		t.Fatalf("invalid batch response JSON error = %v", err)
	}
	if response.OK {
		t.Fatalf("invalid batch response = %#v, want command error", response)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "safe"}); !got.OK || got.Value != "0" {
		t.Fatalf("safe key EXISTS after invalid batch = %#v, want no partial mutation", got)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "broken"}); !got.OK || got.Value != "0" {
		t.Fatalf("broken key EXISTS after invalid batch = %#v, want no mutation", got)
	}
}

func TestMonitoringHandlerRejectsMalformedLegacyInternalReplicationBatchWithoutPartialMutation(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{ReplicationAuthToken: "replica-secret"}).Handler()
	body := mustMarshalMonitoringTestCommand(t, CacheCommandRequest{
		Command: "INTERNALBATCH",
		Values: Slice{
			mustMarshalMonitoringTestCommand(t, CacheCommandRequest{
				Command: "INTERNALSET",
				Key:     "safe",
				Value:   `{"type":"string","string":"should-not-apply"}`,
			}),
			`{"command":"INTERNALSET","key":"broken","value":"{\"type\":\"string\""`,
		},
	})
	req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(body))
	req.Header.Set("X-Hatrie-Replication-Token", "replica-secret")
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("malformed legacy batch status = %d body %q, want 200 command error", resp.Code, resp.Body.String())
	}
	var response CacheCommandResponse
	if err := json.Unmarshal(resp.Body.Bytes(), &response); err != nil {
		t.Fatalf("malformed legacy batch response JSON error = %v", err)
	}
	if response.OK {
		t.Fatalf("malformed legacy batch response = %#v, want command error", response)
	}
	if got := ht.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "safe"}); !got.OK || got.Value != "0" {
		t.Fatalf("safe key EXISTS after malformed legacy batch = %#v, want no partial mutation", got)
	}
}

func mustMarshalMonitoringTestCommand(t *testing.T, request CacheCommandRequest) string {
	t.Helper()
	data, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("Marshal(%#v) error = %v", request, err)
	}
	return string(data)
}

func TestMonitoringHandlerExposesRuntimeConfig(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		RuntimeConfig: map[string]interface{}{
			"monitoring_addr":       "127.0.0.1:8080",
			"monitoring_auth_token": "<redacted>",
			"rate_limit":            7,
		},
	}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/config", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("config status = %d, want 200", resp.Code)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(resp.Body.Bytes(), &got); err != nil {
		t.Fatalf("config JSON error = %v", err)
	}
	if got["monitoring_auth_token"] != "<redacted>" || got["monitoring_addr"] != "127.0.0.1:8080" || got["rate_limit"] != float64(7) {
		t.Fatalf("config = %#v, want redacted payload", got)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/config", nil))
	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("config POST status = %d, want 405", resp.Code)
	}
}

func TestMonitoringAuditLogRecordsDangerousActions(t *testing.T) {
	ht := newTestTrie(t)
	var audit bytes.Buffer
	auditLog := NewAuditLogger(&audit)
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		AuditLog: auditLog,
		Snapshot: func() error {
			return nil
		},
	}).Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"name","value":"ivi"}`))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("command status = %d, want 200", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/snapshot", strings.NewReader(`{}`)))
	if resp.Code != http.StatusOK {
		t.Fatalf("snapshot status = %d, want 200", resp.Code)
	}

	events := auditEventsFromJSONL(t, audit.String())
	if len(events) != 2 {
		t.Fatalf("audit events = %#v, want command and snapshot", events)
	}
	if events[0].Protocol != "http" || events[0].Action != "command" || events[0].Command != "SETSTR" || events[0].Key != "name" || !events[0].OK {
		t.Fatalf("command audit event = %#v, want safe command event", events[0])
	}
	if events[1].Action != "snapshot" || !events[1].OK {
		t.Fatalf("snapshot audit event = %#v, want successful snapshot", events[1])
	}
	if strings.Contains(audit.String(), "ivi") {
		t.Fatalf("audit log leaked command value: %s", audit.String())
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/audit?limit=1", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("audit status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	var status auditStatus
	if err := json.Unmarshal(resp.Body.Bytes(), &status); err != nil {
		t.Fatalf("audit status JSON error = %v", err)
	}
	if !status.Configured || status.Limit != 1 || len(status.Events) != 1 || status.Events[0].Action != "snapshot" {
		t.Fatalf("audit status = %#v, want latest snapshot event", status)
	}
	if strings.Contains(resp.Body.String(), "ivi") {
		t.Fatalf("audit status leaked command value: %s", resp.Body.String())
	}
}

func TestMonitoringAuditStatusReportsUnconfiguredLogger(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/api/audit", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("audit status = %d, want 200: %s", resp.Code, resp.Body.String())
	}
	var status auditStatus
	if err := json.Unmarshal(resp.Body.Bytes(), &status); err != nil {
		t.Fatalf("audit status JSON error = %v", err)
	}
	if status.Configured || len(status.Events) != 0 {
		t.Fatalf("audit status = %#v, want unconfigured empty event list", status)
	}
}

func TestMonitoringWriteProtectionRejectsDangerousActions(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{WriteProtected: true}).Handler()

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"name","value":"ivi"}`))
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusForbidden {
		t.Fatalf("write-protected command status = %d, want 403", resp.Code)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("write-protected command wrote value %q, want empty", got)
	}

	resp = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"BATCH","batch":[{"command":"GET","key":"name"},{"command":"SETSTR","key":"name","value":"ivi"}]}`))
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusForbidden {
		t.Fatalf("write-protected batch status = %d, want 403", resp.Code)
	}
	if got := ht.GetString("name"); got != "" {
		t.Fatalf("write-protected batch wrote value %q, want empty", got)
	}
}

func TestMonitoringRateLimitRejectsDangerousActions(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{RateLimiter: NewRateLimiter(1, time.Second)}).Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"one","value":"1"}`))
	req.RemoteAddr = "127.0.0.1:12345"
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("first command status = %d, want 200", resp.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"two","value":"2"}`))
	req.RemoteAddr = "127.0.0.1:12345"
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusTooManyRequests {
		t.Fatalf("second command status = %d, want 429", resp.Code)
	}
}

func TestMonitoringHandlerExposesPrometheusMetrics(t *testing.T) {
	ht := newTestTrie(t)
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatalf("OpenCommandJournal() error = %v", err)
	}
	defer journal.Close()
	if got := journal.ExecuteCommand(ht, CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "ivi"}); !got.OK {
		t.Fatalf("journaled SETSTR response = %#v, want ok", got)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName: "node-a",
		StartAt:  time.Now().Add(-2 * time.Second),
		Journal:  journal,
	}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", resp.Code)
	}
	if contentType := resp.Header().Get("Content-Type"); !strings.Contains(contentType, "text/plain") {
		t.Fatalf("metrics content type = %q, want text/plain", contentType)
	}
	body := resp.Body.String()
	for _, token := range []string{
		"# HELP hatrie_cache_up",
		"# TYPE hatrie_cache_reads_total counter",
		`hatrie_cache_up{node="node-a"} 1`,
		`hatrie_cache_keys{node="node-a"} 1`,
		`hatrie_cache_audit_events_total{node="node-a"} 0`,
		`hatrie_cache_audit_errors_total{node="node-a"} 0`,
		`hatrie_cache_write_protection_rejections_total{node="node-a"} 0`,
		`hatrie_cache_rate_limit_rejections_total{node="node-a"} 0`,
		`hatrie_cache_write_protection_enabled{node="node-a"} 0`,
		`hatrie_cache_rate_limit_per_second{node="node-a"} 0`,
		`hatrie_cache_journal_sequence{node="node-a"} 1`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("metrics body missing %q:\n%s", token, body)
		}
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/metrics", nil))
	if resp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("metrics POST status = %d, want 405", resp.Code)
	}
}

func TestMonitoringPrometheusMetricsCountAuditAndLimitRejections(t *testing.T) {
	ht := newTestTrie(t)
	metrics := NewAPIMetrics()
	var audit bytes.Buffer
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:    "node-a",
		AuditLog:    NewAuditLogger(&audit),
		RateLimiter: NewRateLimiter(1, time.Second),
		Metrics:     metrics,
	}).Handler()

	req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"one","value":"1"}`))
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("first command status = %d, want 200", resp.Code)
	}
	req = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"two","value":"2"}`))
	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusTooManyRequests {
		t.Fatalf("second command status = %d, want 429", resp.Code)
	}

	protectedHandler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:       "node-a",
		WriteProtected: true,
		Metrics:        metrics,
	}).Handler()
	resp = httptest.NewRecorder()
	protectedHandler.ServeHTTP(resp, httptest.NewRequest(http.MethodPost, "/api/snapshot", nil))
	if resp.Code != http.StatusForbidden {
		t.Fatalf("write-protected snapshot status = %d, want 403", resp.Code)
	}

	resp = httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", resp.Code)
	}
	body := resp.Body.String()
	for _, token := range []string{
		`hatrie_cache_audit_events_total{node="node-a"} 2`,
		`hatrie_cache_audit_errors_total{node="node-a"} 0`,
		`hatrie_cache_rate_limit_rejections_total{node="node-a"} 1`,
		`hatrie_cache_write_protection_rejections_total{node="node-a"} 1`,
		`hatrie_cache_rate_limit_per_second{node="node-a"} 1`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("metrics body missing %q:\n%s", token, body)
		}
	}
}

func TestMonitoringPrometheusMetricsExposeReplicationHealthScore(t *testing.T) {
	ht := newTestTrie(t)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		AsyncQueueSize: 2,
	})
	defer replicator.Close()
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:   "node-a",
		Replicator: replicator,
	}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", resp.Code)
	}
	body := resp.Body.String()
	for _, token := range []string{
		"# HELP hatrie_cache_replication_health_score",
		"# TYPE hatrie_cache_replication_health_score gauge",
		`hatrie_cache_replication_health_score{node="node-a"} 100`,
		`hatrie_cache_replication_dead_letters{node="node-a"} 0`,
		`hatrie_cache_replication_queue_capacity{node="node-a"} 2`,
		`hatrie_cache_replication_queue_enqueued_total{node="node-a"} 0`,
		`hatrie_cache_replication_retried_total{node="node-a"} 0`,
		`hatrie_cache_replication_queue_oldest_age_millis{node="node-a"} 0`,
		`hatrie_cache_replication_in_flight_age_millis{node="node-a"} 0`,
		`hatrie_cache_replication_last_retry_age_millis{node="node-a"} 0`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("metrics body missing %q:\n%s", token, body)
		}
	}
}

func TestMonitoringPrometheusMetricsExposeReplicationLatencyAndBatchHistograms(t *testing.T) {
	ht := newTestTrie(t)
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{Self: "node-a"})
	replicator.recordReplicationTargetLatency(TopologyNode{ID: "node-b"}, 12*time.Millisecond)
	replicator.recordReplicationBatchSize(TopologyNode{ID: "node-b"}, 3)
	replicator.recordReplicationRetryDelay(25 * time.Millisecond)
	replicator.recordReplicationCircuitTransition(TopologyNode{ID: "node-b"}, replicationCircuitOpen)
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:   "node-a",
		Replicator: replicator,
	}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", resp.Code)
	}
	body := resp.Body.String()
	for _, token := range []string{
		"# HELP hatrie_cache_replication_target_latency_millis",
		"# TYPE hatrie_cache_replication_target_latency_millis histogram",
		`hatrie_cache_replication_target_latency_millis_bucket{node="node-a",target="node-b",le="+Inf"} 1`,
		`hatrie_cache_replication_target_latency_millis_count{node="node-a",target="node-b"} 1`,
		"# HELP hatrie_cache_replication_batch_items",
		`hatrie_cache_replication_batch_items_bucket{node="node-a",target="node-b",le="+Inf"} 1`,
		`hatrie_cache_replication_batch_items_count{node="node-a",target="node-b"} 1`,
		"# HELP hatrie_cache_replication_retry_delay_millis",
		`hatrie_cache_replication_retry_delay_millis_bucket{node="node-a",le="+Inf"} 1`,
		`hatrie_cache_replication_retry_delay_millis_count{node="node-a"} 1`,
		"# HELP hatrie_cache_replication_circuit_breaker_transitions_total",
		`hatrie_cache_replication_circuit_breaker_transitions_total{node="node-a",target="node-b",state="open"} 1`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("metrics body missing %q:\n%s", token, body)
		}
	}
}

func TestMonitoringPrometheusMetricsExposeStorageOpsState(t *testing.T) {
	ht := newTestTrie(t)
	store, err := OpenLevelDBStore(filepath.Join(t.TempDir(), "cache.leveldb"))
	if err != nil {
		t.Fatalf("OpenLevelDBStore() error = %v", err)
	}
	defer store.Close()
	dirty := NewLevelDBDirtyTracker()
	dirty.Mark("session:dirty")
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:            "node-a",
		LevelDBStore:        store,
		LevelDBDirtyTracker: dirty,
	})
	handler.recordStorageFlush(LevelDBFlushResult{Store: "leveldb", Keys: 3, DurationMillis: 7})
	handler.recordStorageCompact(LevelDBCompactionResult{Store: "leveldb", DurationMillis: 9, SizeBytesDelta: -4})
	handler.recordStorageSpill(LevelDBSpillResult{Store: "leveldb", KeysSpilled: 2, HotBytesBefore: 512, HotBytesAfter: 128, DurationMillis: 11})
	if !handler.beginStorageOperation("flush") {
		t.Fatal("beginStorageOperation(flush) = false, want true")
	}
	defer handler.finishStorageOperation()

	resp := httptest.NewRecorder()
	handler.Handler().ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", resp.Code)
	}
	body := resp.Body.String()
	for _, token := range []string{
		`hatrie_cache_leveldb_dirty_keys{node="node-a"} 1`,
		`hatrie_cache_storage_operation_running{node="node-a"} 1`,
		`hatrie_cache_storage_last_flush_keys{node="node-a"} 3`,
		`hatrie_cache_storage_last_flush_duration_millis{node="node-a"} 7`,
		`hatrie_cache_storage_last_compact_duration_millis{node="node-a"} 9`,
		`hatrie_cache_storage_last_compact_size_bytes_delta{node="node-a"} -4`,
		`hatrie_cache_storage_last_spill_keys{node="node-a"} 2`,
		`hatrie_cache_storage_last_spill_duration_millis{node="node-a"} 11`,
		`hatrie_cache_storage_last_spill_hot_bytes_before{node="node-a"} 512`,
		`hatrie_cache_storage_last_spill_hot_bytes_after{node="node-a"} 128`,
	} {
		if !strings.Contains(body, token) {
			t.Fatalf("metrics body missing %q:\n%s", token, body)
		}
	}
}

func TestMonitoringHandlerManagesTopology(t *testing.T) {
	ht := newTestTrie(t)
	store, err := NewTopologyStore(SingleNodeTopology("node-a", "127.0.0.1:8080"))
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{Topology: store}).Handler()

	getResp := httptest.NewRecorder()
	handler.ServeHTTP(getResp, httptest.NewRequest(http.MethodGet, "/api/topology", nil))
	if getResp.Code != http.StatusOK {
		t.Fatalf("topology GET status = %d, want 200", getResp.Code)
	}
	var got ClusterTopology
	if err := json.Unmarshal(getResp.Body.Bytes(), &got); err != nil {
		t.Fatalf("topology GET JSON error = %v", err)
	}
	if got.Self != "node-a" || got.Mode != TopologyModeFullReplica || len(got.Shards) != 0 {
		t.Fatalf("topology GET = %#v, want node-a with sharding off", got)
	}

	update := `{"version":1,"self":"node-b","nodes":[{"id":"node-b","address":"127.0.0.1:8081"}],"shards":[{"id":0,"primary":"node-b"}]}`
	putResp := httptest.NewRecorder()
	handler.ServeHTTP(putResp, httptest.NewRequest(http.MethodPut, "/api/topology", bytes.NewBufferString(update)))
	if putResp.Code != http.StatusOK {
		t.Fatalf("topology PUT status = %d body %q, want 200", putResp.Code, putResp.Body.String())
	}
	if store.Get().Self != "node-b" {
		t.Fatalf("stored topology = %#v, want node-b", store.Get())
	}

	routeResp := httptest.NewRecorder()
	handler.ServeHTTP(routeResp, httptest.NewRequest(http.MethodGet, "/api/topology?key=session:1", nil))
	if routeResp.Code != http.StatusOK {
		t.Fatalf("topology route status = %d, want 200", routeResp.Code)
	}
	var route TopologyRoute
	if err := json.Unmarshal(routeResp.Body.Bytes(), &route); err != nil {
		t.Fatalf("route JSON error = %v", err)
	}
	if route.Key != "session:1" || route.Shard.Primary != "node-b" {
		t.Fatalf("route = %#v, want node-b primary", route)
	}

	badResp := httptest.NewRecorder()
	handler.ServeHTTP(badResp, httptest.NewRequest(http.MethodPut, "/api/topology", bytes.NewBufferString(`{"version":1}`)))
	if badResp.Code != http.StatusBadRequest {
		t.Fatalf("bad topology status = %d, want 400", badResp.Code)
	}
}

func TestMonitoringHandlerManagesElection(t *testing.T) {
	ht := newTestTrie(t)
	topology, err := NewTopologyStore(ClusterTopology{
		Version: 1,
		Nodes: []TopologyNode{
			{ID: "node-a"},
			{ID: "node-b"},
		},
		Shards: []TopologyShard{
			{ID: 0, Primary: "node-a", Replicas: []string{"node-b"}},
		},
	})
	if err != nil {
		t.Fatalf("NewTopologyStore() error = %v", err)
	}
	election := NewElectionStore(topology, ElectionOptions{})
	handler := NewMonitoringHandler(ht, MonitoringOptions{Topology: topology, Election: election}).Handler()

	statusResp := httptest.NewRecorder()
	handler.ServeHTTP(statusResp, httptest.NewRequest(http.MethodGet, "/api/election", nil))
	if statusResp.Code != http.StatusOK {
		t.Fatalf("election GET status = %d, want 200", statusResp.Code)
	}
	var status ElectionStatus
	if err := json.Unmarshal(statusResp.Body.Bytes(), &status); err != nil {
		t.Fatalf("election status JSON error = %v", err)
	}
	if len(status.Leaders) != 1 || status.Leaders[0].Leader != "node-a" {
		t.Fatalf("election status = %#v, want node-a leader", status)
	}

	offlineResp := httptest.NewRecorder()
	handler.ServeHTTP(offlineResp, httptest.NewRequest(http.MethodPost, "/api/election", bytes.NewBufferString(`{"node":"node-a","online":false}`)))
	if offlineResp.Code != http.StatusOK {
		t.Fatalf("election offline status = %d body %q, want 200", offlineResp.Code, offlineResp.Body.String())
	}
	if got := election.Status().Leaders[0]; got.Leader != "node-b" {
		t.Fatalf("leader after offline = %#v, want node-b", got)
	}

	routeResp := httptest.NewRecorder()
	handler.ServeHTTP(routeResp, httptest.NewRequest(http.MethodGet, "/api/election?key=session:1", nil))
	if routeResp.Code != http.StatusOK {
		t.Fatalf("election route status = %d, want 200", routeResp.Code)
	}
	var route ElectionKeyRoute
	if err := json.Unmarshal(routeResp.Body.Bytes(), &route); err != nil {
		t.Fatalf("election route JSON error = %v", err)
	}
	if route.Key != "session:1" || route.Leader.Leader != "node-b" {
		t.Fatalf("election route = %#v, want node-b leader", route)
	}

	badResp := httptest.NewRecorder()
	handler.ServeHTTP(badResp, httptest.NewRequest(http.MethodPost, "/api/election", bytes.NewBufferString(`{"node":"missing"}`)))
	if badResp.Code != http.StatusBadRequest {
		t.Fatalf("bad election status = %d, want 400", badResp.Code)
	}

	methodResp := httptest.NewRecorder()
	handler.ServeHTTP(methodResp, httptest.NewRequest(http.MethodDelete, "/api/election", nil))
	if methodResp.Code != http.StatusMethodNotAllowed {
		t.Fatalf("election DELETE status = %d, want 405", methodResp.Code)
	}
}

func TestMonitoringHandlerEnforcesLeaderWrites(t *testing.T) {
	ht := newTestTrie(t)
	topology := replicationTestTopology(t, "127.0.0.1:1")
	election := NewElectionStore(topology, ElectionOptions{})
	if err := election.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(node-a) error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:            "node-a",
		Topology:            topology,
		Election:            election,
		EnforceLeaderWrites: true,
	}).Handler()

	writeResp := httptest.NewRecorder()
	handler.ServeHTTP(writeResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"session:1","value":"value"}`)))
	if writeResp.Code != http.StatusConflict {
		t.Fatalf("follower write status = %d, want 409", writeResp.Code)
	}
	var rejected CacheCommandResponse
	if err := json.Unmarshal(writeResp.Body.Bytes(), &rejected); err != nil {
		t.Fatalf("follower write JSON error = %v", err)
	}
	if rejected.OK || !strings.Contains(rejected.Message, "leader is node-b") {
		t.Fatalf("follower write response = %#v, want leader rejection", rejected)
	}
	if got := ht.GetString("session:1"); got != "" {
		t.Fatalf("follower write stored %q, want no local write", got)
	}

	readResp := httptest.NewRecorder()
	handler.ServeHTTP(readResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"GET","key":"session:1"}`)))
	if readResp.Code != http.StatusOK {
		t.Fatalf("follower read status = %d, want 200", readResp.Code)
	}

	internalResp := httptest.NewRecorder()
	handler.ServeHTTP(internalResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"INTERNALSET","key":"session:1","value":"{\"type\":\"string\",\"string\":\"replicated\"}"}`)))
	if internalResp.Code != http.StatusOK {
		t.Fatalf("internal replication status = %d, want 200", internalResp.Code)
	}
	if got := ht.GetString("session:1"); got != "replicated" {
		t.Fatalf("internal replicated value = %q, want replicated", got)
	}
}

func TestMonitoringHandlerAllowsElectedLeaderWrites(t *testing.T) {
	ht := newTestTrie(t)
	topology := replicationTestTopology(t, "127.0.0.1:1")
	election := NewElectionStore(topology, ElectionOptions{})
	if err := election.MarkOffline("node-a"); err != nil {
		t.Fatalf("MarkOffline(node-a) error = %v", err)
	}
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:            "node-b",
		Topology:            topology,
		Election:            election,
		EnforceLeaderWrites: true,
	}).Handler()

	writeResp := httptest.NewRecorder()
	handler.ServeHTTP(writeResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"session:1","value":"value"}`)))
	if writeResp.Code != http.StatusOK {
		t.Fatalf("leader write status = %d, want 200", writeResp.Code)
	}
	if got := ht.GetString("session:1"); got != "value" {
		t.Fatalf("leader write stored %q, want value", got)
	}
}

func TestMonitoringHandlerReplicatesCommands(t *testing.T) {
	ht := newTestTrie(t)
	var gotRequest CacheCommandRequest
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		gotRequest = mustDecodeReplicationTestCommand(t, w, r)
		writeJSON(w, CacheCommandResponse{OK: true, Message: "replicated"})
	}))
	defer target.Close()

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:   "node-a",
		Topology:   topology,
		Election:   election,
		Replicator: replicator,
	}).Handler()

	commandResp := httptest.NewRecorder()
	handler.ServeHTTP(commandResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"session:1","value":"value"}`)))
	if commandResp.Code != http.StatusOK {
		t.Fatalf("command status = %d, want 200", commandResp.Code)
	}
	if !isTypedReplicationSetPayload(gotRequest, "session:1") {
		t.Fatalf("replicated request = %#v, want typed binary snapshot", gotRequest)
	}

	replicationResp := httptest.NewRecorder()
	handler.ServeHTTP(replicationResp, httptest.NewRequest(http.MethodGet, "/api/replication", nil))
	if replicationResp.Code != http.StatusOK {
		t.Fatalf("replication status = %d, want 200", replicationResp.Code)
	}
	var result ReplicationResult
	if err := json.Unmarshal(replicationResp.Body.Bytes(), &result); err != nil {
		t.Fatalf("replication JSON error = %v", err)
	}
	if len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("replication result = %#v, want one ok target", result)
	}
	assertReplicationResultTiming(t, result)
}

func TestMonitoringHandlerReportsAsyncReplicationQueue(t *testing.T) {
	ht := newTestTrie(t)
	release := make(chan struct{})
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-release
		writeJSON(w, CacheCommandResponse{OK: true, Message: "replicated"})
	}))
	defer target.Close()
	defer close(release)

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:           "node-a",
		Topology:       topology,
		Election:       election,
		Client:         target.Client(),
		AsyncQueueSize: 2,
	})
	defer replicator.Close()
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:   "node-a",
		Topology:   topology,
		Election:   election,
		Replicator: replicator,
	}).Handler()

	commandResp := httptest.NewRecorder()
	handler.ServeHTTP(commandResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"session:1","value":"value"}`)))
	if commandResp.Code != http.StatusOK {
		t.Fatalf("command status = %d, want 200", commandResp.Code)
	}

	replicationResp := httptest.NewRecorder()
	handler.ServeHTTP(replicationResp, httptest.NewRequest(http.MethodGet, "/api/replication", nil))
	if replicationResp.Code != http.StatusOK {
		t.Fatalf("replication status = %d, want 200", replicationResp.Code)
	}
	var result ReplicationResult
	if err := json.Unmarshal(replicationResp.Body.Bytes(), &result); err != nil {
		t.Fatalf("replication JSON error = %v", err)
	}
	if !result.Queued || result.Queue == nil || !result.Queue.Enabled || result.Queue.Capacity != 2 || result.Queue.Enqueued != 1 {
		t.Fatalf("async replication result = %#v, want queued result with queue stats", result)
	}
	if result.Health == "" || result.HealthScore <= 0 || result.HealthScore > 100 {
		t.Fatalf("async replication health = %q/%d, want populated score", result.Health, result.HealthScore)
	}
}

func TestMonitoringHandlerReportsReplicationDeadLetters(t *testing.T) {
	ht := newTestTrie(t)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "down", http.StatusBadGateway)
	}))
	defer target.Close()

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:             "node-a",
		Topology:         topology,
		Election:         election,
		Client:           target.Client(),
		AsyncQueueSize:   1,
		AsyncMaxAttempts: 1,
	})
	defer replicator.Close()
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:   "node-a",
		Topology:   topology,
		Election:   election,
		Replicator: replicator,
	}).Handler()

	commandResp := httptest.NewRecorder()
	handler.ServeHTTP(commandResp, httptest.NewRequest(http.MethodPost, "/api/commands", bytes.NewBufferString(`{"command":"SETSTR","key":"session:dead","value":"value"}`)))
	if commandResp.Code != http.StatusOK {
		t.Fatalf("command status = %d, want 200", commandResp.Code)
	}

	var result ReplicationResult
	waitUntil(t, time.Second, func() bool {
		replicationResp := httptest.NewRecorder()
		handler.ServeHTTP(replicationResp, httptest.NewRequest(http.MethodGet, "/api/replication", nil))
		if replicationResp.Code != http.StatusOK {
			return false
		}
		result = ReplicationResult{}
		if err := json.Unmarshal(replicationResp.Body.Bytes(), &result); err != nil {
			return false
		}
		return result.DeadLetterCount == 1
	})
	if len(result.DeadLetters) != 1 || result.DeadLetters[0].Key != "session:dead" || result.DeadLetters[0].Attempts != 1 {
		t.Fatalf("replication dead letters = %#v, want retained session:dead failure", result.DeadLetters)
	}
}

func TestMonitoringHandlerSyncsReplication(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("session:1", "value")
	requests := make(chan CacheCommandRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/commands" {
			t.Fatalf("path = %s, want /api/commands", r.URL.Path)
		}
		request := mustDecodeReplicationTestCommand(t, w, r)
		if rejectReplicationDigestTestCommand(t, w, r, request) {
			return
		}
		requests <- request
		writeJSON(w, CacheCommandResponse{OK: true, Message: "replicated"})
	}))
	defer target.Close()

	topology := replicationTestTopology(t, target.URL)
	election := NewElectionStore(topology, ElectionOptions{})
	replicator := NewHTTPReplicator(HTTPReplicatorOptions{
		Self:     "node-a",
		Topology: topology,
		Election: election,
		Client:   target.Client(),
	})
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		NodeName:   "node-a",
		Topology:   topology,
		Election:   election,
		Replicator: replicator,
	}).Handler()

	replicationResp := httptest.NewRecorder()
	handler.ServeHTTP(replicationResp, httptest.NewRequest(http.MethodPost, "/api/replication", bytes.NewBufferString(`{"prefix":"session:"}`)))
	if replicationResp.Code != http.StatusOK {
		t.Fatalf("replication sync status = %d, want 200", replicationResp.Code)
	}
	var result ReplicationResult
	if err := json.Unmarshal(replicationResp.Body.Bytes(), &result); err != nil {
		t.Fatalf("replication sync JSON error = %v", err)
	}
	if result.Skipped || result.Command != "SYNC" || result.Key != "session:" || result.Entries != 1 || len(result.Targets) != 1 || !result.Targets[0].OK {
		t.Fatalf("replication sync result = %#v, want one ok target", result)
	}
	select {
	case request := <-requests:
		if !isTypedReplicationSetPayload(request, "session:1") {
			t.Fatalf("replication sync request = %#v, want typed binary snapshot", request)
		}
	default:
		t.Fatal("replication sync did not reach remote target")
	}

	invalidResp := httptest.NewRecorder()
	handler.ServeHTTP(invalidResp, httptest.NewRequest(http.MethodPost, "/api/replication", bytes.NewBufferString(`{"unknown":true}`)))
	if invalidResp.Code != http.StatusBadRequest {
		t.Fatalf("invalid replication sync status = %d, want 400", invalidResp.Code)
	}

	oversizedResp := httptest.NewRecorder()
	oversizedBody := `{"prefix":"` + strings.Repeat("x", maxMonitoringJSONRequestBytes) + `"}`
	handler.ServeHTTP(oversizedResp, httptest.NewRequest(http.MethodPost, "/api/replication", strings.NewReader(oversizedBody)))
	if oversizedResp.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized replication sync status = %d, want 413", oversizedResp.Code)
	}
}

func canceledMonitoringRequest(method string, target string, body string) *http.Request {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return httptest.NewRequest(method, target, bytes.NewBufferString(body)).WithContext(ctx)
}

func entryKeysFromMonitoring(entries []MonitoringEntry) []string {
	keys := make([]string, len(entries))
	for idx, entry := range entries {
		keys[idx] = entry.Key
	}
	return keys
}
