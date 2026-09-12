package hatCache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMemoryAccountingBreakdownMatchesBackingTotal(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("tt046:string", "value")

	report := trie.MemoryAccounting()
	if report.NativeTrieBytes == 0 {
		t.Fatal("native trie bytes = 0, want allocated trie")
	}
	if report.TotalBackingBytes == 0 {
		t.Fatal("total backing bytes = 0, want allocated backing")
	}
	var backing uint64
	var stringsBytes uint64
	for _, structure := range report.Structures {
		backing += structure.BackingBytes
		if structure.Name == "strings" {
			stringsBytes = structure.BackingBytes
		}
	}
	if backing != report.TotalBackingBytes {
		t.Fatalf("structure backing sum = %d, report total = %d", backing, report.TotalBackingBytes)
	}
	if stringsBytes == 0 {
		t.Fatal("strings backing bytes = 0, want allocated string backing")
	}
	if report.TotalBytes != report.NativeTrieBytes+report.TotalBackingBytes {
		t.Fatalf("total bytes = %d, native + backing = %d", report.TotalBytes, report.NativeTrieBytes+report.TotalBackingBytes)
	}
}

func TestMemoryAccountingAggregatesLocalPartitions(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	if err := trie.ConfigureLocalPartitions(2); err != nil {
		t.Fatalf("ConfigureLocalPartitions() error = %v", err)
	}
	trie.UpsertString("tt046:partition:one", "one")
	trie.UpsertString("tt046:partition:two", "two")

	report := trie.MemoryAccounting()
	if report.NativeTrieBytes == 0 || report.TotalBackingBytes == 0 {
		t.Fatalf("partitioned report = %#v, want native and backing bytes", report)
	}
	var backing uint64
	for _, structure := range report.Structures {
		backing += structure.BackingBytes
	}
	if backing != report.TotalBackingBytes {
		t.Fatalf("partitioned structure backing sum = %d, report total = %d", backing, report.TotalBackingBytes)
	}
}

func TestMonitoringMemoryStructuresEndpoint(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("tt046:http", "value")
	handler := NewMonitoringHandler(trie, MonitoringOptions{}).Handler()

	request := httptest.NewRequest(http.MethodGet, "/api/memory/structures", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET /api/memory/structures status = %d, want %d", response.Code, http.StatusOK)
	}
	var report MemoryAccountingReport
	if err := json.NewDecoder(response.Body).Decode(&report); err != nil {
		t.Fatalf("decode memory accounting response: %v", err)
	}
	if report.TotalBytes == 0 || len(report.Structures) == 0 {
		t.Fatalf("memory accounting response = %#v, want populated report", report)
	}
}

func TestMonitoringMemoryAccountingPrometheusAndOpenAPI(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	trie.UpsertString("tt046:metrics", "value")
	handler := NewMonitoringHandler(trie, MonitoringOptions{}).Handler()

	metricsResponse := httptest.NewRecorder()
	handler.ServeHTTP(metricsResponse, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if metricsResponse.Code != http.StatusOK {
		t.Fatalf("GET /metrics status = %d, want %d", metricsResponse.Code, http.StatusOK)
	}
	metrics := metricsResponse.Body.String()
	for _, token := range []string{
		"# HELP hatrie_cache_native_trie_bytes",
		"# HELP hatrie_cache_backing_bytes",
		"# HELP hatrie_cache_structure_backing_bytes",
		"structure=\"strings\"",
	} {
		if !strings.Contains(metrics, token) {
			t.Fatalf("metrics missing %q", token)
		}
	}

	openAPIResponse := httptest.NewRecorder()
	handler.ServeHTTP(openAPIResponse, httptest.NewRequest(http.MethodGet, "/openapi.json", nil))
	if openAPIResponse.Code != http.StatusOK {
		t.Fatalf("GET /openapi.json status = %d, want %d", openAPIResponse.Code, http.StatusOK)
	}
	var document struct {
		Paths      map[string]interface{} `json:"paths"`
		Components struct {
			Schemas map[string]interface{} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.NewDecoder(openAPIResponse.Body).Decode(&document); err != nil {
		t.Fatalf("decode OpenAPI response: %v", err)
	}
	if _, ok := document.Paths["/api/memory/structures"]; !ok {
		t.Fatal("OpenAPI document missing /api/memory/structures")
	}
	if _, ok := document.Components.Schemas["MemoryAccountingReport"]; !ok {
		t.Fatal("OpenAPI document missing MemoryAccountingReport schema")
	}
}

func BenchmarkTT046MemoryAccounting(b *testing.B) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	for index := 0; index < 256; index++ {
		trie.UpsertString("tt046:report:"+string(rune(index)), "value")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		report := trie.MemoryAccounting()
		if report.TotalBytes == 0 {
			b.Fatal("memory accounting returned zero bytes")
		}
	}
}
