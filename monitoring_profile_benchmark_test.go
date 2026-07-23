package hatriecache

import (
	"io"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"testing"
)

func BenchmarkMonitoringProfilingDisabled(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	handler := NewMonitoringHandler(trie, MonitoringOptions{NodeName: "benchmark"}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/profile", nil)
	response := httptest.NewRecorder()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response.Body.Reset()
		response.Code = http.StatusOK
		handler.ServeHTTP(response, request)
	}
}

func BenchmarkMonitoringProfileCapture(b *testing.B) {
	tests := []struct {
		name string
		body string
	}{
		{name: "CPU1s", body: `{"type":"cpu","duration_millis":1000}`},
		{name: "Heap", body: `{"type":"heap"}`},
		{name: "Goroutine", body: `{"type":"goroutine"}`},
	}
	for _, tt := range tests {
		b.Run(tt.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			handler := NewMonitoringHandler(trie, MonitoringOptions{
				NodeName:             "benchmark",
				AuthToken:            "benchmark-secret",
				DiagnosticsProfiling: true,
			}).Handler()
			var profileBytes int64
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				request := httptest.NewRequest(http.MethodPost, "/api/profile", strings.NewReader(tt.body))
				request.Header.Set("Authorization", "Bearer benchmark-secret")
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusOK {
					b.Fatalf("profile status = %d: %s", response.Code, response.Body.String())
				}
				profileBytes += int64(response.Body.Len())
			}
			b.ReportMetric(float64(profileBytes)/float64(b.N), "profile-B/op")
		})
	}
}

func BenchmarkMonitoringCPUProfileCommandOverhead(b *testing.B) {
	for _, profiling := range []bool{false, true} {
		name := "Baseline"
		if profiling {
			name = "DuringCPUProfile"
		}
		b.Run(name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			if response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETSTR", Key: "profile:key", Value: "value"}); !response.OK {
				b.Fatalf("SETSTR response = %#v", response)
			}
			profileStarted := false
			if profiling {
				if err := pprof.StartCPUProfile(io.Discard); err != nil {
					b.Fatal(err)
				}
				profileStarted = true
			}
			b.Cleanup(func() {
				if profileStarted {
					pprof.StopCPUProfile()
				}
			})
			request := CacheCommandRequest{Command: "GET", Key: "profile:key"}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				response := trie.ExecuteCommand(request)
				if !response.OK {
					b.Fatalf("GET response = %#v", response)
				}
				benchmarkCommandResponseSink = response
			}
			b.StopTimer()
			if profileStarted {
				pprof.StopCPUProfile()
				profileStarted = false
			}
		})
	}
}
