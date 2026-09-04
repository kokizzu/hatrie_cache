package hatCache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func BenchmarkMonitoringCommandSlowCapture(b *testing.B) {
	for _, test := range []struct {
		name    string
		options MonitoringOptions
	}{
		{name: "Disabled", options: MonitoringOptions{}},
		{name: "Enabled", options: MonitoringOptions{SlowCommandThreshold: time.Nanosecond, SlowCommandCapacity: 128}},
	} {
		b.Run(test.name, func(b *testing.B) {
			trie := CreateHatTrie()
			defer trie.Destroy()
			handler := NewMonitoringHandler(trie, test.options).Handler()
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				request := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"benchmark:key","value":"value"}`))
				request.Header.Set("Content-Type", "application/json")
				response := httptest.NewRecorder()
				handler.ServeHTTP(response, request)
				if response.Code != http.StatusOK {
					b.Fatalf("command status = %d: %s", response.Code, response.Body.String())
				}
			}
		})
	}
}
