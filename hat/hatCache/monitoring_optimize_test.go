package hatCache

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestMonitoringOptimizeRouteDisabledByDefault(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/storage/optimize", strings.NewReader(`{"target":"events/part-1"}`)))
	if response.Code != http.StatusNotFound {
		t.Fatalf("disabled optimize status = %d, want 404", response.Code)
	}
}

func TestMonitoringOptimizeRouteRunsInjectedController(t *testing.T) {
	controller, err := hatStorage.NewCompactionController(hatStorage.CompactionControllerOptions{
		MaxPending:      4,
		HistoryCapacity: 4,
	})
	if err != nil {
		t.Fatalf("NewCompactionController() error = %v", err)
	}
	var request MonitoringOptimizeRequest
	runs := 0
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		OptimizeController: controller,
		OptimizeResolver: func(ctx context.Context, got MonitoringOptimizeRequest) (hatStorage.CompactionRequest, error) {
			if ctx == nil {
				t.Fatal("resolver context is nil")
			}
			request = got
			return hatStorage.CompactionRequest{
				Target:         got.Target,
				Priority:       got.Priority,
				EstimatedBytes: got.EstimatedBytes,
				Run: func(context.Context) error {
					runs++
					return nil
				},
			}, nil
		},
	}).Handler()

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/storage/optimize", strings.NewReader(`{"target":" events/part-1 ","priority":7,"estimated_bytes":4096}`)))
	if response.Code != http.StatusOK {
		t.Fatalf("optimize status = %d, want 200: %s", response.Code, response.Body.String())
	}
	var result struct {
		Accepted bool                     `json:"accepted"`
		Job      hatStorage.CompactionJob `json:"job"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatalf("optimize response JSON error = %v", err)
	}
	if !result.Accepted || result.Job.State != hatStorage.CompactionJobSucceeded || result.Job.Target != "events/part-1" {
		t.Fatalf("optimize result = %#v, want accepted succeeded job", result)
	}
	if runs != 1 || request.Target != "events/part-1" || request.Priority != 7 || request.EstimatedBytes != 4096 {
		t.Fatalf("resolver request/runs = %#v/%d, want normalized request and one run", request, runs)
	}
}

func TestMonitoringOptimizeRouteRejectsResolverError(t *testing.T) {
	controller, err := hatStorage.NewCompactionController(hatStorage.CompactionControllerOptions{})
	if err != nil {
		t.Fatalf("NewCompactionController() error = %v", err)
	}
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		OptimizeController: controller,
		OptimizeResolver: func(context.Context, MonitoringOptimizeRequest) (hatStorage.CompactionRequest, error) {
			return hatStorage.CompactionRequest{}, errors.New("target is not authorized")
		},
	}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/storage/optimize", strings.NewReader(`{"target":"events/private"}`)))
	if response.Code != http.StatusBadRequest {
		t.Fatalf("resolver rejection status = %d, want 400", response.Code)
	}
	if !strings.Contains(response.Body.String(), "target is not authorized") {
		t.Fatalf("resolver rejection body = %q, want error", response.Body.String())
	}
}

func TestMonitoringOptimizeRouteHonorsWriteProtection(t *testing.T) {
	controller, err := hatStorage.NewCompactionController(hatStorage.CompactionControllerOptions{})
	if err != nil {
		t.Fatalf("NewCompactionController() error = %v", err)
	}
	runs := 0
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		WriteProtected:     true,
		OptimizeController: controller,
		OptimizeResolver: func(context.Context, MonitoringOptimizeRequest) (hatStorage.CompactionRequest, error) {
			return hatStorage.CompactionRequest{
				Target: "events/protected",
				Run: func(context.Context) error {
					runs++
					return nil
				},
			}, nil
		},
	}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/storage/optimize", strings.NewReader(`{"target":"events/protected"}`)))
	if response.Code != http.StatusForbidden {
		t.Fatalf("write-protected status = %d, want 403", response.Code)
	}
	if runs != 0 {
		t.Fatalf("write-protected runs = %d, want zero", runs)
	}
}

func BenchmarkMonitoringOptimize(b *testing.B) {
	controller, err := hatStorage.NewCompactionController(hatStorage.CompactionControllerOptions{})
	if err != nil {
		b.Fatalf("NewCompactionController() error = %v", err)
	}
	trie := CreateHatTrie()
	defer trie.Destroy()
	handler := NewMonitoringHandler(trie, MonitoringOptions{
		OptimizeController: controller,
		OptimizeResolver: func(_ context.Context, request MonitoringOptimizeRequest) (hatStorage.CompactionRequest, error) {
			return hatStorage.CompactionRequest{
				Target: request.Target,
				Run:    func(context.Context) error { return nil },
			}, nil
		},
	}).Handler()
	b.ReportAllocs()
	for range b.N {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/storage/optimize", strings.NewReader(`{"target":"events/part-1"}`)))
		if response.Code != http.StatusOK {
			b.Fatalf("optimize status = %d: %s", response.Code, response.Body.String())
		}
	}
}
