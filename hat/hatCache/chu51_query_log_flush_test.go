package hatCache

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestMonitoringSQLQueryLogFlush(t *testing.T) {
	queryLog, err := OpenSQLQueryLog(filepath.Join(t.TempDir(), "query.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer queryLog.Close()
	if err := queryLog.AppendEntry(SQLQueryLogEntry{
		QueryID:    "query-1",
		State:      SQLQueryStateSucceeded,
		StartedAt:  time.Unix(100, 0).UTC(),
		FinishedAt: time.Unix(100, int64(time.Millisecond)).UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	var audit bytes.Buffer
	auditLogger := NewAuditLogger(&audit)
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		AuthToken: "operator-token",
		AuditLog:  auditLogger,
		QueryLog:  queryLog,
	}).Handler()

	t.Run("requires authentication", func(t *testing.T) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/sql/query-log/flush", nil))
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("unauthenticated status = %d, want %d: %s", response.Code, http.StatusUnauthorized, response.Body.String())
		}
	})

	t.Run("rejects non-post", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/api/sql/query-log/flush", nil)
		request.Header.Set("Authorization", "Bearer operator-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET status = %d, want %d: %s", response.Code, http.StatusMethodNotAllowed, response.Body.String())
		}
	})

	t.Run("flushes and audits", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodPost, "/api/sql/query-log/flush", nil)
		request.Header.Set("Authorization", "Bearer operator-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("POST status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
		}
		var result struct {
			Flushed bool `json:"flushed"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if !result.Flushed {
			t.Fatalf("response = %s, want flushed=true", response.Body.String())
		}
		events, err := auditLogger.Query(AuditQuery{Action: "sql.query_log.flush"})
		if err != nil {
			t.Fatal(err)
		}
		if len(events) != 1 || !events[0].OK || events[0].Status != http.StatusOK {
			t.Fatalf("flush audit events = %#v, want one successful event", events)
		}
	})

	t.Run("documents opt-in route", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/openapi.json", nil)
		request.Header.Set("Authorization", "Bearer operator-token")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("OpenAPI status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
		}
		var document struct {
			Paths map[string]interface{} `json:"paths"`
		}
		if err := json.Unmarshal(response.Body.Bytes(), &document); err != nil {
			t.Fatalf("decode OpenAPI: %v", err)
		}
		if _, ok := document.Paths["/api/sql/query-log/flush"]; !ok {
			t.Fatalf("OpenAPI paths omit query-log flush: %s", response.Body.String())
		}
	})
}

func TestMonitoringSQLQueryLogFlushRouteIsOptIn(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/sql/query-log/flush", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("unconfigured route status = %d, want %d: %s", response.Code, http.StatusNotFound, response.Body.String())
	}
}

func TestMonitoringSQLQueryLogFlushReportsSyncFailure(t *testing.T) {
	queryLog, err := OpenSQLQueryLog(filepath.Join(t.TempDir(), "query.log"))
	if err != nil {
		t.Fatal(err)
	}
	if err := queryLog.Close(); err != nil {
		t.Fatal(err)
	}

	var audit bytes.Buffer
	auditLogger := NewAuditLogger(&audit)
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		AuditLog: auditLogger,
		QueryLog: queryLog,
	}).Handler()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPost, "/api/sql/query-log/flush", nil))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed-log status = %d, want %d: %s", response.Code, http.StatusServiceUnavailable, response.Body.String())
	}
	events, err := auditLogger.Query(AuditQuery{Action: "sql.query_log.flush"})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].OK || events[0].Status != http.StatusServiceUnavailable {
		t.Fatalf("failed flush audit events = %#v, want one failed event", events)
	}
}

func BenchmarkCHU51MonitoringSQLQueryLogFlush(b *testing.B) {
	queryLog, err := OpenSQLQueryLog(filepath.Join(b.TempDir(), "query.log"))
	if err != nil {
		b.Fatal(err)
	}
	defer queryLog.Close()
	handler := NewMonitoringHandler(nil, MonitoringOptions{QueryLog: queryLog}).Handler()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		request := httptest.NewRequest(http.MethodPost, "/api/sql/query-log/flush", nil)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			b.Fatalf("flush status = %d: %s", response.Code, response.Body.String())
		}
	}
}
