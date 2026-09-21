package hatCache

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMonitoringSQLRouteStreamsColumnarBlocks(t *testing.T) {
	t.Parallel()
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES (1), (2), (3) AS values(id) WHERE id > 1 SELECT id","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", hatSql.SQLColumnarBlockStreamContentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != hatSql.SQLColumnarBlockStreamContentType {
		t.Fatalf("status/content-type = %d/%q, want 200/%q: %s", response.Code, response.Header().Get("Content-Type"), hatSql.SQLColumnarBlockStreamContentType, response.Body.String())
	}
	reader, err := hatSql.NewSQLColumnarBlockStreamReader(strings.NewReader(response.Body.String()))
	if err != nil {
		t.Fatalf("NewSQLColumnarBlockStreamReader() error = %v", err)
	}
	var rows []hatSql.Row
	for reader.NextBlock() {
		rows = append(rows, reader.Block()...)
	}
	if err := reader.Err(); err != nil {
		t.Fatalf("reader Err() = %v", err)
	}
	if !reflect.DeepEqual(rows, []hatSql.Row{{"id": int64(2)}, {"id": int64(3)}}) {
		t.Fatalf("rows = %#v", rows)
	}
}

func TestMonitoringSQLRouteDoesNotSelectDisabledColumnar(t *testing.T) {
	t.Parallel()
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES (1) AS values(id) SELECT id","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", hatSql.SQLColumnarBlockStreamContentType+`;q=0, application/x-ndjson`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "application/x-ndjson") {
		t.Fatalf("status/content-type = %d/%q", response.Code, response.Header().Get("Content-Type"))
	}
}
