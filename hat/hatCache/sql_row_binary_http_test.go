package hatCache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestMonitoringSQLRouteStreamsRowBinaryRows(t *testing.T) {
	t.Parallel()
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES (1), (2), (3) AS values(id) WHERE id > 1 SELECT id","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", hatSql.SQLRowBinaryStreamContentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != hatSql.SQLRowBinaryStreamContentType {
		t.Fatalf("status/content-type = %d/%q, want 200/%q: %s", response.Code, response.Header().Get("Content-Type"), hatSql.SQLRowBinaryStreamContentType, response.Body.String())
	}
	columns, rows, err := hatSql.DecodeSQLRowBinaryStream(response.Body.Bytes())
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryStream() error = %v", err)
	}
	if !reflect.DeepEqual(columns, []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryInt64, Nullable: true}}) {
		t.Fatalf("columns = %#v", columns)
	}
	if !reflect.DeepEqual(rows, []hatSql.Row{{"id": int64(2)}, {"id": int64(3)}}) {
		t.Fatalf("rows = %#v", rows)
	}
}

func TestMonitoringSQLRouteStreamsEmptyRowBinaryResult(t *testing.T) {
	t.Parallel()
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES (1) AS values(id) WHERE id > 1 SELECT id","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", hatSql.SQLRowBinaryStreamContentType)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Header().Get("Content-Type") != hatSql.SQLRowBinaryStreamContentType {
		t.Fatalf("status/content-type = %d/%q", response.Code, response.Header().Get("Content-Type"))
	}
	columns, rows, err := hatSql.DecodeSQLRowBinaryStream(response.Body.Bytes())
	if err != nil {
		t.Fatalf("DecodeSQLRowBinaryStream() error = %v", err)
	}
	if len(rows) != 0 || !reflect.DeepEqual(columns, []hatSql.SQLRowBinaryColumn{{Name: "id", Type: hatSql.SQLRowBinaryJSON, Nullable: true}}) {
		t.Fatalf("empty result = columns %#v rows %#v", columns, rows)
	}
}

func TestMonitoringSQLRouteDoesNotSelectDisabledRowBinary(t *testing.T) {
	t.Parallel()
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler()
	request := httptest.NewRequest(http.MethodPost, "/api/sql", strings.NewReader(`{"query":"FROM VALUES (1) AS values(id) SELECT id","stream":true}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", hatSql.SQLRowBinaryStreamContentType+`;q=0, application/x-ndjson`)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Header().Get("Content-Type"), "application/x-ndjson") {
		t.Fatalf("status/content-type = %d/%q", response.Code, response.Header().Get("Content-Type"))
	}
}

func TestSQLClientReadsRowBinaryStream(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler())
	defer server.Close()
	reader, err := hatSql.QueryRowBinaryIterator(context.Background(), hatSql.NewConn(server.URL, ""), "FROM VALUES (1), (2) AS values(id) SELECT id", nil)
	if err != nil {
		t.Fatalf("QueryRowBinaryIterator() error = %v", err)
	}
	defer reader.Close()
	if !reader.Next() || !reflect.DeepEqual(reader.Row(), hatSql.Row{"id": int64(1)}) {
		t.Fatalf("first row = %#v", reader.Row())
	}
	if !reader.Next() || !reflect.DeepEqual(reader.Row(), hatSql.Row{"id": int64(2)}) {
		t.Fatalf("second row = %#v", reader.Row())
	}
	if reader.Next() || reader.Err() != nil {
		t.Fatalf("completion = next %v err %v", reader.Next(), reader.Err())
	}
}
