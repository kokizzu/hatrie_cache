package hatSql

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestConnQueryRowBinaryRowsNegotiatesAndDecodes(t *testing.T) {
	wire := sqlClientRowBinaryFixture(t, 2)
	var accept string
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		accept = request.Header.Get("Accept")
		if request.Method != http.MethodPost || request.URL.Path != "/api/sql" {
			t.Errorf("request = %s %s, want POST /api/sql", request.Method, request.URL.Path)
		}
		var payload QueryRequest
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			t.Errorf("request decode error = %v", err)
		}
		if !payload.Stream {
			t.Errorf("request.Stream = false, want true")
		}
		writer.Header().Set("Content-Type", SQLRowBinaryStreamContentType)
		_, _ = writer.Write(wire)
	}))
	defer server.Close()

	var rows []Row
	stop := errors.New("stop")
	n, err := QueryRowBinaryRows(context.Background(), NewConn(server.URL, "secret"), "SELECT id, name", func(row Row) error {
		rows = append(rows, row)
		if len(rows) == 1 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) || n != 1 {
		t.Fatalf("QueryRowBinaryRows() n/error = %d/%v, want 1/stop", n, err)
	}
	if rows[0]["id"] != int64(0) || rows[0]["name"] != "name-0" {
		t.Fatalf("first row = %#v", rows[0])
	}
	if accept != SQLRowBinaryStreamContentType {
		t.Fatalf("Accept = %q, want %q", accept, SQLRowBinaryStreamContentType)
	}
}

func TestConnQueryRowBinaryStreamReportsMalformedPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", SQLRowBinaryStreamContentType)
		_, _ = io.WriteString(writer, "not-a-rowbinary-stream")
	}))
	defer server.Close()

	_, err := NewConn(server.URL, "").QueryRowBinaryStream(context.Background(), "SELECT 1", nil)
	if err == nil || !strings.Contains(err.Error(), "RowBinary") {
		t.Fatalf("QueryRowBinaryStream() error = %v, want RowBinary protocol error", err)
	}
}

func TestConnQueryRowBinaryStreamRejectsUnexpectedContentType(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(writer, "{}")
	}))
	defer server.Close()

	_, err := NewConn(server.URL, "").QueryRowBinaryStream(context.Background(), "SELECT 1", nil)
	if err == nil || !strings.Contains(err.Error(), "Content-Type") {
		t.Fatalf("QueryRowBinaryStream() error = %v, want content-type error", err)
	}
}

func BenchmarkSQLClientNDJSONRows(b *testing.B) {
	payload := sqlClientNDJSONFixture(128)
	conn := sqlClientStaticConn(payload, "application/x-ndjson")
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportMetric(float64(len(payload)), "wire-bytes")
	for index := 0; index < b.N; index++ {
		if _, err := QueryRows(context.Background(), conn, "SELECT id, name", func(Row) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLClientRowBinaryRows(b *testing.B) {
	payload := sqlClientRowBinaryFixture(b, 128)
	conn := sqlClientStaticConn(payload, SQLRowBinaryStreamContentType)
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportMetric(float64(len(payload)), "wire-bytes")
	for index := 0; index < b.N; index++ {
		if _, err := QueryRowBinaryRows(context.Background(), conn, "SELECT id, name", func(Row) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func sqlClientStaticConn(payload []byte, contentType string) *Conn {
	return &Conn{
		BaseURL: "http://sql.test",
		Client:  &http.Client{Transport: sqlClientStaticTransport{payload: payload, contentType: contentType}},
	}
}

type sqlClientStaticTransport struct {
	payload     []byte
	contentType string
}

func (transport sqlClientStaticTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{"Content-Type": []string{transport.contentType}},
		Body:       io.NopCloser(bytes.NewReader(transport.payload)),
		Request:    request,
	}, nil
}

func sqlClientNDJSONFixture(rows int) []byte {
	var builder strings.Builder
	_, _ = fmt.Fprintln(&builder, `{"type":"columns","columns":["id","name"]}`)
	for index := 0; index < rows; index++ {
		_, _ = fmt.Fprintf(&builder, `{"type":"row","row":{"id":%d,"name":"name-%d"}}`+"\n", index, index)
	}
	_, _ = fmt.Fprintf(&builder, `{"type":"done","rows":%d}`+"\n", rows)
	return []byte(builder.String())
}

func sqlClientRowBinaryFixture(t testing.TB, rows int) []byte {
	t.Helper()
	var wire bytes.Buffer
	writer := NewSQLRowBinaryStreamWriter(&wire, []string{"id", "name"})
	for index := 0; index < rows; index++ {
		if err := writer.WriteRow(Row{"id": int64(index), "name": fmt.Sprintf("name-%d", index)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	return wire.Bytes()
}
