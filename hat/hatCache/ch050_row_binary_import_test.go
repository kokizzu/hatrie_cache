package hatCache

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"

	json "github.com/goccy/go-json"
)

func ch050EncodeStream(t *testing.T, columns []hatSql.SQLRowBinaryColumn, rows []hatSql.SQLRow) []byte {
	t.Helper()
	var encoded bytes.Buffer
	writer, err := hatSql.NewSQLRowBinaryStreamWriterWithColumns(&encoded, columns)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := writer.WriteRow(row); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Finish(); err != nil {
		t.Fatal(err)
	}
	return encoded.Bytes()
}

func TestExecuteSQLRowBinaryInsertStoresStringRows(t *testing.T) {
	trie := newTestTrie(t)
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	}, []hatSql.SQLRow{
		{"key": "import:one", "value": "alpha"},
		{"key": "import:two", "value": "beta"},
	})

	result, err := ExecuteSQLRowBinaryInsert(context.Background(), trie, "INSERT INTO CACHE(key, value)", bytes.NewReader(payload), SQLRowBinaryImportOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLRowBinaryInsert() error = %v", err)
	}
	if result.Affected != 2 || result.Batches != 1 || !result.Response.OK {
		t.Fatalf("result = %#v", result)
	}
	for key, want := range map[string]string{"import:one": "alpha", "import:two": "beta"} {
		response := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: key})
		if !response.OK || response.Value != want {
			t.Fatalf("GETSTR %q = %#v, want %q", key, response, want)
		}
	}
}

func TestExecuteSQLRowBinaryInsertMapsCounterAndExpirationColumns(t *testing.T) {
	trie := newTestTrie(t)
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "incoming_key", Type: hatSql.SQLRowBinaryString},
		{Name: "incoming_value", Type: hatSql.SQLRowBinaryInt64},
		{Name: "expires_at", Type: hatSql.SQLRowBinaryInt64},
	}, []hatSql.SQLRow{
		{"incoming_key": "import:counter", "incoming_value": int64(42), "expires_at": int64(4102444800)},
	})

	result, err := ExecuteSQLRowBinaryInsert(context.Background(), trie, "INSERT INTO CACHE(key, counter, unix_seconds)", bytes.NewReader(payload), SQLRowBinaryImportOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLRowBinaryInsert() error = %v", err)
	}
	if result.Affected != 1 || result.Batches != 1 {
		t.Fatalf("result = %#v", result)
	}
	response := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "import:counter"})
	if !response.OK || response.Value != "42" {
		t.Fatalf("GET = %#v, want 42", response)
	}
	ttl := trie.ExecuteCommand(CacheCommandRequest{Command: "TTL", Key: "import:counter"})
	if !ttl.OK || ttl.Value == "-1" {
		t.Fatalf("TTL = %#v, want expiring key", ttl)
	}
}

func TestExecuteSQLRowBinaryInsertConvertsEveryScalarType(t *testing.T) {
	decimal128, err := hatSql.ParseSQLDecimal128("12.30", 2)
	if err != nil {
		t.Fatal(err)
	}
	decimal256, err := hatSql.ParseSQLDecimal256("12345678901234567890.1234", 4)
	if err != nil {
		t.Fatal(err)
	}
	ipv4, err := hatSql.ParseSQLIPv4("192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	ipv6, err := hatSql.ParseSQLIPv6("2001:db8::1")
	if err != nil {
		t.Fatal(err)
	}
	var uuid [16]byte
	copy(uuid[:], []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff})
	tests := []struct {
		name   string
		column hatSql.SQLRowBinaryColumn
		value  interface{}
		want   string
	}{
		{name: "int64", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryInt64}, value: int64(-7), want: "-7"},
		{name: "uint64", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryUint64}, value: uint64(7), want: "7"},
		{name: "float64", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryFloat64}, value: float64(1.25), want: "1.25"},
		{name: "bool", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryBool}, value: true, want: "true"},
		{name: "string", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryString}, value: "text", want: "text"},
		{name: "bytes", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryBytes}, value: []byte("bytes"), want: "bytes"},
		{name: "date", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryDate}, value: time.Date(2026, time.January, 2, 0, 0, 0, 0, time.UTC), want: "2026-01-02"},
		{name: "datetime", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryDateTime}, value: time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC), want: "2026-01-02T03:04:05Z"},
		{name: "duration", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryDuration}, value: 2 * time.Second, want: "2s"},
		{name: "uuid", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryUUID}, value: uuid, want: "00112233-4455-6677-8899-aabbccddeeff"},
		{name: "json-string", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryJSON}, value: json.RawMessage(`"json"`), want: "json"},
		{name: "ipv4", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryIPv4}, value: ipv4, want: "192.0.2.1"},
		{name: "ipv6", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryIPv6}, value: ipv6, want: "2001:db8::1"},
		{name: "enum8", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryEnum8, EnumValues: []string{"ready"}}, value: hatSql.SQLEnum8(0), want: "ready"},
		{name: "enum16", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryEnum16, EnumValues: []string{"done"}}, value: hatSql.SQLEnum16(0), want: "done"},
		{name: "decimal128", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryDecimal128, DecimalScale: 2, DecimalPrecision: 38}, value: decimal128, want: "12.30"},
		{name: "decimal256", column: hatSql.SQLRowBinaryColumn{Type: hatSql.SQLRowBinaryDecimal256, DecimalScale: 4, DecimalPrecision: 76}, value: decimal256, want: "12345678901234567890.1234"},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trie := newTestTrie(t)
			column := test.column
			column.Name = "value"
			payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
				{Name: "key", Type: hatSql.SQLRowBinaryString},
				column,
			}, []hatSql.SQLRow{{"key": fmt.Sprintf("import:scalar:%02d", index), "value": test.value}})
			result, err := ExecuteSQLRowBinaryInsert(context.Background(), trie, "INSERT INTO CACHE(key, value)", bytes.NewReader(payload), SQLRowBinaryImportOptions{})
			if err != nil {
				t.Fatalf("ExecuteSQLRowBinaryInsert() error = %v", err)
			}
			if result.Affected != 1 {
				t.Fatalf("result = %#v", result)
			}
			stored := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: fmt.Sprintf("import:scalar:%02d", index)})
			if !stored.OK || stored.Value != test.want {
				t.Fatalf("GET = %#v, want %q", stored, test.want)
			}
		})
	}
}

func TestExecuteSQLRowBinaryInsertBatchesWithoutRetainingTheStream(t *testing.T) {
	trie := newTestTrie(t)
	rows := make([]hatSql.SQLRow, 5)
	for index := range rows {
		rows[index] = hatSql.SQLRow{"key": "import:batch:" + string(rune('a'+index)), "value": "v"}
	}
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	}, rows)

	result, err := ExecuteSQLRowBinaryInsert(context.Background(), trie, "INSERT INTO CACHE(key, value)", bytes.NewReader(payload), SQLRowBinaryImportOptions{BatchSize: 2})
	if err != nil {
		t.Fatalf("ExecuteSQLRowBinaryInsert() error = %v", err)
	}
	if result.Affected != 5 || result.Batches != 3 {
		t.Fatalf("result = %#v, want 5 rows in 3 batches", result)
	}
}

func TestExecuteSQLRowBinaryInsertRejectsMalformedBatchBeforeApplyingIt(t *testing.T) {
	trie := newTestTrie(t)
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString, Nullable: true},
	}, []hatSql.SQLRow{
		{"key": "import:valid", "value": "kept-out"},
		{"key": "import:invalid", "value": nil},
	})

	result, err := ExecuteSQLRowBinaryInsert(context.Background(), trie, "INSERT INTO CACHE(key, value)", bytes.NewReader(payload), SQLRowBinaryImportOptions{BatchSize: 2})
	if err == nil || !strings.Contains(err.Error(), "value must be a scalar") {
		t.Fatalf("error = %v, want scalar validation error", err)
	}
	if result.Affected != 0 || result.Batches != 0 {
		t.Fatalf("result = %#v, want no applied batch", result)
	}
	exists := trie.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "import:valid"})
	if !exists.OK || exists.Value != "0" {
		t.Fatalf("valid row after rejected batch = %#v", exists)
	}
}

func TestMonitoringSQLRowBinaryImport(t *testing.T) {
	trie := newTestTrie(t)
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	}, []hatSql.SQLRow{{"key": "import:http", "value": "ok"}})
	target := url.QueryEscape("INSERT INTO CACHE(key, value)")
	request := httptest.NewRequest(http.MethodPost, "/api/sql/import?query="+target, bytes.NewReader(payload))
	request.Header.Set("Content-Type", hatSql.SQLRowBinaryStreamContentType)
	response := httptest.NewRecorder()
	NewMonitoringHandler(trie, MonitoringOptions{}).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var result SQLRowBinaryImportResult
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Affected != 1 || result.Batches != 1 || !result.Response.OK {
		t.Fatalf("HTTP result = %#v", result)
	}
	stored := trie.ExecuteCommand(CacheCommandRequest{Command: "GETSTR", Key: "import:http"})
	if !stored.OK || stored.Value != "ok" {
		t.Fatalf("stored HTTP value = %#v", stored)
	}
}

func TestMonitoringSQLRowBinaryImportRejectsWritesWhenProtected(t *testing.T) {
	trie := newTestTrie(t)
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	}, []hatSql.SQLRow{{"key": "import:blocked", "value": "no"}})
	target := url.QueryEscape("INSERT INTO CACHE(key, value)")
	request := httptest.NewRequest(http.MethodPost, "/api/sql/import?query="+target, bytes.NewReader(payload))
	request.Header.Set("Content-Type", hatSql.SQLRowBinaryStreamContentType)
	response := httptest.NewRecorder()
	NewMonitoringHandler(trie, MonitoringOptions{WriteProtected: true}).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	exists := trie.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "import:blocked"})
	if !exists.OK || exists.Value != "0" {
		t.Fatalf("blocked write exists response = %#v", exists)
	}
}

func TestMonitoringSQLRowBinaryImportRejectsInvalidTarget(t *testing.T) {
	trie := newTestTrie(t)
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	}, []hatSql.SQLRow{{"key": "import:invalid-target", "value": "no"}})
	request := httptest.NewRequest(http.MethodPost, "/api/sql/import?query="+url.QueryEscape("INSERT INTO CACHE(key, value) SELECT key, value"), bytes.NewReader(payload))
	request.Header.Set("Content-Type", hatSql.SQLRowBinaryStreamContentType)
	response := httptest.NewRecorder()
	NewMonitoringHandler(trie, MonitoringOptions{}).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestMonitoringSQLRowBinaryImportRejectsUnsupportedContentType(t *testing.T) {
	request := httptest.NewRequest(http.MethodPost, "/api/sql/import?query="+url.QueryEscape("INSERT INTO CACHE(key, value)"), strings.NewReader("not-rowbinary"))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	NewMonitoringHandler(newTestTrie(t), MonitoringOptions{}).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestMonitoringSQLRowBinaryImportHonorsMaintenanceReadOnly(t *testing.T) {
	trie := newTestTrie(t)
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	}, []hatSql.SQLRow{{"key": "import:maintenance", "value": "no"}})
	request := httptest.NewRequest(http.MethodPost, "/api/sql/import?query="+url.QueryEscape("INSERT INTO CACHE(key, value)"), bytes.NewReader(payload))
	request.Header.Set("Content-Type", hatSql.SQLRowBinaryStreamContentType)
	response := httptest.NewRecorder()
	NewMonitoringHandler(trie, MonitoringOptions{MaintenanceReadOnly: true}).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusLocked {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	exists := trie.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "import:maintenance"})
	if !exists.OK || exists.Value != "0" {
		t.Fatalf("maintenance write exists response = %#v", exists)
	}
}

func TestMonitoringSQLRowBinaryImportHonorsBodyLimit(t *testing.T) {
	trie := newTestTrie(t)
	payload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	}, []hatSql.SQLRow{{"key": "import:limited", "value": "no"}})
	request := httptest.NewRequest(http.MethodPost, "/api/sql/import?query="+url.QueryEscape("INSERT INTO CACHE(key, value)"), bytes.NewReader(payload))
	request.Header.Set("Content-Type", hatSql.SQLRowBinaryStreamContentType)
	response := httptest.NewRecorder()
	NewMonitoringHandler(trie, MonitoringOptions{SQLRowBinaryImportMaxBytes: int64(len(payload) - 1)}).Handler().ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	exists := trie.ExecuteCommand(CacheCommandRequest{Command: "EXISTS", Key: "import:limited"})
	if !exists.OK || exists.Value != "0" {
		t.Fatalf("limited write exists response = %#v", exists)
	}
}

func TestSQLRowBinaryImportResultJSONShape(t *testing.T) {
	encoded, err := json.Marshal(SQLRowBinaryImportResult{Affected: 2, Batches: 1, Response: CacheCommandResponse{OK: true, Message: "stored"}})
	if err != nil {
		t.Fatal(err)
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded["affected"], float64(2)) || !reflect.DeepEqual(decoded["batches"], float64(1)) {
		t.Fatalf("decoded = %#v", decoded)
	}
}

func TestCH050RowBinaryPayloadIsSmallerThanJSONRows(t *testing.T) {
	rows := ch050BenchmarkRows()
	jsonPayload, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	rowBinaryPayload := ch050EncodeStream(t, []hatSql.SQLRowBinaryColumn{
		{Name: "key", Type: hatSql.SQLRowBinaryString},
		{Name: "value", Type: hatSql.SQLRowBinaryString},
	}, rows)
	t.Logf("JSON bytes=%d RowBinary bytes=%d ratio=%.2fx", len(jsonPayload), len(rowBinaryPayload), float64(len(jsonPayload))/float64(len(rowBinaryPayload)))
	if len(rowBinaryPayload) >= len(jsonPayload) {
		t.Fatalf("RowBinary payload %d is not smaller than JSON payload %d", len(rowBinaryPayload), len(jsonPayload))
	}
}
