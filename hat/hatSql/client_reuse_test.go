package hatSql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRowIteratorNextIntoReusesAndClearsMap(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = fmt.Fprintln(writer, `{"type":"columns","columns":["id","name"]}`)
		_, _ = fmt.Fprintln(writer, `{"type":"row","row":{"id":1,"name":"first","stale":"remove"}}`)
		_, _ = fmt.Fprintln(writer, `{"type":"row","row":{"id":2,"name":"second"}}`)
		_, _ = fmt.Fprintln(writer, `{"type":"done"}`)
	}))
	defer server.Close()

	iterator, err := QueryIterator[SQLRow](context.Background(), NewConn(server.URL, ""), "SELECT id, name", nil)
	if err != nil {
		t.Fatalf("QueryIterator() error = %v", err)
	}
	defer iterator.Close()

	row := make(SQLRow)
	if !iterator.NextInto(&row) {
		t.Fatalf("first NextInto() = false, err = %v", iterator.Err())
	}
	if got := row["stale"]; got != "remove" {
		t.Fatalf("first row stale value = %#v, want remove", got)
	}
	firstMap := fmt.Sprintf("%p", row)
	if !iterator.NextInto(&row) {
		t.Fatalf("second NextInto() = false, err = %v", iterator.Err())
	}
	if row["id"] != float64(2) || row["name"] != "second" {
		t.Fatalf("second row = %#v, want id 2/name second", row)
	}
	if _, ok := row["stale"]; ok {
		t.Fatalf("second row retained stale key: %#v", row)
	}
	if got := fmt.Sprintf("%p", row); got != firstMap {
		t.Fatalf("row map identity changed from %s to %s", firstMap, got)
	}
	if iterator.NextInto(&row) {
		t.Fatal("third NextInto() = true, want stream completion")
	}
	if err := iterator.Err(); err != nil {
		t.Fatalf("iterator.Err() = %v, want nil", err)
	}
}

func TestRowIteratorNextKeepsRowsIndependent(t *testing.T) {
	iterator := newBenchmarkRowIterator(rowIteratorBenchmarkPayload())
	defer iterator.Close()
	if !iterator.Next() {
		t.Fatalf("first Next() = false, err = %v", iterator.Err())
	}
	first := iterator.Row()
	if !iterator.Next() {
		t.Fatalf("second Next() = false, err = %v", iterator.Err())
	}
	second := iterator.Row()
	if first["id"] != float64(0) || second["id"] != float64(1) {
		t.Fatalf("rows changed across Next(): first=%#v second=%#v", first, second)
	}
}

func TestRowIteratorNextIntoResetsStructDestination(t *testing.T) {
	payload := []byte("{\"type\":\"row\",\"row\":{\"id\":1,\"name\":\"first\"}}\n" +
		"{\"type\":\"row\",\"row\":{\"id\":2}}\n" +
		"{\"type\":\"done\"}\n")
	iterator := newRowIteratorFromPayload[reusableIteratorStruct](payload)
	defer iterator.Close()
	var row reusableIteratorStruct
	if !iterator.NextInto(&row) {
		t.Fatalf("first NextInto() = false, err = %v", iterator.Err())
	}
	if row.Name != "first" {
		t.Fatalf("first row = %#v, want name first", row)
	}
	if !iterator.NextInto(&row) {
		t.Fatalf("second NextInto() = false, err = %v", iterator.Err())
	}
	if row.ID != 2 || row.Name != "" {
		t.Fatalf("second row = %#v, want zeroed name", row)
	}
}

type reusableIteratorStruct struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func BenchmarkRowIteratorNext(b *testing.B) {
	payload := rowIteratorBenchmarkPayload()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		iterator := newBenchmarkRowIterator(payload)
		for iterator.Next() {
		}
		if err := iterator.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRowIteratorNextInto(b *testing.B) {
	payload := rowIteratorBenchmarkPayload()
	row := make(SQLRow, 3)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		iterator := newBenchmarkRowIterator(payload)
		for iterator.NextInto(&row) {
		}
		if err := iterator.Err(); err != nil {
			b.Fatal(err)
		}
	}
}

func rowIteratorBenchmarkPayload() []byte {
	var builder strings.Builder
	_, _ = fmt.Fprintln(&builder, `{"type":"columns","columns":["id","name","group"]}`)
	for index := 0; index < 128; index++ {
		_, _ = fmt.Fprintf(&builder, `{"type":"row","row":{"id":%d,"name":"repeated-name","group":"group-%d"}}`+"\n", index, index%8)
	}
	_, _ = fmt.Fprintln(&builder, `{"type":"done"}`)
	return []byte(builder.String())
}

func newBenchmarkRowIterator(payload []byte) *RowIterator[SQLRow] {
	return newRowIteratorFromPayload[SQLRow](payload)
}

func newRowIteratorFromPayload[T any](payload []byte) *RowIterator[T] {
	return &RowIterator[T]{
		response: &http.Response{Body: io.NopCloser(bytes.NewReader(payload))},
		decoder:  json.NewDecoder(bytes.NewReader(payload)),
	}
}
