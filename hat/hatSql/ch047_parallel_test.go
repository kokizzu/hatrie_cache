//go:build !ch047baseline

package hatSql_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCH047ParallelNDJSONPreservesOrderAndValues(t *testing.T) {
	data := []byte("{\"id\":1,\"state\":\"open\"}\n\n{\"id\":2,\"state\":\"closed\"}\n{\"id\":3,\"state\":null}\n")
	got, err := hatSql.ParseNDJSONParallel(data, 2)
	if err != nil {
		t.Fatalf("ParseNDJSONParallel() error = %v", err)
	}
	want := []hatSql.Row{
		{"id": float64(1), "state": "open"},
		{"id": float64(2), "state": "closed"},
		{"id": float64(3), "state": nil},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rows = %#v, want %#v", got, want)
	}
}

func TestCH047ParallelNDJSONUsesDefaultWorkers(t *testing.T) {
	rows, err := hatSql.ParseNDJSONParallel(ch047ParallelJSONData(32), 0)
	if err != nil {
		t.Fatalf("ParseNDJSONParallel() error = %v", err)
	}
	if len(rows) != 32 {
		t.Fatalf("len(rows) = %d, want 32", len(rows))
	}
}

func TestCH047ParallelNDJSONRejectsMalformedAndEmptyInput(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want string
	}{
		{
			name: "malformed record",
			data: []byte("{\"id\":1}\nnot-json\n{\"id\":3}\n"),
			want: "record 2",
		},
		{
			name: "empty input",
			data: []byte("\n  \n"),
			want: "requires at least one object",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := hatSql.ParseNDJSONParallel(test.data, 2)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func BenchmarkCH047JSONEachRowSerial(b *testing.B) {
	data := ch047ParallelJSONData(20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ch047ParseNDJSONSerial(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCH047JSONEachRowParallel(b *testing.B) {
	data := ch047ParallelJSONData(20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := hatSql.ParseNDJSONParallel(data, 4); err != nil {
			b.Fatal(err)
		}
	}
}

func ch047ParseNDJSONSerial(data []byte) ([]hatSql.Row, error) {
	lines := bytes.Split(data, []byte{'\n'})
	rows := make([]hatSql.Row, 0, len(lines))
	for lineNumber, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var row hatSql.Row
		if err := json.Unmarshal(line, &row); err != nil {
			return nil, fmt.Errorf("parse NDJSON record %d: %w", lineNumber+1, err)
		}
		if row == nil {
			return nil, fmt.Errorf("NDJSON record %d must be an object", lineNumber+1)
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, errors.New("NDJSON requires at least one object record")
	}
	return rows, nil
}

func ch047ParallelJSONData(rows int) []byte {
	var buffer bytes.Buffer
	for index := 0; index < rows; index++ {
		fmt.Fprintf(&buffer, "{\"id\":%d,\"state\":\"state-%d\"}\n", index, index%8)
	}
	return buffer.Bytes()
}
