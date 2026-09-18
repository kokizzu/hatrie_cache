//go:build ch047baseline

package hatSql_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkCH047BaselineJSONEachRow(b *testing.B) {
	data := ch047ParallelBaselineJSONData(20_000)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := ch047ParseNDJSONSerialBaseline(data); err != nil {
			b.Fatal(err)
		}
	}
}

func ch047ParseNDJSONSerialBaseline(data []byte) ([]hatSql.Row, error) {
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
	return rows, nil
}

func ch047ParallelBaselineJSONData(rows int) []byte {
	var buffer bytes.Buffer
	for index := 0; index < rows; index++ {
		fmt.Fprintf(&buffer, "{\"id\":%d,\"state\":\"state-%d\"}\n", index, index%8)
	}
	return buffer.Bytes()
}
