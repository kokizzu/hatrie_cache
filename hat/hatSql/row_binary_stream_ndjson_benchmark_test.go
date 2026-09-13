package hatSql

import (
	"bytes"
	"encoding/json"
	"testing"
)

type ch050NDJSONRowMessage struct {
	Type string `json:"type"`
	Row  Row    `json:"row"`
}

func BenchmarkCH050NDJSONBaseline(b *testing.B) {
	rows := ch050BenchmarkRows()
	b.ReportAllocs()
	bytesWritten := 0
	for iteration := 0; iteration < b.N; iteration++ {
		var encoded bytes.Buffer
		encoder := json.NewEncoder(&encoded)
		for _, row := range rows {
			if err := encoder.Encode(ch050NDJSONRowMessage{Type: "row", Row: row}); err != nil {
				b.Fatal(err)
			}
		}
		bytesWritten = encoded.Len()
	}
	b.SetBytes(int64(bytesWritten))
	b.ReportMetric(float64(bytesWritten), "bytes/result")
}
