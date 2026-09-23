package hatTrace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

var otlpExporterBenchmarkSink error

func BenchmarkOTLPHTTPExporterExport(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	exporter, err := NewOTLPHTTPExporter(OTLPHTTPExporterOptions{Endpoint: server.URL, ServiceName: "benchmark"})
	if err != nil {
		b.Fatal(err)
	}
	spans := make([]Span, 32)
	for index := range spans {
		spans[index] = Span{
			TraceID:       "4bf92f3577b34da6a3ce929d0e0e4736",
			SpanID:        "00f067aa0ba902b7",
			Name:          "hatrie.sql.query",
			StartUnixNano: 10,
			EndUnixNano:   20,
			Status:        "OK",
			Attributes:    map[string]string{"hatrie.sql.query_id": "benchmark"},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		otlpExporterBenchmarkSink = exporter.Export(context.Background(), spans)
	}
}
