package hatPagination

import (
	"encoding/json"
	"testing"
	"time"
)

type jsonCursor struct {
	Namespace string `json:"namespace"`
	Version   uint64 `json:"version"`
	Key       []byte `json:"key"`
	Expires   uint64 `json:"expires"`
}

func BenchmarkCursorTokenEncode(b *testing.B) {
	codec := benchmarkCodec(b)
	key := []byte("customer:000000000001")
	token, err := codec.Encode("orders", 7, key, testCursorTime)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(token)), "wire_bytes/op")
	b.SetBytes(int64(len(token)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.Encode("orders", 7, key, testCursorTime); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCursorTokenDecode(b *testing.B) {
	codec := benchmarkCodec(b)
	token, err := codec.Encode("orders", 7, []byte("customer:000000000001"), testCursorTime)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(token)))
	b.ReportMetric(float64(len(token)), "wire_bytes/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.Decode(token, "orders", 7, testCursorTime); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCursorTokenTextEncode(b *testing.B) {
	codec := benchmarkCodec(b)
	key := []byte("customer:000000000001")
	token, err := codec.EncodeText("orders", 7, key, testCursorTime)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(token)), "wire_bytes/op")
	b.SetBytes(int64(len(token)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.EncodeText("orders", 7, key, testCursorTime); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCursorTokenTextDecode(b *testing.B) {
	codec := benchmarkCodec(b)
	token, err := codec.EncodeText("orders", 7, []byte("customer:000000000001"), testCursorTime)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(token)))
	b.ReportMetric(float64(len(token)), "wire_bytes/op")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := codec.DecodeText(token, "orders", 7, testCursorTime); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONCursorEncode(b *testing.B) {
	cursor := jsonCursor{
		Namespace: "orders",
		Version:   7,
		Key:       []byte("customer:000000000001"),
		Expires:   uint64(testCursorTime.Unix()) + 900,
	}
	encoded, err := json.Marshal(cursor)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ReportMetric(float64(len(encoded)), "wire_bytes/op")
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(cursor); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkCodec(b *testing.B) *Codec {
	b.Helper()
	codec, err := NewCodec([]byte("0123456789abcdef0123456789abcdef"), Config{TTL: time.Minute})
	if err != nil {
		b.Fatal(err)
	}
	return codec
}
