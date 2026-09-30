package hatDataStructure

import "testing"

var cursorTokenEncodeIntoBenchmarkSink []byte

func BenchmarkCursorTokenEncodeIntoReuse(b *testing.B) {
	codec, err := NewCursorTokenCodec([]byte("0123456789abcdef"))
	if err != nil {
		b.Fatal(err)
	}
	key := []byte("2026-09-30T00:00:00Z")
	destination := make([]byte, 0, 1024)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		token, err := codec.EncodeInto(destination[:0], "orders_by_id", 7, key, uint64(index))
		if err != nil {
			b.Fatal(err)
		}
		destination = token
		cursorTokenEncodeIntoBenchmarkSink = token
	}
}
