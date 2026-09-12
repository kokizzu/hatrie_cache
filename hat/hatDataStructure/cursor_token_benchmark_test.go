package hatDataStructure

import "testing"

func BenchmarkCursorTokenOperations(b *testing.B) {
	codec, err := NewCursorTokenCodec([]byte("0123456789abcdef"))
	if err != nil {
		b.Fatal(err)
	}
	key := []byte("2026-09-13T00:00:00Z")
	token, err := codec.Encode("orders_by_id", 7, key, 42)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("encode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := codec.Encode("orders_by_id", 7, key, uint64(i)); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("decode", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := codec.Decode(token); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("decode-for", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := codec.DecodeFor(token, "orders_by_id", 7); err != nil {
				b.Fatal(err)
			}
		}
	})
}
