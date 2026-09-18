package hatSql

import (
	"testing"
	"time"
)

var tr028BenchmarkTokenSink string
var tr028BenchmarkDecodedSink SQLKeysetToken

func TestSQLKeysetTokenWireSize(t *testing.T) {
	raw, err := encodeSQLKeysetCursor(sqlKeysetCursor{
		Fingerprint: "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJK",
		After:       KeysetPosition{Value: int64(123456), Tie: 123456, Valid: true},
		Returned:    100,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	codec, err := NewSQLKeysetTokenCodec(SQLKeysetTokenCodecOptions{
		Secret: []byte("keyset-token-secret-2026"),
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	signed, err := codec.Encode(raw)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("legacy cursor bytes=%d, signed cursor bytes=%d, overhead=%.2fx", len(raw), len(signed), float64(len(signed))/float64(len(raw)))
}

func BenchmarkSQLKeysetTokenEncodeDecode(b *testing.B) {
	cursor := sqlKeysetCursor{
		Fingerprint: "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJK",
		After:       KeysetPosition{Value: int64(123456), Tie: 123456, Valid: true},
		Returned:    100,
	}
	raw, err := encodeSQLKeysetCursor(cursor)
	if err != nil {
		b.Fatal(err)
	}
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	codec, err := NewSQLKeysetTokenCodec(SQLKeysetTokenCodecOptions{
		Secret: []byte("keyset-token-secret-2026"),
		Now:    func() time.Time { return now },
	})
	if err != nil {
		b.Fatal(err)
	}
	signed, err := codec.Encode(raw)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("LegacyEncode", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			tr028BenchmarkTokenSink, err = encodeSQLKeysetCursor(cursor)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SignedEncode", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			tr028BenchmarkTokenSink, err = codec.Encode(raw)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("LegacyDecode", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			_, err = decodeSQLKeysetCursor(raw)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SignedDecode", func(b *testing.B) {
		b.ReportAllocs()
		for index := 0; index < b.N; index++ {
			tr028BenchmarkDecodedSink, err = codec.Decode(signed)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
