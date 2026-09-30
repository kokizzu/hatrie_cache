package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryAdaptiveDecoderBaseline(b *testing.B) {
	columns, rows := rowBinaryAdaptiveEncoderFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		decoded, decodeErr := hatSql.DecodeSQLRowBinaryAdaptive(columns, encoded)
		if decodeErr != nil {
			b.Fatal(decodeErr)
		}
		if len(decoded) != len(rows) {
			b.Fatalf("decoded %d rows, want %d", len(decoded), len(rows))
		}
	}
}

func BenchmarkSQLRowBinaryAdaptiveDecoderReuse(b *testing.B) {
	columns, rows := rowBinaryAdaptiveEncoderFixture()
	encoded, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()

	b.Run("allocating_decode", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			decoded, decodeErr := hatSql.DecodeSQLRowBinaryAdaptive(columns, encoded)
			if decodeErr != nil {
				b.Fatal(decodeErr)
			}
			if len(decoded) != len(rows) {
				b.Fatalf("decoded %d rows, want %d", len(decoded), len(rows))
			}
		}
	})

	b.Run("reusable_rows", func(b *testing.B) {
		var decoded []hatSql.SQLRow
		decoded, err = hatSql.DecodeSQLRowBinaryAdaptiveInto(decoded, columns, encoded)
		if err != nil {
			b.Fatal(err)
		}
		b.ResetTimer()
		for range b.N {
			decoded, err = hatSql.DecodeSQLRowBinaryAdaptiveInto(decoded[:0], columns, encoded)
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("reusable_decoder", func(b *testing.B) {
		var decoder hatSql.SQLRowBinaryAdaptiveDecoder
		decoded, decodeErr := decoder.DecodeInto(nil, columns, encoded)
		if decodeErr != nil {
			b.Fatal(decodeErr)
		}
		b.ResetTimer()
		for range b.N {
			decoded, decodeErr = decoder.DecodeInto(decoded[:0], columns, encoded)
			if decodeErr != nil {
				b.Fatal(decodeErr)
			}
		}
	})
}
