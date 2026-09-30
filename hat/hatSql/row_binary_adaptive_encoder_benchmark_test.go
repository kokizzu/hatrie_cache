package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLRowBinaryAdaptiveEncoderReuse(b *testing.B) {
	columns, rows := rowBinaryAdaptiveEncoderFixture()
	wire, err := hatSql.EncodeSQLRowBinaryAdaptive(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("stateless_into", func(b *testing.B) {
		dst := make([]byte, 0, len(wire)+16)
		b.ReportAllocs()
		b.SetBytes(int64(len(wire)))
		b.ReportMetric(float64(len(wire)), "wire_bytes")
		for range b.N {
			dst, err = hatSql.EncodeSQLRowBinaryAdaptiveInto(dst, columns, rows)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("reusable_encoder", func(b *testing.B) {
		var encoder hatSql.SQLRowBinaryAdaptiveEncoder
		dst := make([]byte, 0, len(wire)+16)
		var warmErr error
		dst, warmErr = encoder.EncodeInto(dst, columns, rows)
		if warmErr != nil {
			b.Fatal(warmErr)
		}
		b.ReportAllocs()
		b.SetBytes(int64(len(wire)))
		b.ReportMetric(float64(len(wire)), "wire_bytes")
		b.ResetTimer()
		for range b.N {
			dst, err = encoder.EncodeInto(dst, columns, rows)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
