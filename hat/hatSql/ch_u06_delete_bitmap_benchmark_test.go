package hatSql

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

var sqlColumnarDeleteBitmapBenchmarkSink uint64

func BenchmarkSQLColumnarDeleteBitmapBuildAndMarshal(b *testing.B) {
	cases := []struct {
		name        string
		rowCount    int
		deletedRows []uint32
	}{
		{
			name:     "sparse_1m_1pct",
			rowCount: 1_000_000,
			deletedRows: func() []uint32 {
				rows := make([]uint32, 0, 10_000)
				for row := uint32(0); row < 1_000_000; row += 100 {
					rows = append(rows, row)
				}
				return rows
			}(),
		},
		{
			name:     "dense_1m_50pct",
			rowCount: 1_000_000,
			deletedRows: func() []uint32 {
				rows := make([]uint32, 0, 500_000)
				for row := uint32(0); row < 1_000_000; row += 2 {
					rows = append(rows, row)
				}
				return rows
			}(),
		},
	}

	for _, tc := range cases {
		b.Run(tc.name+"/adaptive", func(b *testing.B) {
			bitmap, err := NewSQLColumnarDeleteBitmapFromRows(tc.rowCount, tc.deletedRows)
			if err != nil {
				b.Fatal(err)
			}
			encoded, err := bitmap.MarshalBinary()
			if err != nil {
				b.Fatal(err)
			}
			denseEncoded := marshalDenseDeleteBitmapReference(tc.rowCount, tc.deletedRows)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bitmap, _ = NewSQLColumnarDeleteBitmapFromRows(tc.rowCount, tc.deletedRows)
				encoded, _ = bitmap.MarshalBinary()
			}
			b.StopTimer()
			b.ReportMetric(float64(len(encoded)), "adaptive-wire-B")
			b.ReportMetric(float64(len(denseEncoded)), "dense-wire-B")
			b.ReportMetric(float64(len(encoded))/float64(len(denseEncoded)), "wire-ratio")
			sqlColumnarDeleteBitmapBenchmarkSink += uint64(len(encoded))
		})

		b.Run(tc.name+"/always_dense", func(b *testing.B) {
			encoded := marshalDenseDeleteBitmapReference(tc.rowCount, tc.deletedRows)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				encoded = marshalDenseDeleteBitmapReference(tc.rowCount, tc.deletedRows)
			}
			b.StopTimer()
			b.ReportMetric(float64(len(encoded)), "wire-B")
			sqlColumnarDeleteBitmapBenchmarkSink += uint64(len(encoded))
		})
	}
}

func BenchmarkSQLColumnarDeleteBitmapUnmarshal(b *testing.B) {
	cases := []struct {
		name string
		data []byte
	}{
		{
			name: "sparse_1m_1pct",
			data: func() []byte {
				rows := make([]uint32, 0, 10_000)
				for row := uint32(0); row < 1_000_000; row += 100 {
					rows = append(rows, row)
				}
				bitmap, _ := NewSQLColumnarDeleteBitmapFromRows(1_000_000, rows)
				data, _ := bitmap.MarshalBinary()
				return data
			}(),
		},
		{
			name: "dense_1m_50pct",
			data: func() []byte {
				rows := make([]uint32, 0, 500_000)
				for row := uint32(0); row < 1_000_000; row += 2 {
					rows = append(rows, row)
				}
				bitmap, _ := NewSQLColumnarDeleteBitmapFromRows(1_000_000, rows)
				data, _ := bitmap.MarshalBinary()
				return data
			}(),
		},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				bitmap, _ := UnmarshalSQLColumnarDeleteBitmap(tc.data)
				sqlColumnarDeleteBitmapBenchmarkSink += uint64(bitmap.DeletedCount())
			}
			b.StopTimer()
			b.ReportMetric(float64(len(tc.data)), "wire-B")
		})
	}
}

func BenchmarkSQLColumnarDeleteBitmapLookup(b *testing.B) {
	lookups := []uint32{0, 1, 99, 100, 101, 500_000, 999_999}
	cases := []struct {
		name        string
		rowCount    int
		deletedRows []uint32
	}{
		{
			name:     "sparse_1m_1pct",
			rowCount: 1_000_000,
			deletedRows: func() []uint32 {
				rows := make([]uint32, 0, 10_000)
				for row := uint32(0); row < 1_000_000; row += 100 {
					rows = append(rows, row)
				}
				return rows
			}(),
		},
		{
			name:     "dense_1m_50pct",
			rowCount: 1_000_000,
			deletedRows: func() []uint32 {
				rows := make([]uint32, 0, 500_000)
				for row := uint32(0); row < 1_000_000; row += 2 {
					rows = append(rows, row)
				}
				return rows
			}(),
		},
	}
	for _, tc := range cases {
		bitmap, err := NewSQLColumnarDeleteBitmapFromRows(tc.rowCount, tc.deletedRows)
		if err != nil {
			b.Fatal(err)
		}
		b.Run(tc.name+"/adaptive", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if bitmap.IsDeleted(lookups[i%len(lookups)]) {
					sqlColumnarDeleteBitmapBenchmarkSink++
				}
			}
		})
		words := denseDeleteWordsReference(tc.rowCount, tc.deletedRows)
		b.Run(tc.name+"/always_dense", func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				row := lookups[i%len(lookups)]
				if words[row>>6]&(uint64(1)<<(row&63)) != 0 {
					sqlColumnarDeleteBitmapBenchmarkSink++
				}
			}
		})
	}
}

func marshalDenseDeleteBitmapReference(rowCount int, deletedRows []uint32) []byte {
	words := denseDeleteWordsReference(rowCount, deletedRows)
	payloadBytes := len(words) * 8
	data := make([]byte, columnarDeleteBitmapHeaderBytes+payloadBytes+columnarDeleteBitmapCRCBytes)
	copy(data[:4], columnarDeleteBitmapMagic)
	data[4] = columnarDeleteBitmapVersion
	data[5] = byte(SQLColumnarDeleteBitmapEncodingDense)
	binary.LittleEndian.PutUint32(data[8:12], uint32(rowCount))
	binary.LittleEndian.PutUint32(data[12:16], uint32(len(deletedRows)))
	binary.LittleEndian.PutUint32(data[16:20], uint32(payloadBytes))
	for index, word := range words {
		binary.LittleEndian.PutUint64(data[columnarDeleteBitmapHeaderBytes+index*8:], word)
	}
	binary.LittleEndian.PutUint32(data[len(data)-columnarDeleteBitmapCRCBytes:], crc32.ChecksumIEEE(data[:len(data)-columnarDeleteBitmapCRCBytes]))
	return data
}

func denseDeleteWordsReference(rowCount int, deletedRows []uint32) []uint64 {
	words := make([]uint64, densePayloadBytes(rowCount)/8)
	for _, row := range deletedRows {
		words[row>>6] |= uint64(1) << (row & 63)
	}
	return words
}
