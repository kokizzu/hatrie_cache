package persistent_delete_bitmap_contract_test

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

const chu06BenchmarkRows = uint32(1_000_000)

var chu06BaselineCRCTable = crc32.MakeTable(crc32.Castagnoli)

type baselineDeleteBitmap struct {
	rowCount uint32
	words    []uint64
}

func newBaselineDeleteBitmap(rowCount uint32) *baselineDeleteBitmap {
	return &baselineDeleteBitmap{
		rowCount: rowCount,
		words:    make([]uint64, (rowCount+63)/64),
	}
}

func (bitmap *baselineDeleteBitmap) mark(row uint32) {
	if row >= bitmap.rowCount {
		return
	}
	bitmap.words[row/64] |= uint64(1) << (row % 64)
}

func (bitmap *baselineDeleteBitmap) encode() []byte {
	const headerBytes = 20
	data := make([]byte, headerBytes+len(bitmap.words)*8+4)
	binary.LittleEndian.PutUint32(data[0:4], bitmap.rowCount)
	binary.LittleEndian.PutUint32(data[4:8], uint32(len(bitmap.words)))
	for index, word := range bitmap.words {
		binary.LittleEndian.PutUint64(data[headerBytes+index*8:], word)
	}
	binary.LittleEndian.PutUint32(data[len(data)-4:], crc32.Checksum(data[:len(data)-4], chu06BaselineCRCTable))
	return data
}

func chu06SparseRows() []uint32 {
	rows := make([]uint32, 0, 1_000)
	for row := uint32(3); row < chu06BenchmarkRows; row += 1_000 {
		rows = append(rows, row)
	}
	return rows
}

func chu06DenseRows() []uint32 {
	rows := make([]uint32, 200_000)
	for index := range rows {
		rows[index] = 400_000 + uint32(index)
	}
	return rows
}

func chu06RandomRows() []uint32 {
	rows := make([]uint32, 0, 250_000)
	state := uint32(0x9e3779b9)
	for row := uint32(0); row < chu06BenchmarkRows; row++ {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		if state&3 == 0 {
			rows = append(rows, row)
		}
	}
	return rows
}

func buildBaselineDeleteBitmap(rows []uint32) *baselineDeleteBitmap {
	bitmap := newBaselineDeleteBitmap(chu06BenchmarkRows)
	for _, row := range rows {
		bitmap.mark(row)
	}
	return bitmap
}

var chu06BaselineEncoded []byte

func BenchmarkCHU06BaselineSparseEncode(b *testing.B) {
	bitmap := buildBaselineDeleteBitmap(chu06SparseRows())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chu06BaselineEncoded = bitmap.encode()
	}
}

func BenchmarkCHU06BaselineDenseEncode(b *testing.B) {
	bitmap := buildBaselineDeleteBitmap(chu06DenseRows())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chu06BaselineEncoded = bitmap.encode()
	}
}

func BenchmarkCHU06BaselineRandomEncode(b *testing.B) {
	bitmap := buildBaselineDeleteBitmap(chu06RandomRows())
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		chu06BaselineEncoded = bitmap.encode()
	}
}
