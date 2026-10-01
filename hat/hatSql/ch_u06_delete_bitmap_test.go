package hatSql

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"testing"
)

func TestSQLColumnarDeleteBitmapSparseRoundTrip(t *testing.T) {
	rows := []uint32{900, 4, 900, 100}
	bitmap, err := NewSQLColumnarDeleteBitmapFromRows(1_000_000, rows)
	if err != nil {
		t.Fatalf("build bitmap: %v", err)
	}
	if got := bitmap.Encoding(); got != SQLColumnarDeleteBitmapEncodingSparse {
		t.Fatalf("encoding = %v, want sparse", got)
	}
	if got := bitmap.DeletedCount(); got != 3 {
		t.Fatalf("deleted count = %d, want 3", got)
	}

	data, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal bitmap: %v", err)
	}
	restored, err := UnmarshalSQLColumnarDeleteBitmap(data)
	if err != nil {
		t.Fatalf("unmarshal bitmap: %v", err)
	}
	if got := restored.RowCount(); got != 1_000_000 {
		t.Fatalf("row count = %d, want 1000000", got)
	}
	for _, row := range []uint32{4, 100, 900} {
		if !restored.IsDeleted(row) {
			t.Fatalf("row %d is live, want deleted", row)
		}
	}
	for _, row := range []uint32{0, 5, 899, 901} {
		if restored.IsDeleted(row) {
			t.Fatalf("row %d is deleted, want live", row)
		}
	}
	filtered := restored.FilterLiveRows([]uint32{0, 4, 900, 999_999, 1_000_000})
	if len(filtered) != 2 || filtered[0] != 0 || filtered[1] != 999_999 {
		t.Fatalf("filtered live rows = %v, want [0 999999]", filtered)
	}

	rows[0] = 0
	if restored.IsDeleted(900) == false {
		t.Fatal("bitmap changed after caller input mutation")
	}
	sortedRows := []uint32{4, 100}
	sortedBitmap, err := NewSQLColumnarDeleteBitmapFromRows(1_000_000, sortedRows)
	if err != nil {
		t.Fatalf("build sorted bitmap: %v", err)
	}
	sortedRows[0] = 0
	if !sortedBitmap.IsDeleted(4) {
		t.Fatal("sparse bitmap aliased sorted caller input")
	}
}

func TestSQLColumnarDeleteBitmapDenseRoundTripAndFiltering(t *testing.T) {
	deleted := make([]uint32, 0, 2048)
	for row := uint32(0); row < 4096; row += 2 {
		deleted = append(deleted, row)
	}
	bitmap, err := NewSQLColumnarDeleteBitmapFromRows(4096, deleted)
	if err != nil {
		t.Fatalf("build bitmap: %v", err)
	}
	if got := bitmap.Encoding(); got != SQLColumnarDeleteBitmapEncodingDense {
		t.Fatalf("encoding = %v, want dense", got)
	}

	rows := []uint32{0, 1, 2, 3, 4095}
	rows = bitmap.AppendLiveRows(rows[:0])
	if len(rows) != 2048 {
		t.Fatalf("live rows = %d, want 2048", len(rows))
	}
	for i, row := range rows {
		want := uint32(2*i + 1)
		if row != want {
			t.Fatalf("live row %d = %d, want %d", i, row, want)
		}
	}

	data, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal bitmap: %v", err)
	}
	restored, err := UnmarshalSQLColumnarDeleteBitmap(data)
	if err != nil {
		t.Fatalf("unmarshal bitmap: %v", err)
	}
	if got := restored.AppendDeletedRows(nil); len(got) != 2048 {
		t.Fatalf("deleted rows = %d, want 2048", len(got))
	}
}

func TestSQLColumnarDeleteBitmapRejectsInvalidInput(t *testing.T) {
	if _, err := NewSQLColumnarDeleteBitmapFromRows(-1, nil); err == nil {
		t.Fatal("negative row count accepted")
	}
	if _, err := NewSQLColumnarDeleteBitmapFromRows(3, []uint32{3}); err == nil {
		t.Fatal("out-of-range deleted row accepted")
	}
	duplicates := []uint32{1, 1, 1, 1, 1, 1, 1, 1}
	duplicateBitmap, err := NewSQLColumnarDeleteBitmapFromRows(64, duplicates)
	if err != nil {
		t.Fatalf("build duplicate bitmap: %v", err)
	}
	if duplicateBitmap.Encoding() != SQLColumnarDeleteBitmapEncodingSparse || duplicateBitmap.DeletedCount() != 1 {
		t.Fatalf("duplicate bitmap = encoding %v, deleted %d; want sparse, 1", duplicateBitmap.Encoding(), duplicateBitmap.DeletedCount())
	}
	if _, err := UnmarshalSQLColumnarDeleteBitmap(nil); err == nil {
		t.Fatal("empty payload accepted")
	}

	bitmap, err := NewSQLColumnarDeleteBitmapFromRows(16, []uint32{1, 7})
	if err != nil {
		t.Fatalf("build bitmap: %v", err)
	}
	data, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal bitmap: %v", err)
	}
	cases := []struct {
		name string
		data []byte
	}{
		{name: "truncated", data: data[:len(data)-1]},
		{name: "bad magic", data: append([]byte("xxxx"), data[4:]...)},
		{name: "bad version", data: func() []byte {
			copyData := append([]byte(nil), data...)
			copyData[4]++
			return copyData
		}()},
		{name: "nonzero reserved", data: mutateDeleteBitmap(data, func(copyData []byte) { copyData[6] = 1 })},
		{name: "unknown encoding", data: mutateDeleteBitmap(data, func(copyData []byte) { copyData[5] = 3 })},
		{name: "bad crc", data: append(append([]byte(nil), data[:len(data)-1]...), data[len(data)-1]^0xff)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := UnmarshalSQLColumnarDeleteBitmap(tc.data); err == nil {
				t.Fatal("invalid payload accepted")
			}
		})
	}
	denseRows := make([]uint32, 16)
	for index := range denseRows {
		denseRows[index] = uint32(index)
	}
	denseBitmap, err := NewSQLColumnarDeleteBitmapFromRows(65, denseRows)
	if err != nil {
		t.Fatalf("build dense malformed fixture: %v", err)
	}
	denseData, err := denseBitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal dense malformed fixture: %v", err)
	}
	denseData[29] |= 0x80
	if _, err := UnmarshalSQLColumnarDeleteBitmap(mutateDeleteBitmap(denseData, func([]byte) {})); err == nil {
		t.Fatal("dense padding bit accepted")
	}
}

func TestSQLColumnarDeleteBitmapMarshalIsDeterministic(t *testing.T) {
	bitmap, err := NewSQLColumnarDeleteBitmapFromRows(1000, []uint32{999, 1, 500, 1})
	if err != nil {
		t.Fatalf("build bitmap: %v", err)
	}
	first, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("first marshal: %v", err)
	}
	second, err := bitmap.MarshalBinary()
	if err != nil {
		t.Fatalf("second marshal: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("repeated marshal changed bytes")
	}
}

func mutateDeleteBitmap(data []byte, mutate func([]byte)) []byte {
	copyData := append([]byte(nil), data...)
	mutate(copyData)
	binary.LittleEndian.PutUint32(copyData[len(copyData)-columnarDeleteBitmapCRCBytes:], crc32.ChecksumIEEE(copyData[:len(copyData)-columnarDeleteBitmapCRCBytes]))
	return copyData
}
