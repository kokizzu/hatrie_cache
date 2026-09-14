package hatSql

import "testing"

var tr018BorrowedRowBinarySink uint64

func BenchmarkTR018BorrowedRowBinaryDecode(b *testing.B) {
	columns := tr018RowBinaryColumns()
	rows := tr018RowBinaryRows(512)
	encoded, err := EncodeSQLRowBinary(columns, rows)
	if err != nil {
		b.Fatal(err)
	}
	reader, err := NewSQLRowBinaryBorrowedReader(columns, encoded)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(encoded)))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := reader.Reset(encoded); err != nil {
			b.Fatal(err)
		}
		var checksum uint64
		for reader.Next() {
			for _, field := range reader.Row().Fields {
				checksum += uint64(len(field.Data))
				if field.Null {
					checksum++
				}
			}
		}
		if err := reader.Err(); err != nil {
			b.Fatal(err)
		}
		tr018BorrowedRowBinarySink = checksum
	}
}
