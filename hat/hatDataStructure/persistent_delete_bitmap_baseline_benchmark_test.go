package hatDataStructure

import (
	"encoding/binary"
	"hash/crc32"
	"testing"
)

var persistentDeleteBitmapBaselineSink []byte
var persistentDeleteBitmapBaselineBoolSink []bool
var persistentDeleteBitmapBaselineCRCTable = crc32.MakeTable(crc32.Castagnoli)

func BenchmarkPersistentDeleteBitmapBaselineBoolEncode(b *testing.B) {
	const rows = 100_000
	deleted := make([]bool, rows)
	for index := 0; index < rows; index += 3 {
		deleted[index] = true
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		encoded := make([]byte, 0, 4+4+rows+4)
		encoded = append(encoded, 'H', 'T', 'B', '0')
		var buffer [4]byte
		binary.LittleEndian.PutUint32(buffer[:], rows)
		encoded = append(encoded, buffer[:]...)
		for _, value := range deleted {
			if value {
				encoded = append(encoded, 1)
			} else {
				encoded = append(encoded, 0)
			}
		}
		checksum := crc32.Checksum(encoded, persistentDeleteBitmapBaselineCRCTable)
		binary.LittleEndian.PutUint32(buffer[:], checksum)
		encoded = append(encoded, buffer[:]...)
		persistentDeleteBitmapBaselineSink = encoded
	}
}

func BenchmarkPersistentDeleteBitmapBaselineBoolDecode(b *testing.B) {
	const rows = 100_000
	encoded := make([]byte, 0, 4+4+rows+4)
	encoded = append(encoded, 'H', 'T', 'B', '0')
	var buffer [4]byte
	binary.LittleEndian.PutUint32(buffer[:], rows)
	encoded = append(encoded, buffer[:]...)
	for index := 0; index < rows; index++ {
		if index%3 == 0 {
			encoded = append(encoded, 1)
		} else {
			encoded = append(encoded, 0)
		}
	}
	checksum := crc32.Checksum(encoded, persistentDeleteBitmapBaselineCRCTable)
	binary.LittleEndian.PutUint32(buffer[:], checksum)
	encoded = append(encoded, buffer[:]...)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		payloadLength := len(encoded) - 4
		if binary.LittleEndian.Uint32(encoded[payloadLength:]) != crc32.Checksum(encoded[:payloadLength], persistentDeleteBitmapBaselineCRCTable) {
			b.Fatal("checksum mismatch")
		}
		decoded := make([]bool, rows)
		for row, value := range encoded[8:payloadLength] {
			decoded[row] = value != 0
		}
		persistentDeleteBitmapBaselineBoolSink = decoded
	}
}
