package hatJournal

import (
	"bytes"
	"testing"
)

func BenchmarkTR008RecordEncoding(b *testing.B) {
	key := []byte("12345678901234567890123456789012")
	encryptor, err := NewRecordEncryptor(EncryptionOptions{KeyID: "current", Key: key})
	if err != nil {
		b.Fatalf("NewRecordEncryptor() error = %v", err)
	}
	record := bytes.Repeat([]byte("x"), 256)

	b.Run("legacy-copy", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(record)))
		destination := make([]byte, len(record))
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			copy(destination, record)
		}
		if len(destination) != len(record) {
			b.Fatal("legacy benchmark did not write a record")
		}
	})

	b.Run("aes-gcm-frame", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(record)))
		b.ResetTimer()
		for index := 0; index < b.N; index++ {
			if _, err := encryptor.AppendRecord(nil, record); err != nil {
				b.Fatalf("AppendRecord() error = %v", err)
			}
		}
	})
}
