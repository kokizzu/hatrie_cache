package hatriecache

import (
	"bytes"
	"fmt"
	"testing"

	"hatrie_cache/internal/jsonwire"
)

func BenchmarkCommandJournalTailWire10k(b *testing.B) {
	tail := CommandJournalTail{LastSequence: 10000, Limit: 10000, Entries: make([]CommandJournalRecord, 10000)}
	for index := range tail.Entries {
		tail.Entries[index] = CommandJournalRecord{
			Sequence: uint64(index + 1),
			Request:  CacheCommandRequest{Command: "SETINT", Key: fmt.Sprintf("wire:%05d", index), Value: "42"},
		}
	}
	for _, test := range []struct {
		name   string
		encode func() ([]byte, error)
		decode func([]byte) error
	}{
		{name: "JSON", encode: func() ([]byte, error) { return jsonwire.Marshal(tail) }, decode: func(data []byte) error {
			_, err := decodeCommandJournalTailResponse(bytes.NewReader(data))
			return err
		}},
		{name: "Binary", encode: func() ([]byte, error) { return marshalCommandJournalTailBinary(tail) }, decode: func(data []byte) error {
			_, err := decodeCommandJournalTailBinaryResponse(bytes.NewReader(data), int64(len(data)))
			return err
		}},
	} {
		b.Run(test.name, func(b *testing.B) {
			payload, err := test.encode()
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				payload, err = test.encode()
				if err == nil {
					err = test.decode(payload)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(len(payload)), "wire_B/op")
		})
	}
}
