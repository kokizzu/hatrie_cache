package hatriecache

import (
	"bytes"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"hatrie_cache/internal/jsonwire"
)

func BenchmarkCommandJournalTailWire10k(b *testing.B) {
	tail := benchmarkCommandJournalTail10k()
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

func BenchmarkCommandJournalTailOwnership10k(b *testing.B) {
	payload, err := marshalCommandJournalTailBinary(benchmarkCommandJournalTail10k())
	if err != nil {
		b.Fatal(err)
	}
	b.Run("CloneAll", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			tail, err := decodeCommandJournalTailBinaryResponse(bytes.NewReader(payload), int64(len(payload)))
			if err != nil {
				b.Fatal(err)
			}
			for index := range tail.Entries {
				request := &tail.Entries[index].Request
				request.Key = strings.Clone(request.Key)
				request.Value = strings.Clone(request.Value)
				request.Subkey = strings.Clone(request.Subkey)
			}
			runtime.KeepAlive(tail)
		}
	})
	b.Run("Selective", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			tail, err := decodeCommandJournalTailBinaryResponse(bytes.NewReader(payload), int64(len(payload)))
			if err != nil {
				b.Fatal(err)
			}
			detachCommandJournalTailBorrowedStrings(trie, &tail, nil)
			runtime.KeepAlive(tail)
		}
	})
	b.Run("SelectiveDirtyKeys", func(b *testing.B) {
		trie := CreateHatTrie()
		b.Cleanup(trie.Destroy)
		dirtyTracker := NewLevelDBDirtyTracker()
		b.ReportAllocs()
		b.ResetTimer()
		for iteration := 0; iteration < b.N; iteration++ {
			tail, err := decodeCommandJournalTailBinaryResponse(bytes.NewReader(payload), int64(len(payload)))
			if err != nil {
				b.Fatal(err)
			}
			detachCommandJournalTailBorrowedStrings(trie, &tail, dirtyTracker)
			runtime.KeepAlive(tail)
		}
	})
}

func BenchmarkCommandJournalTailCompactDecode10k(b *testing.B) {
	payload, err := marshalCommandJournalTailBinary(benchmarkCommandJournalTail10k())
	if err != nil {
		b.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		decode func() (CommandJournalTail, error)
	}{
		{
			name: "FullRequests",
			decode: func() (CommandJournalTail, error) {
				return decodeCommandJournalTailBinaryResponse(bytes.NewReader(payload), int64(len(payload)))
			},
		},
		{
			name: "CompactScalar",
			decode: func() (CommandJournalTail, error) {
				return decodeCommandJournalTailBinaryPullResponse(bytes.NewReader(payload), int64(len(payload)))
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for iteration := 0; iteration < b.N; iteration++ {
				tail, err := test.decode()
				if err != nil {
					b.Fatal(err)
				}
				runtime.KeepAlive(tail)
			}
		})
	}
}

func benchmarkCommandJournalTail10k() CommandJournalTail {
	tail := CommandJournalTail{LastSequence: 10000, Limit: 10000, Entries: make([]CommandJournalRecord, 10000)}
	for index := range tail.Entries {
		tail.Entries[index] = CommandJournalRecord{
			Sequence: uint64(index + 1),
			Request:  CacheCommandRequest{Command: "SETINT", Key: fmt.Sprintf("wire:%05d", index), Value: "42"},
		}
	}
	return tail
}
