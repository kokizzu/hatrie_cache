package hatDataStructure

import (
	"path/filepath"
	"testing"
)

var t250RawSequenceSink uint64
var t250DurableSequenceBytesSink int

func BenchmarkT250RawCounter(b *testing.B) {
	var current uint64
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		current++
	}
	t250RawSequenceSink = current
}

func BenchmarkT250DurableSequenceMemory(b *testing.B) {
	sequence, err := NewDurableSequence(DurableSequenceOptions{})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := sequence.Allocate(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	t250RawSequenceSink = sequence.Current()
}

func BenchmarkT250DurableSequenceMarshal(b *testing.B) {
	sequence, err := NewDurableSequence(DurableSequenceOptions{Initial: 41})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		data, err := sequence.MarshalBinary()
		if err != nil {
			b.Fatal(err)
		}
		t250DurableSequenceBytesSink = len(data)
	}
}

func BenchmarkT250DurableSequenceFileAllocate(b *testing.B) {
	path := filepath.Join(b.TempDir(), "sequence.bin")
	sequence, err := NewDurableSequence(DurableSequenceOptions{Path: path})
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := sequence.Allocate(); err != nil {
			b.Fatal(err)
		}
	}
}
