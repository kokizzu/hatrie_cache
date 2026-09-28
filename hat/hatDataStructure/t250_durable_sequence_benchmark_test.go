package hatDataStructure

import "testing"

func BenchmarkT250DurableNext(b *testing.B) {
	sequence, err := NewDurableSequence(0, func(uint64) error { return nil })
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := sequence.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkT250DurableCurrent(b *testing.B) {
	sequence, err := NewDurableSequence(1, func(uint64) error { return nil })
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if sequence.Current() == 0 {
			b.Fatal("unexpected zero sequence")
		}
	}
}
