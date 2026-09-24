package hatPipeline

import "testing"

type mz006BenchmarkTimestamp struct {
	epoch  uint64
	offset uint64
}

var mz006BenchmarkSink int

func mz006BenchmarkLessEqual(left, right mz006BenchmarkTimestamp) bool {
	return left.epoch <= right.epoch && left.offset <= right.offset
}

func BenchmarkMZ006AntichainInsert(b *testing.B) {
	b.ReportAllocs()
	for range b.N {
		antichain, err := NewAntichain(mz006BenchmarkLessEqual)
		if err != nil {
			b.Fatal(err)
		}
		for value := 4096; value > 0; value-- {
			if _, err := antichain.Insert(mz006BenchmarkTimestamp{uint64(value), uint64(value)}); err != nil {
				b.Fatal(err)
			}
		}
		mz006BenchmarkSink += antichain.Len()
	}
}

func BenchmarkMZ006AntichainCovers(b *testing.B) {
	antichain, err := NewAntichain(mz006BenchmarkLessEqual)
	if err != nil {
		b.Fatal(err)
	}
	for value := 4096; value > 0; value-- {
		if _, err := antichain.Insert(mz006BenchmarkTimestamp{uint64(value), uint64(value)}); err != nil {
			b.Fatal(err)
		}
	}
	target := mz006BenchmarkTimestamp{}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		covered, err := antichain.Covers(target)
		if err != nil {
			b.Fatal(err)
		}
		if covered {
			mz006BenchmarkSink++
		}
	}
}
