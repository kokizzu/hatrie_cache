package hatSql

import "testing"

var mz035IncrementalMultisetSink []DifferentialRow

func BenchmarkMZ035IncrementalMultisetApply(b *testing.B) {
	multiset := NewIncrementalMultiset()
	if _, err := multiset.Apply(mz035BenchmarkMultisetSeed()); err != nil {
		b.Fatal(err)
	}
	update := []DifferentialRow{{Key: "key-0001", Time: 100, Diff: 1}}
	b.ReportAllocs()
	for range b.N {
		changes, err := multiset.Apply(update)
		if err != nil {
			b.Fatal(err)
		}
		mz035IncrementalMultisetSink = changes
	}
}
