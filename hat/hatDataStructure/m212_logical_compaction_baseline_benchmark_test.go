package hatDataStructure

import "testing"

type m212BaselineKey struct {
	data int
	time uint64
}

type m212BaselineMultiset struct {
	entries map[m212BaselineKey]int64
}

var m212ExistingDifferentialSink interface{}

func newM212BaselineMultiset() *m212BaselineMultiset {
	return &m212BaselineMultiset{entries: make(map[m212BaselineKey]int64)}
}

func (multiset *m212BaselineMultiset) add(data int, timestamp uint64, diff int64) {
	key := m212BaselineKey{data: data, time: timestamp}
	next := multiset.entries[key] + diff
	if next == 0 {
		delete(multiset.entries, key)
		return
	}
	multiset.entries[key] = next
}

func BenchmarkM212ExistingDifferentialAddBaseline(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		multiset := newM212BaselineMultiset()
		for index := 0; index < 64; index++ {
			multiset.add(index&15, uint64(index), 1)
		}
		m212ExistingDifferentialSink = multiset
	}
}

func BenchmarkM212ExistingDifferentialMultisetAdd(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		multiset := NewDifferentialMultiset[int]()
		for index := 0; index < 64; index++ {
			if err := multiset.Add(index&15, uint64(index), 1); err != nil {
				b.Fatal(err)
			}
		}
		m212ExistingDifferentialSink = multiset
	}
}
