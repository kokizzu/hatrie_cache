package hatDataStructure

import (
	"strconv"
	"testing"
)

type tu23BaselineRecord struct {
	tags []string
}

type tu23BaselineMultiKeyIndex struct {
	postings map[string]map[uint64]struct{}
	entries  map[uint64]tu23BaselineRecord
}

func newTU23BaselineMultiKeyIndex(capacity int) *tu23BaselineMultiKeyIndex {
	return &tu23BaselineMultiKeyIndex{
		postings: make(map[string]map[uint64]struct{}, capacity),
		entries:  make(map[uint64]tu23BaselineRecord, capacity),
	}
}

func (index *tu23BaselineMultiKeyIndex) upsert(id uint64, value tu23BaselineRecord) {
	if old, exists := index.entries[id]; exists {
		for _, key := range old.tags {
			delete(index.postings[key], id)
		}
	}
	for _, key := range value.tags {
		posting := index.postings[key]
		if posting == nil {
			posting = make(map[uint64]struct{})
			index.postings[key] = posting
		}
		posting[id] = struct{}{}
	}
	index.entries[id] = value
}

func tu23BaselineValues() []tu23BaselineRecord {
	values := make([]tu23BaselineRecord, 1024)
	for i := range values {
		prefix := "tag-" + strconv.Itoa(i)
		values[i] = tu23BaselineRecord{tags: []string{prefix + "-a", prefix + "-b", prefix + "-c"}}
	}
	return values
}

func BenchmarkTU23BaselineMultiKeyUpsert(b *testing.B) {
	index := newTU23BaselineMultiKeyIndex(1024)
	values := tu23BaselineValues()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.upsert(uint64(i%len(values)), values[i%len(values)])
	}
}
