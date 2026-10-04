package hatQueryHistory

import "testing"

type mg37BaselineRecord struct {
	queryID string
	state   string
}

func BenchmarkMG37InMemoryHistoryAppendBaseline(b *testing.B) {
	retained := make([]mg37BaselineRecord, 256)
	record := mg37BaselineRecord{queryID: "query-1", state: "succeeded"}
	var index int
	b.ReportAllocs()
	for b.Loop() {
		retained[index%len(retained)] = record
		index++
	}
	if retained[0].state == "" {
		b.Fatal("baseline record was not retained")
	}
}
