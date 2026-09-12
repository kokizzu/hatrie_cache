package hatSql

import (
	"encoding/json"
	"strconv"
	"testing"
)

func BenchmarkQuerySubscriptionDifferentialPayload(b *testing.B) {
	previous := QueryResult{Columns: []string{"id", "name"}, Rows: make([]Row, 1024)}
	next := QueryResult{Columns: []string{"id", "name"}, Rows: make([]Row, 1024)}
	for index := range previous.Rows {
		previous.Rows[index] = Row{"id": int64(index), "name": "person-" + strconv.Itoa(index)}
		next.Rows[index] = Row{"id": int64(index), "name": "person-" + strconv.Itoa(index)}
	}
	next.Rows[777] = Row{"id": int64(777), "name": "renamed"}
	fullSnapshot := QuerySubscriptionSnapshot{ID: 1, Revision: 2, Frontier: 2, Result: next}
	differential := querySubscriptionDeltaBatch(fullSnapshot, previous, true)
	fullPayload, err := json.Marshal(fullSnapshot)
	if err != nil {
		b.Fatal(err)
	}
	differentialPayload, err := json.Marshal(differential)
	if err != nil {
		b.Fatal(err)
	}
	b.Run("full_snapshot", func(b *testing.B) {
		b.ReportMetric(float64(len(fullPayload)), "payload-B/op")
		b.ReportAllocs()
		for range b.N {
			cloned := cloneQueryResult(next)
			if _, err := json.Marshal(QuerySubscriptionSnapshot{ID: 1, Revision: 2, Frontier: 2, Result: cloned}); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("differential", func(b *testing.B) {
		b.ReportMetric(float64(len(differentialPayload)), "payload-B/op")
		b.ReportAllocs()
		for range b.N {
			batch := querySubscriptionDeltaBatch(fullSnapshot, previous, true)
			if _, err := json.Marshal(batch); err != nil {
				b.Fatal(err)
			}
		}
	})
}
