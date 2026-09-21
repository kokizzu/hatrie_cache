package hatSql

import (
	"errors"
	"sort"
	"testing"
)

var errM208BaselineOverflow = errors.New("m208 baseline differential overflow")

type m208BaselineDelta struct {
	key   string
	delta QuerySubscriptionDelta
}

var m208BenchmarkDeltaSink []QuerySubscriptionDelta

func BenchmarkM208SortFoldBaseline(b *testing.B) {
	deltas := m208BenchmarkDeltas()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		folded, err := m208SortFoldBaseline(deltas)
		if err != nil {
			b.Fatal(err)
		}
		m208BenchmarkDeltaSink = folded
	}
}

func BenchmarkM208HashFold(b *testing.B) {
	deltas := m208BenchmarkDeltas()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		folded, err := FoldQuerySubscriptionDeltas(deltas)
		if err != nil {
			b.Fatal(err)
		}
		m208BenchmarkDeltaSink = folded
	}
}

func m208SortFoldBaseline(deltas []QuerySubscriptionDelta) ([]QuerySubscriptionDelta, error) {
	entries := make([]m208BaselineDelta, 0, len(deltas))
	for _, delta := range deltas {
		if delta.Diff == 0 {
			continue
		}
		entries = append(entries, m208BaselineDelta{
			key:   querySubscriptionRowKey(delta.Row),
			delta: delta,
		})
	}
	sort.SliceStable(entries, func(left, right int) bool {
		return entries[left].key < entries[right].key
	})
	result := make([]QuerySubscriptionDelta, 0, len(entries))
	for _, entry := range entries {
		if len(result) == 0 || querySubscriptionRowKey(result[len(result)-1].Row) != entry.key {
			entry.delta.Row = cloneDifferentialRow(entry.delta.Row)
			result = append(result, entry.delta)
			continue
		}
		combined, ok := addDifferentialCounts(result[len(result)-1].Diff, entry.delta.Diff)
		if !ok {
			return nil, errM208BaselineOverflow
		}
		result[len(result)-1].Diff = combined
	}
	filtered := result[:0]
	for _, delta := range result {
		if delta.Diff != 0 {
			filtered = append(filtered, delta)
		}
	}
	if len(filtered) == 0 {
		return nil, nil
	}
	return filtered, nil
}

func m208BenchmarkDeltas() []QuerySubscriptionDelta {
	deltas := make([]QuerySubscriptionDelta, 4096)
	for index := range deltas {
		diff := int64(1)
		if index%5 == 0 {
			diff = -1
		}
		deltas[index] = QuerySubscriptionDelta{
			Row:  Row{"id": int64(index % 128), "kind": "event"},
			Diff: diff,
		}
	}
	return deltas
}
