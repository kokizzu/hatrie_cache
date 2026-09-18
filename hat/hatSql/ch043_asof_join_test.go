package hatSql

import (
	"reflect"
	"sort"
	"testing"
	"time"
)

func TestCH043AsOfJoinReturnsLatestEarlierVersionByKey(t *testing.T) {
	table := NewTemporalTable()
	base := time.Unix(100, 0).UTC()
	table.Upsert("account-a", base, Row{"version": "old"})
	table.Upsert("account-a", base.Add(10*time.Second), Row{"version": "new"})
	table.Upsert("account-b", base.Add(2*time.Second), Row{"version": "b"})
	table.Upsert("account-a", base.Add(10*time.Second), Row{"version": "latest-at-tie"})

	inputs := []AsOfJoinRow{
		{Key: "account-a", At: base.Add(5 * time.Second), Row: Row{"id": "a-5"}},
		{Key: "account-b", At: base.Add(3 * time.Second), Row: Row{"id": "b-3"}},
		{Key: "missing", At: base.Add(3 * time.Second), Row: Row{"id": "missing"}},
		{Key: "account-a", At: base.Add(10 * time.Second), Row: Row{"id": "a-10"}},
		{Key: "account-a", At: base.Add(-time.Second), Row: Row{"id": "a-before"}},
	}
	matches := table.AsOfJoin(inputs)
	want := []AsOfJoinMatch{
		{Key: "account-a", LeftAt: base.Add(5 * time.Second), RightAt: base, Left: Row{"id": "a-5"}, Right: Row{"version": "old"}},
		{Key: "account-b", LeftAt: base.Add(3 * time.Second), RightAt: base.Add(2 * time.Second), Left: Row{"id": "b-3"}, Right: Row{"version": "b"}},
		{Key: "account-a", LeftAt: base.Add(10 * time.Second), RightAt: base.Add(10 * time.Second), Left: Row{"id": "a-10"}, Right: Row{"version": "latest-at-tie"}},
	}
	if !reflect.DeepEqual(matches, want) {
		t.Fatalf("AsOfJoin() = %#v, want %#v", matches, want)
	}

	inputs[0].Row["id"] = "mutated-input"
	matches[0].Right["version"] = "mutated-result"
	if got, ok := table.AsOf("account-a", base.Add(5*time.Second)); !ok || got["version"] != "old" {
		t.Fatalf("AsOf() after result mutation = %#v, %v; want old version", got, ok)
	}
}

func TestCH043AsOfJoinHandlesOutOfOrderRowsAndEmptyInputs(t *testing.T) {
	table := NewTemporalTable()
	base := time.Unix(200, 0).UTC()
	for index := 0; index < 4; index++ {
		table.Upsert("key", base.Add(time.Duration(index)*time.Second), Row{"value": index})
	}
	inputs := []AsOfJoinRow{
		{Key: "key", At: base.Add(3 * time.Second), Row: Row{"id": "late"}},
		{Key: "key", At: base.Add(time.Second), Row: Row{"id": "early"}},
		{Key: "key", At: base.Add(2 * time.Second), Row: Row{"id": "middle"}},
	}
	matches := table.AsOfJoin(inputs)
	if len(matches) != len(inputs) || matches[0].Right["value"] != 3 || matches[1].Right["value"] != 1 || matches[2].Right["value"] != 2 {
		t.Fatalf("out-of-order matches = %#v", matches)
	}
	if got := table.AsOfJoin(nil); got != nil {
		t.Fatalf("AsOfJoin(nil) = %#v, want nil", got)
	}
	var nilTable *TemporalTable
	if got := nilTable.AsOfJoin(inputs); got != nil {
		t.Fatalf("nil table AsOfJoin() = %#v, want nil", got)
	}
}

func BenchmarkCH043RepeatedAsOf(b *testing.B) {
	table, inputs := benchmarkCH043Fixture(b)
	b.ReportAllocs()
	var sink []AsOfJoinMatch
	b.ResetTimer()
	for range b.N {
		sink = benchmarkCH043RepeatedAsOfRun(table, inputs)
	}
	_ = sink
}

func BenchmarkCH043BatchAsOfJoin(b *testing.B) {
	table, inputs := benchmarkCH043Fixture(b)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = table.AsOfJoin(inputs)
	}
}

func benchmarkCH043Fixture(b *testing.B) (*TemporalTable, []AsOfJoinRow) {
	b.Helper()
	table := NewTemporalTable()
	base := time.Unix(1_000, 0).UTC()
	for index := 0; index < 4096; index++ {
		table.Upsert("key", base.Add(time.Duration(index)*time.Second), Row{"value": index})
	}
	inputs := make([]AsOfJoinRow, 16_384)
	for index := range inputs {
		at := base.Add(time.Duration(index%4096)*time.Second + 500*time.Millisecond)
		inputs[index] = AsOfJoinRow{Key: "key", At: at, Row: Row{"id": index}}
	}
	return table, inputs
}

func benchmarkCH043RepeatedAsOfRun(table *TemporalTable, inputs []AsOfJoinRow) []AsOfJoinMatch {
	matches := make([]AsOfJoinMatch, 0, len(inputs))
	for _, input := range inputs {
		versions := table.versions[input.Key]
		index := sort.Search(len(versions), func(index int) bool { return versions[index].At.After(input.At) })
		if index == 0 {
			continue
		}
		version := versions[index-1]
		matches = append(matches, AsOfJoinMatch{
			Key:     input.Key,
			LeftAt:  input.At,
			RightAt: version.At,
			Left:    CloneRows([]Row{input.Row})[0],
			Right:   CloneRows([]Row{version.Row})[0],
		})
	}
	return matches
}
