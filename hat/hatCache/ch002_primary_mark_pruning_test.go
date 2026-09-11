package hatCache

import (
	"context"
	"testing"
)

const ch002OrderedRangeRows = `[
  {"id":1,"score":10},
  {"id":2,"score":20},
  {"id":3,"score":20},
  {"id":4,"score":30},
  {"id":5},
  {"id":6,"score":null},
  {"id":7,"score":40}
]`

type ch002OrderedRangeProbe struct {
	*HatTrie
	materializedRangeCalls int
	streamRangeCalls       int
}

func (probe *ch002OrderedRangeProbe) ResolveSQLOrderedSourceRange(name, key, field string, desc, nullsFirst, nullsLast bool, operator string, value interface{}) ([]SQLRow, bool, error) {
	probe.materializedRangeCalls++
	return probe.HatTrie.ResolveSQLOrderedSourceRange(name, key, field, desc, nullsFirst, nullsLast, operator, value)
}

func (probe *ch002OrderedRangeProbe) StreamSQLOrderedSourceRange(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, operator string, value interface{}, visit func(SQLRow) error) (bool, error) {
	probe.streamRangeCalls++
	return probe.HatTrie.StreamSQLOrderedSourceRange(ctx, name, key, field, desc, nullsFirst, nullsLast, operator, value, visit)
}

func TestSQLOrderedRangePruningUsesRealIndexes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		configure func(*HatTrie) error
		threshold interface{}
	}{
		{
			name: "generic",
			configure: func(trie *HatTrie) error {
				return trie.CreateSQLJSONFieldIndex("events", "score")
			},
			threshold: float64(20),
		},
		{
			name: "typed-int64",
			configure: func(trie *HatTrie) error {
				return trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{
					CacheKey: "events",
					Fields:   []string{"score"},
					Type:     SQLIndexInt64,
				})
			},
			threshold: int64(20),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			trie := newTestTrie(t)
			trie.UpsertString("events", ch002OrderedRangeRows)
			if err := test.configure(trie); err != nil {
				t.Fatalf("configure index: %v", err)
			}

			ascending, available, err := trie.ResolveSQLOrderedSourceRange("CACHE", "events", "score", false, false, false, ">=", test.threshold)
			if err != nil || !available {
				t.Fatalf("ResolveSQLOrderedSourceRange(ascending) = %#v, %v, %v", ascending, available, err)
			}
			assertCH002RowIDs(t, ascending, []float64{2, 3, 4, 7})

			descending, available, err := trie.ResolveSQLOrderedSourceRange("CACHE", "events", "score", true, false, false, "<=", test.threshold)
			if err != nil || !available {
				t.Fatalf("ResolveSQLOrderedSourceRange(descending) = %#v, %v, %v", descending, available, err)
			}
			assertCH002RowIDs(t, descending, []float64{2, 3, 1})

			var streamed []float64
			available, err = trie.StreamSQLOrderedSourceRange(context.Background(), "CACHE", "events", "score", false, false, false, ">=", test.threshold, func(row SQLRow) error {
				streamed = append(streamed, row["id"].(float64))
				return nil
			})
			if err != nil || !available {
				t.Fatalf("StreamSQLOrderedSourceRange() = %v, %v", available, err)
			}
			assertFloat64Slice(t, streamed, []float64{2, 3, 4, 7})

			query := "FROM CACHE('events') AS event WHERE event.score >= 20 ORDER BY event.score LIMIT 3 SELECT event.id, event.score"
			probe := &ch002OrderedRangeProbe{HatTrie: trie}
			result, err := ExecuteSQLQuery(query, probe)
			if err != nil {
				t.Fatalf("ExecuteSQLQuery() error = %v", err)
			}
			assertCH002RowIDs(t, result.Rows, []float64{2, 3, 4})
			if probe.materializedRangeCalls+probe.streamRangeCalls != 1 {
				t.Fatalf("ordered range calls after materialized query = materialized %d, stream %d; want one", probe.materializedRangeCalls, probe.streamRangeCalls)
			}

			var rowIDs []float64
			err = ExecuteSQLQueryRows(context.Background(), query, probe, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
				rowIDs = append(rowIDs, row["id"].(float64))
				return nil
			})
			if err != nil {
				t.Fatalf("ExecuteSQLQueryRows() error = %v", err)
			}
			assertFloat64Slice(t, rowIDs, []float64{2, 3, 4})
			if probe.materializedRangeCalls+probe.streamRangeCalls != 2 {
				t.Fatalf("ordered range calls after row stream = materialized %d, stream %d; want two total", probe.materializedRangeCalls, probe.streamRangeCalls)
			}
		})
	}
}

func TestSQLOrderedRangePruningRejectsUnsafeOR(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", ch002OrderedRangeRows)
	if err := trie.CreateSQLJSONFieldIndex("events", "score"); err != nil {
		t.Fatalf("CreateSQLJSONFieldIndex() error = %v", err)
	}
	probe := &ch002OrderedRangeProbe{HatTrie: trie}
	result, err := ExecuteSQLQuery("FROM CACHE('events') AS event WHERE event.score >= 20 OR event.id = 1 ORDER BY event.score SELECT event.id", probe)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	assertCH002RowIDs(t, result.Rows, []float64{1, 2, 3, 4, 7})
	if probe.materializedRangeCalls != 0 || probe.streamRangeCalls != 0 {
		t.Fatalf("unsafe OR used ordered range: materialized %d, stream %d", probe.materializedRangeCalls, probe.streamRangeCalls)
	}
}

func assertCH002RowIDs(t *testing.T, rows []SQLRow, want []float64) {
	t.Helper()
	if len(rows) != len(want) {
		t.Fatalf("row count = %d, want %d: %#v", len(rows), len(want), rows)
	}
	for index, row := range rows {
		if row["id"] != want[index] {
			t.Fatalf("row %d id = %#v, want %v: %#v", index, row["id"], want[index], rows)
		}
	}
}

func assertFloat64Slice(t *testing.T, got, want []float64) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("values = %v, want %v", got, want)
	}
	for index := range got {
		if got[index] != want[index] {
			t.Fatalf("values = %v, want %v", got, want)
		}
	}
}
