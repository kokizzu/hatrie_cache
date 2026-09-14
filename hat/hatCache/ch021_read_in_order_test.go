package hatCache

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

type ch021OrderedLimitProbe struct {
	*HatTrie
	visited int
}

func (probe *ch021OrderedLimitProbe) StreamSQLOrderedSource(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, visit func(SQLRow) error) (bool, error) {
	return probe.HatTrie.StreamSQLOrderedSource(ctx, name, key, field, desc, nullsFirst, nullsLast, func(row SQLRow) error {
		probe.visited++
		return visit(row)
	})
}

func TestCH021OrderedLimitStopsMaterializedQueryAtLimit(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("events", `[{"id":0},{"id":1},{"id":2},{"id":3},{"id":4},{"id":5},{"id":6},{"id":7}]`)
	if err := trie.CreateSQLJSONFieldIndex("events", "id"); err != nil {
		t.Fatal(err)
	}
	probe := &ch021OrderedLimitProbe{HatTrie: trie}
	result, err := ExecuteSQLQuery("FROM CACHE('events') AS event SELECT event.id ORDER BY event.id LIMIT 3", probe)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 3 || result.Rows[0]["id"] != float64(0) || result.Rows[2]["id"] != float64(2) {
		t.Fatalf("rows = %#v, want first three ordered rows", result.Rows)
	}
	if probe.visited != 3 {
		t.Fatalf("ordered source visited %d rows, want 3", probe.visited)
	}

	probe.visited = 0
	offsetResult, err := ExecuteSQLQuery("FROM CACHE('events') AS event SELECT event.id ORDER BY event.id LIMIT 3 OFFSET 2", probe)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() with offset error = %v", err)
	}
	if len(offsetResult.Rows) != 3 || offsetResult.Rows[0]["id"] != float64(2) || offsetResult.Rows[2]["id"] != float64(4) {
		t.Fatalf("offset rows = %#v, want ids 2 through 4", offsetResult.Rows)
	}
	if probe.visited != 5 {
		t.Fatalf("ordered source with offset visited %d rows, want 5", probe.visited)
	}

	probe.visited = 0
	zeroResult, err := ExecuteSQLQuery("FROM CACHE('events') AS event SELECT event.id ORDER BY event.id LIMIT 0", probe)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() with zero limit error = %v", err)
	}
	if len(zeroResult.Rows) != 0 || probe.visited != 0 {
		t.Fatalf("zero-limit result/visits = %#v/%d, want empty/zero", zeroResult.Rows, probe.visited)
	}

	budgetProbe := &ch021OrderedLimitProbe{HatTrie: trie}
	_, err = ExecuteSQLQueryContext(context.Background(), "FROM CACHE('events') AS event SELECT event.id ORDER BY event.id LIMIT 1", budgetProbe, SQLQueryOptions{MaxRows: 2})
	if err == nil || !strings.Contains(err.Error(), "exceeds the 2 row limit") {
		t.Fatalf("ExecuteSQLQueryContext() with source budget error = %v, want source-row limit", err)
	}
}

var ch021OrderedLimitBenchmarkSink int

func BenchmarkCH021OrderedLimit(b *testing.B) {
	trie := CreateHatTrie()
	b.Cleanup(trie.Destroy)
	var data strings.Builder
	data.WriteByte('[')
	for row := 0; row < 20_000; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		fmt.Fprintf(&data, `{"id":%d,"payload":"payload-%05d"}`, row, row)
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	if err := trie.CreateSQLJSONFieldIndex("events", "id"); err != nil {
		b.Fatal(err)
	}
	query := "FROM CACHE('events') AS event SELECT event.id, event.payload ORDER BY event.id LIMIT 3"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			b.Fatal(err)
		}
		ch021OrderedLimitBenchmarkSink = len(result.Rows)
	}
}
