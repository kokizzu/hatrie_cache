package hatCache

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestHatTrieSQLColumnarStringBloomSegmentSkipMatchesMaterializedQuery(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	var data strings.Builder
	data.WriteByte('[')
	for row := 0; row < 512; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		fmt.Fprintf(&data, `{"id":%d,"tag":"tag-%03d"}`, row, row)
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())

	query := "FROM CACHE('events') AS event WHERE event.tag = 'tag-511' SELECT event.id"
	for warmup := 0; warmup < 2; warmup++ {
		if _, err := ExecuteSQLQuery(query, trie); err != nil {
			t.Fatalf("warm-up ExecuteSQLQuery() error = %v", err)
		}
	}
	columnar, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("columnar ExecuteSQLQuery() error = %v", err)
	}
	materialized, err := ExecuteSQLQuery(query, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("materialized ExecuteSQLQuery() error = %v", err)
	}
	if !reflect.DeepEqual(columnar, materialized) {
		t.Fatalf("columnar result = %#v, materialized result = %#v", columnar, materialized)
	}

	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery(EXPLAIN ANALYZE) error = %v", err)
	}
	for _, step := range explained.Plan {
		if step.Node == "COLUMNAR BLOOM SEGMENT SKIP" && step.ActualOutputRows != nil && *step.ActualOutputRows == 256 {
			return
		}
	}
	t.Fatalf("plan = %#v, want COLUMNAR BLOOM SEGMENT SKIP of 256 rows", explained.Plan)
}
