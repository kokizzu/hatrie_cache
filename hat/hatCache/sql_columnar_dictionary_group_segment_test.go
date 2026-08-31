package hatCache

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestSQLColumnarDictionaryGroupAggregateSkipsNumericSegments(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	var values strings.Builder
	values.WriteByte('[')
	for id := 0; id < 4096; id++ {
		if id > 0 {
			values.WriteByte(',')
		}
		if _, err := fmt.Fprintf(&values, `{"state":%q,"id":%d,"value":%d}`, []string{"queued", "running", "done"}[id%3], id, id%97); err != nil {
			t.Fatal(err)
		}
	}
	values.WriteByte(']')
	trie.UpsertString("jobs", values.String())
	fields := []string{"id", "state", "value"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			t.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}

	query := "FROM CACHE('jobs') AS job WHERE job.id >= 3072 AND job.id < 3328 SELECT job.state, COUNT(*) AS count, SUM(job.value) AS total, MIN(job.value) AS minimum, MAX(job.value) AS maximum GROUP BY job.state ORDER BY job.state"
	columnar, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatal(err)
	}
	materialized, err := ExecuteSQLQuery(query, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(columnar, materialized) {
		t.Fatalf("columnar result = %#v, materialized result = %#v", columnar, materialized)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatal(err)
	}
	hasAggregate, hasSegmentSkip := false, false
	for _, step := range explained.Plan {
		hasAggregate = hasAggregate || step.Node == "COLUMNAR DICTIONARY GROUP AGGREGATE"
		hasSegmentSkip = hasSegmentSkip || step.Node == "COLUMNAR SEGMENT SKIP"
	}
	if !hasAggregate || !hasSegmentSkip {
		t.Fatalf("plan = %#v, want COLUMNAR DICTIONARY GROUP AGGREGATE and COLUMNAR SEGMENT SKIP", explained.Plan)
	}
}
