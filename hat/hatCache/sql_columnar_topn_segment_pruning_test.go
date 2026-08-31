package hatCache

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestHatTrieSQLColumnarTopNNumericSegmentPruning(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	var data strings.Builder
	data.Grow(32000)
	data.WriteByte('[')
	for row := 0; row < 1024; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		score := row
		if row >= 256 {
			score += 10000
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteString(`,"score":`)
		data.WriteString(strconv.Itoa(score))
		data.WriteByte('}')
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())

	fields := []string{"id", "score"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			t.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	query := "FROM CACHE('jobs') AS job SELECT job.id ORDER BY job.score ASC LIMIT 50"
	want, err := ExecuteSQLQuery(query, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("row executor error = %v", err)
	}
	got, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("columnar query error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("columnar result = %#v, want %#v", got, want)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	if !sqlColumnarSortedProjectionPlanHasNode(explained.Plan, "COLUMNAR TOP-N SEGMENT SKIP") {
		t.Fatalf("plan = %#v, want COLUMNAR TOP-N SEGMENT SKIP", explained.Plan)
	}
}
