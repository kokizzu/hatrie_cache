package hatCache

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestHatTrieSQLColumnarSortedProjectionUsesWarmOrderAndInvalidates(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	var data strings.Builder
	data.WriteByte('[')
	for row := 0; row < 128; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteString(`,"score":`)
		data.WriteString(strconv.Itoa(row % 8))
		data.WriteByte('}')
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())

	fields := []string{"score", "id"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			t.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	query := "FROM CACHE('jobs') AS job SELECT job.id, job.score ORDER BY job.score ASC LIMIT 5"
	want, err := ExecuteSQLQuery(query, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("row executor error = %v", err)
	}
	for run := 0; run < sqlColumnarLayoutOrderCacheMinReads; run++ {
		got, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			t.Fatalf("columnar query run %d error = %v", run, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("columnar query run %d = %#v, want %#v", run, got, want)
		}
	}
	for index, row := range want.Rows {
		if got, expected := row["id"], float64(index*8); got != expected {
			t.Fatalf("stable score tie row %d id = %#v, want %#v", index, got, expected)
		}
	}
	descendingQuery := "FROM CACHE('jobs') AS job SELECT job.id, job.score ORDER BY job.score DESC LIMIT 5"
	descending, err := ExecuteSQLQuery(descendingQuery, trie)
	if err != nil {
		t.Fatalf("descending columnar query error = %v", err)
	}
	descendingWant, err := ExecuteSQLQuery(descendingQuery, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("descending row executor error = %v", err)
	}
	if !reflect.DeepEqual(descending, descendingWant) {
		t.Fatalf("descending columnar result = %#v, want %#v", descending, descendingWant)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	if !sqlColumnarSortedProjectionPlanHasNode(explained.Plan, "COLUMNAR SORTED PROJECTION") {
		t.Fatalf("plan = %#v, want COLUMNAR SORTED PROJECTION", explained.Plan)
	}

	trie.UpsertString("jobs", `[{"id":999,"score":0}]`)
	got, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("post-write ExecuteSQLQuery() error = %v", err)
	}
	if want := (SQLQueryResult{Columns: []string{"id", "score"}, Rows: []SQLRow{{"id": float64(999), "score": float64(0)}}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("post-write result = %#v, want %#v", got, want)
	}
	explained, err = ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("post-write EXPLAIN ANALYZE error = %v", err)
	}
	if sqlColumnarSortedProjectionPlanHasNode(explained.Plan, "COLUMNAR SORTED PROJECTION") {
		t.Fatalf("post-write plan = %#v, must not use invalidated sorted projection", explained.Plan)
	}
}

func TestHatTrieSQLColumnarCompositeSortedProjectionUsesWarmOrderAndInvalidates(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("jobs", `[{"id":10,"priority":2,"score":1},{"id":11,"priority":1,"score":2},{"id":12,"priority":1,"score":1},{"id":13,"priority":2,"score":1}]`)
	fields := []string{"priority", "score", "id"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			t.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	query := "FROM CACHE('jobs') AS job SELECT job.id, job.priority, job.score ORDER BY job.priority ASC, job.score ASC LIMIT 4"
	want, err := ExecuteSQLQuery(query, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("row executor error = %v", err)
	}
	for run := 0; run < sqlColumnarLayoutOrderCacheMinReads; run++ {
		got, err := ExecuteSQLQuery(query, trie)
		if err != nil {
			t.Fatalf("columnar query run %d error = %v", run, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("columnar query run %d = %#v, want %#v", run, got, want)
		}
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE error = %v", err)
	}
	if !sqlColumnarSortedProjectionPlanHasNode(explained.Plan, "COLUMNAR COMPOSITE SORTED PROJECTION") {
		t.Fatalf("plan = %#v, want COLUMNAR COMPOSITE SORTED PROJECTION", explained.Plan)
	}
	reorderedQuery := "FROM CACHE('jobs') AS job SELECT job.id, job.priority, job.score ORDER BY job.score ASC, job.priority ASC LIMIT 4"
	reorderedWant, err := ExecuteSQLQuery(reorderedQuery, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("reordered row executor error = %v", err)
	}
	reordered, err := ExecuteSQLQuery(reorderedQuery, trie)
	if err != nil {
		t.Fatalf("reordered columnar query error = %v", err)
	}
	if !reflect.DeepEqual(reordered, reorderedWant) {
		t.Fatalf("reordered columnar result = %#v, want %#v", reordered, reorderedWant)
	}

	descendingQuery := "FROM CACHE('jobs') AS job SELECT job.id, job.priority, job.score ORDER BY job.priority DESC, job.score DESC LIMIT 4"
	descendingWant, err := ExecuteSQLQuery(descendingQuery, sqlRowsOnlyResolver{trie: trie})
	if err != nil {
		t.Fatalf("descending row executor error = %v", err)
	}
	descending, err := ExecuteSQLQuery(descendingQuery, trie)
	if err != nil {
		t.Fatalf("descending columnar query error = %v", err)
	}
	if !reflect.DeepEqual(descending, descendingWant) {
		t.Fatalf("descending columnar result = %#v, want %#v", descending, descendingWant)
	}

	trie.UpsertString("jobs", `[{"id":99,"priority":0,"score":0}]`)
	got, err := ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("post-write ExecuteSQLQuery() error = %v", err)
	}
	if want := (SQLQueryResult{Columns: []string{"id", "priority", "score"}, Rows: []SQLRow{{"id": float64(99), "priority": float64(0), "score": float64(0)}}}); !reflect.DeepEqual(got, want) {
		t.Fatalf("post-write result = %#v, want %#v", got, want)
	}
	explained, err = ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("post-write EXPLAIN ANALYZE error = %v", err)
	}
	if sqlColumnarSortedProjectionPlanHasNode(explained.Plan, "COLUMNAR COMPOSITE SORTED PROJECTION") {
		t.Fatalf("post-write plan = %#v, must not use invalidated composite sorted projection", explained.Plan)
	}
}

func sqlColumnarSortedProjectionPlanHasNode(plan []SQLExplainStep, node string) bool {
	for _, step := range plan {
		if step.Node == node {
			return true
		}
	}
	return false
}
