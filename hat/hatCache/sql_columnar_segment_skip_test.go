package hatCache

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestHatTrieBorrowSQLColumnarSourceSegmentsPromotesAndInvalidates(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	var data strings.Builder
	data.WriteByte('[')
	for row := 0; row < 512; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteByte('}')
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())
	fields := []string{"id"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			t.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}

	batch, segments, available, err := trie.BorrowSQLColumnarSourceSegments("CACHE", "jobs", fields)
	if err != nil || !available || batch.Rows != 512 || segments == nil || segments.RowsPerSegment != 256 {
		t.Fatalf("BorrowSQLColumnarSourceSegments() = %#v, %#v, %t, %v", batch, segments, available, err)
	}
	got := segments.Columns["id"]
	if len(got) != 2 || !got[0].Valid || got[0].Minimum != 0 || got[0].Maximum != 255 || !got[1].Valid || got[1].Minimum != 256 || got[1].Maximum != 511 {
		t.Fatalf("id segments = %#v", got)
	}

	trie.UpsertString("jobs", `[{"id":999}]`)
	_, segments, available, err = trie.BorrowSQLColumnarSourceSegments("CACHE", "jobs", fields)
	if err != nil || !available || segments != nil {
		t.Fatalf("post-write BorrowSQLColumnarSourceSegments() segments = %#v, available = %t, error = %v", segments, available, err)
	}
}

func TestHatTrieSQLColumnarDictionarySegmentSkipMatchesMaterializedQuery(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	var data strings.Builder
	data.WriteByte('[')
	for row := 0; row < 512; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		state := "cold"
		if row >= 256 {
			state = "hot"
		}
		data.WriteString(`{"state":"`)
		data.WriteString(state)
		data.WriteString(`","value":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteByte('}')
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())

	query := "FROM CACHE('events') AS event WHERE event.state IN ('hot') SELECT COUNT(*) AS total"
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
		if step.Node == "COLUMNAR SEGMENT SKIP" && step.ActualOutputRows != nil && *step.ActualOutputRows == 256 {
			return
		}
	}
	t.Fatalf("plan = %#v, want dictionary COLUMNAR SEGMENT SKIP of 256 rows", explained.Plan)
}
