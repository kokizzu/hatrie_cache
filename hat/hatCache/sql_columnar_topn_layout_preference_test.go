package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

func TestHatTrieSQLColumnarTopNUsesWarmLayout(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	var data strings.Builder
	data.Grow(130000)
	data.WriteByte('[')
	for row := 0; row < 4096; row++ {
		if row > 0 {
			data.WriteByte(',')
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(row))
		data.WriteString(`,"state":"`)
		data.WriteString([]string{"queued", "running", "done"}[row%3])
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("jobs", data.String())

	fields := []string{"id", "state"}
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "jobs", fields); err != nil || !available {
			t.Fatalf("warm-up ResolveSQLColumnarSource() available = %t, error = %v", available, err)
		}
	}
	before := trie.sqlColumnarLayouts.stats()
	if before.Entries != 1 || before.Hits != 0 {
		t.Fatalf("warm layout stats before query = %#v", before)
	}

	result, err := ExecuteSQLQuery(`FROM CACHE('jobs') AS job WHERE job.id >= 3072 AND job.id < 3328 SELECT job.id, job.state ORDER BY job.id DESC LIMIT 16`, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 16 {
		t.Fatalf("ExecuteSQLQuery() rows = %d, want 16", len(result.Rows))
	}
	for index, row := range result.Rows {
		wantID := float64(3327 - index)
		if got := row["id"]; got != wantID {
			t.Fatalf("row %d id = %#v, want %#v", index, got, wantID)
		}
	}
	after := trie.sqlColumnarLayouts.stats()
	if after.Hits <= before.Hits {
		t.Fatalf("normal Top-N query did not borrow its warm columnar layout: before = %#v, after = %#v", before, after)
	}
}
