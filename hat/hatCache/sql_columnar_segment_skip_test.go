package hatCache

import (
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
