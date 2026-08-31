package hatCache

import (
	"strconv"
	"strings"
	"testing"
)

func TestSQLColumnarLayoutBuildsNGramFiltersForLargeWarmedStringSource(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	var data strings.Builder
	data.WriteByte('[')
	for index := 0; index < 4096; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		name := "alpha-" + strconv.Itoa(index)
		if index == 3000 {
			name = "needle"
		}
		data.WriteString(`{"name":"`)
		data.WriteString(name)
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	trie.UpsertString("events", data.String())
	for warmup := 0; warmup < 2; warmup++ {
		if _, available, err := trie.ResolveSQLColumnarSource("CACHE", "events", []string{"name"}); err != nil || !available {
			t.Fatalf("warm-up %d available = %t, error = %v", warmup, available, err)
		}
	}
	_, segments, available, err := trie.BorrowSQLColumnarSourceSegments("CACHE", "events", []string{"name"})
	if err != nil || !available || segments == nil {
		t.Fatalf("BorrowSQLColumnarSourceSegments() = %#v, %t, %v", segments, available, err)
	}
	filters := segments.StringNGramBloomFilters["name"]
	if len(filters) != 16 || filters[0].MayContainSubstring("needle") || !filters[11].MayContainSubstring("needle") {
		t.Fatalf("n-gram filters = %#v", filters)
	}
}
