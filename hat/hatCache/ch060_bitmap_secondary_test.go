package hatCache

import (
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestCH060BitmapSecondaryCombination(t *testing.T) {
	t.Parallel()
	for _, rows := range []int{5, 5000} {
		rows := rows
		t.Run(strconv.Itoa(rows), func(t *testing.T) {
			trie := newCH060BitmapSecondaryTrie(t, rows)
			for _, test := range []struct {
				name      string
				operation string
				want      []int64
			}{
				{name: "and", operation: "AND", want: ch060ExpectedIDs(rows, func(index int) bool { return index%4 == 0 })},
				{name: "or", operation: "OR", want: ch060ExpectedIDs(rows, func(index int) bool { return index%2 == 0 })},
			} {
				t.Run(test.name, func(t *testing.T) {
					got, available, err := trie.ResolveSQLSecondaryIndexedSource(
						"CACHE",
						"events",
						test.operation,
						[]string{"state", "team"},
						[]interface{}{"ready", "edge"},
					)
					if err != nil || !available {
						t.Fatalf("ResolveSQLSecondaryIndexedSource() available/error = %t/%v", available, err)
					}
					if ids := ch060RowIDs(got); !reflect.DeepEqual(ids, test.want) {
						t.Fatalf("ResolveSQLSecondaryIndexedSource() ids = %#v, want %#v", ids, test.want)
					}
				})
			}
		})
	}
}

func newCH060BitmapSecondaryTrie(t *testing.T, rows int) *HatTrie {
	t.Helper()
	trie := newTestTrie(t)
	trie.UpsertString("events", ch060BitmapSecondaryData(rows))
	if err := trie.CreateSQLJSONBitmapIndex("events", "state"); err != nil {
		t.Fatal(err)
	}
	if err := trie.CreateSQLJSONBitmapIndex("events", "team"); err != nil {
		t.Fatal(err)
	}
	return trie
}

func ch060BitmapSecondaryData(rows int) string {
	var data strings.Builder
	data.Grow(rows * 72)
	data.WriteByte('[')
	for index := 0; index < rows; index++ {
		if index > 0 {
			data.WriteByte(',')
		}
		team := "core"
		if index%4 == 0 {
			team = "edge"
		}
		data.WriteString(`{"id":`)
		data.WriteString(strconv.Itoa(index))
		data.WriteString(`,"state":"`)
		if index%2 == 0 {
			data.WriteString("ready")
		} else {
			data.WriteString("idle")
		}
		data.WriteString(`","team":"`)
		data.WriteString(team)
		data.WriteString(`"}`)
	}
	data.WriteByte(']')
	return data.String()
}

func ch060ExpectedIDs(rows int, include func(index int) bool) []int64 {
	ids := make([]int64, 0, rows)
	for index := 0; index < rows; index++ {
		if include(index) {
			ids = append(ids, int64(index))
		}
	}
	return ids
}

func ch060RowIDs(rows []SQLRow) []int64 {
	ids := make([]int64, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, int64(row["id"].(float64)))
	}
	return ids
}
