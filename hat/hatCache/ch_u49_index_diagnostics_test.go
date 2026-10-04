package hatCache

import (
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestHatTrieSQLJSONIndexDiagnostics(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		kind       string
		field      string
		value      string
		candidates int
		configure  func(*HatTrie) error
	}{
		{
			name:       "field",
			kind:       "json_field",
			field:      "team",
			value:      "'red'",
			candidates: 2,
			configure: func(trie *HatTrie) error {
				return trie.CreateSQLJSONFieldIndex("people", "team")
			},
		},
		{
			name:       "typed_int64",
			kind:       "json_typed_int64",
			field:      "age",
			value:      "2",
			candidates: 2,
			configure: func(trie *HatTrie) error {
				return trie.CreateSQLTypedJSONIndex(SQLJSONIndexSpec{
					CacheKey: "people",
					Fields:   []string{"age"},
					Type:     SQLIndexInt64,
				})
			},
		},
		{
			name:       "bitmap",
			kind:       "json_bitmap",
			field:      "team",
			value:      "'red'",
			candidates: 2,
			configure: func(trie *HatTrie) error {
				return trie.CreateSQLJSONBitmapIndex("people", "team")
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trie := newTestTrie(t)
			trie.UpsertString("people", `[
  {"id":1,"team":"red","age":2},
  {"id":2,"team":"blue","age":3},
  {"id":3,"team":"red","age":2},
  {"id":4,"age":4}
]`)
			if err := test.configure(trie); err != nil {
				t.Fatal(err)
			}
			query := "FROM CACHE('people') AS person WHERE person." + test.field + " = " + test.value + " SELECT person.id"
			result, err := hatSql.ExecuteSQLQuery(query, trie)
			if err != nil {
				t.Fatalf("query error = %v", err)
			}
			if len(result.Rows) != test.candidates {
				t.Fatalf("query rows = %d, want %d", len(result.Rows), test.candidates)
			}
			for index, row := range result.Rows {
				wantID := float64(index*2 + 1)
				if row["id"] != wantID {
					t.Fatalf("query row %d = %#v, want id %v", index, row, wantID)
				}
			}

			explained, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
			if err != nil {
				t.Fatalf("EXPLAIN ANALYZE error = %v", err)
			}
			var indexStep *hatSql.ExplainStep
			for index := range explained.Plan {
				if explained.Plan[index].Node == "INDEX SCAN" {
					indexStep = &explained.Plan[index]
					break
				}
			}
			if indexStep == nil || indexStep.Index == nil {
				t.Fatalf("plan = %#v, want INDEX SCAN with diagnostics", explained.Plan)
			}
			diagnostics := indexStep.Index
			if diagnostics.Kind != test.kind || diagnostics.Field != test.field {
				t.Fatalf("diagnostics identity = %#v, want kind=%q field=%q", diagnostics, test.kind, test.field)
			}
			if diagnostics.TotalRows != 4 || diagnostics.CandidateRows != test.candidates || diagnostics.SkippedRows != 4-test.candidates {
				t.Fatalf("diagnostics rows = %#v, want total=4 candidates=%d skipped=%d", diagnostics, test.candidates, 4-test.candidates)
			}
			if diagnostics.IndexBytes <= 0 {
				t.Fatalf("diagnostics bytes = %d, want positive index footprint", diagnostics.IndexBytes)
			}
		})
	}
}
