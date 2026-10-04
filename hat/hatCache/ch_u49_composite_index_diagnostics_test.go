package hatCache

import (
	"reflect"
	"testing"
)

func TestHatTrieSQLCompositeJSONIndexDiagnostics(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("users", `[{"id":1,"team_id":20,"enabled":true},{"id":2,"team_id":20,"enabled":false},{"id":3,"team_id":20,"enabled":true},{"id":4,"team_id":30,"enabled":true}]`)
	if err := trie.CreateSQLJSONCompositeIndex("users", "team_id", "enabled"); err != nil {
		t.Fatalf("CreateSQLJSONCompositeIndex() error = %v", err)
	}
	query := "FROM CACHE('users') AS users WHERE users.team_id = 20 AND users.enabled = TRUE SELECT users.id ORDER BY users.id"
	want := []SQLRow{{"id": float64(1)}, {"id": float64(3)}}
	result, err := ExecuteSQLQuery(query, trie)
	if err != nil || !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("composite query rows/error = %#v/%v, want %#v", result.Rows, err, want)
	}
	explained, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("composite EXPLAIN ANALYZE error = %v", err)
	}
	for _, step := range explained.Plan {
		if step.Node != "INDEX SCAN" {
			continue
		}
		if step.Index == nil {
			t.Fatalf("composite INDEX SCAN = %#v, want diagnostics", step)
		}
		if step.Index.Kind != "json_composite" || step.Index.Field != "team_id,enabled" || step.Index.TotalRows != 4 || step.Index.CandidateRows != 2 || step.Index.SkippedRows != 2 || step.Index.IndexBytes <= 0 {
			t.Fatalf("composite diagnostics = %#v, want composite 4/2/2 with bytes", *step.Index)
		}
		break
	}

	trie.UpsertString("users", `[{"id":4,"team_id":20,"enabled":true}]`)
	refreshed, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, trie)
	if err != nil {
		t.Fatalf("refreshed composite EXPLAIN ANALYZE error = %v", err)
	}
	for _, step := range refreshed.Plan {
		if step.Node == "INDEX SCAN" && step.Index != nil {
			if step.Index.TotalRows != 1 || step.Index.CandidateRows != 1 || step.Index.SkippedRows != 0 {
				t.Fatalf("refreshed composite diagnostics = %#v, want 1/1/0", *step.Index)
			}
			return
		}
	}
	t.Fatalf("refreshed composite plan = %#v, want diagnostics", refreshed.Plan)
}
