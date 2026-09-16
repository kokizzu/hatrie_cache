package hatCache

import (
	"encoding/json"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLJSONPathSkipIndexExplainDiagnostics(t *testing.T) {
	t.Parallel()
	trie := newTestTrie(t)
	trie.UpsertString("people", `[
  {"id":1,"profile":{"city":"Singapore"}},
  {"id":2,"profile":{"city":"Jakarta"}},
  {"id":3,"profile":{"city":"Jakarta"}},
  {"id":4,"profile":{"city":"Jakarta"}}
]`)
	if err := trie.CreateSQLJSONPathSkipIndex(SQLJSONPathSkipIndexSpec{
		CacheKey:       "people",
		Paths:          []string{"$.profile.city"},
		RowsPerSegment: 2,
		BitsPerSegment: 256,
	}); err != nil {
		t.Fatalf("CreateSQLJSONPathSkipIndex() error = %v", err)
	}
	query := "FROM CACHE('people') AS p WHERE JSON_VALUE(p.profile, '$.city') = 'Singapore' SELECT p.id"
	result, err := hatSql.ExecuteSQLQuery(query, trie)
	if err != nil {
		t.Fatalf("ExecuteSQLQuery() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["id"] != float64(1) {
		t.Fatalf("result.Rows = %#v, want id 1", result.Rows)
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
	if diagnostics.Kind != "json_path_skip" || diagnostics.Field != "$.profile.city" {
		t.Fatalf("index diagnostics identity = %#v, want JSON path skip", diagnostics)
	}
	if diagnostics.IndexBytes != 64 || diagnostics.TotalRows != 4 || diagnostics.CandidateRows != 2 || diagnostics.SkippedRows != 2 {
		t.Fatalf("index diagnostics rows/bytes = %#v, want bytes=64 total=4 candidates=2 skipped=2", diagnostics)
	}
	if diagnostics.Segments != 2 || diagnostics.CandidateSegments != 1 || diagnostics.SkippedSegments != 1 {
		t.Fatalf("index diagnostics segments = %#v, want total=2 candidate=1 skipped=1", diagnostics)
	}
	if indexStep.Pruning == nil {
		t.Fatalf("index step pruning = nil, want residual exact-check metrics")
	}
	pruning := indexStep.Pruning
	if pruning.TotalRows != 4 || pruning.SkippedRows != 2 || pruning.ScannedRows != 2 || pruning.MatchedRows != 1 || pruning.ResidualRows != 1 || pruning.ResidualFalsePositiveRate != 50 {
		t.Fatalf("index pruning = %#v, want total=4 skipped=2 scanned=2 matched=1 residual=1 rate=50", pruning)
	}

	encoded, err := json.Marshal(explained)
	if err != nil {
		t.Fatalf("json.Marshal(EXPLAIN ANALYZE) error = %v", err)
	}
	var wire struct {
		Rows []map[string]interface{} `json:"rows"`
	}
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatalf("json.Unmarshal(EXPLAIN ANALYZE) error = %v", err)
	}
	foundWireIndex := false
	for _, row := range wire.Rows {
		if row["node"] != "INDEX SCAN" {
			continue
		}
		indexPayload, ok := row["index"].(map[string]interface{})
		if !ok || indexPayload["kind"] != "json_path_skip" || indexPayload["index_bytes"] != float64(64) {
			t.Fatalf("wire index payload = %#v, want JSON path skip with 64 bytes", row["index"])
		}
		foundWireIndex = true
	}
	if !foundWireIndex {
		t.Fatalf("wire rows = %#v, want INDEX SCAN index payload", wire.Rows)
	}

	wrapped := monitoringSQLResolver{source: trie}
	if diagnostics, available, err := wrapped.ResolveSQLIndexDiagnostics("CACHE", "people", "$.profile.city", "Singapore"); err != nil || !available || diagnostics.IndexBytes != 64 {
		t.Fatalf("monitoring diagnostics = %#v, %t, %v", diagnostics, available, err)
	}
	catalog := hatSql.CatalogResolver{Source: trie}
	if diagnostics, available, err := catalog.ResolveSQLIndexDiagnostics("CACHE", "people", "$.profile.city", "Singapore"); err != nil || !available || diagnostics.IndexBytes != 64 {
		t.Fatalf("catalog diagnostics = %#v, %t, %v", diagnostics, available, err)
	}
}
