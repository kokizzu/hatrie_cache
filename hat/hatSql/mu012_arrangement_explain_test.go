package hatSql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestMU012ExplainIncludesBoundedArrangementMetadata(t *testing.T) {
	resolver := mu012ArrangementResolver{}
	result, err := ExecuteSQLQuery("EXPLAIN SELECT id FROM CACHE('events')", resolver)
	if err != nil {
		t.Fatalf("EXPLAIN returned error: %v", err)
	}
	if len(result.Plan) == 0 || len(result.Plan[0].Arrangements) != 2 {
		t.Fatalf("explain arrangements = %#v, want two entries", result.Plan)
	}
	want := []SQLArrangementMetadata{
		{Key: "events_by_id", Kind: "HASH", Reused: true, Cardinality: 12, MemoryBytes: 4096},
		{Key: "events_by_time", Kind: "ORDERED", Reused: false, Cardinality: 12, MemoryBytes: 8192},
	}
	if !reflect.DeepEqual(result.Plan[0].Arrangements, want) {
		t.Fatalf("arrangements = %#v, want %#v", result.Plan[0].Arrangements, want)
	}
	if !reflect.DeepEqual(result.Columns, []string{"node", "detail", "estimated_rows", "arrangements"}) {
		t.Fatalf("EXPLAIN columns = %v, want arrangement column", result.Columns)
	}
	if _, ok := result.Rows[0]["arrangements"]; !ok {
		t.Fatalf("EXPLAIN row = %#v, want arrangements", result.Rows[0])
	}

	graph := BuildExplainDataflowGraph(result.Plan)
	graph.Nodes[0].Step.Arrangements[0].Key = "mutated"
	if result.Plan[0].Arrangements[0].Key != "events_by_id" {
		t.Fatal("dataflow graph shares arrangement metadata with the plan")
	}
}

func TestMU012ExplainIgnoresArrangementMetadataErrors(t *testing.T) {
	result, err := ExecuteSQLQuery("EXPLAIN SELECT id FROM CACHE('events')", mu012ArrangementErrorResolver{})
	if err != nil {
		t.Fatalf("EXPLAIN returned error: %v", err)
	}
	if len(result.Plan) == 0 || len(result.Plan[0].Arrangements) != 0 {
		t.Fatalf("explain arrangements after resolver error = %#v, want none", result.Plan)
	}
	if reflect.DeepEqual(result.Columns, []string{"node", "detail", "estimated_rows", "arrangements"}) {
		t.Fatal("EXPLAIN exposed an empty arrangement column")
	}
}

func TestMU012ExplainPipelineIncludesArrangementMetadata(t *testing.T) {
	result, err := ExecuteSQLQuery("EXPLAIN PIPELINE SELECT id FROM CACHE('events')", mu012ArrangementResolver{})
	if err != nil {
		t.Fatalf("EXPLAIN PIPELINE returned error: %v", err)
	}
	if !reflect.DeepEqual(result.Columns, []string{"node", "detail", "stage", "worker", "workers", "estimated_rows", "arrangements"}) {
		t.Fatalf("EXPLAIN PIPELINE columns = %v, want arrangement column", result.Columns)
	}
	if len(result.Plan) == 0 || len(result.Plan[0].Arrangements) != 2 {
		t.Fatalf("EXPLAIN PIPELINE arrangements = %#v, want two entries", result.Plan)
	}
}

func TestMU012CatalogResolverForwardsArrangementMetadata(t *testing.T) {
	resolver := CatalogResolver{Source: mu012ArrangementResolver{}}
	result, err := ExecuteSQLQuery("EXPLAIN SELECT id FROM CACHE('events')", resolver)
	if err != nil {
		t.Fatalf("EXPLAIN with catalog resolver returned error: %v", err)
	}
	if len(result.Plan) == 0 || len(result.Plan[0].Arrangements) != 2 {
		t.Fatalf("catalog-forwarded arrangements = %#v, want two entries", result.Plan)
	}
}

func TestMU012ExplainAnalyzeUsesFinalPlanShape(t *testing.T) {
	result, err := ExecuteSQLQuery("EXPLAIN ANALYZE SELECT id FROM CACHE('events')", mu012ArrangementResolver{})
	if err != nil {
		t.Fatalf("EXPLAIN ANALYZE returned error: %v", err)
	}
	if sqlExplainHasArrangementMetadata(result.Plan) {
		if !containsSQLColumn(result.Columns, "arrangements") {
			t.Fatalf("EXPLAIN ANALYZE plan has arrangements but columns = %v", result.Columns)
		}
	} else if containsSQLColumn(result.Columns, "arrangements") {
		t.Fatalf("EXPLAIN ANALYZE exposed stale arrangement column: %v", result.Columns)
	}
}

func containsSQLColumn(columns []string, want string) bool {
	for _, column := range columns {
		if column == want {
			return true
		}
	}
	return false
}

func TestMU012ArrangementMetadataHasBoundedOutput(t *testing.T) {
	arrangements := resolveSQLArrangementMetadata(mu012ArrangementManyResolver{}, sqlSource{kind: "CACHE", key: "events"})
	if len(arrangements) != maxSQLExplainArrangementEntries {
		t.Fatalf("arrangement count = %d, want %d", len(arrangements), maxSQLExplainArrangementEntries)
	}
	if len(arrangements[0].Key) != maxSQLExplainArrangementText || len(arrangements[0].Kind) != maxSQLExplainArrangementText {
		t.Fatalf("arrangement text lengths = %d, %d, want %d", len(arrangements[0].Key), len(arrangements[0].Kind), maxSQLExplainArrangementText)
	}
}

type mu012ArrangementResolver struct{}

func (mu012ArrangementResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (mu012ArrangementResolver) ResolveSQLArrangementMetadata(string, string) ([]SQLArrangementMetadata, error) {
	return []SQLArrangementMetadata{
		{Key: "events_by_id", Kind: "HASH", Reused: true, Cardinality: 12, MemoryBytes: 4096},
		{Key: "events_by_time", Kind: "ORDERED", Cardinality: 12, MemoryBytes: 8192},
	}, nil
}

type mu012ArrangementErrorResolver struct{}

func (mu012ArrangementErrorResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (mu012ArrangementErrorResolver) ResolveSQLArrangementMetadata(string, string) ([]SQLArrangementMetadata, error) {
	return nil, errors.New("arrangement metadata unavailable")
}

type mu012ArrangementManyResolver struct{}

func (mu012ArrangementManyResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1)}}, nil
}

func (mu012ArrangementManyResolver) ResolveSQLArrangementMetadata(string, string) ([]SQLArrangementMetadata, error) {
	arrangements := make([]SQLArrangementMetadata, maxSQLExplainArrangementEntries+8)
	for index := range arrangements {
		arrangements[index] = SQLArrangementMetadata{
			Key:  strings.Repeat("k", maxSQLExplainArrangementText+32),
			Kind: strings.Repeat("i", maxSQLExplainArrangementText+32),
		}
	}
	return arrangements, nil
}
