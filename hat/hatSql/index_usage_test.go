package hatSql

import (
	"context"
	"testing"
)

func TestSQLIndexUseRecorderReportsUnusedAndRedundantIndexes(t *testing.T) {
	recorder := NewSQLIndexUseRecorder()
	resolver := indexUseResolver{}
	if _, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('people') AS person WHERE person.id = 2 SELECT person.name", resolver, nil, QueryOptions{IndexUseRecorder: recorder}); err != nil {
		t.Fatal(err)
	}
	report := recorder.Report([]SQLIndexDefinition{
		{Key: "people", Field: "id", Kind: "field"},
		{Key: "people", Field: "id", Kind: "bitmap"},
		{Key: "people", Field: "name", Kind: "field"},
	})
	if len(report) != 3 || !report[0].Used || !report[0].Redundant || !report[1].Used || !report[1].Redundant || report[2].Used || !report[2].Unused {
		t.Fatalf("Report() = %#v", report)
	}
}

type indexUseResolver struct{}

func (indexUseResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"id": int64(1), "name": "Ada"}, {"id": int64(2), "name": "Lin"}}, nil
}

func (indexUseResolver) ResolveSQLIndexedSource(string, string, string, interface{}) ([]Row, bool, error) {
	return []Row{{"id": int64(2), "name": "Lin"}}, true, nil
}
