package hatSchema

import (
	"context"
	"errors"
	"fmt"
	"hatrie_cache/hat/hatSql"
	"strings"
	"sync"
	"testing"
)

func TestTR023MaterializedFunctionalIndexSupportsSQLAndMaintenance(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "name"}})
	for _, row := range []Row{
		{"id": int64(1), "name": "Ada"},
		{"id": int64(2), "name": "ADA"},
		{"id": int64(3), "name": "Grace"},
	} {
		if _, err := source.Insert(row); err != nil {
			t.Fatal(err)
		}
	}

	indexName := hatSql.LowerIndexField("name")
	report, err := source.BuildFunctionalIndex(indexName, []string{"name"}, func(row Row) (interface{}, error) {
		name, ok := row["name"].(string)
		if !ok {
			return nil, fmt.Errorf("name is not text")
		}
		return strings.ToLower(name), nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Name != indexName || report.Rows != 3 || report.Attempts != 1 || !source.HasIndex(indexName) {
		t.Fatalf("functional index report/state = %#v, has=%t", report, source.HasIndex(indexName))
	}

	rows := source.Lookup(indexName, "ada")
	if len(rows) != 2 || rows[0]["id"] != int64(1) || rows[1]["id"] != int64(2) {
		t.Fatalf("functional lookup = %#v, want ids 1 and 2", rows)
	}

	if _, err := source.Insert(Row{"id": int64(4), "name": "AdA"}); err != nil {
		t.Fatal(err)
	}
	adapter := SQLResolverAdapter{Sources: map[string]*MaterializedSource{"people": source}}
	result, err := hatSql.ExecuteSQLQueryContext(context.Background(), "FROM CACHE('people') AS person WHERE LOWER(person.name) = 'ada' SELECT person.id ORDER BY person.id", adapter, hatSql.SQLQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows) != 3 || result.Rows[0]["id"] != int64(1) || result.Rows[1]["id"] != int64(2) || result.Rows[2]["id"] != int64(4) {
		t.Fatalf("functional SQL result = %#v, want ids 1, 2, 4", result.Rows)
	}
}

func TestTR023FunctionalIndexUsesClonedRowsAndDoesNotPublishOnEvaluationError(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "name"}})
	if _, err := source.Insert(Row{"id": int64(1), "name": "Ada"}); err != nil {
		t.Fatal(err)
	}

	_, err := source.BuildFunctionalIndex("lower_name", []string{"name"}, func(row Row) (interface{}, error) {
		row["name"] = "mutated"
		return nil, errors.New("expression failed")
	})
	if err == nil || !strings.Contains(err.Error(), "expression failed") {
		t.Fatalf("BuildFunctionalIndex() error = %v, want expression failure", err)
	}
	if source.HasIndex("lower_name") || len(source.Rows()) != 1 || source.Rows()[0]["name"] != "Ada" {
		t.Fatalf("failed build changed source: has=%t rows=%#v", source.HasIndex("lower_name"), source.Rows())
	}
}

func TestTR023FunctionalIndexBuildRetriesAfterConcurrentInsert(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "id"}, {Name: "name"}})
	for index := 0; index < 64; index++ {
		if _, err := source.Insert(Row{"id": int64(index), "name": fmt.Sprintf("name-%d", index)}); err != nil {
			t.Fatal(err)
		}
	}
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var wait sync.WaitGroup
	wait.Add(1)
	var report FunctionalIndexBuildReport
	var buildErr error
	go func() {
		defer wait.Done()
		report, buildErr = source.BuildFunctionalIndex("name_copy", []string{"name"}, func(row Row) (interface{}, error) {
			once.Do(func() {
				close(started)
				<-release
			})
			return row["name"], nil
		})
	}()
	<-started
	if _, err := source.Insert(Row{"id": int64(64), "name": "name-64"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	wait.Wait()
	if buildErr != nil {
		t.Fatal(buildErr)
	}
	if report.Attempts < 2 || report.Rows != 65 {
		t.Fatalf("functional build report = %#v, want retry and 65 rows", report)
	}
	rows := source.Lookup("name_copy", "name-64")
	if len(rows) != 1 || rows[0]["id"] != int64(64) {
		t.Fatalf("concurrent functional lookup = %#v", rows)
	}
}

func TestTR023FunctionalIndexValidation(t *testing.T) {
	source := NewMaterializedSource([]DerivedColumn{{Name: "name"}})
	cases := []struct {
		name string
		deps []string
		fn   FunctionalIndexEvaluator
		want error
	}{
		{name: "", deps: []string{"name"}, fn: func(Row) (interface{}, error) { return "", nil }, want: ErrMaterializedSourceFunctionalIndexNameRequired},
		{name: "index", deps: nil, fn: func(Row) (interface{}, error) { return "", nil }, want: ErrMaterializedSourceFunctionalIndexDependenciesRequired},
		{name: "index", deps: []string{"missing"}, fn: func(Row) (interface{}, error) { return "", nil }, want: ErrMaterializedSourceColumnUnknown},
		{name: "index", deps: []string{"name"}, fn: nil, want: ErrMaterializedSourceFunctionalIndexEvaluatorRequired},
	}
	for _, test := range cases {
		t.Run(test.want.Error(), func(t *testing.T) {
			_, err := source.BuildFunctionalIndex(test.name, test.deps, test.fn)
			if !errors.Is(err, test.want) {
				t.Fatalf("BuildFunctionalIndex() error = %v, want %v", err, test.want)
			}
		})
	}
}
