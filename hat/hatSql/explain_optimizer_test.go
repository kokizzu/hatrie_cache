package hatSql_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

type explainOptimizerResolver struct{}

func (explainOptimizerResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "orders" {
		return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
	}
	return []hatSql.Row{{"id": 1, "status": "open", "region": "us"}}, nil
}

func (explainOptimizerResolver) SQLJSONIndexStats(key string, fields ...string) (hatSql.JSONIndexStats, bool, error) {
	if key != "orders" || len(fields) != 1 || (fields[0] != "status" && fields[0] != "region") {
		return hatSql.JSONIndexStats{}, false, nil
	}
	return hatSql.JSONIndexStats{Key: key, Fields: fields, Rows: 100, DistinctKeys: 10}, true, nil
}

type selectedExplainOptimizerResolver struct{}

func (selectedExplainOptimizerResolver) ResolveSQLSource(name, key string) ([]hatSql.Row, error) {
	if name != "CACHE" || key != "orders" {
		return nil, fmt.Errorf("unexpected source %s(%q)", name, key)
	}
	return []hatSql.Row{{"id": 1}}, nil
}

func (selectedExplainOptimizerResolver) ResolveSQLIndexedSource(name, key, field string, value interface{}) ([]hatSql.Row, bool, error) {
	if name != "CACHE" || key != "orders" || field != "id" {
		return nil, false, nil
	}
	return []hatSql.Row{{"id": 1}}, true, nil
}

func (selectedExplainOptimizerResolver) SQLJSONIndexStats(key string, fields ...string) (hatSql.JSONIndexStats, bool, error) {
	if key != "orders" || len(fields) != 1 || (fields[0] != "id" && fields[0] != "region") {
		return hatSql.JSONIndexStats{}, false, nil
	}
	return hatSql.JSONIndexStats{Key: key, Fields: fields, Rows: 100, DistinctKeys: 10}, true, nil
}

func TestExplainAnalyzePublishesRejectedOptimizerAlternatives(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('orders') AS o WHERE o.status = 'open' AND o.region = 'us' SELECT o.id", explainOptimizerResolver{})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if step.Node != "INDEX CANDIDATES" {
			continue
		}
		if len(step.Alternatives) != 2 {
			t.Fatalf("optimizer alternatives = %#v, want two rejected candidates", step.Alternatives)
		}
		if len(step.Notices) != 2 {
			t.Fatalf("optimizer notices = %#v, want one notice per rejected candidate", step.Notices)
		}
		for index, alternative := range step.Alternatives {
			if alternative.Expression == "" || alternative.EstimatedRows != 10 || alternative.EstimatedCost != 11 || alternative.Selected || alternative.RejectedReason != "index unavailable" {
				t.Fatalf("optimizer alternative %d = %#v, want unavailable index candidate", index, alternative)
			}
			if step.Notices[index].Code != "optimizer_alternative_rejected" || step.Notices[index].Detail == "" {
				t.Fatalf("optimizer notice %d = %#v, want structured rejection notice", index, step.Notices[index])
			}
		}
		return
	}
	t.Fatalf("EXPLAIN ANALYZE plan = %#v, want INDEX CANDIDATES step", result.Plan)
}

func TestExplainAnalyzeMarksSelectedOptimizerAlternative(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN ANALYZE FROM CACHE('orders') AS o WHERE o.id = 1 AND o.region = 'us' SELECT o.id", selectedExplainOptimizerResolver{})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if step.Node != "INDEX CANDIDATES" {
			continue
		}
		if len(step.Alternatives) != 1 || !step.Alternatives[0].Selected || step.Alternatives[0].RejectedReason != "" {
			t.Fatalf("selected optimizer alternatives = %#v, want one selected candidate", step.Alternatives)
		}
		if len(step.Notices) != 0 {
			t.Fatalf("selected optimizer notices = %#v, want no rejection notices", step.Notices)
		}
		return
	}
	t.Fatalf("EXPLAIN ANALYZE plan = %#v, want INDEX CANDIDATES step", result.Plan)
}

func TestRegularExplainOmitsOptimizerMetadata(t *testing.T) {
	result, err := hatSql.ExecuteSQLQuery("EXPLAIN FROM VALUES (1) AS values(id) SELECT id", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range result.Plan {
		if len(step.Alternatives) != 0 || len(step.Notices) != 0 {
			t.Fatalf("regular EXPLAIN step = %#v, want no optimizer metadata", step)
		}
	}
	payload, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(payload), "alternatives") || strings.Contains(string(payload), "notices") {
		t.Fatalf("regular EXPLAIN JSON = %s, want optimizer metadata omitted", payload)
	}
}
