package hatSql

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

type ch042SampledResolver struct {
	rows         []Row
	sampledRows  []Row
	reportedRows int
	fullCalls    int
	sampleCalls  int
	lastRequest  SQLSampleRequest
}

func (resolver *ch042SampledResolver) ResolveSQLSource(string, string) ([]Row, error) {
	resolver.fullCalls++
	return resolver.rows, nil
}

func (resolver *ch042SampledResolver) ResolveSQLSampledSource(name, key string, request SQLSampleRequest) ([]Row, int, bool, error) {
	if name != "CACHE" || key != "events" {
		return nil, 0, false, nil
	}
	resolver.sampleCalls++
	resolver.lastRequest = request
	inputRows := len(resolver.rows)
	if resolver.reportedRows != 0 {
		inputRows = resolver.reportedRows
	}
	return resolver.sampledRows, inputRows, true, nil
}

func TestCH042StorageAwareSampleAvoidsFullSourceMaterialization(t *testing.T) {
	resolver := &ch042SampledResolver{
		rows:        []Row{{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}},
		sampledRows: []Row{{"id": 2}, {"id": 4}},
	}
	query := `SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (50) REPEATABLE (7)`
	result, err := ExecuteSQLQuery(query, resolver)
	if err != nil {
		t.Fatalf("storage-aware sample: %v", err)
	}
	if !reflect.DeepEqual(result.Rows, []SQLRow{{"id": 2}, {"id": 4}}) {
		t.Fatalf("sampled rows = %#v", result.Rows)
	}
	if resolver.sampleCalls != 1 || resolver.fullCalls != 0 {
		t.Fatalf("resolver calls = sampled %d full %d, want sampled 1 full 0", resolver.sampleCalls, resolver.fullCalls)
	}
	if resolver.lastRequest != (SQLSampleRequest{Mode: "BERNOULLI", Value: 50, Seed: 7}) {
		t.Fatalf("sample request = %#v", resolver.lastRequest)
	}

	analysis, err := ExecuteSQLQuery("EXPLAIN ANALYZE "+query, resolver)
	if err != nil {
		t.Fatalf("explain storage-aware sample: %v", err)
	}
	if !strings.Contains(strings.ToUpper(fmt.Sprint(analysis.Plan)), "TABLESAMPLE STORAGE") {
		t.Fatalf("sampled plan = %#v", analysis.Plan)
	}
}

func TestCH042StorageAwareSamplePreservesFullSourceRowBudget(t *testing.T) {
	resolver := &ch042SampledResolver{
		rows:        []Row{{"id": 1}, {"id": 2}, {"id": 3}},
		sampledRows: []Row{{"id": 2}},
	}
	_, err := ExecuteSQLQueryParameters(
		context.Background(),
		`SELECT id FROM CACHE('events') TABLESAMPLE RESERVOIR (1)`,
		resolver,
		nil,
		SQLQueryOptions{MaxRows: 2},
	)
	if err == nil || !strings.Contains(err.Error(), "exceeds the 2 row limit") {
		t.Fatalf("storage-aware row-budget error = %v", err)
	}
}

func TestCH042StorageAwareSampleRejectsInvalidCardinality(t *testing.T) {
	resolver := &ch042SampledResolver{
		rows:         []Row{{"id": 1}},
		sampledRows:  []Row{{"id": 1}},
		reportedRows: -1,
	}
	_, err := ExecuteSQLQuery(`SELECT id FROM CACHE('events') TABLESAMPLE BERNOULLI (50)`, resolver)
	if err == nil || !strings.Contains(err.Error(), "invalid input row count") {
		t.Fatalf("invalid sampled cardinality error = %v", err)
	}
}
