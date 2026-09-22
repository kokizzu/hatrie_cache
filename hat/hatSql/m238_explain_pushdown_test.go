package hatSql

import (
	"strings"
	"testing"
)

var m238ExplainBenchmarkSink QueryResult

type m238ExplainResolver struct{}

func (m238ExplainResolver) ResolveSQLSource(name, key string) ([]Row, error) {
	if name == "CACHE" && key == "orders" {
		return []Row{{"id": int64(1), "customer_id": int64(7), "region": "apac"}}, nil
	}
	return []Row{{"id": int64(7)}}, nil
}

func (m238ExplainResolver) ResolveSQLArrangementMetadata(name, key string) ([]SQLArrangementMetadata, error) {
	if name != "CACHE" || key != "orders" {
		return nil, nil
	}
	return []SQLArrangementMetadata{
		{
			Key:         "orders_region",
			Kind:        "hash-index",
			Reused:      true,
			Fields:      []string{"region"},
			MemoryBytes: 1024,
		},
	}, nil
}

func TestM238ExplainReportsFilterPushdownAndArrangementReuse(t *testing.T) {
	result, err := ExecuteSQLQuery("EXPLAIN FROM CACHE('orders') AS o JOIN CACHE('customers') AS c ON o.customer_id = c.id WHERE o.region = 'apac' SELECT o.id", m238ExplainResolver{})
	if err != nil {
		t.Fatalf("EXPLAIN error = %v", err)
	}

	var scan, filter *ExplainStep
	for index := range result.Plan {
		step := &result.Plan[index]
		switch step.Node {
		case "SCAN":
			scan = step
		case "FILTER":
			filter = step
		}
	}
	if scan == nil || len(scan.Arrangements) != 1 || !scan.Arrangements[0].Reused {
		t.Fatalf("scan arrangement metadata = %#v, want one reused arrangement", scan)
	}
	if filter == nil {
		t.Fatalf("EXPLAIN plan = %#v, want FILTER step", result.Plan)
	}

	findNotice := func(step *ExplainStep, code string) (ExplainNotice, bool) {
		for _, notice := range step.Notices {
			if notice.Code == code {
				return notice, true
			}
		}
		return ExplainNotice{}, false
	}
	filterNotice, ok := findNotice(filter, "FILTER_PUSHDOWN")
	if !ok || !strings.Contains(filterNotice.Detail, "before joins") {
		t.Fatalf("filter notices = %#v, want FILTER_PUSHDOWN before joins", filter.Notices)
	}
}

func BenchmarkM238ExplainQuery(b *testing.B) {
	query := "EXPLAIN FROM CACHE('orders') AS o JOIN CACHE('customers') AS c ON o.customer_id = c.id WHERE o.region = 'apac' SELECT o.id"
	resolver := m238ExplainResolver{}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		result, err := ExecuteSQLQuery(query, resolver)
		if err != nil {
			b.Fatal(err)
		}
		m238ExplainBenchmarkSink = result
	}
}
