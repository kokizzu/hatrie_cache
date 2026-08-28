package hatSql

import (
	"context"
	"strings"
	"testing"
)

func TestSQLIndexHintForbidsOnlyRequestedField(t *testing.T) {
	var event SQLQueryEvent
	_, err := ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name`, planGuardResolver{}, nil, SQLQueryOptions{
		IndexHint: SQLIndexHint{Field: "id", Mode: SQLIndexHintForbid},
		Observer:  SQLQueryObserverFunc(func(value SQLQueryEvent) { event = value }),
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	for _, operator := range event.Operators {
		if operator.Node == "INDEX SCAN" || operator.Node == "FORCED INDEX SCAN" {
			t.Fatalf("forbidden index was used: %#v", event.Operators)
		}
	}
}

func TestSQLIndexHintForcesRequestedField(t *testing.T) {
	var event SQLQueryEvent
	_, err := ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name`, planGuardResolver{}, nil, SQLQueryOptions{
		IndexHint: SQLIndexHint{Field: "id", Mode: SQLIndexHintForce},
		Observer:  SQLQueryObserverFunc(func(value SQLQueryEvent) { event = value }),
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	for _, operator := range event.Operators {
		if operator.Node == "FORCED INDEX SCAN" {
			return
		}
	}
	t.Fatalf("forced index was not used: %#v", event.Operators)
}

func TestSQLIndexHintFailsWhenForcedFieldIsUnavailable(t *testing.T) {
	var event SQLQueryEvent
	_, err := ExecuteSQLQueryParameters(context.Background(), `FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name`, noIndexHintResolver{}, nil, SQLQueryOptions{
		IndexHint: SQLIndexHint{Field: "id", Mode: SQLIndexHintForce},
		Observer:  SQLQueryObserverFunc(func(value SQLQueryEvent) { event = value }),
	})
	if err == nil || !strings.Contains(err.Error(), "forced index") {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v, event = %#v, want forced-index diagnostic", err, event)
	}
}

func TestSQLIndexHintPageFailsWhenForcedFieldIsUnavailable(t *testing.T) {
	_, err := ExecuteSQLQueryPage(context.Background(), `FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name`, noIndexHintResolver{}, nil, SQLQueryOptions{
		IndexHint: SQLIndexHint{Field: "id", Mode: SQLIndexHintForce},
	}, 10, "")
	if err == nil || !strings.Contains(err.Error(), "forced index") {
		t.Fatalf("ExecuteSQLQueryPage() error = %v, want forced-index diagnostic", err)
	}
}

func TestSQLIndexHintRowsRejectsUnsupportedStreamingOverride(t *testing.T) {
	err := ExecuteSQLQueryRows(context.Background(), `FROM CACHE('people') AS p WHERE p.id = 1 SELECT p.name`, planGuardResolver{}, nil, SQLQueryOptions{
		IndexHint: SQLIndexHint{Field: "id", Mode: SQLIndexHintForce},
	}, func([]string, SQLRow) error { return nil })
	if err == nil || !strings.Contains(err.Error(), "streamed SQL rows") {
		t.Fatalf("ExecuteSQLQueryRows() error = %v, want streaming-hint diagnostic", err)
	}
}

type noIndexHintResolver struct{}

func (noIndexHintResolver) ResolveSQLSource(_, _ string) ([]SQLRow, error) {
	return []SQLRow{{"id": int64(1), "name": "Ada"}}, nil
}

func (noIndexHintResolver) ResolveSQLIndexedSource(_, _, _ string, _ interface{}) ([]SQLRow, bool, error) {
	return nil, false, nil
}
