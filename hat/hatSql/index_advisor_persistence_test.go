package hatSql

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestSQLIndexAdvisorSnapshotRoundTrip(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	resolver := SourceResolverFunc(func(name, _ string) ([]Row, error) {
		return []Row{{"id": int64(1), "amount": int64(10), "name": name}}, nil
	})
	queries := []string{
		"FROM CACHE('people') AS person WHERE person.id >= 1 SELECT person.name",
		"FROM CACHE('people') AS person WHERE person.id >= 2 SELECT person.name",
		"FROM CACHE('orders') AS order WHERE order.amount >= 1 SELECT order.id",
	}
	for _, query := range queries {
		if _, err := ExecuteQueryParameters(context.Background(), query, resolver, nil, QueryOptions{
			SlowQueryThreshold: time.Nanosecond,
			IndexAdvisor:       advisor,
		}); err != nil {
			t.Fatal(err)
		}
	}

	var encoded bytes.Buffer
	if err := advisor.Save(&encoded); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(encoded.Bytes()) {
		t.Fatalf("Save() produced invalid JSON: %q", encoded.String())
	}

	restored := NewSQLIndexAdvisor(8)
	if err := restored.Load(bytes.NewReader(encoded.Bytes())); err != nil {
		t.Fatal(err)
	}
	if got, want := restored.Recommendations(), advisor.Recommendations(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored Recommendations() = %#v, want %#v", got, want)
	}
}

func TestSQLIndexAdvisorLoadRejectsInvalidSnapshotsWithoutMutation(t *testing.T) {
	advisor := NewSQLIndexAdvisor(8)
	resolver := SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"id": int64(1)}}, nil
	})
	if _, err := ExecuteQueryParameters(context.Background(), "FROM CACHE('people') AS person WHERE person.id >= 1 SELECT person.id", resolver, nil, QueryOptions{
		SlowQueryThreshold: time.Nanosecond,
		IndexAdvisor:       advisor,
	}); err != nil {
		t.Fatal(err)
	}
	before := advisor.Recommendations()

	cases := []struct {
		name    string
		payload string
	}{
		{name: "version", payload: `{"version":2,"entries":[]}`},
		{name: "duplicate", payload: `{"version":1,"entries":[{"key":"people","field":"id","slow_queries":1},{"key":"people","field":"id","slow_queries":2}]}`},
		{name: "unknown field", payload: `{"version":1,"entries":[],"extra":true}`},
		{name: "trailing data", payload: `{"version":1,"entries":[]} {"version":1,"entries":[]}`},
		{name: "oversized field", payload: `{"version":1,"entries":[{"key":"people","field":"` + strings.Repeat("x", 1025) + `","slow_queries":1}]}`},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if err := advisor.Load(strings.NewReader(testCase.payload)); err == nil {
				t.Fatal("Load() error = nil")
			}
			if got := advisor.Recommendations(); !reflect.DeepEqual(got, before) {
				t.Fatalf("Load() mutated advisor: got %#v, want %#v", got, before)
			}
		})
	}
}

func TestSQLIndexAdvisorLoadRejectsSnapshotsLargerThanCapacity(t *testing.T) {
	payload := `{"version":1,"entries":[{"key":"orders","field":"amount","slow_queries":1},{"key":"people","field":"id","slow_queries":1}]}`
	if err := NewSQLIndexAdvisor(1).Load(strings.NewReader(payload)); err == nil {
		t.Fatal("Load() error = nil")
	}
}

func TestSQLIndexAdvisorSnapshotBoundsAndEmptyCapacity(t *testing.T) {
	advisor := NewSQLIndexAdvisor(1)
	if err := advisor.Save(nil); err == nil {
		t.Fatal("Save(nil) error = nil")
	}
	if err := advisor.Load(nil); err == nil {
		t.Fatal("Load(nil) error = nil")
	}
	if err := advisor.Load(strings.NewReader(strings.Repeat("x", DefaultSQLIndexAdvisorSnapshotMaxBytes+1))); err == nil {
		t.Fatal("Load() oversized error = nil")
	}
	if err := NewSQLIndexAdvisor(0).Load(strings.NewReader(`{"version":1,"entries":[]}`)); err != nil {
		t.Fatalf("Load() empty snapshot with zero capacity: %v", err)
	}
}
