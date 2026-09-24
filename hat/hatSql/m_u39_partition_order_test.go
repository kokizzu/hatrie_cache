package hatSql

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func m039PartitionOrderDeclaration() SQLPartitionOrderDeclaration {
	return SQLPartitionOrderDeclaration{
		Source:          " CACHE ",
		Key:             " events ",
		PartitionFields: []string{" region "},
		OrderFields: []SQLPartitionOrderField{
			{Field: " event_time ", Desc: true, NullsLast: true},
			{Field: " id "},
		},
	}
}

func TestM039PartitionOrderRegistryCopiesAndBoundsDeclarations(t *testing.T) {
	registry := NewSQLPartitionOrderRegistry(1)
	declaration := m039PartitionOrderDeclaration()
	if err := registry.Register(declaration); err != nil {
		t.Fatal(err)
	}
	declaration.PartitionFields[0] = "mutated"
	declaration.OrderFields[0].Field = "mutated"

	got, available, err := registry.ResolveSQLPartitionOrder("CACHE", "events")
	if err != nil {
		t.Fatal(err)
	}
	if !available {
		t.Fatal("registered partition order was not available")
	}
	want := SQLPartitionOrderDeclaration{
		Source:          "CACHE",
		Key:             "events",
		PartitionFields: []string{"region"},
		OrderFields: []SQLPartitionOrderField{
			{Field: "event_time", Desc: true, NullsLast: true},
			{Field: "id"},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("declaration = %#v, want %#v", got, want)
	}
	if err := registry.Register(SQLPartitionOrderDeclaration{Source: "CACHE", Key: "cold", PartitionFields: []string{"region"}}); !errors.Is(err, ErrSQLPartitionOrderRegistryFull) {
		t.Fatalf("second declaration error = %v, want %v", err, ErrSQLPartitionOrderRegistryFull)
	}
}

func TestM039PartitionOrderDeclarationValidationAndPredicateSupport(t *testing.T) {
	registry := NewSQLPartitionOrderRegistry(0)
	if err := registry.Register(SQLPartitionOrderDeclaration{Source: "events"}); !errors.Is(err, ErrSQLPartitionOrderInvalid) {
		t.Fatalf("invalid declaration error = %v, want %v", err, ErrSQLPartitionOrderInvalid)
	}
	declaration := m039PartitionOrderDeclaration()
	if err := registry.Register(declaration); err != nil {
		t.Fatal(err)
	}
	got, available, err := registry.ResolveSQLPartitionOrder("missing", "source")
	if err != nil {
		t.Fatal(err)
	}
	if available || got.Source != "" {
		t.Fatalf("missing declaration = %#v, available=%v", got, available)
	}
	if !declaration.SupportsPartitionPredicate(SQLPartitionPredicate{Field: "REGION", Operator: "IN", Values: []interface{}{"apac"}}) {
		t.Fatal("partition equality predicate should be eligible")
	}
	if declaration.SupportsPartitionPredicate(SQLPartitionPredicate{Field: "event_time", Operator: "LIKE", Values: []interface{}{"2026%"}}) {
		t.Fatal("non-partition LIKE predicate should not be eligible")
	}
	if got := declaration.String(); !strings.Contains(got, "PARTITION BY region") || !strings.Contains(got, "event_time DESC NULLS LAST") {
		t.Fatalf("declaration string = %q, want partition and order details", got)
	}
}

func TestM039ExplainExposesPartitionOrderAndFallsBack(t *testing.T) {
	query := &sqlQuery{
		from:    &sqlSource{kind: "CACHE", key: "events"},
		selects: []sqlSelectItem{{expr: sqlExpr{kind: "field", name: "region"}}},
	}
	registry := NewSQLPartitionOrderRegistry(0)
	if err := registry.Register(m039PartitionOrderDeclaration()); err != nil {
		t.Fatal(err)
	}
	steps := sqlExplainStepsWithPartitionOrder(query, nil, registry)
	if len(steps) == 0 {
		t.Fatal("EXPLAIN returned no steps")
	}
	if steps[0].PartitionOrder == nil {
		t.Fatalf("scan step = %#v, want partition order metadata", steps[0])
	}
	if !strings.Contains(steps[0].Detail, "PARTITION BY region") {
		t.Fatalf("scan detail = %q, want partition declaration", steps[0].Detail)
	}
	legacy := sqlExplainStepsWithResolver(query, nil)
	if legacy[0].PartitionOrder != nil || strings.Contains(legacy[0].Detail, "PARTITION BY") {
		t.Fatalf("legacy explain unexpectedly exposed partition metadata: %#v", legacy[0])
	}
	result, err := explainSQLQuery("EXPLAIN", query, nil, &sqlExecutionControl{
		options: SQLQueryOptions{PartitionOrderResolver: registry},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Plan) == 0 || result.Plan[0].PartitionOrder == nil {
		t.Fatalf("options-backed EXPLAIN plan = %#v, want partition order metadata", result.Plan)
	}
	pipelineQuery := *query
	pipelineQuery.pipeline = true
	pipeline, err := explainSQLPipelineQueryWithPartitionOrder(&pipelineQuery, nil, registry)
	if err != nil {
		t.Fatal(err)
	}
	if len(pipeline.Plan) == 0 || pipeline.Plan[0].PartitionOrder == nil {
		t.Fatalf("pipeline plan = %#v, want partition order metadata", pipeline.Plan)
	}
}
