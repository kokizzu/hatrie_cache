//go:build mu12 && !mu12baseline

package hatSql

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuildExplainArrangementPlanIsDeterministicAndIndependent(t *testing.T) {
	estimatedRows := 12
	steps := []ExplainStep{
		{Node: "SCAN", Detail: "orders", EstimatedRows: &estimatedRows},
		{Node: "AGGREGATE", Detail: "region"},
	}
	annotations := []ExplainArrangementAnnotation{
		{
			StepIndex:      1,
			Key:            "orders_by_region",
			Action:         TypedTableArrangementAdvisorReuse,
			References:     2,
			Shared:         true,
			EstimatedBytes: 4096,
		},
		{
			StepIndex:      0,
			Key:            "orders_scan",
			Action:         TypedTableArrangementAdvisorHydrateThenReuse,
			Stale:          true,
			EstimatedBytes: 8192,
		},
	}

	got, err := BuildExplainArrangementPlan(steps, annotations)
	if err != nil {
		t.Fatalf("BuildExplainArrangementPlan() error = %v", err)
	}
	reordered, err := BuildExplainArrangementPlan(steps, []ExplainArrangementAnnotation{annotations[1], annotations[0]})
	if err != nil {
		t.Fatalf("reordered BuildExplainArrangementPlan() error = %v", err)
	}
	if !reflect.DeepEqual(got, reordered) {
		t.Fatalf("annotation order changed plan:\nfirst=%#v\nsecond=%#v", got, reordered)
	}
	if got.Format != "hatrie-cache-explain-arrangements/v1" || len(got.Steps) != 2 {
		t.Fatalf("plan header/steps = %#v", got)
	}
	if got.Steps[1].Arrangement == nil || got.Steps[1].Arrangement.Key != "orders_by_region" || got.Steps[1].Arrangement.Action != TypedTableArrangementAdvisorReuse {
		t.Fatalf("aggregate arrangement metadata = %#v", got.Steps[1].Arrangement)
	}
	if got.Steps[0].Arrangement == nil || !got.Steps[0].Arrangement.Stale || got.Steps[0].Arrangement.EstimatedBytes != 8192 {
		t.Fatalf("scan arrangement metadata = %#v", got.Steps[0].Arrangement)
	}
	got.Steps[0].Step.Node = "mutated"
	*got.Steps[0].Step.EstimatedRows = 99
	if steps[0].Node != "SCAN" || *steps[0].EstimatedRows != 12 {
		t.Fatal("arrangement plan shares mutable step state with input")
	}
}

func TestBuildExplainArrangementPlanRejectsInvalidAnnotations(t *testing.T) {
	tests := []ExplainArrangementAnnotation{
		{StepIndex: -1, Key: "key", Action: TypedTableArrangementAdvisorCreate},
		{StepIndex: 2, Key: "key", Action: TypedTableArrangementAdvisorCreate},
		{StepIndex: 0, Key: "", Action: TypedTableArrangementAdvisorCreate},
		{StepIndex: 0, Key: "key", Action: ""},
		{StepIndex: 0, Key: "key", Action: TypedTableArrangementAdvisorCreate, References: -1},
	}
	for index, annotation := range tests {
		if _, err := BuildExplainArrangementPlan([]ExplainStep{{Node: "SCAN"}}, []ExplainArrangementAnnotation{annotation}); err == nil {
			t.Errorf("case %d returned nil error for %#v", index, annotation)
		}
	}
	if _, err := BuildExplainArrangementPlan(
		[]ExplainStep{{Node: "SCAN"}},
		[]ExplainArrangementAnnotation{
			{StepIndex: 0, Key: "first", Action: TypedTableArrangementAdvisorReuse},
			{StepIndex: 0, Key: "second", Action: TypedTableArrangementAdvisorCreate},
		},
	); err == nil {
		t.Fatal("duplicate step index returned nil error")
	}
}

func TestExplainArrangementJSONAndDOTContainMetadata(t *testing.T) {
	steps := []ExplainStep{{Node: "SCAN", Detail: "orders"}}
	annotations := []ExplainArrangementAnnotation{{
		StepIndex:      0,
		Key:            "orders_primary",
		Action:         TypedTableArrangementAdvisorCreate,
		EstimatedBytes: 256,
	}}
	encoded, err := MarshalExplainArrangementJSON(steps, annotations)
	if err != nil {
		t.Fatalf("MarshalExplainArrangementJSON() error = %v", err)
	}
	text := string(encoded)
	for _, want := range []string{`"format":"hatrie-cache-explain-arrangements/v1"`, `"key":"orders_primary"`, `"action":"create"`, `"estimated_bytes":256`} {
		if !strings.Contains(text, want) {
			t.Fatalf("JSON %q missing %q", text, want)
		}
	}
	plan, err := BuildExplainArrangementPlan(steps, annotations)
	if err != nil {
		t.Fatal(err)
	}
	dot := ExplainArrangementDOT(plan)
	if !strings.Contains(dot, "orders_primary") || !strings.Contains(dot, "create") {
		t.Fatalf("DOT %q missing arrangement metadata", dot)
	}
}
