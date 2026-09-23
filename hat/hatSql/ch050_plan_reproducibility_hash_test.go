package hatSql_test

import (
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCH050PlanReproducibilityHashIsStableAcrossFormattingAndRuntimeMetrics(t *testing.T) {
	estimatedRows := 12
	actualRows := 99
	elapsed := int64(12345)
	baseSteps := []hatSql.ExplainStep{{
		Node:          "SCAN",
		Detail:        "users",
		EstimatedRows: &estimatedRows,
	}}
	input := hatSql.SQLPlanReproducibilityInput{
		Query:               "SELECT id FROM users WHERE id = 1",
		SchemaFingerprint:   "schema-v1",
		SettingsFingerprint: "settings-v1",
		Steps:               baseSteps,
	}
	before := baseSteps[0]
	first, err := hatSql.SQLPlanReproducibilityHash(input)
	if err != nil {
		t.Fatalf("SQLPlanReproducibilityHash() error = %v", err)
	}

	runtimeSteps := []hatSql.ExplainStep{{
		Node:             "SCAN",
		Detail:           "users",
		EstimatedRows:    &estimatedRows,
		ActualInputRows:  &actualRows,
		ActualOutputRows: &actualRows,
		ElapsedNanos:     &elapsed,
		Worker:           7,
	}}
	second, err := hatSql.SQLPlanReproducibilityHash(hatSql.SQLPlanReproducibilityInput{
		Query:               " select id from users where id = 999 ",
		SchemaFingerprint:   "schema-v1",
		SettingsFingerprint: "settings-v1",
		Steps:               runtimeSteps,
	})
	if err != nil {
		t.Fatalf("SQLPlanReproducibilityHash() runtime variant error = %v", err)
	}
	if first != second {
		t.Fatalf("runtime or literal changes changed reproducibility hash: %q != %q", first, second)
	}
	if len(first) != 64 {
		t.Fatalf("hash length = %d, want SHA-256 hex length 64", len(first))
	}
	if !reflect.DeepEqual(baseSteps[0], before) {
		t.Fatal("hashing mutated the input plan")
	}
}

func TestCH050PlanReproducibilityHashIncludesSchemaSettingsAndPlanShape(t *testing.T) {
	base := hatSql.SQLPlanReproducibilityInput{
		Query:               "SELECT id FROM users",
		SchemaFingerprint:   "schema-v1",
		SettingsFingerprint: "settings-v1",
		Steps:               []hatSql.ExplainStep{{Node: "SCAN", Detail: "users"}},
	}
	first, err := hatSql.SQLPlanReproducibilityHash(base)
	if err != nil {
		t.Fatalf("base hash: %v", err)
	}
	variants := []hatSql.SQLPlanReproducibilityInput{
		{Query: base.Query, SchemaFingerprint: "schema-v2", SettingsFingerprint: base.SettingsFingerprint, Steps: base.Steps},
		{Query: base.Query, SchemaFingerprint: base.SchemaFingerprint, SettingsFingerprint: "settings-v2", Steps: base.Steps},
		{Query: base.Query, SchemaFingerprint: base.SchemaFingerprint, SettingsFingerprint: base.SettingsFingerprint, Steps: []hatSql.ExplainStep{{Node: "FILTER", Detail: "users"}}},
	}
	for index, variant := range variants {
		got, err := hatSql.SQLPlanReproducibilityHash(variant)
		if err != nil {
			t.Fatalf("variant %d hash: %v", index, err)
		}
		if got == first {
			t.Fatalf("variant %d has the base hash %q", index, got)
		}
	}
}

func TestCH050PlanReproducibilityHashValidatesInput(t *testing.T) {
	base := hatSql.SQLPlanReproducibilityInput{
		Query:               "SELECT id FROM users",
		SchemaFingerprint:   "schema-v1",
		SettingsFingerprint: "settings-v1",
	}
	for name, input := range map[string]hatSql.SQLPlanReproducibilityInput{
		"invalid query":     {Query: "SELECT FROM", SchemaFingerprint: base.SchemaFingerprint, SettingsFingerprint: base.SettingsFingerprint},
		"missing schema":    {Query: base.Query, SettingsFingerprint: base.SettingsFingerprint},
		"oversized setting": {Query: base.Query, SchemaFingerprint: base.SchemaFingerprint, SettingsFingerprint: string(make([]byte, hatSql.MaxSQLPlanReproducibilityComponentBytes+1))},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := hatSql.SQLPlanReproducibilityHash(input); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
