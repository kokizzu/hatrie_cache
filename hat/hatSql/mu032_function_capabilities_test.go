package hatSql_test

import (
	"testing"

	"hatrie_cache/hat/hatCache"
	"hatrie_cache/hat/hatSql"
)

func TestMU032FunctionCapabilitiesAreCatalogedAndPersisted(t *testing.T) {
	path := t.TempDir() + "/functions.json"
	registry := hatCache.NewSQLFunctionRegistryWithOptions(hatCache.SQLFunctionRegistryOptions{
		PersistencePath: path,
	})
	definition := hatCache.SQLFunctionDefinition{
		Name:          "score_boost",
		Arguments:     []string{"score"},
		ArgumentTypes: []string{"INTEGER"},
		Language:      "GO",
		Source:        "return score + 1",
		Deterministic: true,
		Monotonicity:  hatSql.FunctionMonotonicityNonDecreasing,
		Retractable:   true,
	}
	if err := registry.Register(definition); err != nil {
		t.Fatalf("Register() error = %v", err)
	}

	got, ok := registry.Definition(" SCORE_BOOST ")
	if !ok {
		t.Fatal("Definition() did not find registered function")
	}
	if !got.Deterministic || got.Monotonicity != hatSql.FunctionMonotonicityNonDecreasing || !got.Retractable {
		t.Fatalf("Definition() capabilities = %#v, want deterministic/non-decreasing/retractable", got)
	}

	definitions := registry.Definitions()
	if len(definitions) != 1 || definitions[0].Name != "score_boost" {
		t.Fatalf("Definitions() = %#v, want one normalized definition", definitions)
	}
	definitions[0].Arguments[0] = "changed"
	unchanged, ok := registry.Definition("score_boost")
	if !ok || unchanged.Arguments[0] != "score" {
		t.Fatalf("Definition() returned mutable catalog data: %#v", unchanged)
	}
	registry.Close()

	restored, err := hatCache.OpenSQLFunctionRegistry(hatCache.SQLFunctionRegistryOptions{
		PersistencePath: path,
	})
	if err != nil {
		t.Fatalf("OpenSQLFunctionRegistry() error = %v", err)
	}
	defer restored.Close()
	loaded, ok := restored.Definition("score_boost")
	if !ok || !loaded.Deterministic || loaded.Monotonicity != hatSql.FunctionMonotonicityNonDecreasing || !loaded.Retractable {
		t.Fatalf("persisted capabilities = %#v, want original metadata", loaded)
	}
}

func TestMU032FunctionCapabilitiesRejectUnsafeDeclarations(t *testing.T) {
	registry := hatCache.NewSQLFunctionRegistry()
	base := hatCache.SQLFunctionDefinition{
		Name:          "score_boost",
		Arguments:     []string{"score"},
		ArgumentTypes: []string{"INTEGER"},
		Language:      "GO",
		Source:        "return score + 1",
	}

	base.Monotonicity = hatSql.FunctionMonotonicityNonDecreasing
	if err := registry.Register(base); err == nil {
		t.Fatal("Register() accepted monotonic function without deterministic declaration")
	}

	base.Monotonicity = hatSql.FunctionMonotonicityUnknown
	base.Retractable = true
	if err := registry.Register(base); err == nil {
		t.Fatal("Register() accepted retractable function without deterministic declaration")
	}

	base.Deterministic = true
	base.Monotonicity = hatSql.FunctionMonotonicity("not-a-monotonicity")
	if err := registry.Register(base); err == nil {
		t.Fatal("Register() accepted an unknown monotonicity value")
	}

	base.Monotonicity = hatSql.FunctionMonotonicity(" NON-DECREASING ")
	if err := registry.Register(base); err != nil {
		t.Fatalf("Register() rejected safe metadata: %v", err)
	}
	definition, ok := registry.Definition("score_boost")
	if !ok || definition.Monotonicity != hatSql.FunctionMonotonicityNonDecreasing {
		t.Fatalf("normalized monotonicity = %#v, want %q", definition.Monotonicity, hatSql.FunctionMonotonicityNonDecreasing)
	}
}

func TestMU032LegacyFunctionDefaultsRemainUnknown(t *testing.T) {
	registry := hatCache.NewSQLFunctionRegistry()
	if err := registry.Register(hatCache.SQLFunctionDefinition{
		Name:          "legacy_score",
		Arguments:     []string{"score"},
		ArgumentTypes: []string{"INTEGER"},
		Language:      "GO",
		Source:        "return score + 1",
	}); err != nil {
		t.Fatal(err)
	}
	definition, ok := registry.Definition("legacy_score")
	if !ok {
		t.Fatal("Definition() did not find legacy function")
	}
	if definition.Deterministic || definition.Monotonicity != hatSql.FunctionMonotonicityUnknown || definition.Retractable {
		t.Fatalf("legacy defaults = %#v, want conservative unknown values", definition)
	}
}
