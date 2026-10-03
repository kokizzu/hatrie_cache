package hatSchema

import (
	"errors"
	"reflect"
	"testing"
)

func TestSchemaRegistryRollingPolicyAndCloneIsolation(t *testing.T) {
	initial := schemaCompatibilityFixture(1)
	registry, err := NewSchemaRegistry(SchemaRegistryOptions{
		Policy:                SchemaRegistryPolicyRolling,
		MaxVersionsPerSubject: 2,
	})
	if err != nil {
		t.Fatalf("NewSchemaRegistry() error = %v", err)
	}
	first, err := registry.Register("orders", initial)
	if err != nil {
		t.Fatalf("Register(initial) error = %v", err)
	}
	if !first.Compatible || first.PreviousVersion != 0 || first.NextVersion != 1 {
		t.Fatalf("initial report = %+v", first)
	}

	next := initial.Clone()
	next.Version = 2
	source := next.Sources["users"]
	source.Columns = append(source.Columns, Column{Name: "email", Type: TypeText})
	next.Sources["users"] = source
	second, err := registry.Register("orders", next)
	if err != nil || !second.Compatible {
		t.Fatalf("Register(compatible) report = %+v error = %v", second, err)
	}

	latest, ok := registry.Current("orders")
	if !ok || latest.Version != 2 {
		t.Fatalf("Current() = %+v, %v", latest, ok)
	}
	latest.Sources["users"].Columns[0].Name = "mutated"
	latestAgain, _ := registry.Current("orders")
	if latestAgain.Sources["users"].Columns[0].Name == "mutated" {
		t.Fatal("Current() exposed mutable registry state")
	}

	history := registry.History("orders")
	if len(history) != 2 || history[0].Version != 1 || history[1].Version != 2 {
		t.Fatalf("History() = %+v", history)
	}
	history[0].Sources["users"].Columns[0].Name = "mutated"
	historyAgain := registry.History("orders")
	if historyAgain[0].Sources["users"].Columns[0].Name == "mutated" {
		t.Fatal("History() exposed mutable registry state")
	}
}

func TestSchemaRegistryStrictDefaultRejectsUnsafeAndRollingAccepts(t *testing.T) {
	initial := schemaCompatibilityFixture(1)
	next := initial.Clone()
	next.Version = 2
	source := next.Sources["users"]
	source.Columns = append(source.Columns, Column{Name: "email", Type: TypeText})
	next.Sources["users"] = source

	strict, err := NewSchemaRegistry(SchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("NewSchemaRegistry(strict) error = %v", err)
	}
	if _, err := strict.Register("orders", initial); err != nil {
		t.Fatalf("strict initial Register() error = %v", err)
	}
	report, err := strict.Register("orders", next)
	if !errors.Is(err, ErrSchemaRegistryIncompatible) || report.Compatible {
		t.Fatalf("strict Register() report = %+v error = %v", report, err)
	}

	rolling, err := NewSchemaRegistry(SchemaRegistryOptions{Policy: SchemaRegistryPolicyRolling})
	if err != nil {
		t.Fatalf("NewSchemaRegistry(rolling) error = %v", err)
	}
	if _, err := rolling.Register("orders", initial); err != nil {
		t.Fatalf("rolling initial Register() error = %v", err)
	}
	if _, err := rolling.Register("orders", next); err != nil {
		t.Fatalf("rolling Register() error = %v", err)
	}
}

func TestSchemaRegistryRegistrationIsIdempotentAndHistoryBounded(t *testing.T) {
	registry, err := NewSchemaRegistry(SchemaRegistryOptions{
		Policy:                SchemaRegistryPolicyRolling,
		MaxVersionsPerSubject: 2,
	})
	if err != nil {
		t.Fatalf("NewSchemaRegistry() error = %v", err)
	}
	first := schemaCompatibilityFixture(1)
	if _, err := registry.Register("orders", first); err != nil {
		t.Fatalf("Register(1) error = %v", err)
	}
	duplicate, err := registry.Register("orders", first.Clone())
	if err != nil || len(duplicate.Changes) != 0 || !duplicate.Compatible {
		t.Fatalf("Register(duplicate) report = %+v error = %v", duplicate, err)
	}
	second := first.Clone()
	second.Version = 2
	source := second.Sources["users"]
	source.Columns = append(source.Columns, Column{Name: "email", Type: TypeText})
	second.Sources["users"] = source
	if _, err := registry.Register("orders", second); err != nil {
		t.Fatalf("Register(2) error = %v", err)
	}
	third := second.Clone()
	third.Version = 3
	source = third.Sources["users"]
	source.Columns = append(source.Columns, Column{Name: "phone", Type: TypeText})
	third.Sources["users"] = source
	if _, err := registry.Register("orders", third); err != nil {
		t.Fatalf("Register(3) error = %v", err)
	}
	history := registry.History("orders")
	if got := []uint64{history[0].Version, history[1].Version}; !reflect.DeepEqual(got, []uint64{2, 3}) {
		t.Fatalf("bounded History() versions = %v", got)
	}
}

func TestSchemaRegistryCheckDoesNotPublishAndCapacityIsBounded(t *testing.T) {
	registry, err := NewSchemaRegistry(SchemaRegistryOptions{
		MaxSubjects:           1,
		MaxVersionsPerSubject: 1,
		Policy:                SchemaRegistryPolicyRolling,
	})
	if err != nil {
		t.Fatalf("NewSchemaRegistry() error = %v", err)
	}
	initial := schemaCompatibilityFixture(1)
	if _, err := registry.Register("orders", initial); err != nil {
		t.Fatalf("Register(initial) error = %v", err)
	}
	candidate := initial.Clone()
	candidate.Version = 2
	report, err := registry.Check("orders", candidate)
	if err != nil || !report.Compatible {
		t.Fatalf("Check(candidate) report = %+v error = %v", report, err)
	}
	current, ok := registry.Current("orders")
	if !ok || current.Version != 1 {
		t.Fatalf("Check() published version %d, want 1", current.Version)
	}
	if _, err := registry.Register("payments", initial); !errors.Is(err, ErrSchemaRegistryCapacity) {
		t.Fatalf("Register(second subject) error = %v, want capacity", err)
	}
}

func TestSchemaRegistryRejectsInvalidOptions(t *testing.T) {
	for _, options := range []SchemaRegistryOptions{
		{MaxSubjects: -1},
		{MaxVersionsPerSubject: -1},
		{Policy: SchemaRegistryPolicy(99)},
	} {
		if _, err := NewSchemaRegistry(options); !errors.Is(err, ErrSchemaRegistryOptionsInvalid) {
			t.Fatalf("NewSchemaRegistry(%+v) error = %v", options, err)
		}
	}
}
