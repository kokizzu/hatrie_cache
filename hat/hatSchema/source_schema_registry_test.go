package hatSchema

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestSourceSchemaRegistryActivatesValidatesAndRollsBack(t *testing.T) {
	validatorCalls := 0
	registry, err := NewSourceSchemaRegistry(SourceSchemaRegistryOptions{
		CompatibilityMode: SourceSchemaCompatibilityBackward,
		ValidateCompatibility: func(previous, candidate SourceSchemaVersion, mode SourceSchemaCompatibilityMode) error {
			validatorCalls++
			if mode != SourceSchemaCompatibilityBackward || previous.Version+1 != candidate.Version {
				return fmt.Errorf("unexpected compatibility request")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewSourceSchemaRegistry() error = %v", err)
	}

	first := SourceSchemaVersion{Source: "orders", Version: 1, Fingerprint: "fp-1"}
	assertSourceSchemaActivation(t, registry, first)
	assertSourceSchemaActivation(t, registry, first)
	second := SourceSchemaVersion{Source: "orders", Version: 2, Fingerprint: "fp-2"}
	assertSourceSchemaActivation(t, registry, second)
	if validatorCalls != 1 {
		t.Fatalf("validator calls = %d, want 1", validatorCalls)
	}

	if got, err := registry.Current("orders"); err != nil || got != second {
		t.Fatalf("Current() = %#v, %v, want %#v", got, err, second)
	}
	if _, err := registry.Validate(SourceSchemaVersion{Source: "orders", Version: 1, Fingerprint: "fp-1"}); !errors.Is(err, ErrSourceSchemaVersionStale) {
		t.Fatalf("Validate(stale) error = %v, want ErrSourceSchemaVersionStale", err)
	}

	rolledBack, err := registry.Rollback("orders", 1)
	assertSourceSchemaAccepted(t, rolledBack, err)
	if got, err := registry.Current("orders"); err != nil || got != first {
		t.Fatalf("Current() after rollback = %#v, %v, want %#v", got, err, first)
	}
}

func TestSourceSchemaRegistryRejectsUnsafeCandidateWithoutLeakingValidatorError(t *testing.T) {
	registry, err := NewSourceSchemaRegistry(SourceSchemaRegistryOptions{
		CompatibilityMode: SourceSchemaCompatibilityFull,
		ValidateCompatibility: func(SourceSchemaVersion, SourceSchemaVersion, SourceSchemaCompatibilityMode) error {
			return errors.New("secret raw schema payload")
		},
	})
	if err != nil {
		t.Fatalf("NewSourceSchemaRegistry() error = %v", err)
	}
	assertSourceSchemaActivation(t, registry, SourceSchemaVersion{Source: "users", Version: 1, Fingerprint: "fp-1"})

	result, err := registry.Validate(SourceSchemaVersion{Source: "users", Version: 2, Fingerprint: "fp-2"})
	if !errors.Is(err, ErrSourceSchemaIncompatible) {
		t.Fatalf("Validate() error = %v, want ErrSourceSchemaIncompatible", err)
	}
	if result.Accepted || result.Reason != SourceSchemaRejectedIncompatible {
		t.Fatalf("Validate() result = %#v, want rejected incompatible result", result)
	}
	if strings.Contains(err.Error(), "secret raw schema payload") {
		t.Fatalf("Validate() leaked validator error: %v", err)
	}
}

func TestSourceSchemaRegistryBoundsHistoryAndCopiesSnapshot(t *testing.T) {
	registry, err := NewSourceSchemaRegistry(SourceSchemaRegistryOptions{
		MaxSources:           1,
		MaxVersionsPerSource: 2,
		CompatibilityMode:    SourceSchemaCompatibilityBackward,
		ValidateCompatibility: func(SourceSchemaVersion, SourceSchemaVersion, SourceSchemaCompatibilityMode) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewSourceSchemaRegistry() error = %v", err)
	}
	for version := uint64(1); version <= 3; version++ {
		assertSourceSchemaActivation(t, registry, SourceSchemaVersion{
			Source:      "events",
			Version:     version,
			Fingerprint: fmt.Sprintf("fp-%d", version),
		})
	}
	if _, err := registry.Rollback("events", 1); !errors.Is(err, ErrSourceSchemaVersionNotFound) {
		t.Fatalf("Rollback(evicted) error = %v, want ErrSourceSchemaVersionNotFound", err)
	}

	snapshot := registry.Snapshot()
	if len(snapshot.Sources) != 1 || len(snapshot.Sources[0].Versions) != 2 {
		t.Fatalf("Snapshot() = %#v, want one source with two versions", snapshot)
	}
	snapshot.Sources[0].Versions[0].Fingerprint = "mutated"
	again := registry.Snapshot()
	if again.Sources[0].Versions[0].Fingerprint == "mutated" {
		t.Fatal("Snapshot() returned mutable registry state")
	}

	if _, err := registry.Activate(SourceSchemaVersion{Source: "other", Version: 1, Fingerprint: "fp"}); !errors.Is(err, ErrSourceSchemaRegistryCapacity) {
		t.Fatalf("Activate(second source) error = %v, want ErrSourceSchemaRegistryCapacity", err)
	}
}

func TestSourceSchemaRegistryDefaultsAndInvalidInputs(t *testing.T) {
	registry, err := NewSourceSchemaRegistry(SourceSchemaRegistryOptions{})
	if err != nil {
		t.Fatalf("NewSourceSchemaRegistry(defaults) error = %v", err)
	}
	if got := registry.Options(); got.MaxSources != DefaultSourceSchemaRegistryMaxSources || got.MaxVersionsPerSource != DefaultSourceSchemaRegistryMaxVersionsPerSource {
		t.Fatalf("Options() = %#v, want defaults", got)
	}
	for _, candidate := range []SourceSchemaVersion{
		{Source: "", Version: 1, Fingerprint: "fp"},
		{Source: "users", Version: 0, Fingerprint: "fp"},
		{Source: "users", Version: 1, Fingerprint: ""},
	} {
		if _, err := registry.Validate(candidate); !errors.Is(err, ErrSourceSchemaInvalid) {
			t.Fatalf("Validate(%#v) error = %v, want ErrSourceSchemaInvalid", candidate, err)
		}
	}
}

func TestSourceSchemaRegistryRejectsConflictsAndForwardRollback(t *testing.T) {
	registry, err := NewSourceSchemaRegistry(SourceSchemaRegistryOptions{
		CompatibilityMode: SourceSchemaCompatibilityBackward,
		ValidateCompatibility: func(SourceSchemaVersion, SourceSchemaVersion, SourceSchemaCompatibilityMode) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewSourceSchemaRegistry() error = %v", err)
	}
	first := SourceSchemaVersion{Source: "orders", Version: 1, Fingerprint: "fp-1"}
	second := SourceSchemaVersion{Source: "orders", Version: 2, Fingerprint: "fp-2"}
	third := SourceSchemaVersion{Source: "orders", Version: 3, Fingerprint: "fp-3"}
	assertSourceSchemaActivation(t, registry, first)
	assertSourceSchemaActivation(t, registry, second)
	assertSourceSchemaActivation(t, registry, third)
	if _, err := registry.Rollback("orders", 2); err != nil {
		t.Fatalf("Rollback(previous) error = %v", err)
	}

	if _, err := registry.Activate(SourceSchemaVersion{Source: "orders", Version: 1, Fingerprint: "different"}); !errors.Is(err, ErrSourceSchemaVersionConflict) {
		t.Fatalf("Activate(version conflict) error = %v, want ErrSourceSchemaVersionConflict", err)
	}
	if _, err := registry.Rollback("orders", 3); !errors.Is(err, ErrSourceSchemaRollbackUnavailable) {
		t.Fatalf("Rollback(forward) error = %v, want ErrSourceSchemaRollbackUnavailable", err)
	}
}

func TestSourceSchemaRegistryConcurrentAdmission(t *testing.T) {
	registry, err := NewSourceSchemaRegistry(SourceSchemaRegistryOptions{
		CompatibilityMode: SourceSchemaCompatibilityBackward,
		ValidateCompatibility: func(SourceSchemaVersion, SourceSchemaVersion, SourceSchemaCompatibilityMode) error {
			return nil
		},
	})
	if err != nil {
		t.Fatalf("NewSourceSchemaRegistry() error = %v", err)
	}
	assertSourceSchemaActivation(t, registry, SourceSchemaVersion{Source: "orders", Version: 1, Fingerprint: "fp-1"})

	candidate := SourceSchemaVersion{Source: "orders", Version: 2, Fingerprint: "fp-2"}
	var group sync.WaitGroup
	for index := 0; index < 16; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := registry.Validate(candidate); err != nil {
				t.Errorf("Validate() error = %v", err)
			}
		}()
	}
	group.Wait()
	assertSourceSchemaActivation(t, registry, candidate)
}

func BenchmarkSourceSchemaDirectMetadataCheck(b *testing.B) {
	current := SourceSchemaVersion{Source: "orders", Version: 2, Fingerprint: "fp-2"}
	candidate := current
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if candidate.Source != current.Source || candidate.Version != current.Version || candidate.Fingerprint != current.Fingerprint {
			b.Fatal("metadata changed")
		}
	}
}

func BenchmarkSourceSchemaRegistryValidate(b *testing.B) {
	registry, err := NewSourceSchemaRegistry(SourceSchemaRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	candidate := SourceSchemaVersion{Source: "orders", Version: 2, Fingerprint: "fp-2"}
	if _, err := registry.Activate(candidate); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := registry.Validate(candidate)
		if err != nil || !result.Accepted {
			b.Fatalf("Validate() = %#v, %v", result, err)
		}
	}
}

func BenchmarkSourceSchemaRegistryActivateSame(b *testing.B) {
	registry, err := NewSourceSchemaRegistry(SourceSchemaRegistryOptions{})
	if err != nil {
		b.Fatal(err)
	}
	candidate := SourceSchemaVersion{Source: "orders", Version: 2, Fingerprint: "fp-2"}
	if _, err := registry.Activate(candidate); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		result, err := registry.Activate(candidate)
		if err != nil || !result.Accepted {
			b.Fatalf("Activate() = %#v, %v", result, err)
		}
	}
}

func BenchmarkSourceSchemaRegistryNew(b *testing.B) {
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		registry, err := NewSourceSchemaRegistry(SourceSchemaRegistryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		_ = registry
	}
}

func BenchmarkSourceSchemaRegistryEagerMapBaseline(b *testing.B) {
	options := SourceSchemaRegistryOptions{
		MaxSources:           DefaultSourceSchemaRegistryMaxSources,
		MaxVersionsPerSource: DefaultSourceSchemaRegistryMaxVersionsPerSource,
	}
	b.ReportAllocs()
	for index := 0; index < b.N; index++ {
		registry := &SourceSchemaRegistry{
			options: options,
			sources: make(map[string]*sourceSchemaHistory, options.MaxSources),
		}
		_ = registry
	}
}

func assertSourceSchemaAccepted(t *testing.T, result SourceSchemaValidationResult, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("registry operation error = %v", err)
	}
	if !result.Accepted || result.Reason != SourceSchemaAccepted {
		t.Fatalf("registry operation result = %#v, want accepted", result)
	}
}

func assertSourceSchemaActivation(t *testing.T, registry *SourceSchemaRegistry, candidate SourceSchemaVersion) {
	t.Helper()
	result, err := registry.Activate(candidate)
	assertSourceSchemaAccepted(t, result, err)
}
