package hatSql

import (
	"errors"
	"sync"
	"testing"
)

func TestSQLIndexRetirementWaitsForReadersBeforeRemoval(t *testing.T) {
	registry := NewSQLIndexRetirementRegistry()
	index := &IncrementalPointLookup{}
	if err := registry.Register("accounts_by_region", index); err != nil {
		t.Fatal(err)
	}
	first, err := registry.Acquire("accounts_by_region")
	if err != nil {
		t.Fatal(err)
	}
	second, err := registry.Acquire("accounts_by_region")
	if err != nil {
		t.Fatal(err)
	}
	if first.Index() != index || second.Index() != index {
		t.Fatal("leases did not retain the registered index")
	}

	retiring, err := registry.Retire("accounts_by_region")
	if err != nil {
		t.Fatal(err)
	}
	if retiring.State != SQLIndexRetirementDraining || retiring.Readers != 2 || retiring.Removed {
		t.Fatalf("retiring result = %#v", retiring)
	}
	if _, err := registry.Acquire("accounts_by_region"); !errors.Is(err, ErrSQLIndexRetirementDraining) {
		t.Fatalf("Acquire() error = %v, want draining", err)
	}
	status, ok := registry.Status("accounts_by_region")
	if !ok || status.State != SQLIndexRetirementDraining || status.Readers != 2 {
		t.Fatalf("status = %#v/%t", status, ok)
	}
	if result, released := first.Release(); !released || result.Removed || result.Readers != 1 {
		t.Fatalf("first release = %#v/%t", result, released)
	}
	if got := second.Index(); got != index {
		t.Fatal("remaining reader lost its index before release")
	}
	result, released := second.Release()
	if !released || !result.Removed || result.State != SQLIndexRetirementRemoved || result.Readers != 0 || result.Index != index {
		t.Fatalf("final release = %#v/%t", result, released)
	}
	if _, ok := registry.Status("accounts_by_region"); ok {
		t.Fatal("retired index remained registered")
	}
	if second.Index() != nil {
		t.Fatal("released lease retained index")
	}
	if _, released := second.Release(); released {
		t.Fatal("second Release() was not idempotent")
	}
}

func TestSQLIndexRetirementCanRemoveIdleIndexAndReRegister(t *testing.T) {
	registry := NewSQLIndexRetirementRegistry()
	firstIndex := &struct{ ID int }{ID: 1}
	secondIndex := &struct{ ID int }{ID: 2}
	if err := registry.Register("idx", firstIndex); err != nil {
		t.Fatal(err)
	}
	removed, err := registry.Retire("idx")
	if err != nil {
		t.Fatal(err)
	}
	if !removed.Removed || removed.State != SQLIndexRetirementRemoved || removed.Index != firstIndex {
		t.Fatalf("idle removal = %#v", removed)
	}
	if err := registry.Register("idx", secondIndex); err != nil {
		t.Fatal(err)
	}
	lease, err := registry.Acquire("idx")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Index() != secondIndex {
		t.Fatal("re-registered index was not acquired")
	}
	lease.Release()
}

func TestSQLIndexRetirementValidationAndConcurrentReaders(t *testing.T) {
	registry := NewSQLIndexRetirementRegistry()
	if err := registry.Register("", struct{}{}); !errors.Is(err, ErrSQLIndexRetirementNameRequired) {
		t.Fatalf("empty name error = %v", err)
	}
	if err := registry.Register("nil", nil); !errors.Is(err, ErrSQLIndexRetirementIndexRequired) {
		t.Fatalf("nil index error = %v", err)
	}
	if err := registry.Register("idx", struct{}{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register("idx", struct{}{}); !errors.Is(err, ErrSQLIndexRetirementDuplicate) {
		t.Fatalf("duplicate error = %v", err)
	}
	if _, err := registry.Acquire("missing"); !errors.Is(err, ErrSQLIndexRetirementNotFound) {
		t.Fatalf("missing acquire error = %v", err)
	}

	const readers = 32
	var group sync.WaitGroup
	group.Add(readers)
	for range readers {
		go func() {
			defer group.Done()
			lease, acquireErr := registry.Acquire("idx")
			if acquireErr != nil {
				return
			}
			_ = lease.Index()
			lease.Release()
		}()
	}
	group.Wait()
	if status, ok := registry.Status("idx"); !ok || status.Readers != 0 || status.State != SQLIndexRetirementActive {
		t.Fatalf("active status = %#v/%t", status, ok)
	}
	if _, err := registry.Retire("missing"); !errors.Is(err, ErrSQLIndexRetirementNotFound) {
		t.Fatalf("missing retire error = %v", err)
	}
	if _, err := registry.Retire("idx"); err != nil {
		t.Fatal(err)
	}
}

func TestSQLIndexRetirementRejectsTypedNilAndSortsStatuses(t *testing.T) {
	registry := NewSQLIndexRetirementRegistry()
	var typedNil *struct{}
	if err := registry.Register("typed-nil", typedNil); !errors.Is(err, ErrSQLIndexRetirementIndexRequired) {
		t.Fatalf("Register(typed nil) error = %v", err)
	}
	if err := registry.Register(" z ", &struct{}{}); err != nil {
		t.Fatalf("Register(z) error = %v", err)
	}
	if err := registry.Register("a", &struct{}{}); err != nil {
		t.Fatalf("Register(a) error = %v", err)
	}
	statuses := registry.Statuses()
	if len(statuses) != 2 || statuses[0].Name != "a" || statuses[1].Name != "z" {
		t.Fatalf("Statuses() = %#v, want sorted names", statuses)
	}

	var nilRegistry *SQLIndexRetirementRegistry
	if err := nilRegistry.Register("name", &struct{}{}); !errors.Is(err, ErrSQLIndexRetirementRegistryNil) {
		t.Fatalf("nil Register() error = %v", err)
	}
	if _, err := nilRegistry.Acquire("name"); !errors.Is(err, ErrSQLIndexRetirementRegistryNil) {
		t.Fatalf("nil Acquire() error = %v", err)
	}
	if _, err := nilRegistry.Retire("name"); !errors.Is(err, ErrSQLIndexRetirementRegistryNil) {
		t.Fatalf("nil Retire() error = %v", err)
	}
	if _, ok := nilRegistry.Status("name"); ok {
		t.Fatal("nil Status() reported an entry")
	}
	if statuses := nilRegistry.Statuses(); statuses != nil {
		t.Fatalf("nil Statuses() = %#v, want nil", statuses)
	}

	var nilLease *SQLIndexReaderLease
	if nilLease.Index() != nil {
		t.Fatal("nil lease Index() was not nil")
	}
	if _, released := nilLease.Release(); released {
		t.Fatal("nil lease Release() was accepted")
	}
}
