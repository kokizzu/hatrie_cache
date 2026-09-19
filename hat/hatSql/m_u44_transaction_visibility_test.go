//go:build mu44

package hatSql

import (
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestSQLDataflowVisibilityCoordinatorPublishesCommonVersion(t *testing.T) {
	coordinator, err := NewSQLDataflowVisibilityCoordinator(SQLDataflowVisibilityOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Register(" orders "); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Register("customers"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Register("orders"); err != nil {
		t.Fatal(err)
	}

	token, err := coordinator.Publish([]string{" customers ", "orders"})
	if err != nil {
		t.Fatal(err)
	}
	wantToken := SQLDataflowVisibilityToken{
		Version:   1,
		Dataflows: []string{"customers", "orders"},
	}
	if !reflect.DeepEqual(token, wantToken) {
		t.Fatalf("publish token = %#v, want %#v", token, wantToken)
	}

	acquired, err := coordinator.Acquire([]string{"orders", "customers"})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(acquired, wantToken) {
		t.Fatalf("acquired token = %#v, want %#v", acquired, wantToken)
	}
	if err := coordinator.Check(token); err != nil {
		t.Fatalf("check current token: %v", err)
	}

	if _, err := coordinator.Publish([]string{"orders"}); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Acquire([]string{"orders", "customers"}); !errors.Is(err, ErrSQLDataflowVisibilityNotReady) {
		t.Fatalf("mixed acquire error = %v, want %v", err, ErrSQLDataflowVisibilityNotReady)
	}
	if err := coordinator.Check(token); !errors.Is(err, ErrSQLDataflowVisibilityStale) {
		t.Fatalf("stale token error = %v, want %v", err, ErrSQLDataflowVisibilityStale)
	}

	if _, err := coordinator.Publish([]string{"customers", "orders"}); err != nil {
		t.Fatal(err)
	}
	current, err := coordinator.Acquire([]string{"orders", "customers"})
	if err != nil {
		t.Fatal(err)
	}
	if current.Version != 3 {
		t.Fatalf("current version = %d, want 3", current.Version)
	}
	if err := coordinator.Check(current); err != nil {
		t.Fatalf("check republished token: %v", err)
	}
}

func TestSQLDataflowVisibilityCoordinatorRestoreIsAtomic(t *testing.T) {
	coordinator, err := NewSQLDataflowVisibilityCoordinator(SQLDataflowVisibilityOptions{MaxDataflows: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Register("orders"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Register("customers"); err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.Publish([]string{"orders", "customers"}); err != nil {
		t.Fatal(err)
	}

	snapshot := coordinator.Snapshot()
	restored, err := NewSQLDataflowVisibilityCoordinator(SQLDataflowVisibilityOptions{MaxDataflows: 4})
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Restore(snapshot); err != nil {
		t.Fatal(err)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("restored snapshot = %#v, want %#v", got, snapshot)
	}

	bad := snapshot
	bad.Dataflows = append(bad.Dataflows, SQLDataflowVisibilityEntry{Name: " orders ", Version: 1})
	if err := restored.Restore(bad); !errors.Is(err, ErrSQLDataflowVisibilityDuplicate) {
		t.Fatalf("duplicate restore error = %v, want %v", err, ErrSQLDataflowVisibilityDuplicate)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("state changed after invalid restore = %#v, want %#v", got, snapshot)
	}

	bad = snapshot
	bad.Dataflows = []SQLDataflowVisibilityEntry{{Name: "orders", Version: 2}}
	bad.NextVersion = 1
	if err := restored.Restore(bad); !errors.Is(err, ErrSQLDataflowVisibilityInvalid) {
		t.Fatalf("invalid version restore error = %v, want %v", err, ErrSQLDataflowVisibilityInvalid)
	}
	if got := restored.Snapshot(); !reflect.DeepEqual(got, snapshot) {
		t.Fatalf("state changed after invalid version restore = %#v, want %#v", got, snapshot)
	}
}

func TestSQLDataflowVisibilityCoordinatorCapacityAndNil(t *testing.T) {
	coordinator, err := NewSQLDataflowVisibilityCoordinator(SQLDataflowVisibilityOptions{MaxDataflows: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Register("one"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Register("two"); err != nil {
		t.Fatal(err)
	}
	if err := coordinator.Register("three"); !errors.Is(err, ErrSQLDataflowVisibilityCapacity) {
		t.Fatalf("capacity error = %v, want %v", err, ErrSQLDataflowVisibilityCapacity)
	}
	if _, err := coordinator.Publish([]string{"one", "one"}); !errors.Is(err, ErrSQLDataflowVisibilityDuplicate) {
		t.Fatalf("duplicate publish error = %v, want %v", err, ErrSQLDataflowVisibilityDuplicate)
	}
	if _, err := coordinator.Publish([]string{"one", "missing"}); !errors.Is(err, ErrSQLDataflowVisibilityUnknown) {
		t.Fatalf("unknown publish error = %v, want %v", err, ErrSQLDataflowVisibilityUnknown)
	}
	if _, err := coordinator.Acquire([]string{"one", "missing"}); !errors.Is(err, ErrSQLDataflowVisibilityUnknown) {
		t.Fatalf("unknown acquire error = %v, want %v", err, ErrSQLDataflowVisibilityUnknown)
	}

	var nilCoordinator *SQLDataflowVisibilityCoordinator
	if err := nilCoordinator.Register("one"); !errors.Is(err, ErrSQLDataflowVisibilityNil) {
		t.Fatalf("nil register error = %v, want %v", err, ErrSQLDataflowVisibilityNil)
	}
	if _, err := nilCoordinator.Publish([]string{"one"}); !errors.Is(err, ErrSQLDataflowVisibilityNil) {
		t.Fatalf("nil publish error = %v, want %v", err, ErrSQLDataflowVisibilityNil)
	}
	if _, err := nilCoordinator.Acquire([]string{"one"}); !errors.Is(err, ErrSQLDataflowVisibilityNil) {
		t.Fatalf("nil acquire error = %v, want %v", err, ErrSQLDataflowVisibilityNil)
	}
	if err := nilCoordinator.Check(SQLDataflowVisibilityToken{}); !errors.Is(err, ErrSQLDataflowVisibilityNil) {
		t.Fatalf("nil check error = %v, want %v", err, ErrSQLDataflowVisibilityNil)
	}
}

func TestSQLDataflowVisibilityCoordinatorConcurrentPublish(t *testing.T) {
	coordinator, err := NewSQLDataflowVisibilityCoordinator(SQLDataflowVisibilityOptions{})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orders", "customers", "inventory"} {
		if err := coordinator.Register(name); err != nil {
			t.Fatal(err)
		}
	}

	var group sync.WaitGroup
	for index := 0; index < 8; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for iteration := 0; iteration < 25; iteration++ {
				if _, err := coordinator.Publish([]string{"orders", "customers", "inventory"}); err != nil {
					t.Errorf("concurrent publish: %v", err)
					return
				}
			}
		}()
	}
	group.Wait()

	token, err := coordinator.Acquire([]string{"orders", "customers", "inventory"})
	if err != nil {
		t.Fatalf("acquire after concurrent publish: %v", err)
	}
	if token.Version != 200 {
		t.Fatalf("version after concurrent publish = %d, want 200", token.Version)
	}
}
