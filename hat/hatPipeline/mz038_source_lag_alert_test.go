package hatPipeline_test

import (
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatPipeline"
)

func TestMZ038SourceLagAlertHysteresisAndRestore(t *testing.T) {
	registry, err := hatPipeline.NewSourceLagAlertRegistry(hatPipeline.SourceLagAlertRegistryOptions{
		Policy: hatPipeline.SourceLagAlertPolicy{
			WarningLag:  100,
			CriticalLag: 1000,
			RecoveryLag: 50,
		},
		MaxSources: 4,
	})
	if err != nil {
		t.Fatalf("NewSourceLagAlertRegistry() error = %v", err)
	}
	cases := []struct {
		lag        uint64
		wantState  hatPipeline.SourceLagAlertState
		wantChange bool
	}{
		{lag: 0, wantState: hatPipeline.SourceLagAlertHealthy},
		{lag: 100, wantState: hatPipeline.SourceLagAlertWarning, wantChange: true},
		{lag: 75, wantState: hatPipeline.SourceLagAlertWarning},
		{lag: 1000, wantState: hatPipeline.SourceLagAlertCritical, wantChange: true},
		{lag: 200, wantState: hatPipeline.SourceLagAlertWarning, wantChange: true},
		{lag: 50, wantState: hatPipeline.SourceLagAlertHealthy, wantChange: true},
	}
	for index, testCase := range cases {
		transition, observeErr := registry.Observe("orders", testCase.lag)
		if observeErr != nil {
			t.Fatalf("Observe(%d) error = %v", index, observeErr)
		}
		if transition.Current != testCase.wantState || transition.Changed != testCase.wantChange {
			t.Fatalf("Observe(%d) = %#v, want state=%q changed=%t", index, transition, testCase.wantState, testCase.wantChange)
		}
	}
	snapshot := registry.SnapshotState()
	restored, err := hatPipeline.NewSourceLagAlertRegistry(hatPipeline.SourceLagAlertRegistryOptions{
		Policy:     snapshot.Policy,
		MaxSources: 4,
	})
	if err != nil {
		t.Fatalf("NewSourceLagAlertRegistry(restored) error = %v", err)
	}
	if err := restored.Restore(snapshot); err != nil {
		t.Fatalf("Restore() error = %v", err)
	}
	if got, want := restored.Snapshot(), registry.Snapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored Snapshot() = %#v, want %#v", got, want)
	}
}

func TestMZ038SourceLagAlertBoundsAndAtomicRestore(t *testing.T) {
	registry, err := hatPipeline.NewSourceLagAlertRegistry(hatPipeline.SourceLagAlertRegistryOptions{MaxSources: 1})
	if err != nil {
		t.Fatalf("NewSourceLagAlertRegistry() error = %v", err)
	}
	if _, err := registry.Observe(" ", 1); !errors.Is(err, hatPipeline.ErrSourceLagAlertSourceRequired) {
		t.Fatalf("blank source error = %v, want ErrSourceLagAlertSourceRequired", err)
	}
	if _, err := registry.Observe("orders", 1); err != nil {
		t.Fatalf("Observe(orders) error = %v", err)
	}
	if _, err := registry.Observe("users", 1); !errors.Is(err, hatPipeline.ErrSourceLagAlertSourceLimit) {
		t.Fatalf("source limit error = %v, want ErrSourceLagAlertSourceLimit", err)
	}
	before := registry.Snapshot()
	bad := hatPipeline.SourceLagAlertRegistrySnapshot{
		Policy: registry.SnapshotState().Policy,
		Alerts: []hatPipeline.SourceLagAlert{{Source: "orders", State: hatPipeline.SourceLagAlertCritical, Lag: 999}},
	}
	if err := registry.Restore(bad); err == nil {
		t.Fatal("Restore() accepted an invalid state")
	}
	if got := registry.Snapshot(); !reflect.DeepEqual(got, before) {
		t.Fatalf("failed Restore() changed state = %#v, want %#v", got, before)
	}
}
