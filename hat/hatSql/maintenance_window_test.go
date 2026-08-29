package hatSql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestMaintenanceWindowsGateCompactionRebuildAndBackup(t *testing.T) {
	now := time.Date(2026, 8, 29, 1, 0, 0, 0, time.UTC)
	coordinator, err := hatSql.NewMaintenanceCoordinator([]hatSql.MaintenanceWindow{{Start: now.Add(time.Hour), End: now.Add(2 * time.Hour)}}, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	runs := 0
	for _, registration := range []struct {
		name string
		add  func(string, func(context.Context) error) error
	}{
		{"compact", coordinator.AddCompaction},
		{"rebuild", coordinator.AddIndexRebuild},
		{"backup", coordinator.AddBackup},
	} {
		if err := registration.add(registration.name, func(context.Context) error { runs++; return nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := coordinator.Run(context.Background(), "compact"); !errors.Is(err, hatSql.ErrOutsideMaintenanceWindow) || runs != 0 {
		t.Fatalf("outside Run() = %v, runs=%d", err, runs)
	}
	now = now.Add(90 * time.Minute)
	for _, name := range []string{"compact", "rebuild", "backup"} {
		if err := coordinator.Run(context.Background(), name); err != nil {
			t.Fatalf("inside Run(%q) = %v", name, err)
		}
	}
	if runs != 3 {
		t.Fatalf("runs = %d, want 3", runs)
	}
}
