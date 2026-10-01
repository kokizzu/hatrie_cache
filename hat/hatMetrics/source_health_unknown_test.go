package hatMetrics

import (
	"strings"
	"testing"
)

func TestSourceHealthRegistryUnknownDoesNotIncreaseFailureStreak(t *testing.T) {
	registry := NewSourceHealthRegistry(1)
	if err := registry.Record("orders", SourceHealthFailed, 7, "timeout"); err != nil {
		t.Fatal(err)
	}
	if err := registry.Record("orders", SourceHealthUnknown, 8, "status unavailable"); err != nil {
		t.Fatal(err)
	}
	rows := registry.Snapshot(8)
	if len(rows) != 1 {
		t.Fatalf("snapshot length = %d, want 1", len(rows))
	}
	if rows[0].Status != SourceHealthUnknown || rows[0].ConsecutiveFailures != 1 || rows[0].LastError != "status unavailable" {
		t.Fatalf("unknown health row = %#v", rows[0])
	}
}

func TestSourceHealthRegistryCapsErrorText(t *testing.T) {
	registry := NewSourceHealthRegistry(1)
	if err := registry.Record("orders", SourceHealthFailed, 1, strings.Repeat("x", 2048)); err != nil {
		t.Fatal(err)
	}
	rows := registry.Snapshot(1)
	if len(rows) != 1 || len(rows[0].LastError) != 1024 {
		t.Fatalf("bounded error length = %d, want 1024", len(rows[0].LastError))
	}
}
