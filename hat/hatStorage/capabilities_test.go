package hatStorage_test

import (
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestInspectBuildsValidatedPortableStoreReport(t *testing.T) {
	report, err := hatStorage.Inspect(testStore{})
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if report.Backend != hatStorage.BackendPebble || report.Format != hatStorage.FormatBinary || report.Path != "/var/lib/hatrie" {
		t.Fatalf("Inspect() = %#v", report)
	}
	if !report.Supports(hatStorage.CapabilityCheckpoint) || !report.Supports(hatStorage.CapabilityCompaction) {
		t.Fatalf("capabilities = %#v, want checkpoint and compaction", report.Capabilities)
	}
	if report.Properties.Stats != "healthy" {
		t.Fatalf("properties = %#v", report.Properties)
	}
}

func TestInspectRejectsInvalidStoreMetadata(t *testing.T) {
	_, err := hatStorage.Inspect(invalidStore{})
	if err == nil {
		t.Fatal("Inspect() error = nil, want invalid backend")
	}
}

type testStore struct{}

func (testStore) Backend() hatStorage.Backend { return hatStorage.BackendPebble }
func (testStore) Path() string                { return "/var/lib/hatrie" }
func (testStore) Format() hatStorage.Format   { return hatStorage.FormatBinary }
func (testStore) Properties() (hatStorage.Properties, error) {
	return hatStorage.Properties{Stats: "healthy"}, nil
}

type invalidStore struct{ testStore }

func (invalidStore) Backend() hatStorage.Backend { return "unknown" }
