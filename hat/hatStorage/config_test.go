package hatStorage_test

import (
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestStorageConfigParsersNormalizeAliases(t *testing.T) {
	backend, err := hatStorage.ParseBackend(" PEBBLE ")
	if err != nil || backend != hatStorage.BackendPebble {
		t.Fatalf("ParseBackend() = %q, %v", backend, err)
	}
	format, err := hatStorage.ParseFormat(" bin ")
	if err != nil || format != hatStorage.FormatBinary {
		t.Fatalf("ParseFormat() = %q, %v", format, err)
	}
	if hatStorage.DefaultBackend != hatStorage.BackendPebble || hatStorage.DefaultFormat != hatStorage.FormatBinary {
		t.Fatalf("unexpected defaults: %q/%q", hatStorage.DefaultBackend, hatStorage.DefaultFormat)
	}
}

func TestStorageConfigParsersRejectUnknownValues(t *testing.T) {
	if _, err := hatStorage.ParseBackend("memory"); err == nil {
		t.Fatal("ParseBackend() error = nil, want rejection")
	}
	if _, err := hatStorage.ParseFormat("msgpack"); err == nil {
		t.Fatal("ParseFormat() error = nil, want rejection")
	}
}
