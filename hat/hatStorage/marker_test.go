package hatStorage

import "testing"

func TestBackendMarkerResolvesAndRejectsMismatch(t *testing.T) {
	path := t.TempDir() + "/store"
	if err := WriteBackendMarker(path, BackendPebble); err != nil {
		t.Fatalf("WriteBackendMarker() error = %v", err)
	}
	backend, err := ResolveBackend(path, BackendAuto)
	if err != nil || backend != BackendPebble {
		t.Fatalf("ResolveBackend(auto) = %q/%v, want pebble/nil", backend, err)
	}
	if _, err := ResolveBackend(path, BackendLevelDB); err == nil {
		t.Fatal("ResolveBackend(leveldb) error = nil, want marker mismatch")
	}
}
