package hatCache

import (
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestOpenPersistentStoreWithProfileSelectsAndReportsEngine(t *testing.T) {
	for _, backend := range []StorageBackend{StorageBackendPebble, StorageBackendLevelDB} {
		t.Run(string(backend), func(t *testing.T) {
			profile, err := hatStorage.ProfileForBackend(hatStorage.Backend(backend))
			if err != nil {
				t.Fatalf("ProfileForBackend(%q): %v", backend, err)
			}
			store, err := OpenPersistentStoreWithProfile(t.TempDir()+"/store", profile, StorageFormatBinary)
			if err != nil {
				t.Fatalf("OpenPersistentStoreWithProfile(%q): %v", backend, err)
			}
			defer store.Close()
			if store.Backend() != backend {
				t.Fatalf("Backend() = %q, want %q", store.Backend(), backend)
			}
			report, err := hatStorage.Inspect(store)
			if err != nil {
				t.Fatalf("Inspect(): %v", err)
			}
			if report.Profile.Name != profile.Name || report.Profile.Backend != profile.Backend {
				t.Fatalf("Inspect().Profile = %#v, want %#v", report.Profile, profile)
			}
		})
	}
}

func TestOpenPersistentStoreWithProfileRejectsMismatchedProfile(t *testing.T) {
	profile, err := hatStorage.ProfileForBackend(hatStorage.BackendPebble)
	if err != nil {
		t.Fatal(err)
	}
	profile.Backend = hatStorage.BackendLevelDB
	if _, err := OpenPersistentStoreWithProfile(t.TempDir()+"/store", profile, StorageFormatBinary); err == nil {
		t.Fatal("OpenPersistentStoreWithProfile accepted a mismatched profile")
	}
}
