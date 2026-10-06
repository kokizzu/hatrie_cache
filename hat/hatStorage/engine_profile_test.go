package hatStorage_test

import (
	"testing"

	"hatrie_cache/hat/hatStorage"
)

func TestEngineProfilesDescribeSelectableLSMBackends(t *testing.T) {
	profiles := hatStorage.EngineProfiles()
	if len(profiles) != 2 {
		t.Fatalf("EngineProfiles() returned %d profiles, want 2", len(profiles))
	}
	seen := make(map[hatStorage.Backend]hatStorage.EngineProfile, len(profiles))
	for _, profile := range profiles {
		if profile.Family != hatStorage.EngineFamilyLSM {
			t.Errorf("profile %q family = %q, want %q", profile.Name, profile.Family, hatStorage.EngineFamilyLSM)
		}
		if profile.Name == "" || profile.ReadAmplification == "" || profile.WriteAmplification == "" || profile.Compaction == "" || profile.Backup == "" || profile.Recovery == "" {
			t.Errorf("profile %q has incomplete tradeoff metadata: %#v", profile.Name, profile)
		}
		seen[profile.Backend] = profile
	}
	if !seen[hatStorage.BackendPebble].Default {
		t.Fatal("Pebble profile must be the default")
	}
	if seen[hatStorage.BackendLevelDB].Default {
		t.Fatal("LevelDB compatibility profile must not be the default")
	}
	if !seen[hatStorage.BackendPebble].Checkpoint {
		t.Fatal("Pebble profile must advertise native checkpoint support")
	}
	if seen[hatStorage.BackendLevelDB].Checkpoint {
		t.Fatal("LevelDB profile must not advertise native Pebble checkpoint support")
	}
}

func TestProfileForBackendRejectsAuto(t *testing.T) {
	if _, err := hatStorage.ProfileForBackend(hatStorage.BackendAuto); err == nil {
		t.Fatal("ProfileForBackend(auto) returned nil error")
	}
	if _, err := hatStorage.ProfileForBackend(hatStorage.Backend("unknown")); err == nil {
		t.Fatal("ProfileForBackend(unknown) returned nil error")
	}
}
