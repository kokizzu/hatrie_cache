package hatCache

import (
	"fmt"

	"hatrie_cache/hat/hatStorage"
)

// OpenPersistentStoreWithProfile opens the explicitly selected engine profile
// while preserving the existing durable backend marker and format checks.
// Passing a profile from hatStorage.EngineProfiles makes the intended LSM
// read/write tradeoff visible at the call site.
func OpenPersistentStoreWithProfile(path string, profile hatStorage.EngineProfile, format StorageFormat) (PersistentStore, error) {
	expected, err := hatStorage.ProfileForBackend(profile.Backend)
	if err != nil {
		return nil, err
	}
	if profile != expected {
		return nil, fmt.Errorf("hatriecache: storage engine profile %q does not match backend %q", profile.Name, profile.Backend)
	}
	return OpenPersistentStoreWithFormat(path, StorageBackend(profile.Backend), format)
}
