package hatCache

import "sort"

// PinStorageKey keeps a key's materialized value resident when explicit cold
// spilling is enabled. Pinning is process-local and is not part of snapshots.
// A missing key can be pinned before it is written.
func (ht *HatTrie) PinStorageKey(key string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if err := validateKey(key); err != nil {
		return err
	}
	if child := ht.localPartitionForKey(key); child != nil {
		return child.PinStorageKey(key)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.ensureOpen()
	if ht.storagePinnedKeys == nil {
		ht.storagePinnedKeys = make(map[string]struct{})
	}
	ht.storagePinnedKeys[key] = struct{}{}

	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		return nil
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	if !hval.IsLevelDBReference() {
		return nil
	}
	_, err := ht.hydrateLevelDBReferenceLocked(key, hval)
	return err
}

// UnpinStorageKey allows a key to become eligible for the next cold spill.
// Unpinning a missing key is a no-op.
func (ht *HatTrie) UnpinStorageKey(key string) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if err := validateKey(key); err != nil {
		return err
	}
	if child := ht.localPartitionForKey(key); child != nil {
		return child.UnpinStorageKey(key)
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	ht.ensureOpen()
	if ht.storagePinnedKeys == nil {
		return nil
	}
	delete(ht.storagePinnedKeys, key)
	if len(ht.storagePinnedKeys) == 0 {
		ht.storagePinnedKeys = nil
	}
	return nil
}

// IsStorageKeyPinned reports whether the process-local pin policy contains a
// key. It returns false for missing keys and for keys that were never pinned.
func (ht *HatTrie) IsStorageKeyPinned(key string) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if err := validateKey(key); err != nil {
		return false, err
	}
	if child := ht.localPartitionForKey(key); child != nil {
		return child.IsStorageKeyPinned(key)
	}

	ht.mu.RLock()
	defer ht.mu.RUnlock()
	ht.ensureOpen()
	_, pinned := ht.storagePinnedKeys[key]
	return pinned, nil
}

// PinnedStorageKeys returns a detached, sorted list of process-local pins.
// The returned slice can be modified by the caller.
func (ht *HatTrie) PinnedStorageKeys() []string {
	if ht == nil {
		return nil
	}
	if set := ht.localPartitionSet(); set != nil {
		var keys []string
		for _, child := range set.tries {
			keys = append(keys, child.PinnedStorageKeys()...)
		}
		if len(keys) == 0 {
			return nil
		}
		sort.Strings(keys)
		return keys
	}

	ht.mu.RLock()
	defer ht.mu.RUnlock()
	ht.ensureOpen()
	if len(ht.storagePinnedKeys) == 0 {
		return nil
	}
	keys := make([]string, 0, len(ht.storagePinnedKeys))
	for key := range ht.storagePinnedKeys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
