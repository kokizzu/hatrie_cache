package hatCache

// CompareAndSwapString replaces an existing string only when it still equals
// expected. The comparison and replacement happen under one trie lock, so a
// competing writer cannot observe an intermediate state. Expiration is kept.
func (ht *HatTrie) CompareAndSwapString(key, expected, replacement string) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.CompareAndSwapString(key, expected, replacement)
	}
	if err := validateKey(key); err != nil {
		return false, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil || hval.Empty() || !hval.IsStringAtRaws() {
		return false, err
	}
	if ht.strings.Get(hval.Index) != expected {
		return false, nil
	}
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		return false, nil
	}
	ht.strings.replaceActive(hval.Index, replacement)
	*rawPtr = hval.toValue()
	ht.recordWriteLocked(key)
	return true, nil
}
