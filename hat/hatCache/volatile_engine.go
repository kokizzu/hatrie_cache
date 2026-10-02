package hatCache

import "errors"

// ErrVolatilePersistence reports an operation that requires durable backing
// storage when called on a volatile trie.
var ErrVolatilePersistence = errors.New("hatriecache: volatile trie does not support persistence")

// CreateVolatileHatTrie creates a memory-only trie. Unlike CreateHatTrie, it
// does not create a temporary disk directory for large raw byte values.
// Volatile tries are opt-in and cannot be used with persistence APIs.
func CreateVolatileHatTrie() *HatTrie {
	return createHatTrieWithStorage(newVolatileStorageValue())
}

// IsVolatile reports whether the trie keeps raw byte values in memory and has
// no internal disk-backed value store.
func (ht *HatTrie) IsVolatile() bool {
	return ht != nil && ht.disks != nil && ht.disks.volatile
}

func newVolatileStorageValue() DiskStorage {
	return DiskStorage{volatile: true}
}

func requirePersistentTrie(trie *HatTrie) error {
	if trie == nil {
		return ErrNilHatTrie
	}
	if trie.IsVolatile() {
		return ErrVolatilePersistence
	}
	return nil
}
