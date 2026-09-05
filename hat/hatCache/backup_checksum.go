package hatCache

import (
	"crypto/sha256"
	"encoding/hex"
)

func backupStateChecksum(trie *HatTrie) (string, error) {
	if trie == nil {
		return "", ErrNilHatTrie
	}
	hasher := sha256.New()
	if err := trie.writeSnapshot(hasher, 0, SnapshotFormatBinary); err != nil {
		return "", err
	}
	return hex.EncodeToString(hasher.Sum(nil)), nil
}
