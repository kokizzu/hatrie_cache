package hatCache

import "errors"

// ErrReplicaReadOnly is returned when a local data mutation is attempted on a
// trie that has been fenced into replica read-only mode.
var ErrReplicaReadOnly = errors.New("hatriecache: replica is read-only")

// SetReplicaReadOnly fences or unfences data-plane mutations on this trie.
// The setting is disabled by default and is propagated to configured local
// partitions. Reads, snapshots, backups, and trusted internal replication
// apply paths remain available while the fence is enabled.
func (ht *HatTrie) SetReplicaReadOnly(readOnly bool) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	ht.replicaReadOnly.Store(readOnly)
	if partitions := ht.localPartitionSet(); partitions != nil {
		for _, partition := range partitions.tries {
			if partition != nil {
				partition.replicaReadOnly.Store(readOnly)
			}
		}
	}
	return nil
}

// ReplicaReadOnly reports whether this trie currently rejects data-plane
// mutations.
func (ht *HatTrie) ReplicaReadOnly() bool {
	return ht != nil && ht.replicaReadOnly.Load()
}

func (ht *HatTrie) checkReplicaWritable() error {
	if ht == nil {
		return ErrNilHatTrie
	}
	if ht.replicaReadOnly.Load() && ht.replicaReadOnlyBypass.Load() == 0 {
		return ErrReplicaReadOnly
	}
	return nil
}

func (ht *HatTrie) checkReplicaCommand(request CacheCommandRequest) error {
	if ht == nil || !ht.replicaReadOnly.Load() || ht.replicaReadOnlyBypass.Load() != 0 {
		return nil
	}
	if isInternalReplicationCommand(request) || commandShouldJournal(request) {
		return ErrReplicaReadOnly
	}
	return nil
}

func (ht *HatTrie) withReplicaReadOnlyBypass(fn func() CacheCommandResponse) CacheCommandResponse {
	if ht == nil {
		return commandError(ErrNilHatTrie.Error())
	}
	tries := []*HatTrie{ht}
	if partitions := ht.localPartitionSet(); partitions != nil {
		tries = append(tries, partitions.tries...)
	}
	for _, trie := range tries {
		if trie != nil {
			trie.replicaReadOnlyBypass.Add(1)
		}
	}
	defer func() {
		for _, trie := range tries {
			if trie != nil {
				trie.replicaReadOnlyBypass.Add(^uint32(0))
			}
		}
	}()
	return fn()
}
