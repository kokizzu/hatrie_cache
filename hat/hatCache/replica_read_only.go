package hatCache

import (
	"errors"
	"sync/atomic"
)

// ErrReplicaReadOnly reports a rejected local mutation on a read-only replica.
var ErrReplicaReadOnly = errors.New("hatriecache: replica is read-only")

func (ht *HatTrie) replicaReadOnlyFlag() *atomic.Bool {
	if ht.replicaReadOnlyState != nil {
		return ht.replicaReadOnlyState
	}
	return &ht.replicaReadOnlyFallback
}

// SetReplicaReadOnly enables or disables local mutation rejection. It is off by
// default. Internal replication operations retain a scoped write bypass.
func (ht *HatTrie) SetReplicaReadOnly(readOnly bool) {
	if ht == nil {
		return
	}
	ht.replicaReadOnlyFlag().Store(readOnly)
}

// ReplicaReadOnly reports whether local writes are currently rejected.
func (ht *HatTrie) ReplicaReadOnly() bool {
	if ht == nil {
		return false
	}
	return ht.replicaReadOnlyFlag().Load()
}

func (ht *HatTrie) checkReplicaWrite() error {
	if ht.replicaReadOnlyFlag().Load() {
		return ErrReplicaReadOnly
	}
	return nil
}

func (ht *HatTrie) checkReplicaWriteLocked() error {
	if ht.replicaInternalWrite {
		return nil
	}
	return ht.checkReplicaWrite()
}

func (ht *HatTrie) commandInternalDelete(key string) bool {
	if ht == nil {
		return false
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.commandInternalDelete(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	previous := ht.replicaInternalWrite
	ht.replicaInternalWrite = true
	defer func() { ht.replicaInternalWrite = previous }()

	deleted := ht.deleteLocked(key)
	if deleted {
		ht.recordDeleteLocked(key)
	}
	return deleted
}

func isReplicaInternalCommand(command string) bool {
	switch normalizedCommand(command) {
	case "INTERNALSET", "INTERNALDEL", "INTERNALBATCH", replicationBatchEnvelopeCommand, replicationSetBinaryCommand, replicationSetCompactCommand:
		return true
	default:
		return false
	}
}
