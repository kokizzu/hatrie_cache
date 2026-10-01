package hatCache

import "errors"

const maintenanceReadOnlyMessage = "node is in maintenance read-only mode"

// ErrMaintenanceReadOnly is returned by mutation admission paths while the
// trie is serving as a read-only replica. Internal replication commands remain
// allowed so a replica can catch up while public writes are fenced.
var ErrMaintenanceReadOnly = errors.New(maintenanceReadOnlyMessage)

// SetMaintenanceReadOnly changes the node-wide public mutation gate. The zero
// value is writable, preserving the existing default for embedded callers.
func (ht *HatTrie) SetMaintenanceReadOnly(readOnly bool) {
	if ht == nil {
		return
	}
	ht.maintenanceReadOnly.Store(readOnly)
}

// MaintenanceReadOnly reports whether public mutation admission is fenced.
func (ht *HatTrie) MaintenanceReadOnly() bool {
	return ht != nil && ht.maintenanceReadOnly.Load()
}

func (ht *HatTrie) checkMaintenanceReadOnlyCommand(request CacheCommandRequest) error {
	if ht == nil || !ht.maintenanceReadOnly.Load() {
		return nil
	}
	if isInternalReplicationCommand(request) || !commandShouldJournal(request) {
		return nil
	}
	return ErrMaintenanceReadOnly
}
