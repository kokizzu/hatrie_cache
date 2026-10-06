package hatCache

import (
	"strings"

	hatReplication "hatrie_cache/hat/hatReplication"
)

type replicaReadOnlyGateRef struct {
	gate *hatReplication.ReplicaReadOnlyGate
}

// SetReplicaReadOnlyGate attaches an optional replica-wide write gate. Passing
// nil disables enforcement. A configured gate is shared with local partitions.
func (ht *HatTrie) SetReplicaReadOnlyGate(gate *hatReplication.ReplicaReadOnlyGate) {
	if ht == nil {
		return
	}
	var ref *replicaReadOnlyGateRef
	if gate != nil {
		ref = &replicaReadOnlyGateRef{gate: gate}
	}
	ht.replicaReadOnlyGate.Store(ref)
	if partitions := ht.localPartitionSet(); partitions != nil {
		for _, child := range partitions.tries {
			child.replicaReadOnlyGate.Store(ref)
		}
	}
}

// ReplicaReadOnlyGate returns the currently attached gate, or nil when the
// default writable behavior is active.
func (ht *HatTrie) ReplicaReadOnlyGate() *hatReplication.ReplicaReadOnlyGate {
	if ht == nil {
		return nil
	}
	ref := ht.replicaReadOnlyGate.Load()
	if ref == nil {
		return nil
	}
	return ref.gate
}

func (ht *HatTrie) replicaWriteError() error {
	if ht == nil {
		return ErrNilHatTrie
	}
	ref := ht.replicaReadOnlyGate.Load()
	if ref == nil || ref.gate == nil {
		return nil
	}
	return ref.gate.Check()
}

func replicaRequestMutates(request CacheCommandRequest) bool {
	command := strings.ToUpper(strings.TrimSpace(request.Command))
	if command == "BATCH" {
		for _, child := range request.Batch {
			if replicaRequestMutates(child) {
				return true
			}
		}
		return false
	}
	switch command {
	case "", "GET", "GETSTR", "DUMP", "EXISTS", "TTL",
		"PEEKMAP", "HEADSLICE", "TAILSLICE", "HASSET", "GETSET",
		"PEEKPQ", "PEEKPRIORITY", "GETPQ", "GETPRIORITY",
		"HASBF", "BFHAS", "BFEXISTS", "INFOBF", "BFINFO",
		"HASCF", "CFHAS", "CFEXISTS", "INFOCF", "CFINFO",
		"HASXF", "XFHAS", "XFEXISTS", "INFOXF", "XFINFO",
		"HASRB", "RBHAS", "RBEXISTS", "COUNTRB", "RBCOUNT", "GETRB", "RBGET", "INFORB", "RBINFO",
		"HASSB", "SBHAS", "SBEXISTS", "COUNTSB", "SBCOUNT", "GETSB", "SBGET", "INFOSB", "SBINFO",
		"GETRT", "RTGET", "HASRT", "RTEXISTS", "RTHAS", "PREFIXRT", "SCANRT", "RTPREFIX", "RTSCAN", "INFORT", "RTINFO",
		"ESTCMS", "QUERYCMS", "CMSQUERY", "CMSCOUNT", "INFOCMS", "CMSINFO",
		"COUNTHLL", "ESTHLL", "HLLCOUNT", "HLLCARD", "INFOHLL", "HLLINFO",
		"ESTTOPK", "QUERYTOPK", "TOPKCOUNT", "GETTOPK", "TOPK", "INFOTOPK", "TOPKINFO",
		"GETRS", "RSGET", "SAMPLE", "INFORS", "RSINFO",
		"ESTQ", "QUERYQ", "QQUERY", "QSQUERY", "QUANTILE", "INFOQ", "QINFO", "INFOQS", "QSINFO",
		"GETFW", "FWGET", "SUMFW", "PREFIXFW", "FWPREFIX", "FWSUM", "RANGEFW", "FWRANGE", "INFOFW", "FWINFO":
		return false
	default:
		return true
	}
}

// deleteInternal is reserved for package-private replication apply paths. It
// deliberately bypasses the public write gate after the replication protocol
// has authenticated and admitted the operation.
func (ht *HatTrie) deleteInternal(key string) bool {
	if ht == nil {
		return false
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.deleteInternal(key)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()
	deleted := ht.deleteLocked(key)
	if deleted {
		ht.recordDeleteLocked(key)
	}
	return deleted
}
