package hatCache

const memoryAccountingStructureCount = 26

var memoryAccountingStructureNames = [...]string{
	"strings",
	"raws",
	"disks",
	"maps",
	"map_small",
	"slices",
	"slice_one_values",
	"slice_two_values",
	"sets",
	"set_one_strings",
	"set_two_strings",
	"priority_queues",
	"bloom_filters",
	"count_min_sketches",
	"hyper_log_logs",
	"top_ks",
	"cuckoo_filters",
	"roaring_bitmaps",
	"quantile_sketches",
	"fenwick_trees",
	"sparse_bitsets",
	"reservoir_samples",
	"xor_filters",
	"radix_trees",
	"dbrefs",
	"replication_merkle",
}

// MemoryAccountingReport is a point-in-time estimate of memory owned by a
// trie. NativeTrieBytes covers the C trie; structure rows cover Go backing
// pools and auxiliary indexes. Values exclude allocator metadata and nested
// payloads referenced by entries.
type MemoryAccountingReport struct {
	NativeTrieBytes   uint64                      `json:"native_trie_bytes"`
	TotalBackingBytes uint64                      `json:"total_backing_bytes"`
	TotalBytes        uint64                      `json:"total_bytes"`
	Structures        []MemoryAccountingStructure `json:"structures"`
}

// MemoryAccountingStructure reports the backing bytes retained by one typed
// storage pool or auxiliary index.
type MemoryAccountingStructure struct {
	Name         string `json:"name"`
	BackingBytes uint64 `json:"backing_bytes"`
}

// MemoryAccounting returns a read-only memory breakdown. Local partitions are
// aggregated so callers see one report for the logical cache.
func (ht *HatTrie) MemoryAccounting() MemoryAccountingReport {
	if ht == nil {
		return MemoryAccountingReport{}
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		report := MemoryAccountingReport{
			Structures: make([]MemoryAccountingStructure, memoryAccountingStructureCount),
		}
		for index, name := range memoryAccountingStructureNames {
			report.Structures[index].Name = name
		}
		for _, child := range partitions.tries {
			childReport := child.MemoryAccounting()
			report.NativeTrieBytes += childReport.NativeTrieBytes
			report.TotalBackingBytes += childReport.TotalBackingBytes
			for index, structure := range childReport.Structures {
				if index >= len(report.Structures) {
					break
				}
				report.Structures[index].BackingBytes += structure.BackingBytes
			}
		}
		report.TotalBytes = report.NativeTrieBytes + report.TotalBackingBytes
		return report
	}

	ht.mu.RLock()
	defer ht.mu.RUnlock()
	if ht.root == nil {
		return MemoryAccountingReport{}
	}
	return ht.memoryAccountingLocked()
}

func (ht *HatTrie) memoryAccountingLocked() MemoryAccountingReport {
	var values [memoryAccountingStructureCount]uint64
	report := MemoryAccountingReport{
		NativeTrieBytes:   ht.nativeTrieBytesLocked(),
		TotalBackingBytes: ht.memoryAccountingBackingValuesLocked(&values),
		Structures:        make([]MemoryAccountingStructure, memoryAccountingStructureCount),
	}
	for index, name := range memoryAccountingStructureNames {
		report.Structures[index] = MemoryAccountingStructure{
			Name:         name,
			BackingBytes: values[index],
		}
	}
	report.TotalBytes = report.NativeTrieBytes + report.TotalBackingBytes
	return report
}

func (ht *HatTrie) memoryAccountingBackingValuesLocked(values *[memoryAccountingStructureCount]uint64) uint64 {
	if ht == nil || ht.root == nil || ht.strings == nil || values == nil {
		return 0
	}
	values[0] = storageSliceBytes(ht.strings.array) + reusableBackingBytes(&ht.strings.reusables)
	values[1] = storageSliceBytes(ht.raws.array) + reusableBackingBytes(&ht.raws.reusables)
	values[2] = storageSliceBytes(ht.disks.paths) + reusableBackingBytes(&ht.disks.reusables)
	values[3] = storageSliceBytes(ht.maps.array) + storageSliceBytes(ht.maps.deleted) + reusableBackingBytes(&ht.maps.reusables)
	values[4] = storageSliceBytes(ht.maps.small) + reusableBackingBytes(&ht.maps.smallReusables)
	values[5] = storageSliceBytes(ht.slices.array) + reusableBackingBytes(&ht.slices.reusables)
	values[6] = storageSliceBytes(ht.slices.oneValues) + reusableBackingBytes(&ht.slices.oneReusable)
	values[7] = storageSliceBytes(ht.slices.twoValues) + reusableBackingBytes(&ht.slices.twoReusable)
	values[8] = storageSliceBytes(ht.sets.array) + reusableBackingBytes(&ht.sets.reusables)
	values[9] = storageSliceBytes(ht.sets.oneStrings) + reusableBackingBytes(&ht.sets.oneReusable)
	values[10] = storageSliceBytes(ht.sets.twoStrings) + reusableBackingBytes(&ht.sets.twoReusable)
	values[11] = storageSliceBytes(ht.priorityQueues.array) + reusableBackingBytes(&ht.priorityQueues.reusables)
	values[12] = storageSliceBytes(ht.bloomFilters.array) + reusableBackingBytes(&ht.bloomFilters.reusables)
	values[13] = storageSliceBytes(ht.countMinSketches.array) + reusableBackingBytes(&ht.countMinSketches.reusables)
	values[14] = storageSliceBytes(ht.hyperLogLogs.array) + reusableBackingBytes(&ht.hyperLogLogs.reusables)
	values[15] = storageSliceBytes(ht.topKs.array) + reusableBackingBytes(&ht.topKs.reusables)
	values[16] = storageSliceBytes(ht.cuckooFilters.array) + reusableBackingBytes(&ht.cuckooFilters.reusables)
	values[17] = storageSliceBytes(ht.roaringBitmaps.array) + reusableBackingBytes(&ht.roaringBitmaps.reusables)
	values[18] = storageSliceBytes(ht.quantileSketches.array) + reusableBackingBytes(&ht.quantileSketches.reusables)
	values[19] = storageSliceBytes(ht.fenwickTrees.array) + reusableBackingBytes(&ht.fenwickTrees.reusables)
	values[20] = storageSliceBytes(ht.sparseBitsets.array) + reusableBackingBytes(&ht.sparseBitsets.reusables)
	values[21] = storageSliceBytes(ht.reservoirSamples.array) + reusableBackingBytes(&ht.reservoirSamples.reusables)
	values[22] = storageSliceBytes(ht.xorFilters.array) + reusableBackingBytes(&ht.xorFilters.reusables)
	values[23] = storageSliceBytes(ht.radixTrees.array) + reusableBackingBytes(&ht.radixTrees.reusables)
	values[24] = storageSliceBytes(ht.dbrefs.array) + reusableBackingBytes(&ht.dbrefs.reusables)
	values[25] = memoryMerkleBytes(ht.replicationMerkle)
	var total uint64
	for _, value := range values {
		total += value
	}
	return total
}
