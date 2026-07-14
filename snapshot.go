package hatriecache

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

const snapshotVersion = 1

type snapshotFile struct {
	Version         int             `json:"version"`
	JournalSequence uint64          `json:"journal_sequence,omitempty"`
	Entries         []snapshotEntry `json:"entries"`
}

type SnapshotMetadata struct {
	JournalSequence uint64
}

type snapshotEntry struct {
	Key             string                   `json:"key"`
	Type            string                   `json:"type"`
	Counter         int32                    `json:"counter,omitempty"`
	String          string                   `json:"string,omitempty"`
	Bytes           string                   `json:"bytes,omitempty"`
	Map             Map                      `json:"map"`
	Slice           Slice                    `json:"slice"`
	Set             Set                      `json:"set"`
	PriorityQueue   []priorityQueueItem      `json:"priority_queue"`
	BloomFilter     *bloomFilterSnapshot     `json:"bloom_filter,omitempty"`
	CountMinSketch  *countMinSketchSnapshot  `json:"count_min_sketch,omitempty"`
	HyperLogLog     *hyperLogLogSnapshot     `json:"hyperloglog,omitempty"`
	TopK            *topKSnapshot            `json:"top_k,omitempty"`
	CuckooFilter    *cuckooFilterSnapshot    `json:"cuckoo_filter,omitempty"`
	RoaringBitmap   *roaringBitmapSnapshot   `json:"roaring_bitmap,omitempty"`
	QuantileSketch  *quantileSketchSnapshot  `json:"quantile_sketch,omitempty"`
	FenwickTree     *fenwickTreeSnapshot     `json:"fenwick_tree,omitempty"`
	SparseBitset    *sparseBitsetSnapshot    `json:"sparse_bitset,omitempty"`
	ReservoirSample *reservoirSampleSnapshot `json:"reservoir_sample,omitempty"`
	XorFilter       *xorFilterSnapshot       `json:"xor_filter,omitempty"`
	RadixTree       *radixTreeSnapshot       `json:"radix_tree,omitempty"`
	ExpiresAt       *time.Time               `json:"expires_at,omitempty"`
	Stats           *KeyStats                `json:"stats,omitempty"`
}

type snapshotOperation struct {
	entry snapshotEntry
	bytes []byte
}

func (ht *HatTrie) SaveSnapshot(path string) error {
	return ht.SaveSnapshotWithJournalSequence(path, 0)
}

func (ht *HatTrie) SaveSnapshotWithJournalSequence(path string, journalSequence uint64) error {
	snapshot, err := ht.snapshot()
	if err != nil {
		return err
	}
	snapshot.JournalSequence = journalSequence
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFileAtomic(path, data)
}

func (ht *HatTrie) LoadSnapshot(path string) error {
	_, err := ht.LoadSnapshotWithMetadata(path)
	return err
}

func (ht *HatTrie) LoadSnapshotWithMetadata(path string) (SnapshotMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SnapshotMetadata{}, err
	}

	snapshot, err := decodeSnapshotFileJSON(data)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	if snapshot.Version != snapshotVersion {
		return SnapshotMetadata{}, errors.New("hatriecache: unsupported snapshot version")
	}

	now := ht.currentTime()
	operations := make([]snapshotOperation, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if entry.ExpiresAt != nil && !now.Before(*entry.ExpiresAt) {
			continue
		}
		operation, err := validateSnapshotEntry(entry)
		if err != nil {
			return SnapshotMetadata{}, err
		}
		operations = append(operations, operation)
	}

	for _, operation := range operations {
		if err := ht.applySnapshotOperation(operation); err != nil {
			return SnapshotMetadata{}, err
		}
	}
	return SnapshotMetadata{JournalSequence: snapshot.JournalSequence}, nil
}

func (ht *HatTrie) snapshot() (snapshotFile, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.ensureOpen()
	entries := ht.entriesWithPrefixLocked("", true)
	snapshot := snapshotFile{
		Version: snapshotVersion,
		Entries: make([]snapshotEntry, 0, len(entries)),
	}
	for _, entry := range entries {
		snapshotEntry, err := ht.snapshotEntryLocked(entry)
		if err != nil {
			return snapshotFile{}, err
		}
		snapshot.Entries = append(snapshot.Entries, snapshotEntry)
	}
	return snapshot, nil
}

func (ht *HatTrie) snapshotEntryLocked(entry Entry) (snapshotEntry, error) {
	out := snapshotEntry{
		Key:       entry.Key,
		Type:      monitoringType(entry.Value),
		ExpiresAt: snapshotExpiresAt(ht.expires[entry.Key]),
	}
	if stats, ok := ht.keyStats[entry.Key]; ok {
		stats.updateRates()
		out.Stats = &stats
	}
	switch entry.Value.Type() {
	case DATAVALUE_TYPE_COUNTER:
		out.Counter = entry.Value.Index
	case DATAVALUE_TYPE_RAW_STRING:
		out.String = string(ht.raws.array[entry.Value.Index])
	case DATAVALUE_TYPE_RAW_BYTES:
		value, err := ht.bytesValueLocked(entry.Value)
		if err != nil {
			return snapshotEntry{}, err
		}
		out.Bytes = base64.StdEncoding.EncodeToString(value)
	case DATAVALUE_TYPE_MAP:
		out.Map = cloneMap(ht.maps.array[entry.Value.Index])
	case DATAVALUE_TYPE_SLICE:
		out.Slice = ht.slices.array[entry.Value.Index].Slice()
	case DATAVALUE_TYPE_LEVELDB_REF:
		return ht.levelDBReferenceSnapshotEntryLocked(entry.Key, entry.Value)
	case DATAVALUE_TYPE_SET:
		out.Set = ht.sets.array[entry.Value.Index].Values()
	case DATAVALUE_TYPE_PRIORITY_QUEUE:
		out.PriorityQueue = ht.priorityQueues.array[entry.Value.Index].SnapshotItems()
	case DATAVALUE_TYPE_BLOOM_FILTER:
		snapshot := ht.bloomFilters.array[entry.Value.Index].Snapshot()
		out.BloomFilter = &snapshot
	case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
		snapshot := ht.countMinSketches.array[entry.Value.Index].Snapshot()
		out.CountMinSketch = &snapshot
	case DATAVALUE_TYPE_HYPERLOGLOG:
		snapshot := ht.hyperLogLogs.array[entry.Value.Index].Snapshot()
		out.HyperLogLog = &snapshot
	case DATAVALUE_TYPE_TOP_K:
		snapshot := ht.topKs.array[entry.Value.Index].Snapshot()
		out.TopK = &snapshot
	case DATAVALUE_TYPE_CUCKOO_FILTER:
		snapshot := ht.cuckooFilters.array[entry.Value.Index].Snapshot()
		out.CuckooFilter = &snapshot
	case DATAVALUE_TYPE_ROARING_BITMAP:
		snapshot := ht.roaringBitmaps.array[entry.Value.Index].Snapshot()
		out.RoaringBitmap = &snapshot
	case DATAVALUE_TYPE_QUANTILE_SKETCH:
		snapshot := ht.quantileSketches.array[entry.Value.Index].Snapshot()
		out.QuantileSketch = &snapshot
	case DATAVALUE_TYPE_FENWICK_TREE:
		snapshot := ht.fenwickTrees.array[entry.Value.Index].Snapshot()
		out.FenwickTree = &snapshot
	case DATAVALUE_TYPE_SPARSE_BITSET:
		snapshot := ht.sparseBitsets.array[entry.Value.Index].Snapshot()
		out.SparseBitset = &snapshot
	case DATAVALUE_TYPE_RESERVOIR_SAMPLE:
		snapshot := ht.reservoirSamples.array[entry.Value.Index].Snapshot()
		out.ReservoirSample = &snapshot
	case DATAVALUE_TYPE_XOR_FILTER:
		snapshot := ht.xorFilters.array[entry.Value.Index].Snapshot()
		out.XorFilter = &snapshot
	case DATAVALUE_TYPE_RADIX_TREE:
		snapshot := ht.radixTrees.array[entry.Value.Index].Snapshot()
		out.RadixTree = &snapshot
	default:
		return snapshotEntry{}, errors.New("hatriecache: unsupported snapshot value type")
	}
	return out, nil
}

func snapshotExpiresAt(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func (ht *HatTrie) bytesValueLocked(hval HatValue) ([]byte, error) {
	if hval.OnDisk() {
		return ht.disks.Get(hval.Index)
	}
	return cloneBytes(ht.raws.array[hval.Index]), nil
}

func validateSnapshotEntry(entry snapshotEntry) (snapshotOperation, error) {
	operation := snapshotOperation{entry: entry}
	switch entry.Type {
	case "counter", "string", "map", "slice", "set", "priority_queue":
		return operation, nil
	case "bloom_filter":
		if entry.BloomFilter == nil {
			return snapshotOperation{}, errors.New("hatriecache: bloom filter snapshot is required")
		}
		if err := validateBloomFilterSnapshot(*entry.BloomFilter); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "count_min_sketch":
		if entry.CountMinSketch == nil {
			return snapshotOperation{}, errors.New("hatriecache: count-min sketch snapshot is required")
		}
		if err := validateCountMinSketchSnapshot(*entry.CountMinSketch); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "hyperloglog":
		if entry.HyperLogLog == nil {
			return snapshotOperation{}, errors.New("hatriecache: hyperloglog snapshot is required")
		}
		if err := validateHyperLogLogSnapshot(*entry.HyperLogLog); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "top_k":
		if entry.TopK == nil {
			return snapshotOperation{}, errors.New("hatriecache: top-k snapshot is required")
		}
		if err := validateTopKSnapshot(*entry.TopK); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "cuckoo_filter":
		if entry.CuckooFilter == nil {
			return snapshotOperation{}, errors.New("hatriecache: cuckoo filter snapshot is required")
		}
		if err := validateCuckooFilterSnapshot(*entry.CuckooFilter); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "roaring_bitmap":
		if entry.RoaringBitmap == nil {
			return snapshotOperation{}, errors.New("hatriecache: roaring bitmap snapshot is required")
		}
		if err := validateRoaringBitmapSnapshot(*entry.RoaringBitmap); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "quantile_sketch":
		if entry.QuantileSketch == nil {
			return snapshotOperation{}, errors.New("hatriecache: quantile sketch snapshot is required")
		}
		if err := validateQuantileSketchSnapshot(*entry.QuantileSketch); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "fenwick_tree":
		if entry.FenwickTree == nil {
			return snapshotOperation{}, errors.New("hatriecache: fenwick tree snapshot is required")
		}
		if err := validateFenwickTreeSnapshot(*entry.FenwickTree); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "sparse_bitset":
		if entry.SparseBitset == nil {
			return snapshotOperation{}, errors.New("hatriecache: sparse bitset snapshot is required")
		}
		if err := validateSparseBitsetSnapshot(*entry.SparseBitset); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "reservoir_sample":
		if entry.ReservoirSample == nil {
			return snapshotOperation{}, errors.New("hatriecache: reservoir sample snapshot is required")
		}
		if err := validateReservoirSampleSnapshot(*entry.ReservoirSample); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "xor_filter":
		if entry.XorFilter == nil {
			return snapshotOperation{}, errors.New("hatriecache: xor filter snapshot is required")
		}
		if err := validateXorFilterSnapshot(*entry.XorFilter); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "radix_tree":
		if entry.RadixTree == nil {
			return snapshotOperation{}, errors.New("hatriecache: radix tree snapshot is required")
		}
		if err := validateRadixTreeSnapshot(*entry.RadixTree); err != nil {
			return snapshotOperation{}, err
		}
		return operation, nil
	case "bytes":
		value, err := base64.StdEncoding.DecodeString(entry.Bytes)
		if err != nil {
			return snapshotOperation{}, err
		}
		operation.bytes = value
		return operation, nil
	default:
		return snapshotOperation{}, errors.New("hatriecache: unsupported snapshot value type")
	}
}

func decodeSnapshotEntryJSON(data []byte) (snapshotEntry, error) {
	return decodeSnapshotEntryJSONRequiredKey(data, false)
}

func decodeSnapshotFileJSON(data []byte) (snapshotFile, error) {
	var raw struct {
		Version         int               `json:"version"`
		JournalSequence uint64            `json:"journal_sequence,omitempty"`
		Entries         []json.RawMessage `json:"entries"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return snapshotFile{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return snapshotFile{}, errors.New("hatriecache: invalid snapshot JSON")
		}
		return snapshotFile{}, err
	}

	snapshot := snapshotFile{
		Version:         raw.Version,
		JournalSequence: raw.JournalSequence,
		Entries:         make([]snapshotEntry, 0, len(raw.Entries)),
	}
	for _, data := range raw.Entries {
		entry, err := decodeSnapshotEntryJSONRequiredKey(data, true)
		if err != nil {
			return snapshotFile{}, err
		}
		snapshot.Entries = append(snapshot.Entries, entry)
	}
	return snapshot, nil
}

func decodeSnapshotEntryJSONRequiredKey(data []byte, requiredKey bool) (snapshotEntry, error) {
	if requiredKey {
		hasKey, err := snapshotEntryHasKey(data)
		if err != nil {
			return snapshotEntry{}, err
		}
		if !hasKey {
			return snapshotEntry{}, errors.New("hatriecache: snapshot entry key is required")
		}
	}

	var entry snapshotEntry
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entry); err != nil {
		return snapshotEntry{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return snapshotEntry{}, errors.New("hatriecache: invalid snapshot entry JSON")
		}
		return snapshotEntry{}, err
	}
	return entry, nil
}

func snapshotEntryHasKey(data []byte) (bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return false, err
	}
	value, ok := fields["key"]
	if ok && string(value) == "null" {
		return false, errors.New("hatriecache: snapshot entry key must be a string")
	}
	return ok, nil
}

func (ht *HatTrie) applySnapshotOperation(operation snapshotOperation) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	_, err := ht.applySnapshotOperationLocked(operation)
	return err
}

func (ht *HatTrie) applySnapshotOperationLocked(operation snapshotOperation) (HatValue, error) {
	entry := operation.entry
	if entry.ExpiresAt != nil && !ht.currentTime().Before(*entry.ExpiresAt) {
		if ht.deleteLocked(entry.Key) {
			ht.recordExpirationLocked(entry.Key)
		}
		return HatValue{}, nil
	}

	rawPtr := ht.tryLocation(entry.Key)
	old := HatValue{}
	if rawPtr != nil {
		old.fromValue(*rawPtr)
	}

	hval := HatValue{}
	oldBytesStorageHandled := false
	switch entry.Type {
	case "counter":
		hval = HatValue{Index: entry.Counter, Flags: DATAVALUE_TYPE_COUNTER}
	case "string":
		idx := ht.raws.Add([]byte(entry.String))
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}
	case "bytes":
		next, err := ht.storeBytesValueLocked(old, operation.bytes)
		if err != nil {
			return HatValue{}, err
		}
		hval = next
		oldBytesStorageHandled = old.IsBytesAtRaws()
	case "map":
		idx := ht.maps.Add(entry.Map)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}
	case "slice":
		idx := ht.slices.Add(entry.Slice)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}
	case "set":
		idx := ht.sets.Add(entry.Set)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SET}
	case "priority_queue":
		idx := ht.priorityQueues.AddItems(entry.PriorityQueue)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_PRIORITY_QUEUE}
	case "bloom_filter":
		data, err := newBloomFilterDataFromSnapshot(*entry.BloomFilter)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.bloomFilters.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_BLOOM_FILTER}
	case "count_min_sketch":
		data, err := newCountMinSketchDataFromSnapshot(*entry.CountMinSketch)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.countMinSketches.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_COUNT_MIN_SKETCH}
	case "hyperloglog":
		data, err := newHyperLogLogDataFromSnapshot(*entry.HyperLogLog)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.hyperLogLogs.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_HYPERLOGLOG}
	case "top_k":
		data, err := newTopKDataFromSnapshot(*entry.TopK)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.topKs.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_TOP_K}
	case "cuckoo_filter":
		data, err := newCuckooFilterDataFromSnapshot(*entry.CuckooFilter)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.cuckooFilters.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_CUCKOO_FILTER}
	case "roaring_bitmap":
		data, err := newRoaringBitmapDataFromSnapshot(*entry.RoaringBitmap)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.roaringBitmaps.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_ROARING_BITMAP}
	case "quantile_sketch":
		data, err := newQuantileSketchDataFromSnapshot(*entry.QuantileSketch)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.quantileSketches.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_QUANTILE_SKETCH}
	case "fenwick_tree":
		data, err := newFenwickTreeDataFromSnapshot(*entry.FenwickTree)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.fenwickTrees.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_FENWICK_TREE}
	case "sparse_bitset":
		data, err := newSparseBitsetDataFromSnapshot(*entry.SparseBitset)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.sparseBitsets.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SPARSE_BITSET}
	case "reservoir_sample":
		data, err := newReservoirSampleDataFromSnapshot(*entry.ReservoirSample)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.reservoirSamples.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RESERVOIR_SAMPLE}
	case "xor_filter":
		data, err := newXorFilterDataFromSnapshot(*entry.XorFilter)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.xorFilters.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_XOR_FILTER}
	case "radix_tree":
		data, err := newRadixTreeDataFromSnapshot(*entry.RadixTree)
		if err != nil {
			return HatValue{}, err
		}
		idx := ht.radixTrees.AddData(data)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RADIX_TREE}
	default:
		return HatValue{}, errors.New("hatriecache: unsupported snapshot value type")
	}
	if rawPtr == nil {
		rawPtr = ht.upsertLocation(entry.Key)
	}
	if !old.Empty() && !oldBytesStorageHandled {
		ht.returnStorage(old)
	}
	ht.clearExpirationLocked(entry.Key)
	*rawPtr = hval.toValue()
	if entry.ExpiresAt != nil {
		hval = ht.setExpirationLocked(entry.Key, *entry.ExpiresAt, rawPtr, hval)
	}
	ht.restoreKeyStatsLocked(entry.Key, entry.Stats)
	return hval, nil
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return syncDirectory(dir)
}

func syncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
