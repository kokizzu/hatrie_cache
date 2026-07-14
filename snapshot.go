package hatriecache

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

type snapshotFileMetadata struct {
	Version         int
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
	return writeFileAtomicStream(path, func(writer io.Writer) error {
		return ht.writeSnapshotJSON(writer, journalSequence)
	})
}

func (ht *HatTrie) LoadSnapshot(path string) error {
	_, err := ht.LoadSnapshotWithMetadata(path)
	return err
}

func (ht *HatTrie) LoadSnapshotWithMetadata(path string) (SnapshotMetadata, error) {
	file, err := os.Open(path)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	defer file.Close()

	now := ht.currentTime()
	activeKeys := make(map[string]bool)
	metadata, err := scanSnapshotFileJSONReader(file, func(entry snapshotEntry) error {
		operation, active, err := validateSnapshotLoadEntry(entry, now, false)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		if _, exists := activeKeys[operation.entry.Key]; exists {
			return errors.New("hatriecache: snapshot contains duplicate active key")
		}
		activeKeys[operation.entry.Key] = false
		return nil
	})
	if err != nil {
		return SnapshotMetadata{}, err
	}
	if metadata.Version != snapshotVersion {
		return SnapshotMetadata{}, errors.New("hatriecache: unsupported snapshot version")
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return SnapshotMetadata{}, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	createdKeys := make(map[string]struct{}, len(activeKeys))
	rollbackOperations := make([]snapshotOperation, 0, len(activeKeys))
	applied := 0
	applyMetadata, err := scanSnapshotFileJSONReader(file, func(entry snapshotEntry) error {
		operation, active, err := validateSnapshotLoadEntry(entry, now, true)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		seen, exists := activeKeys[operation.entry.Key]
		if !exists {
			return errors.New("hatriecache: snapshot changed during load")
		}
		if seen {
			return errors.New("hatriecache: snapshot contains duplicate active key")
		}
		activeKeys[operation.entry.Key] = true

		rollbackOperation, existed, err := ht.restoreRollbackOperationLocked(operation.entry.Key)
		if err != nil {
			return err
		}
		if _, err := ht.applySnapshotOperationAtLocked(operation, now); err != nil {
			if existed {
				rollbackOperations = append(rollbackOperations, rollbackOperation)
			}
			return err
		}
		if existed {
			rollbackOperations = append(rollbackOperations, rollbackOperation)
		} else {
			createdKeys[operation.entry.Key] = struct{}{}
		}
		applied++
		return nil
	})
	if err != nil {
		return SnapshotMetadata{}, ht.restoreApplyErrorLocked(err, createdKeys, rollbackOperations, now)
	}
	if applyMetadata.Version != snapshotVersion || applyMetadata.JournalSequence != metadata.JournalSequence || applied != len(activeKeys) {
		err := errors.New("hatriecache: snapshot changed during load")
		return SnapshotMetadata{}, ht.restoreApplyErrorLocked(err, createdKeys, rollbackOperations, now)
	}
	ht.deleteKeysNotInLocked(activeKeys, now)
	return SnapshotMetadata{JournalSequence: metadata.JournalSequence}, nil
}

func validateSnapshotLoadEntry(entry snapshotEntry, now time.Time, prepareOperation bool) (snapshotOperation, bool, error) {
	if entry.ExpiresAt != nil && !now.Before(*entry.ExpiresAt) {
		return snapshotOperation{}, false, nil
	}
	if !prepareOperation {
		if err := validateSnapshotEntryFields(entry, true); err != nil {
			return snapshotOperation{}, false, err
		}
		return snapshotOperation{entry: entry}, true, nil
	}
	if entry.Type == "bytes" {
		operation, err := prepareSnapshotBytesOperation(entry)
		if err != nil {
			return snapshotOperation{}, false, err
		}
		return operation, true, nil
	}
	operation, err := validateSnapshotEntry(entry)
	if err != nil {
		return snapshotOperation{}, false, err
	}
	return operation, true, nil
}

func prepareSnapshotBytesOperation(entry snapshotEntry) (snapshotOperation, error) {
	if err := validateSnapshotEntryFields(entry, true); err != nil {
		return snapshotOperation{}, err
	}
	operation := snapshotOperation{entry: entry}
	size, ok := base64DecodedSize(entry.Bytes)
	if ok && size > DiskBytesThreshold {
		return operation, nil
	}
	value, err := base64.StdEncoding.DecodeString(entry.Bytes)
	if err != nil {
		return snapshotOperation{}, err
	}
	operation.bytes = value
	return operation, nil
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

func (ht *HatTrie) writeSnapshotJSON(writer io.Writer, journalSequence uint64) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.ensureOpen()
	if _, err := io.WriteString(writer, "{\n"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "  \"version\": %d,\n", snapshotVersion); err != nil {
		return err
	}
	if journalSequence != 0 {
		if _, err := fmt.Fprintf(writer, "  \"journal_sequence\": %d,\n", journalSequence); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(writer, "  \"entries\": ["); err != nil {
		return err
	}

	now := time.Time{}
	if len(ht.expires) > 0 {
		now = ht.currentTime()
	}
	first := true
	err := ht.scanEntriesWithPrefixAtLockedChecked("", true, now, func(entry Entry) error {
		if first {
			if _, err := io.WriteString(writer, "\n"); err != nil {
				return err
			}
			first = false
		} else if _, err := io.WriteString(writer, ",\n"); err != nil {
			return err
		}
		return ht.writeSnapshotEntryJSONLocked(writer, entry, "    ")
	})
	if err != nil {
		return err
	}
	if first {
		if _, err := io.WriteString(writer, "]\n"); err != nil {
			return err
		}
	} else if _, err := io.WriteString(writer, "\n  ]\n"); err != nil {
		return err
	}
	_, err = io.WriteString(writer, "}\n")
	return err
}

func (ht *HatTrie) writeSnapshotEntryJSONLocked(writer io.Writer, entry Entry, prefix string) error {
	if entry.Value.Type() == DATAVALUE_TYPE_RAW_BYTES && entry.Value.OnDisk() {
		return ht.writeSnapshotDiskBytesEntryJSONLocked(writer, entry, prefix)
	}
	snapshotEntry, err := ht.snapshotEntryLocked(entry)
	if err != nil {
		return err
	}
	return writeIndentedJSON(writer, snapshotEntry, prefix)
}

func (ht *HatTrie) writeSnapshotDiskBytesEntryJSONLocked(writer io.Writer, entry Entry, prefix string) error {
	if entry.Value.Index < 0 || int(entry.Value.Index) >= len(ht.disks.paths) {
		return ht.writeSnapshotBytesEntryJSONLocked(writer, entry, prefix, nil)
	}
	path := ht.disks.paths[entry.Value.Index]
	if path == "" {
		return ht.writeSnapshotBytesEntryJSONLocked(writer, entry, prefix, nil)
	}
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return ht.writeSnapshotBytesEntryJSONLocked(writer, entry, prefix, file)
}

func (ht *HatTrie) writeSnapshotBytesEntryJSONLocked(writer io.Writer, entry Entry, prefix string, reader io.Reader) error {
	expiresAt := snapshotExpiresAt(ht.expires[entry.Key])
	var stats *KeyStats
	if keyStats, ok := ht.keyStats[entry.Key]; ok {
		keyStats.updateRates()
		stats = &keyStats
	}

	if _, err := io.WriteString(writer, prefix+"{\n"); err != nil {
		return err
	}
	fieldPrefix := prefix + "  "
	if err := writeSnapshotJSONField(writer, fieldPrefix, "key", entry.Key, true); err != nil {
		return err
	}
	if err := writeSnapshotJSONField(writer, fieldPrefix, "type", "bytes", true); err != nil {
		return err
	}
	if err := writeSnapshotBase64Field(writer, fieldPrefix, "bytes", reader, true); err != nil {
		return err
	}
	if err := writeSnapshotJSONField(writer, fieldPrefix, "map", nil, true); err != nil {
		return err
	}
	if err := writeSnapshotJSONField(writer, fieldPrefix, "slice", nil, true); err != nil {
		return err
	}
	if err := writeSnapshotJSONField(writer, fieldPrefix, "set", nil, true); err != nil {
		return err
	}
	if err := writeSnapshotJSONField(writer, fieldPrefix, "priority_queue", nil, expiresAt != nil || stats != nil); err != nil {
		return err
	}
	if expiresAt != nil {
		if err := writeSnapshotJSONField(writer, fieldPrefix, "expires_at", expiresAt, stats != nil); err != nil {
			return err
		}
	}
	if stats != nil {
		if err := writeSnapshotJSONField(writer, fieldPrefix, "stats", stats, false); err != nil {
			return err
		}
	}
	_, err := io.WriteString(writer, prefix+"}")
	return err
}

func writeSnapshotJSONField(writer io.Writer, prefix string, name string, value interface{}, comma bool) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "%s%q: ", prefix, name); err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}
	if comma {
		if _, err := io.WriteString(writer, ","); err != nil {
			return err
		}
	}
	_, err = io.WriteString(writer, "\n")
	return err
}

func writeSnapshotBase64Field(writer io.Writer, prefix string, name string, reader io.Reader, comma bool) error {
	if _, err := fmt.Fprintf(writer, "%s%q: \"", prefix, name); err != nil {
		return err
	}
	encoder := base64.NewEncoder(base64.StdEncoding, writer)
	if reader != nil {
		if _, err := io.Copy(encoder, reader); err != nil {
			_ = encoder.Close()
			return err
		}
	}
	if err := encoder.Close(); err != nil {
		return err
	}
	if _, err := io.WriteString(writer, "\""); err != nil {
		return err
	}
	if comma {
		if _, err := io.WriteString(writer, ","); err != nil {
			return err
		}
	}
	_, err := io.WriteString(writer, "\n")
	return err
}

func writeIndentedJSON(writer io.Writer, value interface{}, prefix string) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	lines := bytes.Split(data, []byte{'\n'})
	for idx, line := range lines {
		if _, err := io.WriteString(writer, prefix); err != nil {
			return err
		}
		if _, err := writer.Write(line); err != nil {
			return err
		}
		if idx < len(lines)-1 {
			if _, err := io.WriteString(writer, "\n"); err != nil {
				return err
			}
		}
	}
	return nil
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
	if err := validateSnapshotEntryFields(entry, false); err != nil {
		return snapshotOperation{}, err
	}

	operation := snapshotOperation{entry: entry}
	if entry.Type == "bytes" {
		value, err := base64.StdEncoding.DecodeString(entry.Bytes)
		if err != nil {
			return snapshotOperation{}, err
		}
		operation.bytes = value
	}
	return operation, nil
}

func validateSnapshotEntryFields(entry snapshotEntry, validateBytesEncoding bool) error {
	if err := validateKey(entry.Key); err != nil {
		return err
	}
	if err := validateKeyStatsSnapshot(entry.Stats); err != nil {
		return err
	}

	switch entry.Type {
	case "counter", "string":
		return nil
	case "map":
		if entry.Map == nil {
			return errors.New("hatriecache: map snapshot is required")
		}
		if err := validateMapValue(entry.Map); err != nil {
			return err
		}
		return nil
	case "slice":
		if entry.Slice == nil {
			return errors.New("hatriecache: slice snapshot is required")
		}
		if err := validateSliceValue(entry.Slice); err != nil {
			return err
		}
		return nil
	case "set":
		if entry.Set == nil {
			return errors.New("hatriecache: set snapshot is required")
		}
		if _, err := newSetDataChecked(entry.Set); err != nil {
			return err
		}
		return nil
	case "priority_queue":
		if entry.PriorityQueue == nil {
			return errors.New("hatriecache: priority queue snapshot is required")
		}
		if err := validatePriorityQueueSnapshotItems(entry.PriorityQueue); err != nil {
			return err
		}
		return nil
	case "bloom_filter":
		if entry.BloomFilter == nil {
			return errors.New("hatriecache: bloom filter snapshot is required")
		}
		if err := validateBloomFilterSnapshot(*entry.BloomFilter); err != nil {
			return err
		}
		return nil
	case "count_min_sketch":
		if entry.CountMinSketch == nil {
			return errors.New("hatriecache: count-min sketch snapshot is required")
		}
		if err := validateCountMinSketchSnapshot(*entry.CountMinSketch); err != nil {
			return err
		}
		return nil
	case "hyperloglog":
		if entry.HyperLogLog == nil {
			return errors.New("hatriecache: hyperloglog snapshot is required")
		}
		if err := validateHyperLogLogSnapshot(*entry.HyperLogLog); err != nil {
			return err
		}
		return nil
	case "top_k":
		if entry.TopK == nil {
			return errors.New("hatriecache: top-k snapshot is required")
		}
		if err := validateTopKSnapshot(*entry.TopK); err != nil {
			return err
		}
		return nil
	case "cuckoo_filter":
		if entry.CuckooFilter == nil {
			return errors.New("hatriecache: cuckoo filter snapshot is required")
		}
		if err := validateCuckooFilterSnapshot(*entry.CuckooFilter); err != nil {
			return err
		}
		return nil
	case "roaring_bitmap":
		if entry.RoaringBitmap == nil {
			return errors.New("hatriecache: roaring bitmap snapshot is required")
		}
		if err := validateRoaringBitmapSnapshot(*entry.RoaringBitmap); err != nil {
			return err
		}
		return nil
	case "quantile_sketch":
		if entry.QuantileSketch == nil {
			return errors.New("hatriecache: quantile sketch snapshot is required")
		}
		if err := validateQuantileSketchSnapshot(*entry.QuantileSketch); err != nil {
			return err
		}
		return nil
	case "fenwick_tree":
		if entry.FenwickTree == nil {
			return errors.New("hatriecache: fenwick tree snapshot is required")
		}
		if err := validateFenwickTreeSnapshot(*entry.FenwickTree); err != nil {
			return err
		}
		return nil
	case "sparse_bitset":
		if entry.SparseBitset == nil {
			return errors.New("hatriecache: sparse bitset snapshot is required")
		}
		if err := validateSparseBitsetSnapshot(*entry.SparseBitset); err != nil {
			return err
		}
		return nil
	case "reservoir_sample":
		if entry.ReservoirSample == nil {
			return errors.New("hatriecache: reservoir sample snapshot is required")
		}
		if err := validateReservoirSampleSnapshot(*entry.ReservoirSample); err != nil {
			return err
		}
		return nil
	case "xor_filter":
		if entry.XorFilter == nil {
			return errors.New("hatriecache: xor filter snapshot is required")
		}
		if err := validateXorFilterSnapshot(*entry.XorFilter); err != nil {
			return err
		}
		return nil
	case "radix_tree":
		if entry.RadixTree == nil {
			return errors.New("hatriecache: radix tree snapshot is required")
		}
		if err := validateRadixTreeSnapshot(*entry.RadixTree); err != nil {
			return err
		}
		return nil
	case "bytes":
		if validateBytesEncoding {
			return validateBase64String(entry.Bytes)
		}
		return nil
	default:
		return errors.New("hatriecache: unsupported snapshot value type")
	}
}

func validatePriorityQueueSnapshotItems(items []priorityQueueItem) error {
	seenSequences := make(map[uint64]struct{}, len(items))
	for _, item := range items {
		if err := validatePriorityQueueItemValue(item.Value); err != nil {
			return err
		}
		if item.Sequence == ^uint64(0) {
			return errors.New("hatriecache: priority queue snapshot sequence is too large")
		}
		if _, exists := seenSequences[item.Sequence]; exists {
			return errors.New("hatriecache: priority queue snapshot contains duplicate sequence")
		}
		seenSequences[item.Sequence] = struct{}{}
	}
	return nil
}

func decodeSnapshotEntryJSON(data []byte) (snapshotEntry, error) {
	return decodeSnapshotEntryJSONRequiredKey(data, false)
}

func decodeSnapshotFileJSON(data []byte) (snapshotFile, error) {
	return decodeSnapshotFileJSONReader(bytes.NewReader(data))
}

func decodeSnapshotFileJSONReader(reader io.Reader) (snapshotFile, error) {
	var snapshot snapshotFile
	metadata, err := scanSnapshotFileJSONReader(reader, func(entry snapshotEntry) error {
		snapshot.Entries = append(snapshot.Entries, entry)
		return nil
	})
	if err != nil {
		return snapshotFile{}, err
	}
	snapshot.Version = metadata.Version
	snapshot.JournalSequence = metadata.JournalSequence
	return snapshot, nil
}

func scanSnapshotFileJSONReader(reader io.Reader, visit func(snapshotEntry) error) (snapshotFileMetadata, error) {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return snapshotFileMetadata{}, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return snapshotFileMetadata{}, errors.New("hatriecache: snapshot JSON must be an object")
	}

	var metadata snapshotFileMetadata
	seenFields := map[string]struct{}{}
	entriesSeen := false
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return snapshotFileMetadata{}, err
		}
		field, ok := token.(string)
		if !ok {
			return snapshotFileMetadata{}, errors.New("hatriecache: invalid snapshot JSON")
		}
		if _, ok := seenFields[field]; ok {
			return snapshotFileMetadata{}, errors.New("hatriecache: duplicate snapshot field")
		}
		seenFields[field] = struct{}{}

		switch field {
		case "version":
			if err := decoder.Decode(&metadata.Version); err != nil {
				return snapshotFileMetadata{}, err
			}
		case "journal_sequence":
			if err := decoder.Decode(&metadata.JournalSequence); err != nil {
				return snapshotFileMetadata{}, err
			}
		case "entries":
			entriesSeen = true
			if err := scanSnapshotEntriesJSON(decoder, visit); err != nil {
				return snapshotFileMetadata{}, err
			}
		default:
			return snapshotFileMetadata{}, fmt.Errorf("hatriecache: unknown snapshot field %q", field)
		}
	}

	token, err = decoder.Token()
	if err != nil {
		return snapshotFileMetadata{}, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return snapshotFileMetadata{}, errors.New("hatriecache: invalid snapshot JSON")
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return snapshotFileMetadata{}, errors.New("hatriecache: invalid snapshot JSON")
		}
		return snapshotFileMetadata{}, err
	}
	if !entriesSeen {
		return snapshotFileMetadata{}, errors.New("hatriecache: snapshot entries are required")
	}
	return metadata, nil
}

func decodeSnapshotEntriesJSON(decoder *json.Decoder) ([]snapshotEntry, error) {
	entries := make([]snapshotEntry, 0)
	if err := scanSnapshotEntriesJSON(decoder, func(entry snapshotEntry) error {
		entries = append(entries, entry)
		return nil
	}); err != nil {
		return nil, err
	}
	return entries, nil
}

func scanSnapshotEntriesJSON(decoder *json.Decoder, visit func(snapshotEntry) error) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return errors.New("hatriecache: snapshot entries must be an array")
	}

	for decoder.More() {
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return err
		}
		entry, err := decodeSnapshotEntryJSONRequiredKey(raw, true)
		if err != nil {
			return err
		}
		if visit != nil {
			if err := visit(entry); err != nil {
				return err
			}
		}
	}
	token, err = decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != ']' {
		return errors.New("hatriecache: invalid snapshot entries JSON")
	}
	return nil
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
	var probe struct {
		Key json.RawMessage `json:"key"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return false, err
	}
	if probe.Key == nil {
		return false, nil
	}
	if string(bytes.TrimSpace(probe.Key)) == "null" {
		return false, errors.New("hatriecache: snapshot entry key must be a string")
	}
	return true, nil
}

func (ht *HatTrie) applySnapshotOperation(operation snapshotOperation) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	_, err := ht.applySnapshotOperationLocked(operation)
	return err
}

func (ht *HatTrie) applySnapshotOperationLocked(operation snapshotOperation) (HatValue, error) {
	return ht.applySnapshotOperationAtLocked(operation, ht.currentTime())
}

func (ht *HatTrie) applySnapshotOperationAtLocked(operation snapshotOperation, now time.Time) (HatValue, error) {
	entry := operation.entry
	if err := validateKey(entry.Key); err != nil {
		return HatValue{}, err
	}
	if entry.ExpiresAt != nil && !now.Before(*entry.ExpiresAt) {
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
		next, handled, err := ht.storeSnapshotBytesValueLocked(old, operation)
		if err != nil {
			return HatValue{}, err
		}
		hval = next
		oldBytesStorageHandled = handled
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

func (ht *HatTrie) storeSnapshotBytesValueLocked(old HatValue, operation snapshotOperation) (HatValue, bool, error) {
	if shouldStreamSnapshotBytes(operation) {
		hval, err := ht.storeBase64BytesValueLocked(old, operation.entry.Bytes)
		return hval, old.IsBytesAtRaws(), err
	}
	hval, err := ht.storeBytesValueLocked(old, operation.bytes)
	return hval, old.IsBytesAtRaws(), err
}

func shouldStreamSnapshotBytes(operation snapshotOperation) bool {
	if operation.entry.Type != "bytes" || operation.entry.Bytes == "" || operation.bytes != nil {
		return false
	}
	size, ok := base64DecodedSize(operation.entry.Bytes)
	return ok && size > DiskBytesThreshold
}

func (ht *HatTrie) storeBase64BytesValueLocked(old HatValue, encoded string) (HatValue, error) {
	writeDecoded := func(writer io.Writer) error {
		_, err := io.Copy(writer, base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded)))
		return err
	}

	if old.IsBytesAtRaws() && old.OnDisk() {
		if err := ht.disks.PutStream(old.Index, writeDecoded); err != nil {
			return HatValue{}, err
		}
		return HatValue{
			Index: old.Index,
			Flags: DATAVALUE_TYPE_RAW_BYTES | (1 << DATAVALUE_DISK_BIT_SHIFT),
		}, nil
	}
	idx, err := ht.disks.AddStream(writeDecoded)
	if err != nil {
		return HatValue{}, err
	}
	if old.IsBytesAtRaws() && !old.OnDisk() {
		ht.raws.Del(old.Index)
	}
	return HatValue{
		Index: idx,
		Flags: DATAVALUE_TYPE_RAW_BYTES | (1 << DATAVALUE_DISK_BIT_SHIFT),
	}, nil
}

func (ht *HatTrie) deleteKeysNotInLocked(keep map[string]bool, now time.Time) {
	var stale []Entry
	_ = ht.scanEntriesWithPrefixAtLockedChecked("", false, now, func(entry Entry) error {
		if _, ok := keep[entry.Key]; ok {
			return nil
		}
		stale = append(stale, entry)
		return nil
	})
	for _, entry := range stale {
		ht.deleteKnownLocked(entry.Key, entry.Value)
	}
}

func (ht *HatTrie) restoreRollbackOperationLocked(key string) (snapshotOperation, bool, error) {
	rawPtr := ht.tryLocation(key)
	if rawPtr == nil {
		return snapshotOperation{}, false, nil
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	entry, err := ht.snapshotEntryLocked(Entry{Key: key, Value: hval})
	if err != nil {
		return snapshotOperation{}, true, err
	}
	operation, err := validateSnapshotEntry(entry)
	if err != nil {
		return snapshotOperation{}, true, err
	}
	return operation, true, nil
}

func (ht *HatTrie) restoreApplyErrorLocked(err error, createdKeys map[string]struct{}, rollbackOperations []snapshotOperation, now time.Time) error {
	if rollbackErr := ht.rollbackRestoreLocked(createdKeys, rollbackOperations, now); rollbackErr != nil {
		return errors.Join(err, fmt.Errorf("hatriecache: restore rollback failed: %w", rollbackErr))
	}
	return err
}

func (ht *HatTrie) rollbackRestoreLocked(createdKeys map[string]struct{}, rollbackOperations []snapshotOperation, now time.Time) error {
	ht.deleteKeysLocked(createdKeys)
	for idx := len(rollbackOperations) - 1; idx >= 0; idx-- {
		if _, err := ht.applySnapshotOperationAtLocked(rollbackOperations[idx], now); err != nil {
			return err
		}
	}
	return nil
}

func (ht *HatTrie) deleteKeysLocked(keys map[string]struct{}) {
	for key := range keys {
		rawPtr := ht.tryLocation(key)
		if rawPtr == nil {
			ht.clearExpirationLocked(key)
			continue
		}
		hval := HatValue{}
		hval.fromValue(*rawPtr)
		ht.deleteKnownLocked(key, hval)
	}
}

func writeFileAtomic(path string, data []byte) error {
	return writeFileAtomicStream(path, func(writer io.Writer) error {
		_, err := writer.Write(data)
		return err
	})
}

func writeJSONFileAtomic(path string, value interface{}) error {
	return writeFileAtomicStream(path, func(writer io.Writer) error {
		encoder := json.NewEncoder(writer)
		encoder.SetIndent("", "  ")
		return encoder.Encode(value)
	})
}

func writeFileAtomicStream(path string, write func(io.Writer) error) error {
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
	buffered := bufio.NewWriter(tmp)
	if err := write(buffered); err != nil {
		cleanup()
		return err
	}
	if err := buffered.Flush(); err != nil {
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
