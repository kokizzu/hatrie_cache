package hatriecache

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"hatrie_cache/internal/jsonwire"

	json "github.com/goccy/go-json"
	"github.com/syndtr/goleveldb/leveldb"
)

const snapshotVersion = 1
const defaultDeleteKeysNotInBatchSize = 1024

var (
	errDeleteKeysNotInPageFull    = errors.New("hatriecache: delete missing keys page full")
	errSnapshotChangedDuringLoad  = errors.New("hatriecache: snapshot changed during load")
	errSnapshotDuplicateActiveKey = errors.New("hatriecache: snapshot contains duplicate active key")
)

type SnapshotFormat string

const (
	SnapshotFormatBinary         SnapshotFormat = "binary"
	SnapshotFormatGzipBinary     SnapshotFormat = "gzip-binary"
	SnapshotFormatGzipBestBinary SnapshotFormat = "gzip-best-binary"
	SnapshotFormatJSON           SnapshotFormat = "json"
	SnapshotFormatGzipJSON       SnapshotFormat = "gzip-json"
	SnapshotFormatGzipBestJSON   SnapshotFormat = "gzip-best-json"
)

const DefaultSnapshotFormat = SnapshotFormatGzipBestBinary

var snapshotBestGzipWriterPool = sync.Pool{
	New: func() interface{} {
		writer, err := gzip.NewWriterLevel(io.Discard, gzip.BestCompression)
		if err != nil {
			panic(err)
		}
		return writer
	},
}

func ParseSnapshotFormat(value string) (SnapshotFormat, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return DefaultSnapshotFormat, nil
	case string(SnapshotFormatGzipBestBinary), "best-gzip-binary", "gzip-small-binary", "small-gzip-binary":
		return SnapshotFormatGzipBestBinary, nil
	case string(SnapshotFormatGzipBinary), "gzip-bin", "binary.gz", "gzbin":
		return SnapshotFormatGzipBinary, nil
	case string(SnapshotFormatBinary), "bin":
		return SnapshotFormatBinary, nil
	case string(SnapshotFormatGzipBestJSON), "gzip-best", "best-gzip-json", "gzip-small-json", "small-gzip-json":
		return SnapshotFormatGzipBestJSON, nil
	case string(SnapshotFormatGzipJSON), "gzip", "json.gz", "gzjson":
		return SnapshotFormatGzipJSON, nil
	case string(SnapshotFormatJSON):
		return SnapshotFormatJSON, nil
	default:
		return "", fmt.Errorf("hatriecache: unsupported snapshot format %q", value)
	}
}

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
	rawBytes        []byte
}

type snapshotOperation struct {
	entry snapshotEntry
	bytes []byte
}

func snapshotEntryBytesBase64(entry snapshotEntry) string {
	if entry.Bytes != "" || entry.rawBytes == nil {
		return entry.Bytes
	}
	return base64.StdEncoding.EncodeToString(entry.rawBytes)
}

func snapshotEntryBytesDecodedSize(entry snapshotEntry) (int64, bool) {
	if entry.rawBytes != nil {
		return int64(len(entry.rawBytes)), true
	}
	return base64DecodedSize(entry.Bytes)
}

func snapshotEntryBytesValue(entry snapshotEntry) ([]byte, error) {
	if entry.rawBytes != nil {
		return entry.rawBytes, nil
	}
	return base64.StdEncoding.DecodeString(entry.Bytes)
}

func materializeSnapshotEntryBytes(entry snapshotEntry) snapshotEntry {
	if entry.Type == "bytes" && entry.Bytes == "" && entry.rawBytes != nil {
		entry.Bytes = base64.StdEncoding.EncodeToString(entry.rawBytes)
	}
	entry.rawBytes = nil
	return entry
}

const (
	snapshotEntryJSONFieldKey uint64 = 1 << iota
	snapshotEntryJSONFieldType
	snapshotEntryJSONFieldCounter
	snapshotEntryJSONFieldString
	snapshotEntryJSONFieldBytes
	snapshotEntryJSONFieldMap
	snapshotEntryJSONFieldSlice
	snapshotEntryJSONFieldSet
	snapshotEntryJSONFieldPriorityQueue
	snapshotEntryJSONFieldBloomFilter
	snapshotEntryJSONFieldCountMinSketch
	snapshotEntryJSONFieldHyperLogLog
	snapshotEntryJSONFieldTopK
	snapshotEntryJSONFieldCuckooFilter
	snapshotEntryJSONFieldRoaringBitmap
	snapshotEntryJSONFieldQuantileSketch
	snapshotEntryJSONFieldFenwickTree
	snapshotEntryJSONFieldSparseBitset
	snapshotEntryJSONFieldReservoirSample
	snapshotEntryJSONFieldXorFilter
	snapshotEntryJSONFieldRadixTree
	snapshotEntryJSONFieldExpiresAt
	snapshotEntryJSONFieldStats
)

func (ht *HatTrie) SaveSnapshot(path string) error {
	return ht.SaveSnapshotWithFormat(path, DefaultSnapshotFormat)
}

func (ht *HatTrie) SaveSnapshotWithFormat(path string, format SnapshotFormat) error {
	return ht.SaveSnapshotWithJournalSequenceAndFormat(path, 0, format)
}

func (ht *HatTrie) SaveSnapshotWithJournalSequence(path string, journalSequence uint64) error {
	return ht.SaveSnapshotWithJournalSequenceAndFormat(path, journalSequence, DefaultSnapshotFormat)
}

func (ht *HatTrie) SaveSnapshotWithJournalSequenceAndFormat(path string, journalSequence uint64, format SnapshotFormat) error {
	if ht == nil {
		return ErrNilHatTrie
	}
	format, err := ParseSnapshotFormat(string(format))
	if err != nil {
		return err
	}
	return writeFileAtomicStream(path, func(writer io.Writer) error {
		return ht.writeSnapshot(writer, journalSequence, format)
	})
}

func (ht *HatTrie) LoadSnapshot(path string) error {
	_, err := ht.LoadSnapshotWithMetadata(path)
	return err
}

func (ht *HatTrie) LoadSnapshotWithMetadata(path string) (SnapshotMetadata, error) {
	if ht == nil {
		return SnapshotMetadata{}, ErrNilHatTrie
	}
	file, err := os.Open(path)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	defer file.Close()

	now := ht.currentTime()
	var activeKeyList []string
	metadata, err := scanSnapshotFileReader(file, func(entry snapshotEntry) error {
		operation, active, err := validateSnapshotLoadEntry(entry, now, false)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		activeKeyList = append(activeKeyList, operation.entry.Key)
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
	activeKeys, err := newSnapshotActiveKeys(activeKeyList)
	if err != nil {
		return SnapshotMetadata{}, err
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	createdKeys := make(map[string]struct{}, len(activeKeys.keys))
	rollbackOperations := make([]snapshotOperation, 0, len(activeKeys.keys))
	applied := 0
	applyMetadata, err := scanSnapshotFileReader(file, func(entry snapshotEntry) error {
		operation, active, err := validateSnapshotLoadEntry(entry, now, true)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		if err := activeKeys.markSeen(operation.entry.Key); err != nil {
			return err
		}

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
	if applyMetadata.Version != snapshotVersion || applyMetadata.JournalSequence != metadata.JournalSequence || applied != len(activeKeys.keys) {
		return SnapshotMetadata{}, ht.restoreApplyErrorLocked(errSnapshotChangedDuringLoad, createdKeys, rollbackOperations, now)
	}
	ht.deleteKeysNotInSortedLocked(activeKeys.keys, now)
	return SnapshotMetadata{JournalSequence: metadata.JournalSequence}, nil
}

type snapshotActiveKeys struct {
	keys []string
	seen []uint64
}

func newSnapshotActiveKeys(keys []string) (snapshotActiveKeys, error) {
	sort.Strings(keys)
	for idx := 1; idx < len(keys); idx++ {
		if keys[idx] == keys[idx-1] {
			return snapshotActiveKeys{}, errSnapshotDuplicateActiveKey
		}
	}
	return snapshotActiveKeys{
		keys: keys,
		seen: make([]uint64, (len(keys)+63)/64),
	}, nil
}

func (keys snapshotActiveKeys) markSeen(key string) error {
	idx := sort.SearchStrings(keys.keys, key)
	if idx == len(keys.keys) || keys.keys[idx] != key {
		return errSnapshotChangedDuringLoad
	}
	word := idx / 64
	mask := uint64(1) << uint(idx%64)
	if keys.seen[word]&mask != 0 {
		return errSnapshotDuplicateActiveKey
	}
	keys.seen[word] |= mask
	return nil
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

func (ht *HatTrie) writeSnapshot(writer io.Writer, journalSequence uint64, format SnapshotFormat) error {
	switch format {
	case SnapshotFormatBinary:
		return ht.writeSnapshotBinary(writer, journalSequence)
	case SnapshotFormatGzipBestBinary:
		return ht.writeSnapshotGzipBinary(writer, journalSequence, acquireSnapshotBestGzipWriter, releaseSnapshotBestGzipWriter)
	case SnapshotFormatGzipBinary:
		return ht.writeSnapshotGzipBinary(writer, journalSequence, jsonwire.AcquireGzipWriter, jsonwire.ReleaseGzipWriter)
	case SnapshotFormatJSON:
		return ht.writeSnapshotJSON(writer, journalSequence)
	case SnapshotFormatGzipBestJSON:
		return ht.writeSnapshotGzipJSON(writer, journalSequence, acquireSnapshotBestGzipWriter, releaseSnapshotBestGzipWriter)
	case SnapshotFormatGzipJSON:
		return ht.writeSnapshotGzipJSON(writer, journalSequence, jsonwire.AcquireGzipWriter, jsonwire.ReleaseGzipWriter)
	default:
		return fmt.Errorf("hatriecache: unsupported snapshot format %q", format)
	}
}

func (ht *HatTrie) writeSnapshotGzipJSON(writer io.Writer, journalSequence uint64, acquire func(io.Writer) *gzip.Writer, release func(*gzip.Writer)) error {
	gzipWriter := acquire(writer)
	err := ht.writeSnapshotJSON(gzipWriter, journalSequence)
	closeErr := gzipWriter.Close()
	release(gzipWriter)
	if err != nil {
		return err
	}
	return closeErr
}

func acquireSnapshotBestGzipWriter(writer io.Writer) *gzip.Writer {
	gzipWriter := snapshotBestGzipWriterPool.Get().(*gzip.Writer)
	gzipWriter.Reset(writer)
	return gzipWriter
}

func releaseSnapshotBestGzipWriter(writer *gzip.Writer) {
	writer.Reset(io.Discard)
	snapshotBestGzipWriterPool.Put(writer)
}

func prepareSnapshotBytesOperation(entry snapshotEntry) (snapshotOperation, error) {
	if err := validateSnapshotEntryFields(entry, true); err != nil {
		return snapshotOperation{}, err
	}
	operation := snapshotOperation{entry: entry}
	if entry.rawBytes != nil {
		operation.bytes = entry.rawBytes
		return operation, nil
	}
	size, ok := snapshotEntryBytesDecodedSize(entry)
	if ok && size > DiskBytesThreshold {
		return operation, nil
	}
	value, err := snapshotEntryBytesValue(entry)
	if err != nil {
		return snapshotOperation{}, err
	}
	operation.bytes = value
	return operation, nil
}

func snapshotOperationForEntry(entry snapshotEntry) (snapshotOperation, error) {
	if entry.Type == "bytes" {
		return prepareSnapshotBytesOperation(entry)
	}
	return validateSnapshotEntry(entry)
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
	if entry.Value.Type() == DATAVALUE_TYPE_LEVELDB_REF {
		if data, ok, err := ht.levelDBReferenceEntryDataLocked(entry.Key, entry.Value); err != nil || ok {
			if err != nil {
				return err
			}
			return writeLevelDBRecordSnapshotJSON(writer, entry.Key, data, prefix)
		}
	}
	if entry.Value.Type() == DATAVALUE_TYPE_RAW_BYTES && entry.Value.OnDisk() {
		return ht.writeSnapshotDiskBytesEntryJSONLocked(writer, entry, prefix)
	}
	snapshotEntry, err := ht.snapshotEntryLocked(entry)
	if err != nil {
		return err
	}
	return writeSnapshotEntryFieldsJSON(writer, snapshotEntry, prefix)
}

func writeSnapshotRawEntryJSON(writer io.Writer, data []byte, prefix string) error {
	data = bytes.TrimSpace(data)
	if prefix != "" {
		if _, err := io.WriteString(writer, prefix); err != nil {
			return err
		}
	}
	_, err := writer.Write(data)
	return err
}

func (ht *HatTrie) writeSnapshotDiskBytesEntryJSONLocked(writer io.Writer, entry Entry, prefix string) error {
	file, _, err := ht.disks.open(entry.Value.Index)
	if err != nil {
		return err
	}
	if file == nil {
		return ht.writeSnapshotBytesEntryJSONLocked(writer, entry, prefix, nil)
	}
	defer file.Close()
	return ht.writeSnapshotBytesEntryJSONLocked(writer, entry, prefix, file)
}

func (ht *HatTrie) writeSnapshotBytesEntryJSONLocked(writer io.Writer, entry Entry, prefix string, reader io.Reader) error {
	expiresAt := snapshotExpiresAt(ht.expires[entry.Key])
	stats := clonedUpdatedKeyStats(ht.keyStats[entry.Key])

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
	if err := writeSnapshotBase64Field(writer, fieldPrefix, "bytes", reader, expiresAt != nil || stats != nil); err != nil {
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
	if _, err := fmt.Fprintf(writer, "%s%q: ", prefix, name); err != nil {
		return err
	}
	encoder := json.NewEncoder(&prefixedJSONLineWriter{writer: writer})
	if err := encoder.Encode(value); err != nil {
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

func writeSnapshotEntryFieldsJSON(writer io.Writer, entry snapshotEntry, prefix string) error {
	if _, err := io.WriteString(writer, prefix+"{\n"); err != nil {
		return err
	}
	fieldPrefix := prefix + "  "
	first := true
	if err := visitSnapshotEntryJSONFields(entry, func(name string, value interface{}) error {
		if !first {
			if _, err := io.WriteString(writer, ",\n"); err != nil {
				return err
			}
		}
		first = false
		return writeSnapshotIndentedJSONField(writer, fieldPrefix, name, value)
	}); err != nil {
		return err
	}
	_, err := io.WriteString(writer, "\n"+prefix+"}")
	return err
}

type snapshotEntryCompactJSON struct {
	Key             string                   `json:"key"`
	Type            string                   `json:"type"`
	Counter         *int32                   `json:"counter,omitempty"`
	String          *string                  `json:"string,omitempty"`
	Bytes           *string                  `json:"bytes,omitempty"`
	Map             *Map                     `json:"map,omitempty"`
	Slice           *Slice                   `json:"slice,omitempty"`
	Set             *Set                     `json:"set,omitempty"`
	PriorityQueue   *[]priorityQueueItem     `json:"priority_queue,omitempty"`
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

func compactSnapshotEntryJSON(entry snapshotEntry) snapshotEntryCompactJSON {
	out := snapshotEntryCompactJSON{
		Key:       entry.Key,
		Type:      entry.Type,
		ExpiresAt: entry.ExpiresAt,
		Stats:     entry.Stats,
	}
	switch entry.Type {
	case "counter":
		if entry.Counter != 0 {
			out.Counter = &entry.Counter
		}
	case "string":
		if entry.String != "" {
			out.String = &entry.String
		}
	case "bytes":
		encoded := snapshotEntryBytesBase64(entry)
		if encoded != "" {
			out.Bytes = &encoded
		}
	case "map":
		out.Map = &entry.Map
	case "slice":
		out.Slice = &entry.Slice
	case "set":
		out.Set = &entry.Set
	case "priority_queue":
		out.PriorityQueue = &entry.PriorityQueue
	case "bloom_filter":
		out.BloomFilter = entry.BloomFilter
	case "count_min_sketch":
		out.CountMinSketch = entry.CountMinSketch
	case "hyperloglog":
		out.HyperLogLog = entry.HyperLogLog
	case "top_k":
		out.TopK = entry.TopK
	case "cuckoo_filter":
		out.CuckooFilter = entry.CuckooFilter
	case "roaring_bitmap":
		out.RoaringBitmap = entry.RoaringBitmap
	case "quantile_sketch":
		out.QuantileSketch = entry.QuantileSketch
	case "fenwick_tree":
		out.FenwickTree = entry.FenwickTree
	case "sparse_bitset":
		out.SparseBitset = entry.SparseBitset
	case "reservoir_sample":
		out.ReservoirSample = entry.ReservoirSample
	case "xor_filter":
		out.XorFilter = entry.XorFilter
	case "radix_tree":
		out.RadixTree = entry.RadixTree
	}
	return out
}

func marshalSnapshotEntryJSON(entry snapshotEntry) ([]byte, error) {
	return json.Marshal(compactSnapshotEntryJSON(entry))
}

func visitSnapshotEntryJSONFields(entry snapshotEntry, visit func(string, interface{}) error) error {
	if err := visit("key", entry.Key); err != nil {
		return err
	}
	if err := visit("type", entry.Type); err != nil {
		return err
	}
	add := func(name string, value interface{}) error {
		return visit(name, value)
	}
	switch entry.Type {
	case "counter":
		if entry.Counter != 0 {
			if err := add("counter", entry.Counter); err != nil {
				return err
			}
		}
	case "string":
		if entry.String != "" {
			if err := add("string", entry.String); err != nil {
				return err
			}
		}
	case "bytes":
		encoded := snapshotEntryBytesBase64(entry)
		if encoded != "" {
			if err := add("bytes", encoded); err != nil {
				return err
			}
		}
	case "map":
		if err := add("map", entry.Map); err != nil {
			return err
		}
	case "slice":
		if err := add("slice", entry.Slice); err != nil {
			return err
		}
	case "set":
		if err := add("set", entry.Set); err != nil {
			return err
		}
	case "priority_queue":
		if err := add("priority_queue", entry.PriorityQueue); err != nil {
			return err
		}
	case "bloom_filter":
		if entry.BloomFilter != nil {
			if err := add("bloom_filter", entry.BloomFilter); err != nil {
				return err
			}
		}
	case "count_min_sketch":
		if entry.CountMinSketch != nil {
			if err := add("count_min_sketch", entry.CountMinSketch); err != nil {
				return err
			}
		}
	case "hyperloglog":
		if entry.HyperLogLog != nil {
			if err := add("hyperloglog", entry.HyperLogLog); err != nil {
				return err
			}
		}
	case "top_k":
		if entry.TopK != nil {
			if err := add("top_k", entry.TopK); err != nil {
				return err
			}
		}
	case "cuckoo_filter":
		if entry.CuckooFilter != nil {
			if err := add("cuckoo_filter", entry.CuckooFilter); err != nil {
				return err
			}
		}
	case "roaring_bitmap":
		if entry.RoaringBitmap != nil {
			if err := add("roaring_bitmap", entry.RoaringBitmap); err != nil {
				return err
			}
		}
	case "quantile_sketch":
		if entry.QuantileSketch != nil {
			if err := add("quantile_sketch", entry.QuantileSketch); err != nil {
				return err
			}
		}
	case "fenwick_tree":
		if entry.FenwickTree != nil {
			if err := add("fenwick_tree", entry.FenwickTree); err != nil {
				return err
			}
		}
	case "sparse_bitset":
		if entry.SparseBitset != nil {
			if err := add("sparse_bitset", entry.SparseBitset); err != nil {
				return err
			}
		}
	case "reservoir_sample":
		if entry.ReservoirSample != nil {
			if err := add("reservoir_sample", entry.ReservoirSample); err != nil {
				return err
			}
		}
	case "xor_filter":
		if entry.XorFilter != nil {
			if err := add("xor_filter", entry.XorFilter); err != nil {
				return err
			}
		}
	case "radix_tree":
		if entry.RadixTree != nil {
			if err := add("radix_tree", entry.RadixTree); err != nil {
				return err
			}
		}
	}
	if entry.ExpiresAt != nil {
		if err := add("expires_at", entry.ExpiresAt); err != nil {
			return err
		}
	}
	if entry.Stats != nil {
		if err := add("stats", entry.Stats); err != nil {
			return err
		}
	}
	return nil
}

func writeSnapshotIndentedJSONField(writer io.Writer, prefix string, name string, value interface{}) error {
	if _, err := fmt.Fprintf(writer, "%s%q: ", prefix, name); err != nil {
		return err
	}
	prefixed := &prefixedJSONLineWriter{
		writer: writer,
		prefix: prefix,
	}
	encoder := json.NewEncoder(prefixed)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

type prefixedJSONLineWriter struct {
	writer         io.Writer
	prefix         string
	lineStart      bool
	pendingNewline bool
}

func (writer *prefixedJSONLineWriter) Write(data []byte) (int, error) {
	written := 0
	for len(data) > 0 {
		if writer.pendingNewline {
			if _, err := io.WriteString(writer.writer, "\n"); err != nil {
				return written, err
			}
			writer.lineStart = true
			writer.pendingNewline = false
		}
		if data[0] == '\n' {
			writer.pendingNewline = true
			data = data[1:]
			written++
			continue
		}
		if writer.lineStart {
			if _, err := io.WriteString(writer.writer, writer.prefix); err != nil {
				return written, err
			}
			writer.lineStart = false
		}
		nextNewline := bytes.IndexByte(data, '\n')
		if nextNewline < 0 {
			if _, err := writer.writer.Write(data); err != nil {
				return written, err
			}
			written += len(data)
			return written, nil
		}
		if nextNewline > 0 {
			if _, err := writer.writer.Write(data[:nextNewline]); err != nil {
				return written, err
			}
			written += nextNewline
			data = data[nextNewline:]
		}
	}
	return written, nil
}

func (ht *HatTrie) snapshotEntryLocked(entry Entry) (snapshotEntry, error) {
	return ht.snapshotEntryForStoreLockedWithStats(entry, nil, nil, true)
}

func (ht *HatTrie) snapshotEntryWithoutStatsLocked(entry Entry) (snapshotEntry, error) {
	return ht.snapshotEntryForStoreLockedWithStats(entry, nil, nil, false)
}

func (ht *HatTrie) snapshotEntryForStoreLocked(entry Entry, currentStore *LevelDBStore, currentDB *leveldb.DB) (snapshotEntry, error) {
	return ht.snapshotEntryForStoreLockedWithStats(entry, currentStore, currentDB, true)
}

func (ht *HatTrie) snapshotEntryForStoreLockedWithStats(entry Entry, currentStore *LevelDBStore, currentDB *leveldb.DB, includeStats bool) (snapshotEntry, error) {
	out := snapshotEntry{
		Key:       entry.Key,
		Type:      monitoringType(entry.Value),
		ExpiresAt: snapshotExpiresAt(ht.expires[entry.Key]),
	}
	if includeStats {
		out.Stats = clonedUpdatedKeyStats(ht.keyStats[entry.Key])
	}
	switch entry.Value.Type() {
	case DATAVALUE_TYPE_COUNTER:
		out.Counter = entry.Value.Index
	case DATAVALUE_TYPE_RAW_STRING:
		out.String = ht.raws.stringValue(entry.Value.Index)
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
		return ht.levelDBReferenceSnapshotEntryForStoreLocked(entry.Key, entry.Value, currentStore, currentDB)
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
		value, err := snapshotEntryBytesValue(entry)
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
			if entry.rawBytes != nil {
				return nil
			}
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

func scanSnapshotFileReader(reader io.Reader, visit func(snapshotEntry) error) (snapshotFileMetadata, error) {
	payloadReader, closeReader, err := snapshotPayloadReader(reader)
	if err != nil {
		return snapshotFileMetadata{}, err
	}
	defer closeReader()
	buffered := bufio.NewReader(payloadReader)
	isBinary, err := snapshotReaderIsBinary(buffered)
	if err != nil {
		return snapshotFileMetadata{}, err
	}
	if isBinary {
		return scanSnapshotFileBinaryReader(buffered, visit)
	}
	return scanSnapshotFileJSONReader(buffered, visit)
}

func snapshotPayloadReader(reader io.Reader) (io.Reader, func() error, error) {
	buffered := bufio.NewReader(reader)
	header, err := buffered.Peek(2)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, nil, err
	}
	if len(header) == 2 && header[0] == 0x1f && header[1] == 0x8b {
		gzipReader, err := gzip.NewReader(buffered)
		if err != nil {
			return nil, nil, err
		}
		return gzipReader, gzipReader.Close, nil
	}
	return buffered, func() error { return nil }, nil
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

func scanSnapshotEntriesJSON(decoder *json.Decoder, visit func(snapshotEntry) error) error {
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return errors.New("hatriecache: snapshot entries must be an array")
	}

	for decoder.More() {
		entry, err := decodeSnapshotEntryJSONDecoder(decoder, true)
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
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	entry, err := decodeSnapshotEntryJSONDecoder(decoder, requiredKey)
	if err != nil {
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

func decodeSnapshotEntryJSONDecoder(decoder *json.Decoder, requiredKey bool) (snapshotEntry, error) {
	token, err := decoder.Token()
	if err != nil {
		return snapshotEntry{}, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return snapshotEntry{}, errors.New("hatriecache: snapshot entry JSON must be an object")
	}

	var entry snapshotEntry
	var seen uint64
	for decoder.More() {
		token, err := decoder.Token()
		if err != nil {
			return snapshotEntry{}, err
		}
		field, ok := token.(string)
		if !ok {
			return snapshotEntry{}, errors.New("hatriecache: invalid snapshot entry JSON")
		}
		if err := markSnapshotEntryJSONField(field, &seen); err != nil {
			return snapshotEntry{}, err
		}
		if err := decodeSnapshotEntryJSONField(decoder, field, &entry); err != nil {
			return snapshotEntry{}, err
		}
	}

	token, err = decoder.Token()
	if err != nil {
		return snapshotEntry{}, err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '}' {
		return snapshotEntry{}, errors.New("hatriecache: invalid snapshot entry JSON")
	}
	if requiredKey && seen&snapshotEntryJSONFieldKey == 0 {
		return snapshotEntry{}, errors.New("hatriecache: snapshot entry key is required")
	}
	return entry, nil
}

func markSnapshotEntryJSONField(field string, seen *uint64) error {
	bit := snapshotEntryJSONFieldBit(field)
	if bit == 0 {
		return fmt.Errorf("hatriecache: unknown snapshot entry field %q", field)
	}
	if *seen&bit != 0 {
		return fmt.Errorf("hatriecache: duplicate snapshot entry field %q", field)
	}
	*seen |= bit
	return nil
}

func snapshotEntryJSONFieldBit(field string) uint64 {
	switch field {
	case "key":
		return snapshotEntryJSONFieldKey
	case "type":
		return snapshotEntryJSONFieldType
	case "counter":
		return snapshotEntryJSONFieldCounter
	case "string":
		return snapshotEntryJSONFieldString
	case "bytes":
		return snapshotEntryJSONFieldBytes
	case "map":
		return snapshotEntryJSONFieldMap
	case "slice":
		return snapshotEntryJSONFieldSlice
	case "set":
		return snapshotEntryJSONFieldSet
	case "priority_queue":
		return snapshotEntryJSONFieldPriorityQueue
	case "bloom_filter":
		return snapshotEntryJSONFieldBloomFilter
	case "count_min_sketch":
		return snapshotEntryJSONFieldCountMinSketch
	case "hyperloglog":
		return snapshotEntryJSONFieldHyperLogLog
	case "top_k":
		return snapshotEntryJSONFieldTopK
	case "cuckoo_filter":
		return snapshotEntryJSONFieldCuckooFilter
	case "roaring_bitmap":
		return snapshotEntryJSONFieldRoaringBitmap
	case "quantile_sketch":
		return snapshotEntryJSONFieldQuantileSketch
	case "fenwick_tree":
		return snapshotEntryJSONFieldFenwickTree
	case "sparse_bitset":
		return snapshotEntryJSONFieldSparseBitset
	case "reservoir_sample":
		return snapshotEntryJSONFieldReservoirSample
	case "xor_filter":
		return snapshotEntryJSONFieldXorFilter
	case "radix_tree":
		return snapshotEntryJSONFieldRadixTree
	case "expires_at":
		return snapshotEntryJSONFieldExpiresAt
	case "stats":
		return snapshotEntryJSONFieldStats
	default:
		return 0
	}
}

func decodeSnapshotEntryJSONField(decoder *json.Decoder, field string, entry *snapshotEntry) error {
	switch field {
	case "key":
		var key *string
		if err := decoder.Decode(&key); err != nil {
			return err
		}
		if key == nil {
			return errors.New("hatriecache: snapshot entry key must be a string")
		}
		entry.Key = *key
	case "type":
		return decoder.Decode(&entry.Type)
	case "counter":
		return decoder.Decode(&entry.Counter)
	case "string":
		return decoder.Decode(&entry.String)
	case "bytes":
		return decoder.Decode(&entry.Bytes)
	case "map":
		return decoder.Decode(&entry.Map)
	case "slice":
		return decoder.Decode(&entry.Slice)
	case "set":
		return decoder.Decode(&entry.Set)
	case "priority_queue":
		return decoder.Decode(&entry.PriorityQueue)
	case "bloom_filter":
		return decoder.Decode(&entry.BloomFilter)
	case "count_min_sketch":
		return decoder.Decode(&entry.CountMinSketch)
	case "hyperloglog":
		return decoder.Decode(&entry.HyperLogLog)
	case "top_k":
		return decoder.Decode(&entry.TopK)
	case "cuckoo_filter":
		return decoder.Decode(&entry.CuckooFilter)
	case "roaring_bitmap":
		return decoder.Decode(&entry.RoaringBitmap)
	case "quantile_sketch":
		return decoder.Decode(&entry.QuantileSketch)
	case "fenwick_tree":
		return decoder.Decode(&entry.FenwickTree)
	case "sparse_bitset":
		return decoder.Decode(&entry.SparseBitset)
	case "reservoir_sample":
		return decoder.Decode(&entry.ReservoirSample)
	case "xor_filter":
		return decoder.Decode(&entry.XorFilter)
	case "radix_tree":
		return decoder.Decode(&entry.RadixTree)
	case "expires_at":
		return decoder.Decode(&entry.ExpiresAt)
	case "stats":
		return decoder.Decode(&entry.Stats)
	default:
		return fmt.Errorf("hatriecache: unknown snapshot entry field %q", field)
	}
	return nil
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
		idx := ht.raws.addStringOwned(entry.String)
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
	if operation.entry.Type != "bytes" || operation.entry.Bytes == "" || operation.entry.rawBytes != nil || operation.bytes != nil {
		return false
	}
	size, ok := snapshotEntryBytesDecodedSize(operation.entry)
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
	ht.deleteKeysNotInBatchesLocked(keep, now, defaultDeleteKeysNotInBatchSize)
}

func (ht *HatTrie) deleteKeysNotInSortedLocked(keep []string, now time.Time) {
	ht.deleteKeysNotInSortedBatchesLocked(keep, now, defaultDeleteKeysNotInBatchSize)
}

func (ht *HatTrie) deleteKeysNotInBatchesLocked(keep map[string]bool, now time.Time, limit int) {
	if limit <= 0 {
		limit = 1
	}

	afterKey := ""
	hasAfterKey := false
	for {
		page := ht.staleKeysNotInPageLocked(keep, now, afterKey, hasAfterKey, limit)
		for _, entry := range page.entries {
			ht.deleteKnownLocked(entry.Key, entry.Value)
		}
		if !page.hasMore {
			return
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
	}
}

func (ht *HatTrie) deleteKeysNotInSortedBatchesLocked(keep []string, now time.Time, limit int) {
	if limit <= 0 {
		limit = 1
	}

	afterKey := ""
	hasAfterKey := false
	for {
		page := ht.staleKeysNotInSortedPageLocked(keep, now, afterKey, hasAfterKey, limit)
		for _, entry := range page.entries {
			ht.deleteKnownLocked(entry.Key, entry.Value)
		}
		if !page.hasMore {
			return
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
	}
}

type staleKeysNotInPage struct {
	entries      []Entry
	nextAfterKey string
	hasMore      bool
}

func (ht *HatTrie) staleKeysNotInPageLocked(keep map[string]bool, now time.Time, afterKey string, hasAfterKey bool, limit int) staleKeysNotInPage {
	if limit <= 0 {
		limit = 1
	}
	page := staleKeysNotInPage{entries: make([]Entry, 0, limit)}
	err := ht.scanEntriesWithPrefixAtLockedChecked("", true, now, func(entry Entry) error {
		if hasAfterKey && entry.Key <= afterKey {
			return nil
		}
		if _, ok := keep[entry.Key]; ok {
			return nil
		}
		page.entries = append(page.entries, entry)
		page.nextAfterKey = entry.Key
		if len(page.entries) >= limit {
			page.hasMore = true
			return errDeleteKeysNotInPageFull
		}
		return nil
	})
	if !errors.Is(err, errDeleteKeysNotInPageFull) {
		page.hasMore = false
	}
	return page
}

func (ht *HatTrie) staleKeysNotInSortedPageLocked(keep []string, now time.Time, afterKey string, hasAfterKey bool, limit int) staleKeysNotInPage {
	if limit <= 0 {
		limit = 1
	}
	keepIndex := 0
	if hasAfterKey {
		keepIndex = sort.Search(len(keep), func(idx int) bool {
			return keep[idx] > afterKey
		})
	}
	page := staleKeysNotInPage{entries: make([]Entry, 0, limit)}
	err := ht.scanEntriesWithPrefixAtLockedChecked("", true, now, func(entry Entry) error {
		if hasAfterKey && entry.Key <= afterKey {
			return nil
		}
		for keepIndex < len(keep) && keep[keepIndex] < entry.Key {
			keepIndex++
		}
		if keepIndex < len(keep) && keep[keepIndex] == entry.Key {
			return nil
		}
		page.entries = append(page.entries, entry)
		page.nextAfterKey = entry.Key
		if len(page.entries) >= limit {
			page.hasMore = true
			return errDeleteKeysNotInPageFull
		}
		return nil
	})
	if !errors.Is(err, errDeleteKeysNotInPageFull) {
		page.hasMore = false
	}
	return page
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
