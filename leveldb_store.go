package hatriecache

import (
	"bytes"
	"encoding/base64"
	"errors"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var levelDBEntryPrefix = []byte("entry:")
var levelDBRecordChecksumTable = crc64.MakeTable(crc64.ISO)

var ErrLevelDBStoreClosed = errors.New("hatriecache: leveldb store is closed")

// LevelDBLoadPolicy controls how LevelDB entries are restored into memory.
type LevelDBLoadPolicy struct {
	HotValuesOnly bool
	MaxValueBytes int64
	MaxLastHitAge time.Duration
	MinHits       uint64
}

// LevelDBLoadResult reports how many keys and materialized values were loaded.
type LevelDBLoadResult struct {
	KeysLoaded   int
	ValuesLoaded int
}

// DefaultLevelDBHotLoadPolicy loads all non-expired keys and only small, recent,
// frequently-hit values.
func DefaultLevelDBHotLoadPolicy() LevelDBLoadPolicy {
	return LevelDBLoadPolicy{
		HotValuesOnly: true,
		MaxValueBytes: 1024,
		MaxLastHitAge: time.Hour,
		MinHits:       1000,
	}
}

type LevelDBStore struct {
	mu     sync.RWMutex
	db     *leveldb.DB
	format StorageFormat
}

func OpenLevelDBStore(path string) (*LevelDBStore, error) {
	return OpenLevelDBStoreWithFormat(path, DefaultStorageFormat)
}

func OpenLevelDBStoreWithFormat(path string, format StorageFormat) (*LevelDBStore, error) {
	if path == "" {
		return nil, errors.New("hatriecache: leveldb path is required")
	}
	format, err := ParseStorageFormat(string(format))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := leveldb.OpenFile(path, &opt.Options{
		Compression: opt.SnappyCompression,
	})
	if err != nil {
		return nil, err
	}
	return &LevelDBStore{db: db, format: format}, nil
}

func (store *LevelDBStore) Close() error {
	if store == nil {
		return nil
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return nil
	}
	db := store.db
	store.db = nil
	return db.Close()
}

func (store *LevelDBStore) Save(trie *HatTrie) error {
	db, unlock, err := store.lockDB()
	if err != nil {
		return err
	}
	defer unlock()

	batch, err := levelDBDiffBatch(store, db, trie)
	if err != nil {
		return err
	}
	if batch.Len() == 0 {
		return nil
	}
	return db.Write(batch, &opt.WriteOptions{Sync: true})
}

func levelDBDiffBatch(store *LevelDBStore, db *leveldb.DB, trie *HatTrie) (*leveldb.Batch, error) {
	batch := new(leveldb.Batch)
	iterator := db.NewIterator(util.BytesPrefix(levelDBEntryPrefix), nil)
	defer iterator.Release()

	hasExisting := iterator.Next()
	err := trie.scanLevelDBEntryDataForStore(store, db, store.format, func(key string, data []byte) error {
		dbKey := levelDBKey(key)
		for hasExisting && bytes.Compare(iterator.Key(), dbKey) < 0 {
			batch.Delete(cloneBytes(iterator.Key()))
			hasExisting = iterator.Next()
		}
		if hasExisting && bytes.Equal(iterator.Key(), dbKey) {
			if !bytes.Equal(iterator.Value(), data) {
				batch.Put(dbKey, data)
			}
			hasExisting = iterator.Next()
			return nil
		}
		batch.Put(dbKey, data)
		return nil
	})
	if err != nil {
		return nil, err
	}

	for hasExisting {
		batch.Delete(cloneBytes(iterator.Key()))
		hasExisting = iterator.Next()
	}
	if err := iterator.Error(); err != nil {
		return nil, err
	}
	return batch, nil
}

func (store *LevelDBStore) Load(trie *HatTrie) (int, error) {
	result, err := store.LoadWithPolicy(trie, LevelDBLoadPolicy{})
	return result.ValuesLoaded, err
}

// LoadWithPolicy restores entries from LevelDB. When HotValuesOnly is true,
// cold values are represented by lightweight references; keep store open while
// those references may be accessed or saved.
func (store *LevelDBStore) LoadWithPolicy(trie *HatTrie, policy LevelDBLoadPolicy) (LevelDBLoadResult, error) {
	db, unlock, err := store.lockDB()
	if err != nil {
		return LevelDBLoadResult{}, err
	}
	defer unlock()

	snapshot, err := db.GetSnapshot()
	if err != nil {
		return LevelDBLoadResult{}, err
	}
	defer snapshot.Release()

	now := trie.currentTime()
	trie.mu.Lock()
	defer trie.mu.Unlock()
	createdKeys := make(map[string]struct{})
	rollbackOperations := make([]snapshotOperation, 0)
	activeKeys := []string{}
	result := LevelDBLoadResult{}
	applied := false
	if err := scanLevelDBSnapshotEntryData(snapshot, func(entry snapshotEntry, data []byte) error {
		loadEntry, active, err := prepareLevelDBLoadEntry(entry, now, policy, true)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		activeKeys = append(activeKeys, loadEntry.entry.Key)
		result.KeysLoaded++
		rollbackOperation, existed, err := trie.restoreRollbackOperationLocked(loadEntry.entry.Key)
		if err != nil {
			return err
		}
		if loadEntry.reference {
			if _, err := trie.applyLevelDBReferenceLocked(store, loadEntry.entry, data); err != nil {
				if existed {
					rollbackOperations = append(rollbackOperations, rollbackOperation)
				}
				return err
			}
		} else {
			if _, err := trie.applySnapshotOperationAtLocked(loadEntry.operation, now); err != nil {
				if existed {
					rollbackOperations = append(rollbackOperations, rollbackOperation)
				}
				return err
			}
			result.ValuesLoaded++
		}
		applied = true
		if existed {
			rollbackOperations = append(rollbackOperations, rollbackOperation)
		} else {
			createdKeys[loadEntry.entry.Key] = struct{}{}
		}
		return nil
	}); err != nil {
		if applied {
			return LevelDBLoadResult{}, trie.restoreApplyErrorLocked(err, createdKeys, rollbackOperations, now)
		}
		return LevelDBLoadResult{}, err
	}
	trie.deleteKeysNotInSortedLocked(activeKeys, now)
	return result, nil
}

type levelDBLoadEntry struct {
	operation snapshotOperation
	entry     snapshotEntry
	reference bool
}

func prepareLevelDBLoadEntry(entry snapshotEntry, now time.Time, policy LevelDBLoadPolicy, prepareOperation bool) (levelDBLoadEntry, bool, error) {
	if entry.ExpiresAt != nil && !now.Before(*entry.ExpiresAt) {
		return levelDBLoadEntry{}, false, nil
	}
	if policy.HotValuesOnly && (!levelDBShouldHotLoadMetadata(entry, now, policy) || levelDBEntryExceedsMaxValueBytes(entry, policy)) {
		if err := validateLevelDBReferenceEntry(entry); err != nil {
			return levelDBLoadEntry{}, false, err
		}
		return levelDBLoadEntry{entry: entry, reference: true}, true, nil
	}
	if !prepareOperation {
		if err := validateSnapshotEntryFields(entry, true); err != nil {
			return levelDBLoadEntry{}, false, err
		}
		return levelDBLoadEntry{entry: entry}, true, nil
	}
	if entry.Type == "bytes" {
		operation, err := prepareSnapshotBytesOperation(entry)
		if err != nil {
			return levelDBLoadEntry{}, false, err
		}
		return levelDBLoadEntry{
			operation: operation,
			entry:     entry,
			reference: policy.HotValuesOnly && !levelDBShouldHotLoad(operation, now, policy),
		}, true, nil
	}
	operation, err := validateSnapshotEntry(entry)
	if err != nil {
		return levelDBLoadEntry{}, false, err
	}
	return levelDBLoadEntry{
		operation: operation,
		entry:     entry,
		reference: policy.HotValuesOnly && !levelDBShouldHotLoad(operation, now, policy),
	}, true, nil
}

func scanLevelDBSnapshotEntryData(snapshot *leveldb.Snapshot, visit func(snapshotEntry, []byte) error) error {
	iterator := snapshot.NewIterator(util.BytesPrefix(levelDBEntryPrefix), nil)
	defer iterator.Release()

	for iterator.Next() {
		key := string(iterator.Key()[len(levelDBEntryPrefix):])
		data := iterator.Value()
		entry, err := decodeLevelDBEntryForKey(key, data)
		if err != nil {
			return err
		}
		if visit != nil {
			if err := visit(entry, data); err != nil {
				return err
			}
		}
	}
	return iterator.Error()
}

func (store *LevelDBStore) Entry(key string) (snapshotEntry, bool, error) {
	db, unlock, err := store.lockDB()
	if err != nil {
		return snapshotEntry{}, false, err
	}
	defer unlock()

	data, err := db.Get(levelDBKey(key), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return snapshotEntry{}, false, nil
	}
	if err != nil {
		return snapshotEntry{}, false, err
	}
	entry, err := decodeLevelDBEntryForKey(key, data)
	if err != nil {
		return snapshotEntry{}, false, err
	}
	return entry, true, nil
}

func (store *LevelDBStore) entryData(key string) ([]byte, bool, error) {
	db, unlock, err := store.lockDB()
	if err != nil {
		return nil, false, err
	}
	defer unlock()

	return store.entryDataFromDB(db, key)
}

func (store *LevelDBStore) entryDataFromDB(db *leveldb.DB, key string) ([]byte, bool, error) {
	data, err := db.Get(levelDBKey(key), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return cloneBytes(data), true, nil
}

func (store *LevelDBStore) lockDB() (*leveldb.DB, func(), error) {
	if store == nil {
		return nil, func() {}, ErrLevelDBStoreClosed
	}
	store.mu.RLock()
	if store.db == nil {
		store.mu.RUnlock()
		return nil, func() {}, ErrLevelDBStoreClosed
	}
	return store.db, store.mu.RUnlock, nil
}

func (trie *HatTrie) SaveLevelDB(path string) error {
	return trie.SaveLevelDBWithFormat(path, DefaultStorageFormat)
}

func (trie *HatTrie) SaveLevelDBWithFormat(path string, format StorageFormat) error {
	store, err := OpenLevelDBStoreWithFormat(path, format)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.Save(trie)
}

func (trie *HatTrie) LoadLevelDB(path string) (int, error) {
	store, err := OpenLevelDBStore(path)
	if err != nil {
		return 0, err
	}
	defer store.Close()
	return store.Load(trie)
}

func levelDBShouldHotLoad(operation snapshotOperation, now time.Time, policy LevelDBLoadPolicy) bool {
	if !levelDBShouldHotLoadMetadata(operation.entry, now, policy) {
		return false
	}
	if policy.MaxValueBytes > 0 {
		size, err := snapshotOperationValueSize(operation)
		if err != nil || size > policy.MaxValueBytes {
			return false
		}
	}
	return true
}

func levelDBShouldHotLoadMetadata(entry snapshotEntry, now time.Time, policy LevelDBLoadPolicy) bool {
	if !policy.HotValuesOnly {
		return true
	}
	if policy.MaxLastHitAge < 0 || policy.MaxValueBytes < 0 {
		return false
	}
	stats := entry.Stats
	if stats == nil || stats.LastHit.IsZero() {
		return false
	}
	if policy.MaxLastHitAge > 0 && now.Sub(stats.LastHit) > policy.MaxLastHitAge {
		return false
	}
	if stats.Hits < policy.MinHits {
		return false
	}
	return true
}

func levelDBEntryExceedsMaxValueBytes(entry snapshotEntry, policy LevelDBLoadPolicy) bool {
	if !policy.HotValuesOnly || policy.MaxValueBytes <= 0 {
		return false
	}
	switch entry.Type {
	case "counter":
		return 4 > policy.MaxValueBytes
	case "string":
		return int64(len(entry.String)) > policy.MaxValueBytes
	case "bytes":
		size, ok := base64DecodedSize(entry.Bytes)
		return ok && size > policy.MaxValueBytes
	default:
		return false
	}
}

func base64DecodedSize(encoded string) (int64, bool) {
	if len(encoded)%4 != 0 {
		return 0, false
	}
	padding := 0
	if strings.HasSuffix(encoded, "==") {
		padding = 2
	} else if strings.HasSuffix(encoded, "=") {
		padding = 1
	}
	return int64(len(encoded)/4*3 - padding), true
}

func validatedBase64DecodedSize(encoded string) (int64, error) {
	size, ok := base64DecodedSize(encoded)
	if err := validateBase64String(encoded); err != nil {
		return 0, err
	}
	if !ok {
		return 0, errors.New("hatriecache: invalid base64 encoding")
	}
	return size, nil
}

func validateLevelDBReferenceEntry(entry snapshotEntry) error {
	if err := validateKey(entry.Key); err != nil {
		return err
	}
	if err := validateKeyStatsSnapshot(entry.Stats); err != nil {
		return err
	}
	switch entry.Type {
	case "counter", "string":
		return nil
	case "bytes":
		return validateBase64String(entry.Bytes)
	default:
		_, err := validateSnapshotEntry(entry)
		return err
	}
}

func validateBase64String(encoded string) error {
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded))
	var buf [32 * 1024]byte
	for {
		if _, err := decoder.Read(buf[:]); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

func snapshotOperationValueSize(operation snapshotOperation) (int64, error) {
	entry := operation.entry
	switch entry.Type {
	case "counter":
		return 4, nil
	case "string":
		return int64(len(entry.String)), nil
	case "bytes":
		if operation.bytes == nil && entry.Bytes != "" {
			if size, ok := base64DecodedSize(entry.Bytes); ok {
				return size, nil
			}
			if err := validateBase64String(entry.Bytes); err != nil {
				return 0, err
			}
			return 0, nil
		}
		return int64(len(operation.bytes)), nil
	case "map":
		return jsonEncodedSize(entry.Map)
	case "slice":
		return jsonEncodedSize(entry.Slice)
	case "set":
		return jsonEncodedSize(entry.Set)
	case "priority_queue":
		return jsonEncodedSize(entry.PriorityQueue)
	case "bloom_filter":
		if entry.BloomFilter == nil {
			return 0, errors.New("hatriecache: bloom filter snapshot is required")
		}
		return validatedBase64DecodedSize(entry.BloomFilter.Bits)
	case "count_min_sketch":
		if entry.CountMinSketch == nil {
			return 0, errors.New("hatriecache: count-min sketch snapshot is required")
		}
		return validatedBase64DecodedSize(entry.CountMinSketch.Counters)
	case "hyperloglog":
		if entry.HyperLogLog == nil {
			return 0, errors.New("hatriecache: hyperloglog snapshot is required")
		}
		return validatedBase64DecodedSize(entry.HyperLogLog.Registers)
	case "top_k":
		if entry.TopK == nil {
			return 0, errors.New("hatriecache: top-k snapshot is required")
		}
		return jsonEncodedSize(entry.TopK)
	case "cuckoo_filter":
		if entry.CuckooFilter == nil {
			return 0, errors.New("hatriecache: cuckoo filter snapshot is required")
		}
		return validatedBase64DecodedSize(entry.CuckooFilter.Fingerprints)
	case "roaring_bitmap":
		if entry.RoaringBitmap == nil {
			return 0, errors.New("hatriecache: roaring bitmap snapshot is required")
		}
		var total int64
		for _, container := range entry.RoaringBitmap.Containers {
			switch container.Kind {
			case roaringBitmapContainerKindArray:
				total += int64(container.Cardinality) * 2
			case roaringBitmapContainerKindBits:
				total += roaringBitmapBitmapWords * 8
			default:
				return 0, errors.New("hatriecache: unsupported roaring bitmap container kind")
			}
		}
		return total, nil
	case "sparse_bitset":
		if entry.SparseBitset == nil {
			return 0, errors.New("hatriecache: sparse bitset snapshot is required")
		}
		var total int64
		for _, container := range entry.SparseBitset.Containers {
			switch container.Kind {
			case sparseBitsetContainerKindArray:
				total += int64(container.Cardinality) * 2
			case sparseBitsetContainerKindBits:
				total += sparseBitsetBitmapWords * 8
			default:
				return 0, errors.New("hatriecache: unsupported sparse bitset container kind")
			}
		}
		return total, nil
	case "quantile_sketch":
		if entry.QuantileSketch == nil {
			return 0, errors.New("hatriecache: quantile sketch snapshot is required")
		}
		return jsonEncodedSize(entry.QuantileSketch)
	case "fenwick_tree":
		if entry.FenwickTree == nil {
			return 0, errors.New("hatriecache: fenwick tree snapshot is required")
		}
		return int64(len(entry.FenwickTree.Tree) * 8), nil
	case "reservoir_sample":
		if entry.ReservoirSample == nil {
			return 0, errors.New("hatriecache: reservoir sample snapshot is required")
		}
		return jsonEncodedSize(entry.ReservoirSample)
	case "xor_filter":
		if entry.XorFilter == nil {
			return 0, errors.New("hatriecache: xor filter snapshot is required")
		}
		return newXorFilterSizeFromSnapshot(*entry.XorFilter)
	case "radix_tree":
		if entry.RadixTree == nil {
			return 0, errors.New("hatriecache: radix tree snapshot is required")
		}
		return newRadixTreeSizeFromSnapshot(*entry.RadixTree)
	default:
		return 0, errors.New("hatriecache: unsupported snapshot value type")
	}
}

func newXorFilterSizeFromSnapshot(snapshot xorFilterSnapshot) (int64, error) {
	if snapshot.Built {
		return validatedBase64DecodedSize(snapshot.Fingerprints)
	}
	return jsonEncodedSize(snapshot.Staged)
}

func newRadixTreeSizeFromSnapshot(snapshot radixTreeSnapshot) (int64, error) {
	return jsonEncodedSize(snapshot.Items)
}

func (trie *HatTrie) applyLevelDBReferenceLocked(store *LevelDBStore, entry snapshotEntry, data []byte) (HatValue, error) {
	if store == nil {
		return HatValue{}, errors.New("hatriecache: leveldb reference store is required")
	}
	rawPtr := trie.upsertLocation(entry.Key)
	old := HatValue{}
	old.fromValue(*rawPtr)
	if !old.Empty() {
		trie.returnStorage(old)
	}
	trie.clearExpirationLocked(entry.Key)

	idx := trie.dbrefs.Add(LevelDBReference{
		Key:            entry.Key,
		Type:           entry.Type,
		Store:          store,
		ExpiresAt:      cloneTimePtr(entry.ExpiresAt),
		Stats:          cloneKeyStatsPtr(entry.Stats),
		RecordBytes:    len(data),
		RecordChecksum: levelDBRecordChecksum(data),
	})
	hval := HatValue{Index: idx, Flags: DATAVALUE_TYPE_LEVELDB_REF}
	if entry.ExpiresAt != nil {
		hval = trie.setExpirationLocked(entry.Key, *entry.ExpiresAt, rawPtr, hval)
	}
	*rawPtr = hval.toValue()
	trie.restoreKeyStatsLocked(entry.Key, entry.Stats)
	return hval, nil
}

func (trie *HatTrie) levelDBReferenceEntryDataLocked(key string, hval HatValue) ([]byte, bool, error) {
	return trie.levelDBReferenceEntryDataForStoreLocked(key, hval, nil, nil)
}

func (trie *HatTrie) levelDBReferenceEntryDataForStoreLocked(key string, hval HatValue, currentStore *LevelDBStore, currentDB *leveldb.DB) ([]byte, bool, error) {
	ref, ok := trie.dbrefs.Get(hval.Index)
	if !ok || ref.Store == nil || ref.Key != key || ref.RecordBytes <= 0 {
		return nil, false, nil
	}
	if !trie.levelDBReferenceMetadataMatchesLocked(key, ref) {
		return nil, false, nil
	}
	var (
		data []byte
		err  error
	)
	if currentStore != nil && currentDB != nil && ref.Store == currentStore {
		data, ok, err = currentStore.entryDataFromDB(currentDB, ref.Key)
	} else {
		data, ok, err = ref.Store.entryData(ref.Key)
	}
	if err != nil || !ok {
		return nil, false, err
	}
	if len(data) != ref.RecordBytes || levelDBRecordChecksum(data) != ref.RecordChecksum {
		return nil, false, nil
	}
	return data, true, nil
}

func (trie *HatTrie) levelDBReferenceMetadataMatchesLocked(key string, ref LevelDBReference) bool {
	if !timePtrEqual(ref.ExpiresAt, snapshotExpiresAt(trie.expires[key])) {
		return false
	}
	var currentStats *KeyStats
	if stats, ok := trie.keyStats[key]; ok {
		stats.updateRates()
		currentStats = &stats
	}
	return keyStatsPtrEqual(ref.Stats, currentStats)
}

func (trie *HatTrie) levelDBReferenceSnapshotEntryLocked(key string, hval HatValue) (snapshotEntry, error) {
	return trie.levelDBReferenceSnapshotEntryForStoreLocked(key, hval, nil, nil)
}

func (trie *HatTrie) levelDBReferenceSnapshotEntryForStoreLocked(key string, hval HatValue, currentStore *LevelDBStore, currentDB *leveldb.DB) (snapshotEntry, error) {
	ref, ok := trie.dbrefs.Get(hval.Index)
	if !ok || ref.Store == nil {
		return snapshotEntry{}, errors.New("hatriecache: missing leveldb reference")
	}
	var (
		entry snapshotEntry
		err   error
	)
	if currentStore != nil && currentDB != nil && ref.Store == currentStore {
		var data []byte
		data, ok, err = currentStore.entryDataFromDB(currentDB, ref.Key)
		if err == nil && ok {
			entry, err = decodeLevelDBEntryForKey(ref.Key, data)
		}
	} else {
		entry, ok, err = ref.Store.Entry(ref.Key)
	}
	if err != nil {
		return snapshotEntry{}, err
	}
	if !ok {
		return snapshotEntry{}, errors.New("hatriecache: missing leveldb referenced entry")
	}
	entry.Key = key
	entry.ExpiresAt = snapshotExpiresAt(trie.expires[key])
	if stats, ok := trie.keyStats[key]; ok {
		stats.updateRates()
		entry.Stats = &stats
	} else {
		entry.Stats = nil
	}
	return entry, nil
}

func (trie *HatTrie) scanLevelDBEntryData(visit func(string, []byte) error) error {
	return trie.scanLevelDBEntryDataForStore(nil, nil, DefaultStorageFormat, visit)
}

func (trie *HatTrie) scanLevelDBEntryDataForStore(currentStore *LevelDBStore, currentDB *leveldb.DB, format StorageFormat, visit func(string, []byte) error) error {
	trie.mu.Lock()
	defer trie.mu.Unlock()

	format, err := ParseStorageFormat(string(format))
	if err != nil {
		return err
	}
	trie.ensureOpen()
	now := time.Time{}
	if len(trie.expires) > 0 {
		now = trie.currentTime()
	}
	return trie.scanEntriesWithPrefixAtLockedChecked("", true, now, func(entry Entry) error {
		data, err := trie.levelDBEntryDataForStoreLocked(entry, currentStore, currentDB, format)
		if err != nil {
			return err
		}
		if visit != nil {
			return visit(entry.Key, data)
		}
		return nil
	})
}

func (trie *HatTrie) levelDBEntryDataLocked(entry Entry) ([]byte, error) {
	return trie.levelDBEntryDataForStoreLocked(entry, nil, nil, DefaultStorageFormat)
}

func (trie *HatTrie) levelDBEntryDataForStoreLocked(entry Entry, currentStore *LevelDBStore, currentDB *leveldb.DB, format StorageFormat) ([]byte, error) {
	if entry.Value.Type() == DATAVALUE_TYPE_LEVELDB_REF {
		if data, ok, err := trie.levelDBReferenceEntryDataForStoreLocked(entry.Key, entry.Value, currentStore, currentDB); err != nil || ok {
			return data, err
		}
	}
	if entry.Value.Type() == DATAVALUE_TYPE_RAW_BYTES && entry.Value.OnDisk() {
		if format == StorageFormatBinary {
			return trie.levelDBDiskBytesEntryDataBinaryLocked(entry)
		}
		var buffer bytes.Buffer
		if err := trie.writeSnapshotEntryJSONLocked(&buffer, entry, ""); err != nil {
			return nil, err
		}
		return buffer.Bytes(), nil
	}
	snapshotEntry, err := trie.snapshotEntryForStoreLocked(entry, currentStore, currentDB)
	if err != nil {
		return nil, err
	}
	return marshalLevelDBEntry(snapshotEntry, format)
}

func (trie *HatTrie) levelDBDiskBytesEntryDataBinaryLocked(entry Entry) ([]byte, error) {
	raw, err := trie.bytesValueLocked(entry.Value)
	if err != nil {
		return nil, err
	}
	expiresAt := snapshotExpiresAt(trie.expires[entry.Key])
	var stats *KeyStats
	if keyStats, ok := trie.keyStats[entry.Key]; ok {
		keyStats.updateRates()
		stats = &keyStats
	}
	return marshalLevelDBBytesEntryBinary(entry.Key, raw, expiresAt, stats)
}

func decodeLevelDBEntry(data []byte) (snapshotEntry, error) {
	if levelDBEntryDataIsBinary(data) {
		return unmarshalLevelDBEntryBinary(data)
	}
	return decodeSnapshotEntryJSONRequiredKey(data, true)
}

func decodeLevelDBEntryForKey(key string, data []byte) (snapshotEntry, error) {
	entry, err := decodeLevelDBEntry(data)
	if err != nil {
		return snapshotEntry{}, err
	}
	if entry.Key != key {
		return snapshotEntry{}, errors.New("hatriecache: leveldb entry key does not match record key")
	}
	return entry, nil
}

func levelDBKey(key string) []byte {
	out := make([]byte, 0, len(levelDBEntryPrefix)+len(key))
	out = append(out, levelDBEntryPrefix...)
	out = append(out, key...)
	return out
}

func levelDBRecordChecksum(data []byte) uint64 {
	return crc64.Checksum(data, levelDBRecordChecksumTable)
}

func cloneTimePtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func cloneKeyStatsPtr(value *KeyStats) *KeyStats {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

func timePtrEqual(left *time.Time, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

func keyStatsPtrEqual(left *KeyStats, right *KeyStats) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Reads == right.Reads &&
		left.Hits == right.Hits &&
		left.Misses == right.Misses &&
		left.Writes == right.Writes &&
		left.LastHit.Equal(right.LastHit) &&
		left.LastMiss.Equal(right.LastMiss) &&
		left.LastWrite.Equal(right.LastWrite) &&
		left.HitRate == right.HitRate &&
		left.CumulativeHitRate == right.CumulativeHitRate
}
