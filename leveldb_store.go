package hatriecache

import (
	"encoding/base64"
	"encoding/json"
	"errors"
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
	mu sync.RWMutex
	db *leveldb.DB
}

func OpenLevelDBStore(path string) (*LevelDBStore, error) {
	if path == "" {
		return nil, errors.New("hatriecache: leveldb path is required")
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
	return &LevelDBStore{db: db}, nil
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
	putBatch, err := trie.levelDBPutBatch()
	if err != nil {
		return err
	}

	db, unlock, err := store.lockDB()
	if err != nil {
		return err
	}
	defer unlock()

	batch := new(leveldb.Batch)
	iterator := db.NewIterator(util.BytesPrefix(levelDBEntryPrefix), nil)
	for iterator.Next() {
		key := cloneBytes(iterator.Key())
		batch.Delete(key)
	}
	if err := iterator.Error(); err != nil {
		iterator.Release()
		return err
	}
	iterator.Release()

	if err := putBatch.Replay(batch); err != nil {
		return err
	}
	return db.Write(batch, &opt.WriteOptions{Sync: true})
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
	activeKeys := map[string]bool{}
	result := LevelDBLoadResult{}
	if err := scanLevelDBSnapshotEntries(snapshot, func(entry snapshotEntry) error {
		loadEntry, active, err := prepareLevelDBLoadEntry(entry, now, policy, false)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		activeKeys[loadEntry.entry.Key] = true
		result.KeysLoaded++
		return nil
	}); err != nil {
		return LevelDBLoadResult{}, err
	}

	trie.mu.Lock()
	defer trie.mu.Unlock()
	createdKeys := make(map[string]struct{})
	rollbackOperations := make([]snapshotOperation, 0)
	applied := false
	if err := scanLevelDBSnapshotEntries(snapshot, func(entry snapshotEntry) error {
		loadEntry, active, err := prepareLevelDBLoadEntry(entry, now, policy, true)
		if err != nil {
			return err
		}
		if !active {
			return nil
		}
		rollbackOperation, existed, err := trie.restoreRollbackOperationLocked(loadEntry.entry.Key)
		if err != nil {
			return err
		}
		if loadEntry.reference {
			if _, err := trie.applyLevelDBReferenceLocked(store, loadEntry.entry); err != nil {
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
	trie.deleteKeysNotInLocked(activeKeys, now)
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

func scanLevelDBSnapshotEntries(snapshot *leveldb.Snapshot, visit func(snapshotEntry) error) error {
	iterator := snapshot.NewIterator(util.BytesPrefix(levelDBEntryPrefix), nil)
	defer iterator.Release()

	for iterator.Next() {
		key := string(iterator.Key()[len(levelDBEntryPrefix):])
		entry, err := decodeLevelDBEntryForKey(key, iterator.Value())
		if err != nil {
			return err
		}
		if visit != nil {
			if err := visit(entry); err != nil {
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
	store, err := OpenLevelDBStore(path)
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
		return int64(len(operation.bytes)), nil
	case "map":
		data, err := json.Marshal(entry.Map)
		return int64(len(data)), err
	case "slice":
		data, err := json.Marshal(entry.Slice)
		return int64(len(data)), err
	case "set":
		data, err := json.Marshal(entry.Set)
		return int64(len(data)), err
	case "priority_queue":
		data, err := json.Marshal(entry.PriorityQueue)
		return int64(len(data)), err
	case "bloom_filter":
		if entry.BloomFilter == nil {
			return 0, errors.New("hatriecache: bloom filter snapshot is required")
		}
		raw, err := base64.StdEncoding.DecodeString(entry.BloomFilter.Bits)
		return int64(len(raw)), err
	case "count_min_sketch":
		if entry.CountMinSketch == nil {
			return 0, errors.New("hatriecache: count-min sketch snapshot is required")
		}
		raw, err := base64.StdEncoding.DecodeString(entry.CountMinSketch.Counters)
		return int64(len(raw)), err
	case "hyperloglog":
		if entry.HyperLogLog == nil {
			return 0, errors.New("hatriecache: hyperloglog snapshot is required")
		}
		raw, err := base64.StdEncoding.DecodeString(entry.HyperLogLog.Registers)
		return int64(len(raw)), err
	case "top_k":
		if entry.TopK == nil {
			return 0, errors.New("hatriecache: top-k snapshot is required")
		}
		data, err := json.Marshal(entry.TopK)
		return int64(len(data)), err
	case "cuckoo_filter":
		if entry.CuckooFilter == nil {
			return 0, errors.New("hatriecache: cuckoo filter snapshot is required")
		}
		raw, err := base64.StdEncoding.DecodeString(entry.CuckooFilter.Fingerprints)
		return int64(len(raw)), err
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
		data, err := json.Marshal(entry.QuantileSketch)
		return int64(len(data)), err
	case "fenwick_tree":
		if entry.FenwickTree == nil {
			return 0, errors.New("hatriecache: fenwick tree snapshot is required")
		}
		return int64(len(entry.FenwickTree.Tree) * 8), nil
	case "reservoir_sample":
		if entry.ReservoirSample == nil {
			return 0, errors.New("hatriecache: reservoir sample snapshot is required")
		}
		data, err := json.Marshal(entry.ReservoirSample)
		return int64(len(data)), err
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
		raw, err := base64.StdEncoding.DecodeString(snapshot.Fingerprints)
		if err != nil {
			return 0, err
		}
		return int64(len(raw)), nil
	}
	data, err := json.Marshal(snapshot.Staged)
	return int64(len(data)), err
}

func newRadixTreeSizeFromSnapshot(snapshot radixTreeSnapshot) (int64, error) {
	data, err := json.Marshal(snapshot.Items)
	return int64(len(data)), err
}

func (trie *HatTrie) applyLevelDBReferenceLocked(store *LevelDBStore, entry snapshotEntry) (HatValue, error) {
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
		Key:   entry.Key,
		Type:  entry.Type,
		Store: store,
	})
	hval := HatValue{Index: idx, Flags: DATAVALUE_TYPE_LEVELDB_REF}
	if entry.ExpiresAt != nil {
		hval = trie.setExpirationLocked(entry.Key, *entry.ExpiresAt, rawPtr, hval)
	}
	*rawPtr = hval.toValue()
	trie.restoreKeyStatsLocked(entry.Key, entry.Stats)
	return hval, nil
}

func (trie *HatTrie) levelDBReferenceSnapshotEntryLocked(key string, hval HatValue) (snapshotEntry, error) {
	ref, ok := trie.dbrefs.Get(hval.Index)
	if !ok || ref.Store == nil {
		return snapshotEntry{}, errors.New("hatriecache: missing leveldb reference")
	}
	entry, ok, err := ref.Store.Entry(ref.Key)
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

func (trie *HatTrie) levelDBEntries() ([]snapshotEntry, error) {
	out := []snapshotEntry{}
	err := trie.scanLevelDBEntries(func(entry snapshotEntry) error {
		out = append(out, entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (trie *HatTrie) levelDBPutBatch() (*leveldb.Batch, error) {
	batch := new(leveldb.Batch)
	err := trie.scanLevelDBEntries(func(entry snapshotEntry) error {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		batch.Put(levelDBKey(entry.Key), data)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return batch, nil
}

func (trie *HatTrie) scanLevelDBEntries(visit func(snapshotEntry) error) error {
	trie.mu.Lock()
	defer trie.mu.Unlock()

	trie.ensureOpen()
	now := time.Time{}
	if len(trie.expires) > 0 {
		now = trie.currentTime()
	}
	return trie.scanEntriesWithPrefixAtLockedChecked("", true, now, func(entry Entry) error {
		snapshotEntry, err := trie.snapshotEntryLocked(entry)
		if err != nil {
			return err
		}
		if visit != nil {
			return visit(snapshotEntry)
		}
		return nil
	})
}

func decodeLevelDBEntry(data []byte) (snapshotEntry, error) {
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
