package hatriecache

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
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
	entries, err := trie.levelDBEntries()
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

	for _, entry := range entries {
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		batch.Put(levelDBKey(entry.Key), data)
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

	iterator := db.NewIterator(util.BytesPrefix(levelDBEntryPrefix), nil)
	defer iterator.Release()

	now := trie.currentTime()
	type levelDBLoadOperation struct {
		operation snapshotOperation
		reference bool
	}
	operations := []levelDBLoadOperation{}
	for iterator.Next() {
		key := string(iterator.Key()[len(levelDBEntryPrefix):])
		entry, err := decodeLevelDBEntryForKey(key, iterator.Value())
		if err != nil {
			return LevelDBLoadResult{}, err
		}
		if entry.ExpiresAt != nil && !now.Before(*entry.ExpiresAt) {
			continue
		}
		operation, err := validateSnapshotEntry(entry)
		if err != nil {
			return LevelDBLoadResult{}, err
		}
		operations = append(operations, levelDBLoadOperation{
			operation: operation,
			reference: policy.HotValuesOnly && !levelDBShouldHotLoad(operation, now, policy),
		})
	}
	if err := iterator.Error(); err != nil {
		return LevelDBLoadResult{}, err
	}

	result := LevelDBLoadResult{KeysLoaded: len(operations)}
	trie.mu.Lock()
	defer trie.mu.Unlock()
	for _, operation := range operations {
		if operation.reference {
			if _, err := trie.applyLevelDBReferenceLocked(store, operation.operation.entry); err != nil {
				return LevelDBLoadResult{}, err
			}
			continue
		}
		if _, err := trie.applySnapshotOperationLocked(operation.operation); err != nil {
			return LevelDBLoadResult{}, err
		}
		result.ValuesLoaded++
	}
	return result, nil
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
	if !policy.HotValuesOnly {
		return true
	}
	if policy.MaxLastHitAge < 0 || policy.MaxValueBytes < 0 {
		return false
	}
	stats := operation.entry.Stats
	if stats == nil || stats.LastHit.IsZero() {
		return false
	}
	if policy.MaxLastHitAge > 0 && now.Sub(stats.LastHit) > policy.MaxLastHitAge {
		return false
	}
	if stats.Hits < policy.MinHits {
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
		return int64(bloomFilterWordCount(entry.BloomFilter.BitCount) * 8), nil
	case "count_min_sketch":
		if entry.CountMinSketch == nil {
			return 0, errors.New("hatriecache: count-min sketch snapshot is required")
		}
		return int64(entry.CountMinSketch.Width * uint64(entry.CountMinSketch.Depth) * 4), nil
	case "hyperloglog":
		if entry.HyperLogLog == nil {
			return 0, errors.New("hatriecache: hyperloglog snapshot is required")
		}
		return int64(hyperLogLogRegisterCount(entry.HyperLogLog.Precision)), nil
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
		return int64(entry.CuckooFilter.BucketCount * uint64(entry.CuckooFilter.BucketSize) * 2), nil
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
	trie.mu.Lock()
	defer trie.mu.Unlock()

	trie.ensureOpen()
	entries := trie.entriesWithPrefixLocked("", true)
	out := make([]snapshotEntry, 0, len(entries))
	for _, entry := range entries {
		snapshotEntry, err := trie.snapshotEntryLocked(entry)
		if err != nil {
			return nil, err
		}
		out = append(out, snapshotEntry)
	}
	return out, nil
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
