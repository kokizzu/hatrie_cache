package hatriecache

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var levelDBEntryPrefix = []byte("entry:")

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
	if store == nil || store.db == nil {
		return nil
	}
	return store.db.Close()
}

func (store *LevelDBStore) Save(trie *HatTrie) error {
	entries, err := trie.levelDBEntries()
	if err != nil {
		return err
	}

	batch := new(leveldb.Batch)
	iterator := store.db.NewIterator(util.BytesPrefix(levelDBEntryPrefix), nil)
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
	return store.db.Write(batch, &opt.WriteOptions{Sync: true})
}

func (store *LevelDBStore) Load(trie *HatTrie) (int, error) {
	result, err := store.LoadWithPolicy(trie, LevelDBLoadPolicy{})
	return result.ValuesLoaded, err
}

// LoadWithPolicy restores entries from LevelDB. When HotValuesOnly is true,
// cold values are represented by lightweight references; keep store open while
// those references may be accessed or saved.
func (store *LevelDBStore) LoadWithPolicy(trie *HatTrie, policy LevelDBLoadPolicy) (LevelDBLoadResult, error) {
	iterator := store.db.NewIterator(util.BytesPrefix(levelDBEntryPrefix), nil)
	defer iterator.Release()

	now := trie.currentTime()
	type levelDBLoadOperation struct {
		operation snapshotOperation
		reference bool
	}
	operations := []levelDBLoadOperation{}
	for iterator.Next() {
		entry, err := decodeLevelDBEntry(iterator.Value())
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
	data, err := store.db.Get(levelDBKey(key), nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return snapshotEntry{}, false, nil
	}
	if err != nil {
		return snapshotEntry{}, false, err
	}
	entry, err := decodeLevelDBEntry(data)
	if err != nil {
		return snapshotEntry{}, false, err
	}
	return entry, true, nil
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
	stats := operation.entry.Stats
	if stats == nil || stats.LastHit.IsZero() {
		return false
	}
	if policy.MaxLastHitAge > 0 && now.Sub(stats.LastHit) > policy.MaxLastHitAge {
		return false
	}
	if stats.Hits <= policy.MinHits {
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
	default:
		return 0, errors.New("hatriecache: unsupported snapshot value type")
	}
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
	delete(trie.expires, entry.Key)

	idx := trie.dbrefs.Add(LevelDBReference{
		Key:   entry.Key,
		Type:  entry.Type,
		Store: store,
	})
	hval := HatValue{Index: idx, Flags: DATAVALUE_TYPE_LEVELDB_REF}
	if entry.ExpiresAt != nil {
		trie.expires[entry.Key] = *entry.ExpiresAt
		hval.Flags |= 1 << DATAVALUE_TTL_BIT_SHIFT
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
	return decodeSnapshotEntryJSON(data)
}

func levelDBKey(key string) []byte {
	out := make([]byte, 0, len(levelDBEntryPrefix)+len(key))
	out = append(out, levelDBEntryPrefix...)
	out = append(out, key...)
	return out
}
