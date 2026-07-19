package hatriecache

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
)

// PebbleStore persists HAT-trie records in a Pebble LSM database.
type PebbleStore struct {
	mu     sync.RWMutex
	path   string
	db     *pebble.DB
	format StorageFormat
}

type pebbleStoredRecord struct {
	key  string
	data []byte
	ok   bool
}

// OpenPebbleStore opens a Pebble store with the default binary record codec.
func OpenPebbleStore(path string) (*PebbleStore, error) {
	return OpenPebbleStoreWithFormat(path, DefaultStorageFormat)
}

// OpenPebbleStoreWithFormat opens a Pebble store with the selected record codec.
func OpenPebbleStoreWithFormat(path string, format StorageFormat) (*PebbleStore, error) {
	if path == "" {
		return nil, errors.New("hatriecache: pebble path is required")
	}
	format, err := ParseStorageFormat(string(format))
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, err
	}
	return &PebbleStore{path: path, db: db, format: format}, nil
}

func (store *PebbleStore) Backend() StorageBackend {
	return StorageBackendPebble
}

func (store *PebbleStore) Path() string {
	if store == nil {
		return ""
	}
	return store.path
}

func (store *PebbleStore) Format() StorageFormat {
	if store == nil {
		return ""
	}
	return store.format
}

func (store *PebbleStore) Properties() (LevelDBProperties, error) {
	db, unlock, err := store.lockDB()
	if err != nil {
		return LevelDBProperties{}, err
	}
	defer unlock()
	return pebbleProperties(db), nil
}

func (store *PebbleStore) Close() error {
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

func (store *PebbleStore) Save(trie *HatTrie) error {
	if trie == nil {
		return ErrNilHatTrie
	}
	records := make([]pebbleStoredRecord, 0, trie.Size())
	if err := trie.scanLevelDBEntryDataForStore(nil, nil, store.format, func(key string, data []byte) error {
		records = append(records, pebbleStoredRecord{key: key, data: data, ok: true})
		return nil
	}); err != nil {
		return err
	}
	db, unlock, err := store.lockDB()
	if err != nil {
		return err
	}
	defer unlock()
	return pebbleReplaceRecords(db, records)
}

func (store *PebbleStore) SaveKeys(trie *HatTrie, keys []string) error {
	return store.SaveKeysWithOptions(trie, keys, LevelDBSaveOptions{})
}

func (store *PebbleStore) SaveKeysWithOptions(trie *HatTrie, keys []string, options LevelDBSaveOptions) error {
	options, err := normalizeLevelDBSaveOptions(options)
	if err != nil {
		return err
	}
	if trie == nil {
		return ErrNilHatTrie
	}
	keys = normalizeLevelDBDirtyKeys(keys)
	if len(keys) == 0 {
		return nil
	}
	records := make([]pebbleStoredRecord, 0, len(keys))
	for _, key := range keys {
		data, ok, err := trie.levelDBEntryDataForKeyForStore(nil, nil, store.format, key)
		if err != nil {
			return err
		}
		records = append(records, pebbleStoredRecord{key: key, data: data, ok: ok})
	}
	db, unlock, err := store.lockDB()
	if err != nil {
		return err
	}
	defer unlock()
	compare := levelDBShouldCompareBeforeWrite(options.CompareBeforeWrite, len(records))
	batch := db.NewBatch()
	defer batch.Close()
	for _, record := range records {
		key := levelDBKey(record.key)
		if !record.ok {
			if err := batch.Delete(key, nil); err != nil {
				return err
			}
			continue
		}
		if compare {
			existing, exists, err := pebbleEntryDataFromDB(db, record.key)
			if err != nil {
				return err
			}
			if exists && bytes.Equal(existing, record.data) {
				continue
			}
		}
		if err := batch.Set(key, record.data, nil); err != nil {
			return err
		}
	}
	if batch.Count() == 0 {
		return nil
	}
	return batch.Commit(pebble.Sync)
}

func (store *PebbleStore) SaveDirty(trie *HatTrie, tracker *LevelDBDirtyTracker) error {
	return store.SaveDirtyWithOptions(trie, tracker, LevelDBSaveOptions{})
}

func (store *PebbleStore) SaveDirtyWithOptions(trie *HatTrie, tracker *LevelDBDirtyTracker, options LevelDBSaveOptions) error {
	if tracker == nil {
		return store.Save(trie)
	}
	snapshot := tracker.Snapshot()
	if len(snapshot.keys) == 0 {
		return nil
	}
	if err := store.SaveKeysWithOptions(trie, snapshot.keys, options); err != nil {
		return err
	}
	tracker.Clear(snapshot)
	return nil
}

func (store *PebbleStore) Load(trie *HatTrie) (int, error) {
	result, err := store.LoadWithPolicy(trie, LevelDBLoadPolicy{})
	return result.ValuesLoaded, err
}

func (store *PebbleStore) LoadWithPolicy(trie *HatTrie, policy LevelDBLoadPolicy) (LevelDBLoadResult, error) {
	db, unlock, err := store.lockDB()
	if err != nil {
		return LevelDBLoadResult{}, err
	}
	defer unlock()
	snapshot := db.NewSnapshot()
	defer snapshot.Close()
	return loadPersistentEntryData(trie, store, policy, func(visit func(snapshotEntry, []byte) error) error {
		return scanPebbleSnapshotEntryData(snapshot, visit)
	})
}

func (store *PebbleStore) Flush(trie *HatTrie) (LevelDBFlushResult, error) {
	startedAt := time.Now().UTC()
	result := LevelDBFlushResult{Store: string(StorageBackendPebble), StartedAt: startedAt}
	err := store.Save(trie)
	result.FinishedAt = time.Now().UTC()
	result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
	if err != nil {
		return result, err
	}
	result.Keys = trie.Size()
	return result, nil
}

func (store *PebbleStore) SpillCold(trie *HatTrie, options LevelDBSpillOptions) (LevelDBSpillResult, error) {
	startedAt := time.Now().UTC()
	result := LevelDBSpillResult{
		Store:         string(StorageBackendPebble),
		MaxHotBytes:   options.MaxHotBytes,
		MinValueBytes: options.MinValueBytes,
		StartedAt:     startedAt,
	}
	finish := func(err error) (LevelDBSpillResult, error) {
		result.FinishedAt = time.Now().UTC()
		result.DurationMillis = result.FinishedAt.Sub(result.StartedAt).Milliseconds()
		return result, err
	}
	if trie == nil {
		return finish(ErrNilHatTrie)
	}
	if options.MaxHotBytes < 0 || options.MinValueBytes < 0 {
		return finish(errors.New("hatriecache: pebble spill limits must be non-negative"))
	}
	db, unlock, err := store.lockDB()
	if err != nil {
		return finish(err)
	}
	defer unlock()
	trie.mu.Lock()
	defer trie.mu.Unlock()
	if err := trie.spillColdPebbleLocked(store, db, options, &result); err != nil {
		return finish(err)
	}
	return finish(nil)
}

func (store *PebbleStore) Compact(options LevelDBCompactionOptions) (LevelDBCompactionResult, error) {
	startedAt := time.Now().UTC()
	result := LevelDBCompactionResult{
		Store:     string(StorageBackendPebble),
		StartKey:  options.StartKey,
		LimitKey:  options.LimitKey,
		StartedAt: startedAt,
	}
	db, unlock, err := store.lockDB()
	if err != nil {
		return result, err
	}
	defer unlock()
	result.SizeBytesBefore, err = directorySizeBytes(store.path)
	if err == nil {
		result.PropertiesBefore = pebbleProperties(db)
		rng := levelDBCompactionRange(options)
		err = db.Compact(rng.Start, rng.Limit, true)
	}
	if err == nil {
		result.SizeBytesAfter, err = directorySizeBytes(store.path)
	}
	result.SizeBytesDelta = result.SizeBytesAfter - result.SizeBytesBefore
	result.PropertiesAfter = pebbleProperties(db)
	result.FinishedAt = time.Now().UTC()
	result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
	return result, err
}

func (store *PebbleStore) Entry(key string) (snapshotEntry, bool, error) {
	data, ok, err := store.entryData(key)
	if err != nil || !ok {
		return snapshotEntry{}, ok, err
	}
	entry, err := decodeLevelDBEntryForKey(key, data)
	if err != nil {
		return snapshotEntry{}, false, err
	}
	return materializeSnapshotEntryBytes(entry), true, nil
}

func (store *PebbleStore) entryData(key string) ([]byte, bool, error) {
	db, unlock, err := store.lockDB()
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	return pebbleEntryDataFromDB(db, key)
}

func (store *PebbleStore) lockDB() (*pebble.DB, func(), error) {
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

func pebbleReplaceRecords(db *pebble.DB, records []pebbleStoredRecord) error {
	upper := bytesPrefixLimit(levelDBEntryPrefix)
	iterator, err := db.NewIter(&pebble.IterOptions{LowerBound: levelDBEntryPrefix, UpperBound: upper})
	if err != nil {
		return err
	}
	defer iterator.Close()
	batch := db.NewBatch()
	defer batch.Close()
	hasExisting := iterator.First()
	for _, record := range records {
		key := levelDBKey(record.key)
		for hasExisting && bytes.Compare(iterator.Key(), key) < 0 {
			if err := batch.Delete(cloneBytes(iterator.Key()), nil); err != nil {
				return err
			}
			hasExisting = iterator.Next()
		}
		if hasExisting && bytes.Equal(iterator.Key(), key) {
			if !bytes.Equal(iterator.Value(), record.data) {
				if err := batch.Set(key, record.data, nil); err != nil {
					return err
				}
			}
			hasExisting = iterator.Next()
			continue
		}
		if err := batch.Set(key, record.data, nil); err != nil {
			return err
		}
	}
	for hasExisting {
		if err := batch.Delete(cloneBytes(iterator.Key()), nil); err != nil {
			return err
		}
		hasExisting = iterator.Next()
	}
	if err := iterator.Error(); err != nil {
		return err
	}
	if batch.Count() == 0 {
		return nil
	}
	return batch.Commit(pebble.Sync)
}

func pebbleEntryDataFromDB(db *pebble.DB, key string) ([]byte, bool, error) {
	data, closer, err := db.Get(levelDBKey(key))
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return cloneBytes(data), true, nil
}

func scanPebbleSnapshotEntryData(snapshot *pebble.Snapshot, visit func(snapshotEntry, []byte) error) error {
	iterator, err := snapshot.NewIter(&pebble.IterOptions{
		LowerBound: levelDBEntryPrefix,
		UpperBound: bytesPrefixLimit(levelDBEntryPrefix),
	})
	if err != nil {
		return err
	}
	defer iterator.Close()
	for valid := iterator.First(); valid; valid = iterator.Next() {
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

func (trie *HatTrie) spillColdPebbleLocked(store *PebbleStore, db *pebble.DB, options LevelDBSpillOptions, result *LevelDBSpillResult) error {
	trie.ensureOpen()
	now := time.Time{}
	if len(trie.expires) > 0 {
		now = trie.currentTime()
	}
	candidates, err := trie.levelDBSpillCandidatesLocked(now, options, result)
	if err != nil {
		return err
	}
	result.HotBytesAfter = result.HotBytesBefore
	if result.HotBytesAfter <= options.MaxHotBytes || len(candidates) == 0 {
		return nil
	}
	sort.Slice(candidates, func(i, j int) bool { return levelDBSpillCandidateLess(candidates[i], candidates[j]) })
	batch := db.NewBatch()
	defer batch.Close()
	prepared := make([]levelDBPreparedSpill, 0, len(candidates))
	selectedHotBytesAfter := result.HotBytesAfter
	for _, candidate := range candidates {
		if selectedHotBytesAfter <= options.MaxHotBytes {
			break
		}
		rawPtr := trie.tryLocation(candidate.key)
		if rawPtr == nil {
			continue
		}
		current := HatValue{}
		current.fromValue(*rawPtr)
		if current != candidate.value || current.IsLevelDBReference() {
			continue
		}
		data, err := trie.levelDBEntryDataForStoreLocked(Entry{Key: candidate.key, Value: current}, nil, nil, store.format)
		if err != nil {
			return err
		}
		entry, err := decodeLevelDBEntryForKey(candidate.key, data)
		if err != nil {
			return err
		}
		if err := batch.Set(levelDBKey(candidate.key), data, nil); err != nil {
			return err
		}
		prepared = append(prepared, levelDBPreparedSpill{candidate: candidate, entry: entry, data: data})
		selectedHotBytesAfter -= candidate.valueBytes
	}
	if len(prepared) == 0 {
		return nil
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	result.WriteBatches++
	for _, item := range prepared {
		if _, err := trie.applyLevelDBReferenceLocked(store, item.entry, item.data); err != nil {
			return err
		}
		result.KeysSpilled++
		result.BytesSpilled += item.candidate.valueBytes
		result.HotBytesAfter -= item.candidate.valueBytes
	}
	return nil
}

func pebbleProperties(db *pebble.DB) LevelDBProperties {
	if db == nil {
		return LevelDBProperties{}
	}
	metrics := db.Metrics()
	return LevelDBProperties{
		Stats:    metrics.String(),
		SSTables: fmt.Sprintf("levels=%d", len(metrics.Levels)),
	}
}
