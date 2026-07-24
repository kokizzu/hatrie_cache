package hatriecache

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc64"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
)

var levelDBEntryPrefix = []byte("entry:")
var persistentAppliedJournalSequenceKey = []byte("\x00hatrie-cache:applied-journal-sequence")
var levelDBRecordChecksumTable = crc64.MakeTable(crc64.ISO)

var ErrLevelDBStoreClosed = errors.New("hatriecache: leveldb store is closed")

const (
	levelDBCompareBeforeWriteAutoKeyLimit = 1024
	levelDBDirtyTrackerInlineLimit        = 32
)

type LevelDBCompareBeforeWriteMode string

const (
	LevelDBCompareBeforeWriteAuto   LevelDBCompareBeforeWriteMode = "auto"
	LevelDBCompareBeforeWriteAlways LevelDBCompareBeforeWriteMode = "always"
	LevelDBCompareBeforeWriteNever  LevelDBCompareBeforeWriteMode = "never"
)

const DefaultLevelDBCompareBeforeWriteMode = LevelDBCompareBeforeWriteAuto

type LevelDBSaveOptions struct {
	CompareBeforeWrite LevelDBCompareBeforeWriteMode `json:"compare_before_write,omitempty"`
}

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

// LevelDBCompactionOptions controls a manual LevelDB compaction. StartKey and
// LimitKey are cache keys, not raw LevelDB keys. Empty values compact all cache
// entry records.
type LevelDBCompactionOptions struct {
	StartKey string `json:"start_key,omitempty"`
	LimitKey string `json:"limit_key,omitempty"`
}

// LevelDBCompactionResult reports a completed manual LevelDB compaction.
type LevelDBCompactionResult struct {
	Store            string            `json:"store"`
	StartKey         string            `json:"start_key,omitempty"`
	LimitKey         string            `json:"limit_key,omitempty"`
	SizeBytesBefore  int64             `json:"size_bytes_before"`
	SizeBytesAfter   int64             `json:"size_bytes_after"`
	SizeBytesDelta   int64             `json:"size_bytes_delta"`
	PropertiesBefore LevelDBProperties `json:"properties_before"`
	PropertiesAfter  LevelDBProperties `json:"properties_after"`
	StartedAt        time.Time         `json:"started_at"`
	FinishedAt       time.Time         `json:"finished_at"`
	DurationMillis   int64             `json:"duration_millis"`
}

// LevelDBProperties reports selected LevelDB engine property snapshots.
type LevelDBProperties struct {
	Stats      string `json:"stats,omitempty"`
	SSTables   string `json:"sstables,omitempty"`
	WriteDelay string `json:"write_delay,omitempty"`
	BlockPool  string `json:"block_pool,omitempty"`
}

// LevelDBFlushResult reports a completed manual LevelDB save/flush.
type LevelDBFlushResult struct {
	Store          string    `json:"store"`
	Keys           int       `json:"keys"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
	DurationMillis int64     `json:"duration_millis"`
}

// LevelDBSpillOptions controls conversion of materialized in-memory values
// into lazy LevelDB references.
type LevelDBSpillOptions struct {
	MaxHotBytes   int64 `json:"max_hot_bytes"`
	MinValueBytes int64 `json:"min_value_bytes,omitempty"`
}

// LevelDBSpillResult reports a completed hot-value spill pass.
type LevelDBSpillResult struct {
	Store          string    `json:"store"`
	MaxHotBytes    int64     `json:"max_hot_bytes"`
	MinValueBytes  int64     `json:"min_value_bytes"`
	KeysScanned    int       `json:"keys_scanned"`
	ValuesScanned  int       `json:"values_scanned"`
	KeysSpilled    int       `json:"keys_spilled"`
	WriteBatches   int       `json:"write_batches"`
	HotBytesBefore int64     `json:"hot_bytes_before"`
	HotBytesAfter  int64     `json:"hot_bytes_after"`
	BytesSpilled   int64     `json:"bytes_spilled"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at"`
	DurationMillis int64     `json:"duration_millis"`
}

// LevelDBDirtyTracker tracks keys that changed since the last successful
// incremental LevelDB save.
type LevelDBDirtyTracker struct {
	mu     sync.Mutex
	seq    uint64
	keys   map[string]uint64
	inline []levelDBDirtyKeyMark
}

type levelDBDirtyKeyMark struct {
	key string
	seq uint64
}

type LevelDBDirtySnapshot struct {
	seq  uint64
	keys []string
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

func NewLevelDBDirtyTracker() *LevelDBDirtyTracker {
	return &LevelDBDirtyTracker{}
}

func ParseLevelDBCompareBeforeWriteMode(value string) (LevelDBCompareBeforeWriteMode, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", string(LevelDBCompareBeforeWriteAuto):
		return LevelDBCompareBeforeWriteAuto, nil
	case string(LevelDBCompareBeforeWriteAlways):
		return LevelDBCompareBeforeWriteAlways, nil
	case string(LevelDBCompareBeforeWriteNever):
		return LevelDBCompareBeforeWriteNever, nil
	default:
		return "", fmt.Errorf("hatriecache: db compare before write must be auto, always, or never")
	}
}

func normalizeLevelDBSaveOptions(options LevelDBSaveOptions) (LevelDBSaveOptions, error) {
	mode, err := ParseLevelDBCompareBeforeWriteMode(string(options.CompareBeforeWrite))
	if err != nil {
		return LevelDBSaveOptions{}, err
	}
	options.CompareBeforeWrite = mode
	return options, nil
}

func (tracker *LevelDBDirtyTracker) Mark(key string) {
	if tracker == nil {
		return
	}
	if err := validateKey(key); err != nil {
		return
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	tracker.seq++
	if tracker.keys != nil {
		tracker.keys[key] = tracker.seq
		return
	}
	for idx := range tracker.inline {
		if tracker.inline[idx].key == key {
			tracker.inline[idx].seq = tracker.seq
			return
		}
	}
	if len(tracker.inline) < levelDBDirtyTrackerInlineLimit {
		tracker.inline = append(tracker.inline, levelDBDirtyKeyMark{key: key, seq: tracker.seq})
		return
	}
	tracker.promoteDirtyKeysLocked(len(tracker.inline) + 1)
	tracker.keys[key] = tracker.seq
}

func (tracker *LevelDBDirtyTracker) Pending() int {
	if tracker == nil {
		return 0
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.keys == nil {
		return len(tracker.inline)
	}
	return len(tracker.keys)
}

func (tracker *LevelDBDirtyTracker) markCommand(request CacheCommandRequest) {
	if tracker == nil || !commandShouldJournal(request) {
		return
	}
	tracker.Mark(strings.TrimSpace(request.Key))
}

func (tracker *LevelDBDirtyTracker) Snapshot() LevelDBDirtySnapshot {
	if tracker == nil {
		return LevelDBDirtySnapshot{}
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.keys == nil {
		keys := make([]string, len(tracker.inline))
		for idx, mark := range tracker.inline {
			keys[idx] = mark.key
		}
		sort.Strings(keys)
		return LevelDBDirtySnapshot{seq: tracker.seq, keys: keys}
	}
	keys := make([]string, 0, len(tracker.keys))
	for key := range tracker.keys {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return LevelDBDirtySnapshot{seq: tracker.seq, keys: keys}
}

func (tracker *LevelDBDirtyTracker) Clear(snapshot LevelDBDirtySnapshot) {
	if tracker == nil || len(snapshot.keys) == 0 {
		return
	}
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.keys == nil {
		kept := tracker.inline[:0]
		for _, mark := range tracker.inline {
			if mark.seq <= snapshot.seq && sortedStringsContains(snapshot.keys, mark.key) {
				continue
			}
			kept = append(kept, mark)
		}
		for idx := len(kept); idx < len(tracker.inline); idx++ {
			tracker.inline[idx] = levelDBDirtyKeyMark{}
		}
		tracker.inline = kept
		return
	}
	for _, key := range snapshot.keys {
		if tracker.keys[key] <= snapshot.seq {
			delete(tracker.keys, key)
		}
	}
	if len(snapshot.keys) <= levelDBDirtyTrackerInlineLimit && len(tracker.keys) <= levelDBDirtyTrackerInlineLimit {
		tracker.demoteDirtyKeysLocked()
	}
}

func (tracker *LevelDBDirtyTracker) promoteDirtyKeysLocked(capacity int) {
	keys := make(map[string]uint64, capacity)
	for _, mark := range tracker.inline {
		keys[mark.key] = mark.seq
	}
	tracker.inline = nil
	tracker.keys = keys
}

func (tracker *LevelDBDirtyTracker) demoteDirtyKeysLocked() {
	if len(tracker.keys) == 0 {
		tracker.keys = nil
		tracker.inline = tracker.inline[:0]
		return
	}
	inline := make([]levelDBDirtyKeyMark, 0, len(tracker.keys))
	for key, seq := range tracker.keys {
		inline = append(inline, levelDBDirtyKeyMark{key: key, seq: seq})
	}
	tracker.keys = nil
	tracker.inline = inline
}

func sortedStringsContains(values []string, value string) bool {
	idx := sort.SearchStrings(values, value)
	return idx < len(values) && values[idx] == value
}

type LevelDBStore struct {
	mu     sync.RWMutex
	path   string
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
	return &LevelDBStore{path: path, db: db, format: format}, nil
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
	return store.saveWithJournalSequence(trie, nil)
}

func (store *LevelDBStore) SaveWithJournalSequence(trie *HatTrie, sequence uint64) error {
	return store.saveWithJournalSequence(trie, &sequence)
}

func (store *LevelDBStore) saveWithJournalSequence(trie *HatTrie, sequence *uint64) error {
	db, unlock, err := store.lockDB()
	if err != nil {
		return err
	}
	defer unlock()
	if trie == nil {
		return ErrNilHatTrie
	}

	batch, err := levelDBDiffBatch(store, db, trie)
	if err != nil {
		return err
	}
	if sequence != nil {
		batch.Put(persistentAppliedJournalSequenceKey, encodePersistentJournalSequence(*sequence))
	} else if batch.Len() > 0 {
		batch.Delete(persistentAppliedJournalSequenceKey)
	}
	if batch.Len() == 0 {
		return nil
	}
	return db.Write(batch, &opt.WriteOptions{Sync: true})
}

func (store *LevelDBStore) SaveKeys(trie *HatTrie, keys []string) error {
	return store.SaveKeysWithOptions(trie, keys, LevelDBSaveOptions{})
}

func (store *LevelDBStore) SaveKeysWithOptions(trie *HatTrie, keys []string, options LevelDBSaveOptions) error {
	options, err := normalizeLevelDBSaveOptions(options)
	if err != nil {
		return err
	}
	db, unlock, err := store.lockDB()
	if err != nil {
		return err
	}
	defer unlock()
	if trie == nil {
		return ErrNilHatTrie
	}

	keys = normalizeLevelDBDirtyKeys(keys)
	if len(keys) == 0 {
		return nil
	}
	compareBeforeWrite := levelDBShouldCompareBeforeWrite(options.CompareBeforeWrite, len(keys))
	batch := new(leveldb.Batch)
	for _, key := range keys {
		data, ok, err := trie.levelDBEntryDataForKeyForStore(store, db, store.format, key)
		if err != nil {
			return err
		}
		dbKey := levelDBKey(key)
		if !ok {
			batch.Delete(dbKey)
			continue
		}
		if !compareBeforeWrite {
			batch.Put(dbKey, data)
			continue
		}
		existing, exists, err := store.entryDataFromDB(db, key)
		if err != nil {
			return err
		}
		if !exists || !bytes.Equal(existing, data) {
			batch.Put(dbKey, data)
		}
	}
	if batch.Len() > 0 {
		batch.Delete(persistentAppliedJournalSequenceKey)
	}
	if batch.Len() == 0 {
		return nil
	}
	return db.Write(batch, &opt.WriteOptions{Sync: true})
}

func (store *LevelDBStore) SaveDirty(trie *HatTrie, tracker *LevelDBDirtyTracker) error {
	return store.SaveDirtyWithOptions(trie, tracker, LevelDBSaveOptions{})
}

func (store *LevelDBStore) SaveDirtyWithOptions(trie *HatTrie, tracker *LevelDBDirtyTracker, options LevelDBSaveOptions) error {
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

func (store *LevelDBStore) SaveDirtyWithJournalSequence(trie *HatTrie, tracker *LevelDBDirtyTracker, options LevelDBSaveOptions, sequence uint64) error {
	if tracker == nil {
		return store.SaveWithJournalSequence(trie, sequence)
	}
	snapshot := tracker.Snapshot()
	if err := store.saveKeysAndJournalSequence(trie, snapshot.keys, options, sequence); err != nil {
		return err
	}
	tracker.Clear(snapshot)
	return nil
}

func (store *LevelDBStore) saveKeysAndJournalSequence(trie *HatTrie, keys []string, options LevelDBSaveOptions, sequence uint64) error {
	options, err := normalizeLevelDBSaveOptions(options)
	if err != nil {
		return err
	}
	db, unlock, err := store.lockDB()
	if err != nil {
		return err
	}
	defer unlock()
	if trie == nil {
		return ErrNilHatTrie
	}

	keys = normalizeLevelDBDirtyKeys(keys)
	if len(keys) == 0 {
		data, getErr := db.Get(persistentAppliedJournalSequenceKey, nil)
		if getErr == nil {
			current, decodeErr := decodePersistentJournalSequence(data)
			if decodeErr != nil {
				return decodeErr
			}
			if current == sequence {
				return nil
			}
		} else if !errors.Is(getErr, leveldb.ErrNotFound) {
			return getErr
		}
	}
	compareBeforeWrite := levelDBShouldCompareBeforeWrite(options.CompareBeforeWrite, len(keys))
	batch := new(leveldb.Batch)
	for _, key := range keys {
		data, ok, err := trie.levelDBEntryDataForKeyForStore(store, db, store.format, key)
		if err != nil {
			return err
		}
		dbKey := levelDBKey(key)
		if !ok {
			batch.Delete(dbKey)
			continue
		}
		if !compareBeforeWrite {
			batch.Put(dbKey, data)
			continue
		}
		existing, exists, err := store.entryDataFromDB(db, key)
		if err != nil {
			return err
		}
		if !exists || !bytes.Equal(existing, data) {
			batch.Put(dbKey, data)
		}
	}
	batch.Put(persistentAppliedJournalSequenceKey, encodePersistentJournalSequence(sequence))
	return db.Write(batch, &opt.WriteOptions{Sync: true})
}

func (store *LevelDBStore) AppliedJournalSequence() (uint64, bool, error) {
	db, unlock, err := store.lockDB()
	if err != nil {
		return 0, false, err
	}
	defer unlock()
	data, err := db.Get(persistentAppliedJournalSequenceKey, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	sequence, err := decodePersistentJournalSequence(data)
	return sequence, err == nil, err
}

func encodePersistentJournalSequence(sequence uint64) []byte {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], sequence)
	return data[:]
}

func decodePersistentJournalSequence(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, errors.New("hatriecache: invalid applied journal sequence metadata")
	}
	return binary.BigEndian.Uint64(data), nil
}

func levelDBShouldCompareBeforeWrite(mode LevelDBCompareBeforeWriteMode, keys int) bool {
	switch mode {
	case LevelDBCompareBeforeWriteAlways:
		return true
	case LevelDBCompareBeforeWriteNever:
		return false
	default:
		return keys <= levelDBCompareBeforeWriteAutoKeyLimit
	}
}

func normalizeLevelDBDirtyKeys(keys []string) []string {
	if len(keys) == 0 {
		return nil
	}
	out := make([]string, 0, len(keys))
	seen := map[string]struct{}{}
	for _, key := range keys {
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func (store *LevelDBStore) Flush(trie *HatTrie) (LevelDBFlushResult, error) {
	startedAt := time.Now().UTC()
	result := LevelDBFlushResult{
		Store:     "leveldb",
		StartedAt: startedAt,
	}
	err := store.Save(trie)
	result.FinishedAt = time.Now().UTC()
	result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
	if err != nil {
		return result, err
	}
	result.Keys = trie.Size()
	return result, nil
}

// SpillCold persists cold materialized values to LevelDB and replaces them
// with lazy references until estimated in-memory hot value bytes are below the
// configured cap or no eligible values remain.
func (store *LevelDBStore) SpillCold(trie *HatTrie, options LevelDBSpillOptions) (LevelDBSpillResult, error) {
	startedAt := time.Now().UTC()
	result := LevelDBSpillResult{
		Store:         "leveldb",
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
	if options.MaxHotBytes < 0 {
		return finish(errors.New("hatriecache: leveldb spill max hot bytes must be non-negative"))
	}
	if options.MinValueBytes < 0 {
		return finish(errors.New("hatriecache: leveldb spill min value bytes must be non-negative"))
	}
	db, unlock, err := store.lockDB()
	if err != nil {
		return finish(err)
	}
	defer unlock()
	if partitions := trie.localPartitionSet(); partitions != nil {
		limits, hotBytes, probe, err := localPartitionSpillPlan(partitions, options)
		if err != nil {
			return finish(err)
		}
		result.KeysScanned = probe.KeysScanned
		result.ValuesScanned = probe.ValuesScanned
		result.HotBytesBefore = probe.HotBytesBefore
		result.HotBytesAfter = probe.HotBytesBefore
		if result.HotBytesBefore <= options.MaxHotBytes {
			return finish(nil)
		}
		for index, child := range partitions.tries {
			if hotBytes[index] <= limits[index] {
				continue
			}
			childOptions := options
			childOptions.MaxHotBytes = limits[index]
			childResult := LevelDBSpillResult{}
			child.mu.Lock()
			err = child.spillColdLevelDBLocked(store, db, childOptions, &childResult)
			child.mu.Unlock()
			if err != nil {
				return finish(err)
			}
			result.KeysScanned += childResult.KeysScanned
			result.ValuesScanned += childResult.ValuesScanned
			result.KeysSpilled += childResult.KeysSpilled
			result.WriteBatches += childResult.WriteBatches
			result.BytesSpilled += childResult.BytesSpilled
			result.HotBytesAfter -= childResult.HotBytesBefore - childResult.HotBytesAfter
		}
		return finish(nil)
	}

	trie.mu.Lock()
	defer trie.mu.Unlock()
	if err := trie.spillColdLevelDBLocked(store, db, options, &result); err != nil {
		return finish(err)
	}
	return finish(nil)
}

func localPartitionSpillPlan(partitions *localPartitionSet, options LevelDBSpillOptions) ([]int64, []int64, LevelDBSpillResult, error) {
	hotBytes := make([]int64, len(partitions.tries))
	probe := LevelDBSpillResult{}
	for index, child := range partitions.tries {
		childResult := LevelDBSpillResult{}
		child.mu.Lock()
		child.ensureOpen()
		now := time.Time{}
		if len(child.expires) > 0 {
			now = child.currentTime()
		}
		_, err := child.levelDBSpillCandidatesLocked(now, options, &childResult)
		child.mu.Unlock()
		if err != nil {
			return nil, nil, LevelDBSpillResult{}, err
		}
		hotBytes[index] = childResult.HotBytesBefore
		probe.KeysScanned += childResult.KeysScanned
		probe.ValuesScanned += childResult.ValuesScanned
		probe.HotBytesBefore += childResult.HotBytesBefore
	}
	limits := make([]int64, len(hotBytes))
	if probe.HotBytesBefore <= options.MaxHotBytes {
		copy(limits, hotBytes)
		return limits, hotBytes, probe, nil
	}
	active := make([]bool, len(hotBytes))
	for index := range active {
		active[index] = true
	}
	remaining := options.MaxHotBytes
	activeCount := len(active)
	for activeCount > 0 {
		share := remaining / int64(activeCount)
		removed := false
		for index, enabled := range active {
			if enabled && hotBytes[index] <= share {
				limits[index] = hotBytes[index]
				remaining -= hotBytes[index]
				active[index] = false
				activeCount--
				removed = true
			}
		}
		if removed {
			continue
		}
		extra := remaining % int64(activeCount)
		for index, enabled := range active {
			if !enabled {
				continue
			}
			limits[index] = share
			if extra > 0 {
				limits[index]++
				extra--
			}
		}
		break
	}
	return limits, hotBytes, probe, nil
}

func (store *LevelDBStore) Compact(options LevelDBCompactionOptions) (LevelDBCompactionResult, error) {
	startedAt := time.Now().UTC()
	result := LevelDBCompactionResult{
		Store:     "leveldb",
		StartKey:  options.StartKey,
		LimitKey:  options.LimitKey,
		StartedAt: startedAt,
	}
	db, unlock, err := store.lockDB()
	if err != nil {
		result.FinishedAt = time.Now().UTC()
		result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
		return result, err
	}
	defer unlock()

	result.SizeBytesBefore, err = directorySizeBytes(store.path)
	if err != nil {
		result.FinishedAt = time.Now().UTC()
		result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
		return result, err
	}
	result.PropertiesBefore = levelDBProperties(db)
	compactErr := db.CompactRange(levelDBCompactionRange(options))
	result.SizeBytesAfter, err = directorySizeBytes(store.path)
	if err != nil {
		result.FinishedAt = time.Now().UTC()
		result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
		return result, err
	}
	result.SizeBytesDelta = result.SizeBytesAfter - result.SizeBytesBefore
	result.PropertiesAfter = levelDBProperties(db)
	result.FinishedAt = time.Now().UTC()
	result.DurationMillis = result.FinishedAt.Sub(startedAt).Milliseconds()
	return result, compactErr
}

func levelDBProperties(db *leveldb.DB) LevelDBProperties {
	if db == nil {
		return LevelDBProperties{}
	}
	property := func(name string) string {
		value, err := db.GetProperty(name)
		if err != nil {
			return ""
		}
		return value
	}
	return LevelDBProperties{
		Stats:      property("leveldb.stats"),
		SSTables:   property("leveldb.sstables"),
		WriteDelay: property("leveldb.writedelay"),
		BlockPool:  property("leveldb.blockpool"),
	}
}

func directorySizeBytes(path string) (int64, error) {
	if strings.TrimSpace(path) == "" {
		return 0, nil
	}
	var size int64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if info == nil || info.IsDir() {
			return nil
		}
		size += info.Size()
		return nil
	})
	return size, err
}

func levelDBCompactionRange(options LevelDBCompactionOptions) util.Range {
	rng := util.Range{
		Start: cloneBytes(levelDBEntryPrefix),
		Limit: bytesPrefixLimit(levelDBEntryPrefix),
	}
	if options.StartKey != "" {
		rng.Start = levelDBKey(options.StartKey)
	}
	if options.LimitKey != "" {
		rng.Limit = levelDBKey(options.LimitKey)
	}
	return rng
}

func bytesPrefixLimit(prefix []byte) []byte {
	limit := cloneBytes(prefix)
	for idx := len(limit) - 1; idx >= 0; idx-- {
		if limit[idx] < 0xff {
			limit[idx]++
			return limit[:idx+1]
		}
	}
	return nil
}

func levelDBDiffBatch(store *LevelDBStore, db *leveldb.DB, trie *HatTrie) (*leveldb.Batch, error) {
	if trie == nil {
		return nil, ErrNilHatTrie
	}
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

type levelDBSpillCandidate struct {
	key        string
	value      HatValue
	valueBytes int64
	hits       uint64
	lastHit    time.Time
}

type levelDBPreparedSpill struct {
	candidate levelDBSpillCandidate
	entry     snapshotEntry
	data      []byte
}

func (trie *HatTrie) spillColdLevelDBLocked(store *LevelDBStore, db *leveldb.DB, options LevelDBSpillOptions, result *LevelDBSpillResult) error {
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
	sort.Slice(candidates, func(i, j int) bool {
		return levelDBSpillCandidateLess(candidates[i], candidates[j])
	})
	batch := new(leveldb.Batch)
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
		data, err := trie.levelDBEntryDataForStoreLocked(Entry{Key: candidate.key, Value: current}, store, db, store.format)
		if err != nil {
			return err
		}
		entry, err := decodeLevelDBEntryForKey(candidate.key, data)
		if err != nil {
			return err
		}
		batch.Put(levelDBKey(candidate.key), data)
		prepared = append(prepared, levelDBPreparedSpill{
			candidate: candidate,
			entry:     entry,
			data:      data,
		})
		selectedHotBytesAfter -= candidate.valueBytes
	}
	if len(prepared) == 0 {
		return nil
	}
	if err := db.Write(batch, &opt.WriteOptions{Sync: true}); err != nil {
		return err
	}
	result.WriteBatches++
	for _, preparedSpill := range prepared {
		if _, err := trie.applyLevelDBReferenceLocked(store, preparedSpill.entry, preparedSpill.data); err != nil {
			return err
		}
		result.KeysSpilled++
		result.BytesSpilled += preparedSpill.candidate.valueBytes
		result.HotBytesAfter -= preparedSpill.candidate.valueBytes
	}
	return nil
}

func (trie *HatTrie) levelDBSpillCandidatesLocked(now time.Time, options LevelDBSpillOptions, result *LevelDBSpillResult) ([]levelDBSpillCandidate, error) {
	if trie.levelDBSpillKeys == nil {
		return trie.levelDBSpillCandidatesFromFullScanLocked(now, options, result)
	}
	return trie.levelDBSpillCandidatesFromIndexLocked(options, result)
}

func (trie *HatTrie) levelDBSpillCandidatesFromFullScanLocked(now time.Time, options LevelDBSpillOptions, result *LevelDBSpillResult) ([]levelDBSpillCandidate, error) {
	index := make(map[string]struct{})
	hotValues := make(map[string]int64)
	candidates := []levelDBSpillCandidate{}
	if err := trie.scanEntriesWithPrefixAtLockedChecked("", true, now, func(entry Entry) error {
		result.KeysScanned++
		valueBytes, err := trie.levelDBHotValueBytesLocked(entry)
		if err != nil {
			return err
		}
		if valueBytes <= 0 {
			return nil
		}
		index[entry.Key] = struct{}{}
		hotValues[entry.Key] = valueBytes
		trie.appendLevelDBSpillCandidateLocked(&candidates, entry, valueBytes, options, result, true)
		return nil
	}); err != nil {
		return nil, err
	}
	trie.levelDBSpillKeys = index
	trie.setLevelDBHotByteAccountingLocked(hotValues, result.HotBytesBefore)
	return candidates, nil
}

func (trie *HatTrie) levelDBSpillCandidatesFromIndexLocked(options LevelDBSpillOptions, result *LevelDBSpillResult) ([]levelDBSpillCandidate, error) {
	accountHotBytes := trie.levelDBHotValues == nil
	if !accountHotBytes {
		result.HotBytesBefore = trie.levelDBHotBytes
		if result.HotBytesBefore <= options.MaxHotBytes {
			return nil, nil
		}
	}
	var hotValues map[string]int64
	if accountHotBytes {
		hotValues = make(map[string]int64, len(trie.levelDBSpillKeys))
	}
	candidates := []levelDBSpillCandidate{}
	for key := range trie.levelDBSpillKeys {
		result.KeysScanned++
		rawPtr := trie.tryLocation(key)
		if rawPtr == nil {
			trie.deleteLevelDBSpillCandidateLocked(key)
			continue
		}
		hval := HatValue{}
		hval.fromValue(*rawPtr)
		if trie.expireIfNeededLocked(key, hval) {
			continue
		}
		valueBytes, err := trie.levelDBHotValueBytesLocked(Entry{Key: key, Value: hval})
		if err != nil {
			return nil, err
		}
		if valueBytes <= 0 {
			trie.deleteLevelDBSpillCandidateLocked(key)
			continue
		}
		if accountHotBytes {
			hotValues[key] = valueBytes
		}
		trie.appendLevelDBSpillCandidateLocked(&candidates, Entry{Key: key, Value: hval}, valueBytes, options, result, accountHotBytes)
	}
	if accountHotBytes {
		trie.setLevelDBHotByteAccountingLocked(hotValues, result.HotBytesBefore)
	} else {
		result.HotBytesBefore = trie.levelDBHotBytes
	}
	return candidates, nil
}

func (trie *HatTrie) appendLevelDBSpillCandidateLocked(candidates *[]levelDBSpillCandidate, entry Entry, valueBytes int64, options LevelDBSpillOptions, result *LevelDBSpillResult, accountHotBytes bool) {
	result.ValuesScanned++
	if accountHotBytes {
		result.HotBytesBefore += valueBytes
	}
	if valueBytes < options.MinValueBytes {
		return
	}
	candidate := levelDBSpillCandidate{
		key:        entry.Key,
		value:      entry.Value,
		valueBytes: valueBytes,
	}
	if stats := trie.keyStats[entry.Key]; stats != nil {
		candidate.hits = stats.Hits
		candidate.lastHit = expandedKeyStatsTime(stats.lastHitSec, stats.lastHitNsec)
	}
	*candidates = append(*candidates, candidate)
}

func (trie *HatTrie) updateLevelDBSpillCandidateForKeyLocked(key string) {
	if trie.levelDBSpillKeys == nil {
		return
	}
	rawPtr := trie.tryLocation(key)
	if rawPtr == nil {
		trie.deleteLevelDBSpillCandidateLocked(key)
		return
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	trie.updateLevelDBSpillCandidateLocked(key, hval)
}

func (trie *HatTrie) updateLevelDBSpillCandidateLocked(key string, hval HatValue) {
	if trie.levelDBSpillKeys == nil {
		return
	}
	if trie.levelDBPotentialSpillCandidateLocked(hval) {
		trie.levelDBSpillKeys[key] = struct{}{}
		return
	}
	trie.deleteLevelDBSpillCandidateLocked(key)
}

func (trie *HatTrie) deleteLevelDBSpillCandidateLocked(key string) {
	if trie.levelDBSpillKeys != nil {
		delete(trie.levelDBSpillKeys, key)
	}
}

func (trie *HatTrie) setLevelDBHotByteAccountingLocked(values map[string]int64, total int64) {
	if values == nil {
		values = map[string]int64{}
	}
	if total < 0 {
		total = 0
	}
	trie.levelDBHotValues = values
	trie.levelDBHotBytes = total
}

func (trie *HatTrie) invalidateLevelDBHotByteAccountingLocked() {
	trie.levelDBHotValues = nil
	trie.levelDBHotBytes = 0
}

func (trie *HatTrie) updateLevelDBHotByteAccountingForKeyLocked(key string) {
	if trie.levelDBHotValues == nil {
		return
	}
	oldBytes := trie.levelDBHotValues[key]
	valueBytes, err := trie.levelDBCurrentHotValueBytesLocked(key)
	if err != nil {
		trie.invalidateLevelDBHotByteAccountingLocked()
		return
	}
	if valueBytes > 0 {
		trie.levelDBHotValues[key] = valueBytes
	} else {
		delete(trie.levelDBHotValues, key)
	}
	trie.levelDBHotBytes += valueBytes - oldBytes
	if trie.levelDBHotBytes < 0 {
		trie.levelDBHotBytes = 0
	}
}

func (trie *HatTrie) deleteLevelDBHotByteAccountingLocked(key string) {
	if trie.levelDBHotValues == nil {
		return
	}
	if valueBytes, ok := trie.levelDBHotValues[key]; ok {
		delete(trie.levelDBHotValues, key)
		trie.levelDBHotBytes -= valueBytes
		if trie.levelDBHotBytes < 0 {
			trie.levelDBHotBytes = 0
		}
	}
}

func (trie *HatTrie) levelDBCurrentHotValueBytesLocked(key string) (int64, error) {
	rawPtr := trie.tryLocation(key)
	if rawPtr == nil {
		return 0, nil
	}
	hval := HatValue{}
	hval.fromValue(*rawPtr)
	return trie.levelDBHotValueBytesLocked(Entry{Key: key, Value: hval})
}

func (trie *HatTrie) levelDBPotentialSpillCandidateLocked(hval HatValue) bool {
	switch hval.Type() {
	case DATAVALUE_TYPE_COUNTER:
		return true
	case DATAVALUE_TYPE_RAW_STRING:
		return hval.Index >= 0 && int(hval.Index) < len(trie.strings.array) && len(trie.strings.array[hval.Index]) > 0
	case DATAVALUE_TYPE_RAW_BYTES:
		return !hval.OnDisk() && hval.Index >= 0 && int(hval.Index) < len(trie.raws.array) && len(trie.raws.array[hval.Index]) > 0
	case DATAVALUE_TYPE_LEVELDB_REF:
		return false
	case DATAVALUE_TYPE_MAP,
		DATAVALUE_TYPE_SLICE,
		DATAVALUE_TYPE_SET,
		DATAVALUE_TYPE_PRIORITY_QUEUE,
		DATAVALUE_TYPE_BLOOM_FILTER,
		DATAVALUE_TYPE_COUNT_MIN_SKETCH,
		DATAVALUE_TYPE_HYPERLOGLOG,
		DATAVALUE_TYPE_TOP_K,
		DATAVALUE_TYPE_CUCKOO_FILTER,
		DATAVALUE_TYPE_ROARING_BITMAP,
		DATAVALUE_TYPE_QUANTILE_SKETCH,
		DATAVALUE_TYPE_FENWICK_TREE,
		DATAVALUE_TYPE_SPARSE_BITSET,
		DATAVALUE_TYPE_RESERVOIR_SAMPLE,
		DATAVALUE_TYPE_XOR_FILTER,
		DATAVALUE_TYPE_RADIX_TREE:
		return true
	default:
		return false
	}
}

func levelDBSpillCandidateLess(left, right levelDBSpillCandidate) bool {
	if left.lastHit.IsZero() != right.lastHit.IsZero() {
		return left.lastHit.IsZero()
	}
	if !left.lastHit.Equal(right.lastHit) {
		return left.lastHit.Before(right.lastHit)
	}
	if left.hits != right.hits {
		return left.hits < right.hits
	}
	if left.valueBytes != right.valueBytes {
		return left.valueBytes > right.valueBytes
	}
	return left.key < right.key
}

func (trie *HatTrie) levelDBHotValueBytesLocked(entry Entry) (int64, error) {
	switch entry.Value.Type() {
	case DATAVALUE_TYPE_COUNTER:
		return 4, nil
	case DATAVALUE_TYPE_RAW_STRING:
		return int64(len(trie.strings.array[entry.Value.Index])), nil
	case DATAVALUE_TYPE_RAW_BYTES:
		if entry.Value.OnDisk() {
			return 0, nil
		}
		return int64(len(trie.raws.array[entry.Value.Index])), nil
	case DATAVALUE_TYPE_MAP:
		return trie.maps.jsonSize(entry.Value.Index)
	case DATAVALUE_TYPE_SLICE:
		return jsonEncodedSize(trie.slices.values(entry.Value.Index))
	case DATAVALUE_TYPE_LEVELDB_REF:
		return 0, nil
	case DATAVALUE_TYPE_SET:
		return jsonEncodedSize(trie.sets.values(entry.Value.Index))
	case DATAVALUE_TYPE_PRIORITY_QUEUE:
		return jsonEncodedSize(trie.priorityQueues.array[entry.Value.Index].SnapshotItems())
	case DATAVALUE_TYPE_BLOOM_FILTER:
		return trie.bloomFilters.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
		return trie.countMinSketches.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_HYPERLOGLOG:
		return trie.hyperLogLogs.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_TOP_K:
		return trie.topKs.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_CUCKOO_FILTER:
		return trie.cuckooFilters.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_ROARING_BITMAP:
		return trie.roaringBitmaps.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_QUANTILE_SKETCH:
		return trie.quantileSketches.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_FENWICK_TREE:
		return trie.fenwickTrees.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_SPARSE_BITSET:
		return trie.sparseBitsets.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_RESERVOIR_SAMPLE:
		return trie.reservoirSamples.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_XOR_FILTER:
		return trie.xorFilters.array[entry.Value.Index].EncodedSize(), nil
	case DATAVALUE_TYPE_RADIX_TREE:
		return trie.radixTrees.array[entry.Value.Index].EncodedSize(), nil
	default:
		return 0, errors.New("hatriecache: unsupported snapshot value type")
	}
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
	if trie == nil {
		return LevelDBLoadResult{}, ErrNilHatTrie
	}

	snapshot, err := db.GetSnapshot()
	if err != nil {
		return LevelDBLoadResult{}, err
	}
	defer snapshot.Release()
	return loadPersistentEntryData(trie, store, policy, func(visit func(snapshotEntry, []byte) error) error {
		return scanLevelDBSnapshotEntryData(snapshot, visit)
	})
}

func loadPersistentEntryData(trie *HatTrie, store persistentReferenceStore, policy LevelDBLoadPolicy, scan func(func(snapshotEntry, []byte) error) error) (LevelDBLoadResult, error) {
	if trie == nil {
		return LevelDBLoadResult{}, ErrNilHatTrie
	}
	if trie.localPartitionSet() != nil {
		return loadLocalPartitionPersistentEntryData(trie, store, policy, scan)
	}
	now := trie.currentTime()
	trie.mu.Lock()
	defer trie.mu.Unlock()
	trie.invalidateReplicationMerkleLocked()
	createdKeys := make(map[string]struct{})
	rollbackOperations := make([]snapshotOperation, 0)
	activeKeys := []string{}
	result := LevelDBLoadResult{}
	applied := false
	if err := scan(func(entry snapshotEntry, data []byte) error {
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
	return materializeSnapshotEntryBytes(entry), true, nil
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
	if trie == nil {
		return ErrNilHatTrie
	}
	store, err := OpenLevelDBStoreWithFormat(path, format)
	if err != nil {
		return err
	}
	defer store.Close()
	return store.Save(trie)
}

func (trie *HatTrie) LoadLevelDB(path string) (int, error) {
	if trie == nil {
		return 0, ErrNilHatTrie
	}
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
		size, ok := snapshotEntryBytesDecodedSize(entry)
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
	size := int64(len(encoded)/4) * 3
	return size - int64(padding), true
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
		if entry.rawBytes != nil {
			return nil
		}
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
		if operation.bytes != nil {
			return int64(len(operation.bytes)), nil
		}
		if entry.rawBytes != nil {
			return int64(len(entry.rawBytes)), nil
		}
		if entry.Bytes != "" {
			return validatedBase64DecodedSize(entry.Bytes)
		}
		if err := validateSnapshotEntryFields(entry, true); err != nil {
			return 0, err
		}
		return 0, nil
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

func (trie *HatTrie) applyLevelDBReferenceLocked(store persistentReferenceStore, entry snapshotEntry, data []byte) (HatValue, error) {
	return trie.applyLevelDBReferenceMetadataLocked(store, entry, len(data), levelDBRecordChecksum(data))
}

func (trie *HatTrie) applyLevelDBReferenceMetadataLocked(store persistentReferenceStore, entry snapshotEntry, recordBytes int, recordChecksum uint64) (HatValue, error) {
	if store == nil {
		return HatValue{}, errors.New("hatriecache: leveldb reference store is required")
	}
	rawPtr := trie.upsertLocation(entry.Key)
	old := HatValue{}
	old.fromValue(*rawPtr)
	if !old.Empty() {
		trie.returnStorage(old)
	}
	trie.clearHotKeyLocked(entry.Key)
	trie.clearExpirationLocked(entry.Key)

	idx := trie.dbrefs.Add(LevelDBReference{
		Key:            entry.Key,
		Type:           entry.Type,
		Store:          store,
		ExpiresAt:      cloneTimePtr(entry.ExpiresAt),
		Stats:          cloneKeyStatsPtr(entry.Stats),
		RecordBytes:    recordBytes,
		RecordChecksum: recordChecksum,
		Token:          trie.nextLevelDBReferenceTokenLocked(),
	})
	hval := HatValue{Index: idx, Flags: DATAVALUE_TYPE_LEVELDB_REF}
	if entry.ExpiresAt != nil {
		hval = trie.setExpirationLocked(entry.Key, *entry.ExpiresAt, rawPtr, hval)
	}
	*rawPtr = hval.toValue()
	trie.updateLevelDBSpillCandidateLocked(entry.Key, hval)
	trie.updateLevelDBHotByteAccountingForKeyLocked(entry.Key)
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
	metadataMatches := trie.levelDBReferenceMetadataMatchesLocked(key, ref)
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
	if metadataMatches {
		return data, true, nil
	}
	expiresAt, stats := trie.levelDBReferenceMetadataLocked(key)
	patched, ok, err := rewriteLevelDBBinaryEntryMetadata(data, key, expiresAt, stats)
	if err != nil || !ok {
		return nil, ok, err
	}
	ref.ExpiresAt = cloneTimePtr(expiresAt)
	ref.Stats = cloneKeyStatsPtr(stats)
	ref.RecordBytes = len(patched)
	ref.RecordChecksum = levelDBRecordChecksum(patched)
	ref.Token = trie.nextLevelDBReferenceTokenLocked()
	trie.dbrefs.Set(hval.Index, ref)
	return patched, true, nil
}

func (trie *HatTrie) nextLevelDBReferenceTokenLocked() uint64 {
	trie.nextDBReferenceToken++
	if trie.nextDBReferenceToken == 0 {
		trie.nextDBReferenceToken++
	}
	return trie.nextDBReferenceToken
}

func (trie *HatTrie) levelDBReferenceMetadataMatchesLocked(key string, ref LevelDBReference) bool {
	if !timePtrEqual(ref.ExpiresAt, snapshotExpiresAt(trie.expirationTimeLocked(key))) {
		return false
	}
	currentStats := clonedUpdatedKeyStats(trie.keyStats[key])
	return keyStatsPtrEqual(ref.Stats, currentStats)
}

func (trie *HatTrie) levelDBReferenceMetadataLocked(key string) (*time.Time, *KeyStats) {
	expiresAt := snapshotExpiresAt(trie.expirationTimeLocked(key))
	return expiresAt, clonedUpdatedKeyStats(trie.keyStats[key])
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
	entry.ExpiresAt = snapshotExpiresAt(trie.expirationTimeLocked(key))
	entry.Stats = clonedUpdatedKeyStats(trie.keyStats[key])
	return entry, nil
}

func (trie *HatTrie) scanLevelDBEntryData(visit func(string, []byte) error) error {
	return trie.scanLevelDBEntryDataForStore(nil, nil, DefaultStorageFormat, visit)
}

func (trie *HatTrie) scanLevelDBEntryDataForStore(currentStore *LevelDBStore, currentDB *leveldb.DB, format StorageFormat, visit func(string, []byte) error) error {
	format, err := ParseStorageFormat(string(format))
	if err != nil {
		return err
	}
	capture, err := trie.captureSnapshotForStore(currentStore, currentDB)
	if err != nil {
		return err
	}
	for _, page := range capture.pages {
		for _, entry := range page {
			data := entry.levelDBRecord
			if data == nil {
				var err error
				data, err = marshalLevelDBEntry(entry, format)
				if err != nil {
					return err
				}
			}
			if visit != nil {
				if err := visit(entry.Key, data); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (trie *HatTrie) levelDBEntryDataForKeyForStore(currentStore *LevelDBStore, currentDB *leveldb.DB, format StorageFormat, key string) ([]byte, bool, error) {
	if trie == nil {
		return nil, false, ErrNilHatTrie
	}
	if partition := trie.localPartitionForKey(key); partition != nil {
		return partition.levelDBEntryDataForKeyForStore(currentStore, currentDB, format, key)
	}
	trie.mu.Lock()
	defer trie.mu.Unlock()

	format, err := ParseStorageFormat(string(format))
	if err != nil {
		return nil, false, err
	}
	if err := validateKey(key); err != nil {
		return nil, false, err
	}
	trie.ensureOpen()
	hval := trie.peekCachedLocked(key)
	if hval.Empty() {
		return nil, false, nil
	}
	data, err := trie.levelDBEntryDataForStoreLocked(Entry{Key: key, Value: hval}, currentStore, currentDB, format)
	if err != nil {
		return nil, false, err
	}
	return data, true, nil
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
	expiresAt := snapshotExpiresAt(trie.expirationTimeLocked(entry.Key))
	stats := clonedUpdatedKeyStats(trie.keyStats[entry.Key])
	file, size, err := trie.disks.open(entry.Value.Index)
	if err != nil {
		return nil, err
	}
	if file == nil {
		return marshalLevelDBBytesEntryBinary(entry.Key, nil, expiresAt, stats)
	}
	defer file.Close()
	return marshalLevelDBBytesEntryBinaryFromReader(entry.Key, size, file, expiresAt, stats)
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
