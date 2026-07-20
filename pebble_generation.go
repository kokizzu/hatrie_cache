package hatriecache

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
)

const (
	pebbleGenerationStageAfterIngest   = "after_ingest"
	pebbleGenerationStageAfterActivate = "after_activate"
	pebbleGenerationOptimisticAttempts = 2
)

var (
	pebbleActiveGenerationKey       = []byte("\x00hatrie-cache:active-generation")
	pebbleGenerationNamespacePrefix = []byte("\x01hatrie-cache:g:")
)

type pebbleRecordSpool struct {
	path   string
	file   *os.File
	writer *bufio.Writer
	count  int
}

type pebbleGenerationSSTBuilder struct {
	dbPath     string
	generation uint64
	path       string
	writer     *sstable.Writer
	keyPrefix  []byte
	keyBuffer  []byte
	count      int
}

func (store *PebbleStore) saveGeneration(trie *HatTrie) error {
	if trie == nil {
		return ErrNilHatTrie
	}
	store.saveMu.Lock()
	defer store.saveMu.Unlock()

	store.mu.Lock()
	if store.db == nil {
		store.mu.Unlock()
		return ErrLevelDBStoreClosed
	}
	generation := store.nextGeneration
	if generation == 0 {
		store.mu.Unlock()
		return errors.New("hatriecache: pebble generation exhausted")
	}
	store.nextGeneration++
	path := store.path
	format := store.format
	store.mu.Unlock()

	var sstPath string
	var records int
	var err error
	dirty := false
	for attempt := 0; attempt < pebbleGenerationOptimisticAttempts; attempt++ {
		sstPath, records, dirty, err = capturePebbleGenerationSST(trie, path, generation, format)
		if err != nil {
			return err
		}
		if !dirty {
			break
		}
	}
	if dirty {
		spool, spoolErr := newPebbleRecordSpool(filepath.Dir(path), filepath.Base(path))
		if spoolErr != nil {
			return spoolErr
		}
		defer spool.remove()
		replacements, captureErr := capturePebbleRecordSpool(trie, format, spool)
		if captureErr != nil {
			return captureErr
		}
		sstPath, records, err = buildPebbleGenerationSST(path, generation, format, spool, replacements)
	}
	if sstPath != "" {
		defer os.Remove(sstPath)
	}
	if err != nil {
		return err
	}

	store.mu.Lock()
	defer store.mu.Unlock()
	if store.db == nil {
		return ErrLevelDBStoreClosed
	}
	if records > 0 {
		if err := store.db.Ingest([]string{sstPath}); err != nil {
			return err
		}
	}
	if hook := store.generationSaveHook; hook != nil {
		if err := hook(pebbleGenerationStageAfterIngest); err != nil {
			return err
		}
	}
	if err := store.db.Set(pebbleActiveGenerationKey, encodePebbleGeneration(generation), pebble.Sync); err != nil {
		return err
	}
	store.activeGeneration = generation
	if hook := store.generationSaveHook; hook != nil {
		if err := hook(pebbleGenerationStageAfterActivate); err != nil {
			return err
		}
	}
	return cleanupPebbleGenerations(store.db, generation)
}

func loadPebbleGenerationState(db *pebble.DB) (active uint64, next uint64, err error) {
	data, closer, getErr := db.Get(pebbleActiveGenerationKey)
	switch {
	case errors.Is(getErr, pebble.ErrNotFound):
		active = 0
	case getErr != nil:
		return 0, 0, getErr
	default:
		if len(data) != 8 {
			closer.Close()
			return 0, 0, errors.New("hatriecache: invalid pebble active generation")
		}
		active = binary.BigEndian.Uint64(data)
		closer.Close()
		if active == 0 {
			return 0, 0, errors.New("hatriecache: invalid zero pebble active generation")
		}
	}
	maximum := active
	iterator, iterErr := db.NewIter(pebblePrefixIterOptions(pebbleGenerationNamespacePrefix))
	if iterErr != nil {
		return 0, 0, iterErr
	}
	for valid := iterator.First(); valid; valid = iterator.Next() {
		generation, ok := pebbleGenerationFromKey(iterator.Key())
		if ok && generation > maximum {
			maximum = generation
		}
	}
	iterErr = iterator.Error()
	closeErr := iterator.Close()
	if iterErr != nil {
		return 0, 0, iterErr
	}
	if closeErr != nil {
		return 0, 0, closeErr
	}
	if maximum == ^uint64(0) {
		return 0, 0, errors.New("hatriecache: pebble generation exhausted")
	}
	return active, maximum + 1, nil
}

func cleanupPebbleGenerations(db *pebble.DB, active uint64) error {
	batch := db.NewBatch()
	defer batch.Close()
	namespaceLimit := bytesPrefixLimit(pebbleGenerationNamespacePrefix)
	if active == 0 {
		if err := batch.DeleteRange(pebbleGenerationNamespacePrefix, namespaceLimit, nil); err != nil {
			return err
		}
	} else {
		activePrefix := pebbleGenerationEntryPrefix(active)
		activeLimit := bytesPrefixLimit(activePrefix)
		if err := batch.DeleteRange(pebbleGenerationNamespacePrefix, activePrefix, nil); err != nil {
			return err
		}
		if err := batch.DeleteRange(activeLimit, namespaceLimit, nil); err != nil {
			return err
		}
		if err := batch.DeleteRange(levelDBEntryPrefix, bytesPrefixLimit(levelDBEntryPrefix), nil); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.NoSync)
}

func compactPebbleGenerations(db *pebble.DB, active uint64, options LevelDBCompactionOptions) error {
	if options.StartKey != "" || options.LimitKey != "" {
		prefix := pebbleGenerationEntryPrefix(active)
		start := prefix
		limit := bytesPrefixLimit(prefix)
		if options.StartKey != "" {
			start = pebbleGenerationEntryKey(active, options.StartKey)
		}
		if options.LimitKey != "" {
			limit = pebbleGenerationEntryKey(active, options.LimitKey)
		}
		return db.Compact(start, limit, true)
	}
	if active == 0 {
		return db.Compact(levelDBEntryPrefix, bytesPrefixLimit(levelDBEntryPrefix), true)
	}
	if err := db.Compact(pebbleGenerationNamespacePrefix, bytesPrefixLimit(pebbleGenerationNamespacePrefix), true); err != nil {
		return err
	}
	return db.Compact(levelDBEntryPrefix, bytesPrefixLimit(levelDBEntryPrefix), true)
}

func pebbleGenerationEntryPrefix(generation uint64) []byte {
	if generation == 0 {
		return levelDBEntryPrefix
	}
	prefix := make([]byte, len(pebbleGenerationNamespacePrefix)+8+len(levelDBEntryPrefix))
	offset := copy(prefix, pebbleGenerationNamespacePrefix)
	binary.BigEndian.PutUint64(prefix[offset:offset+8], generation)
	offset += 8
	copy(prefix[offset:], levelDBEntryPrefix)
	return prefix
}

func pebbleGenerationEntryKey(generation uint64, key string) []byte {
	prefix := pebbleGenerationEntryPrefix(generation)
	result := make([]byte, len(prefix)+len(key))
	copy(result, prefix)
	copy(result[len(prefix):], key)
	return result
}

func pebbleGenerationFromKey(key []byte) (uint64, bool) {
	if len(key) < len(pebbleGenerationNamespacePrefix)+8 || !bytes.HasPrefix(key, pebbleGenerationNamespacePrefix) {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(pebbleGenerationNamespacePrefix):]), true
}

func encodePebbleGeneration(generation uint64) []byte {
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, generation)
	return data
}

func pebblePrefixIterOptions(prefix []byte) *pebble.IterOptions {
	return &pebble.IterOptions{LowerBound: prefix, UpperBound: bytesPrefixLimit(prefix)}
}

func newPebbleRecordSpool(dir string, base string) (*pebbleRecordSpool, error) {
	file, err := os.CreateTemp(dir, base+".capture-*")
	if err != nil {
		return nil, err
	}
	return &pebbleRecordSpool{path: file.Name(), file: file, writer: bufio.NewWriterSize(file, 64<<10)}, nil
}

func (spool *pebbleRecordSpool) append(key string, data []byte) error {
	if uint64(len(key)) > uint64(^uint32(0)) || uint64(len(data)) > uint64(^uint32(0)) {
		return errors.New("hatriecache: pebble spool record too large")
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(key)))
	binary.BigEndian.PutUint32(header[4:], uint32(len(data)))
	if _, err := spool.writer.Write(header[:]); err != nil {
		return err
	}
	if _, err := spool.writer.WriteString(key); err != nil {
		return err
	}
	if _, err := spool.writer.Write(data); err != nil {
		return err
	}
	spool.count++
	return nil
}

func (spool *pebbleRecordSpool) rewind() error {
	if spool.writer != nil {
		if err := spool.writer.Flush(); err != nil {
			return err
		}
		spool.writer = nil
	}
	_, err := spool.file.Seek(0, io.SeekStart)
	return err
}

func (spool *pebbleRecordSpool) next() (string, []byte, bool, error) {
	var header [8]byte
	_, err := io.ReadFull(spool.file, header[:])
	if errors.Is(err, io.EOF) {
		return "", nil, false, nil
	}
	if err != nil {
		return "", nil, false, err
	}
	keyBytes := make([]byte, binary.BigEndian.Uint32(header[:4]))
	data := make([]byte, binary.BigEndian.Uint32(header[4:]))
	if _, err := io.ReadFull(spool.file, keyBytes); err != nil {
		return "", nil, false, err
	}
	if _, err := io.ReadFull(spool.file, data); err != nil {
		return "", nil, false, err
	}
	return string(keyBytes), data, true, nil
}

func (spool *pebbleRecordSpool) remove() {
	if spool == nil {
		return
	}
	if spool.writer != nil {
		_ = spool.writer.Flush()
	}
	if spool.file != nil {
		_ = spool.file.Close()
	}
	_ = os.Remove(spool.path)
}

func capturePebbleRecordSpool(trie *HatTrie, format StorageFormat, spool *pebbleRecordSpool) (map[string]snapshotCaptureReplacement, error) {
	return capturePebbleRecords(trie, format, func(key string, data []byte) error {
		return spool.append(key, data)
	})
}

func capturePebbleGenerationSST(trie *HatTrie, dbPath string, generation uint64, format StorageFormat) (string, int, bool, error) {
	builder := &pebbleGenerationSSTBuilder{dbPath: dbPath, generation: generation}
	replacements, err := capturePebbleRecords(trie, format, func(key string, data []byte) error {
		return builder.write(key, data)
	})
	if err != nil {
		builder.abort()
		return "", 0, false, err
	}
	if err := builder.close(); err != nil {
		builder.abort()
		return "", 0, false, err
	}
	if len(replacements) > 0 {
		builder.abort()
		return "", 0, true, nil
	}
	return builder.path, builder.count, false, nil
}

func capturePebbleRecords(trie *HatTrie, format StorageFormat, visitRecord func(string, []byte) error) (map[string]snapshotCaptureReplacement, error) {
	if trie.localPartitionSet() != nil {
		_, err := trie.visitCapturedLocalPartitionEntries(nil, nil, nil, func(entry snapshotEntry) error {
			data, err := pebbleSnapshotEntryData(entry, format)
			if err != nil {
				return err
			}
			if err := visitRecord(entry.Key, data); err != nil {
				return err
			}
			return nil
		})
		return nil, err
	}
	trie.snapshotCaptureMu.Lock()
	defer trie.snapshotCaptureMu.Unlock()
	tracker := &snapshotMutationTracker{dirty: make(map[string]struct{})}
	trie.mu.Lock()
	func() {
		defer trie.mu.Unlock()
		trie.ensureOpen()
		trie.snapshotMutations = tracker
	}()
	active := true
	defer func() {
		if !active {
			return
		}
		trie.mu.Lock()
		if trie.snapshotMutations == tracker {
			trie.snapshotMutations = nil
		}
		trie.mu.Unlock()
	}()

	cursor := &replicationSyncCursor{}
	afterKey := ""
	hasAfterKey := false
	pageNumber := 0
	for {
		entries := make([]snapshotEntry, 0, snapshotCaptureScanPageEntries)
		page, err := replicationSyncEntriesPageWithCursor(trie, "", afterKey, hasAfterKey, snapshotCaptureScanPageEntries, cursor, func(entry Entry) error {
			captured, err := trie.captureSnapshotEntryForStoreLocked(entry, nil, nil)
			if err != nil {
				return err
			}
			entries = append(entries, captured)
			return nil
		})
		if err != nil {
			cursor.close(trie)
			return nil, err
		}
		for _, captured := range entries {
			data, err := pebbleSnapshotEntryData(captured, format)
			if err != nil {
				cursor.close(trie)
				return nil, err
			}
			if err := visitRecord(captured.Key, data); err != nil {
				cursor.close(trie)
				return nil, err
			}
		}
		pageNumber++
		if hook := trie.snapshotCapturePageHook; hook != nil {
			hook(pageNumber)
		}
		if !page.hasMore {
			break
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
		runtime.Gosched()
	}
	cursor.close(trie)
	replacements, _, err := trie.captureSnapshotMutationReplacements(tracker, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	active = false
	return replacements, nil
}

func (builder *pebbleGenerationSSTBuilder) write(key string, data []byte) error {
	if builder.writer == nil {
		temp, err := os.CreateTemp(filepath.Dir(builder.dbPath), filepath.Base(builder.dbPath)+".generation-*.sst")
		if err != nil {
			return err
		}
		builder.path = temp.Name()
		if err := temp.Close(); err != nil {
			return err
		}
		if err := os.Remove(builder.path); err != nil {
			return err
		}
		file, err := vfs.Default.Create(builder.path)
		if err != nil {
			return err
		}
		builder.writer = sstable.NewWriter(objstorageprovider.NewFileWritable(file), sstable.WriterOptions{})
		builder.keyPrefix = pebbleGenerationEntryPrefix(builder.generation)
		builder.keyBuffer = make([]byte, 0, len(builder.keyPrefix)+len(key))
	}
	builder.keyBuffer = append(builder.keyBuffer[:0], builder.keyPrefix...)
	builder.keyBuffer = append(builder.keyBuffer, key...)
	if err := builder.writer.Set(builder.keyBuffer, data); err != nil {
		return err
	}
	builder.count++
	return nil
}

func (builder *pebbleGenerationSSTBuilder) close() error {
	if builder.writer == nil {
		return nil
	}
	err := builder.writer.Close()
	builder.writer = nil
	return err
}

func (builder *pebbleGenerationSSTBuilder) abort() {
	if builder == nil {
		return
	}
	if builder.writer != nil {
		_ = builder.writer.Close()
		builder.writer = nil
	}
	if builder.path != "" {
		_ = os.Remove(builder.path)
	}
}

func pebbleSnapshotEntryData(entry snapshotEntry, format StorageFormat) ([]byte, error) {
	if entry.levelDBRecord != nil {
		return entry.levelDBRecord, nil
	}
	return marshalLevelDBEntry(entry, format)
}

func buildPebbleGenerationSST(dbPath string, generation uint64, format StorageFormat, spool *pebbleRecordSpool, replacements map[string]snapshotCaptureReplacement) (string, int, error) {
	if err := spool.rewind(); err != nil {
		return "", 0, err
	}
	replacementKeys := make([]string, 0, len(replacements))
	for key := range replacements {
		replacementKeys = append(replacementKeys, key)
	}
	sort.Strings(replacementKeys)

	builder := &pebbleGenerationSSTBuilder{dbPath: dbPath, generation: generation}
	writeRecord := func(key string, data []byte) error {
		return builder.write(key, data)
	}
	writeReplacement := func(key string) error {
		replacement := replacements[key]
		if !replacement.present {
			return nil
		}
		data, err := pebbleSnapshotEntryData(replacement.entry, format)
		if err != nil {
			return err
		}
		return writeRecord(key, data)
	}

	replacementIndex := 0
	for {
		key, data, ok, err := spool.next()
		if err != nil {
			builder.abort()
			return "", 0, err
		}
		if !ok {
			break
		}
		for replacementIndex < len(replacementKeys) && replacementKeys[replacementIndex] < key {
			if err := writeReplacement(replacementKeys[replacementIndex]); err != nil {
				builder.abort()
				return "", 0, err
			}
			replacementIndex++
		}
		if replacementIndex < len(replacementKeys) && replacementKeys[replacementIndex] == key {
			if err := writeReplacement(replacementKeys[replacementIndex]); err != nil {
				builder.abort()
				return "", 0, err
			}
			replacementIndex++
			continue
		}
		if err := writeRecord(key, data); err != nil {
			builder.abort()
			return "", 0, err
		}
	}
	for replacementIndex < len(replacementKeys) {
		if err := writeReplacement(replacementKeys[replacementIndex]); err != nil {
			builder.abort()
			return "", 0, err
		}
		replacementIndex++
	}
	if err := builder.close(); err != nil {
		builder.abort()
		return "", 0, fmt.Errorf("close pebble generation sstable: %w", err)
	}
	return builder.path, builder.count, nil
}
