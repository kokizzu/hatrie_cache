package hatDataStructure

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

const (
	// DefaultSpillableArrangementMemoryLimit is the default retained value
	// payload budget before cold values move to the spill segment.
	DefaultSpillableArrangementMemoryLimit int64 = 64 << 20
	// DefaultSpillableArrangementMaxKeyBytes bounds one retained key.
	DefaultSpillableArrangementMaxKeyBytes int64 = 1 << 20
	// DefaultSpillableArrangementMaxValueBytes bounds one value payload.
	DefaultSpillableArrangementMaxValueBytes int64 = 64 << 20
	spillableArrangementHeaderSize                 = 24
)

var spillableArrangementMagic = [4]byte{'H', 'S', 'A', '1'}

var (
	ErrSpillableArrangementNil           = errors.New("hatDataStructure: spillable arrangement is nil")
	ErrSpillableArrangementDirectory     = errors.New("hatDataStructure: spillable arrangement directory is invalid")
	ErrSpillableArrangementLimitInvalid  = errors.New("hatDataStructure: spillable arrangement limit is invalid")
	ErrSpillableArrangementKeyRequired   = errors.New("hatDataStructure: spillable arrangement key is required")
	ErrSpillableArrangementKeyTooLarge   = errors.New("hatDataStructure: spillable arrangement key is too large")
	ErrSpillableArrangementValueTooLarge = errors.New("hatDataStructure: spillable arrangement value is too large")
	ErrSpillableArrangementDiskLimit     = errors.New("hatDataStructure: spillable arrangement disk limit exceeded")
	ErrSpillableArrangementClosed        = errors.New("hatDataStructure: spillable arrangement is closed")
	ErrSpillableArrangementCorrupt       = errors.New("hatDataStructure: spillable arrangement record is corrupt")
	ErrSpillableArrangementPathInvalid   = errors.New("hatDataStructure: spillable arrangement path is invalid")
)

// SpillableArrangementOptions configures an opt-in local spill tier. A zero
// memory limit selects DefaultSpillableArrangementMemoryLimit. A zero disk
// limit means unlimited spill-file bytes; callers that need a hard disk bound
// should set MaxDiskBytes and call Compact after deletes or replacements.
type SpillableArrangementOptions struct {
	Directory        string
	MemoryLimitBytes int64
	MaxDiskBytes     int64
	MaxKeyBytes      int64
	MaxValueBytes    int64
}

// SpillableArrangementEntry is an independent key/value snapshot entry.
type SpillableArrangementEntry struct {
	Key   string
	Value []byte
}

// SpillableArrangementStats reports retained values and spill-segment usage.
// HotBytes counts value payload bytes only; key and index metadata remain in
// memory so point lookups do not require a full segment scan.
type SpillableArrangementStats struct {
	Entries      int
	ColdEntries  int
	HotBytes     int64
	DiskBytes    int64
	SpillRecords uint64
}

// SpillableArrangement keeps keyed byte values in memory until the configured
// payload budget is exceeded, then moves least-recently-written cold values to
// a local binary segment. The segment can be reopened explicitly after a
// process restart; replication and consensus remain caller responsibilities.
type SpillableArrangement struct {
	mu             sync.RWMutex
	directory      string
	spillPath      string
	file           *os.File
	ownedDirectory bool
	closed         bool
	memoryLimit    int64
	maxDiskBytes   int64
	maxKeyBytes    int64
	maxValueBytes  int64
	hotBytes       int64
	diskBytes      int64
	spillRecords   uint64
	persistedIndex bool
	coldEntries    int
	generation     uint64
	entries        map[string]*spillableArrangementEntry
	queue          []spillableArrangementQueueItem
	queueHead      int
}

type spillableArrangementEntry struct {
	key      string
	value    []byte
	ref      spillableArrangementRef
	gen      uint64
	valueHot bool
}

type spillableArrangementRef struct {
	offset int64
	total  int64
}

type spillableArrangementQueueItem struct {
	key string
	gen uint64
}

// NewSpillableArrangement creates an empty arrangement and its private binary
// spill segment. Existing files are never opened or reused.
func NewSpillableArrangement(options SpillableArrangementOptions) (*SpillableArrangement, error) {
	options, err := normalizeSpillableArrangementOptions(options)
	if err != nil {
		return nil, err
	}
	memoryLimit := options.MemoryLimitBytes
	maxKeyBytes := options.MaxKeyBytes
	maxValueBytes := options.MaxValueBytes
	directory := options.Directory
	ownedDirectory := false
	if directory == "" {
		directory, err = os.MkdirTemp("", "hatrie-spill-")
		if err != nil {
			return nil, err
		}
		ownedDirectory = true
	} else if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(directory, ".hatrie-arrangement-*")
	if err != nil {
		if ownedDirectory {
			_ = os.RemoveAll(directory)
		}
		return nil, err
	}
	return &SpillableArrangement{
		directory:      directory,
		spillPath:      file.Name(),
		file:           file,
		ownedDirectory: ownedDirectory,
		memoryLimit:    memoryLimit,
		maxDiskBytes:   options.MaxDiskBytes,
		maxKeyBytes:    maxKeyBytes,
		maxValueBytes:  maxValueBytes,
		entries:        make(map[string]*spillableArrangementEntry),
	}, nil
}

// OpenSpillableArrangement reopens a previously flushed spill segment. The
// segment is scanned once for record integrity, while only the latest key and
// its file offset are retained in memory. Values remain cold until Get or
// Snapshot reads them.
func OpenSpillableArrangement(path string, options SpillableArrangementOptions) (*SpillableArrangement, error) {
	options, err := normalizeSpillableArrangementOptions(options)
	if err != nil {
		return nil, err
	}
	if path == "" {
		return nil, ErrSpillableArrangementPathInvalid
	}
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, ErrSpillableArrangementPathInvalid
	}
	if options.MaxDiskBytes > 0 && info.Size() > options.MaxDiskBytes {
		return nil, ErrSpillableArrangementDiskLimit
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	arrangement := &SpillableArrangement{
		directory:     filepath.Dir(path),
		spillPath:     path,
		file:          file,
		memoryLimit:   options.MemoryLimitBytes,
		maxDiskBytes:  options.MaxDiskBytes,
		maxKeyBytes:   options.MaxKeyBytes,
		maxValueBytes: options.MaxValueBytes,
		entries:       make(map[string]*spillableArrangementEntry),
	}
	if !arrangement.restorePersistedIndex(info.Size()) {
		if err := arrangement.recoverSegment(info.Size()); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return arrangement, nil
}

func normalizeSpillableArrangementOptions(options SpillableArrangementOptions) (SpillableArrangementOptions, error) {
	if options.MemoryLimitBytes < 0 || options.MaxDiskBytes < 0 || options.MaxKeyBytes < 0 || options.MaxValueBytes < 0 {
		return SpillableArrangementOptions{}, ErrSpillableArrangementLimitInvalid
	}
	if options.MemoryLimitBytes == 0 {
		options.MemoryLimitBytes = DefaultSpillableArrangementMemoryLimit
	}
	if options.MaxKeyBytes == 0 {
		options.MaxKeyBytes = DefaultSpillableArrangementMaxKeyBytes
	}
	if options.MaxValueBytes == 0 {
		options.MaxValueBytes = DefaultSpillableArrangementMaxValueBytes
	}
	return options, nil
}

func (arrangement *SpillableArrangement) recoverSegment(size int64) error {
	if size < 0 {
		return ErrSpillableArrangementCorrupt
	}
	header := make([]byte, spillableArrangementHeaderSize)
	var valueChunk [8 << 10]byte
	keyBuffer := make([]byte, 0, 256)
	if _, err := arrangement.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	reader := bufio.NewReaderSize(arrangement.file, 16<<10)
	var offset int64
	for offset < size {
		if size-offset < spillableArrangementHeaderSize {
			return ErrSpillableArrangementCorrupt
		}
		if _, err := io.ReadFull(reader, header); err != nil {
			return ErrSpillableArrangementCorrupt
		}
		if !equalSpillableArrangementMagic(header[:4]) || binary.LittleEndian.Uint32(header[20:24]) != 0 {
			return ErrSpillableArrangementCorrupt
		}
		keyLength := uint64(binary.LittleEndian.Uint32(header[4:8]))
		valueLength := binary.LittleEndian.Uint64(header[8:16])
		if keyLength == 0 || keyLength > uint64(arrangement.maxKeyBytes) || keyLength > uint64(math.MaxInt) || valueLength > uint64(arrangement.maxValueBytes) {
			return ErrSpillableArrangementCorrupt
		}
		recordSize := int64(spillableArrangementHeaderSize) + int64(keyLength)
		if recordSize < 0 || valueLength > uint64(math.MaxInt64-recordSize) {
			return ErrSpillableArrangementCorrupt
		}
		recordSize += int64(valueLength)
		if recordSize > size-offset {
			return ErrSpillableArrangementCorrupt
		}
		keyLengthInt := int(keyLength)
		if cap(keyBuffer) < keyLengthInt {
			keyBuffer = make([]byte, keyLengthInt)
		} else {
			keyBuffer = keyBuffer[:keyLengthInt]
		}
		if _, err := io.ReadFull(reader, keyBuffer); err != nil {
			return ErrSpillableArrangementCorrupt
		}
		checksum := crc32.NewIEEE()
		_, _ = checksum.Write(keyBuffer)
		remaining := valueLength
		for remaining > 0 {
			chunkLength := uint64(len(valueChunk))
			if remaining < chunkLength {
				chunkLength = remaining
			}
			if _, err := io.ReadFull(reader, valueChunk[:int(chunkLength)]); err != nil {
				return ErrSpillableArrangementCorrupt
			}
			_, _ = checksum.Write(valueChunk[:int(chunkLength)])
			remaining -= chunkLength
		}
		if checksum.Sum32() != binary.LittleEndian.Uint32(header[16:20]) {
			return ErrSpillableArrangementCorrupt
		}
		key := string(keyBuffer)
		if _, exists := arrangement.entries[key]; exists {
			arrangement.coldEntries--
		}
		arrangement.generation++
		arrangement.entries[key] = &spillableArrangementEntry{
			key:      key,
			ref:      spillableArrangementRef{offset: offset, total: recordSize},
			gen:      arrangement.generation,
			valueHot: false,
		}
		arrangement.coldEntries++
		arrangement.spillRecords++
		offset += recordSize
	}
	if _, err := arrangement.file.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	arrangement.diskBytes = size
	return nil
}

// Set inserts or replaces key with a cloned value. If the configured disk
// limit would be exceeded while enforcing the memory limit, Set is rejected
// before any state or segment bytes change.
func (arrangement *SpillableArrangement) Set(key string, value []byte) error {
	if arrangement == nil {
		return ErrSpillableArrangementNil
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if err := arrangement.ensureOpenLocked(); err != nil {
		return err
	}
	if err := arrangement.validateKey(key); err != nil {
		return err
	}
	if int64(len(value)) > arrangement.maxValueBytes {
		return ErrSpillableArrangementValueTooLarge
	}
	previous, existed := arrangement.entries[key]
	previousHotBytes := arrangement.hotBytes
	previousColdEntries := arrangement.coldEntries
	previousQueueLength := len(arrangement.queue)
	if existed && previous.valueHot {
		arrangement.hotBytes -= int64(len(previous.value))
	} else if existed {
		arrangement.coldEntries--
	}
	arrangement.generation++
	entry := &spillableArrangementEntry{key: key, value: append([]byte(nil), value...), gen: arrangement.generation, valueHot: true}
	arrangement.entries[key] = entry
	arrangement.hotBytes += int64(len(entry.value))
	arrangement.queue = append(arrangement.queue, spillableArrangementQueueItem{key: key, gen: entry.gen})
	if err := arrangement.spillToLimitLocked(); err != nil {
		if existed {
			arrangement.entries[key] = previous
		} else {
			delete(arrangement.entries, key)
		}
		arrangement.hotBytes = previousHotBytes
		arrangement.coldEntries = previousColdEntries
		arrangement.queue = arrangement.queue[:previousQueueLength]
		return err
	}
	arrangement.maybeCompactQueueLocked()
	return nil
}

// Get returns an independent copy of key's value.
func (arrangement *SpillableArrangement) Get(key string) ([]byte, bool, error) {
	if arrangement == nil {
		return nil, false, ErrSpillableArrangementNil
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	if err := arrangement.ensureOpenLocked(); err != nil {
		return nil, false, err
	}
	entry, found := arrangement.entries[key]
	if !found {
		return nil, false, nil
	}
	if entry.valueHot {
		return append([]byte(nil), entry.value...), true, nil
	}
	value, err := arrangement.readColdValueLocked(entry)
	if err != nil {
		return nil, false, err
	}
	return value, true, nil
}

// Delete removes key. Stale segment bytes are reclaimed by Compact.
func (arrangement *SpillableArrangement) Delete(key string) bool {
	if arrangement == nil {
		return false
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if arrangement.closed {
		return false
	}
	entry, found := arrangement.entries[key]
	if !found {
		return false
	}
	if entry.valueHot {
		arrangement.hotBytes -= int64(len(entry.value))
	} else {
		arrangement.coldEntries--
	}
	delete(arrangement.entries, key)
	arrangement.maybeCompactQueueLocked()
	return true
}

// Flush spills every hot value and syncs the segment.
func (arrangement *SpillableArrangement) Flush() error {
	if arrangement == nil {
		return ErrSpillableArrangementNil
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if err := arrangement.ensureOpenLocked(); err != nil {
		return err
	}
	hot := arrangement.hotEntriesLocked()
	if err := arrangement.checkSpillDiskLimitLocked(hot); err != nil {
		return err
	}
	if err := arrangement.spillEntriesLocked(hot); err != nil {
		return err
	}
	if err := arrangement.file.Sync(); err != nil {
		return err
	}
	arrangement.maybeCompactQueueLocked()
	_ = arrangement.persistIndexLocked()
	return nil
}

// Sync makes already-written spill records visible to the local filesystem.
func (arrangement *SpillableArrangement) Sync() error {
	if arrangement == nil {
		return ErrSpillableArrangementNil
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	if err := arrangement.ensureOpenLocked(); err != nil {
		return err
	}
	if err := arrangement.file.Sync(); err != nil {
		return err
	}
	_ = arrangement.persistIndexLocked()
	return nil
}

// Compact rewrites live cold values into a fresh segment and discards stale
// records left by deletes and replacements. Hot residency is preserved.
func (arrangement *SpillableArrangement) Compact() error {
	if arrangement == nil {
		return ErrSpillableArrangementNil
	}
	arrangement.mu.Lock()
	defer arrangement.mu.Unlock()
	if err := arrangement.ensureOpenLocked(); err != nil {
		return err
	}
	cold := arrangement.coldEntriesLocked()
	if len(cold) == 0 && arrangement.diskBytes == 0 {
		return arrangement.file.Sync()
	}
	sort.Slice(cold, func(left, right int) bool { return cold[left].key < cold[right].key })
	temporary, err := os.CreateTemp(arrangement.directory, ".hatrie-arrangement-compact-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	pending := make(map[string]spillableArrangementRef, len(cold))
	var offset int64
	for _, entry := range cold {
		value, err := arrangement.readColdValueLocked(entry)
		if err != nil {
			_ = temporary.Close()
			return err
		}
		record, err := spillableArrangementRecord(entry.key, value, arrangement.maxKeyBytes, arrangement.maxValueBytes)
		if err != nil {
			_ = temporary.Close()
			return err
		}
		if err := writeSpillableArrangementAll(temporary, record); err != nil {
			_ = temporary.Close()
			return err
		}
		pending[entry.key] = spillableArrangementRef{offset: offset, total: int64(len(record))}
		offset += int64(len(record))
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := invalidateSpillableArrangementIndex(arrangement.spillPath); err != nil {
		return err
	}
	if err := arrangement.file.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryPath, arrangement.spillPath); err != nil {
		file, reopenErr := os.OpenFile(arrangement.spillPath, os.O_RDWR|os.O_APPEND, 0o600)
		if reopenErr == nil {
			arrangement.file = file
		}
		return err
	}
	file, err := os.OpenFile(arrangement.spillPath, os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	arrangement.file = file
	removeTemporary = false
	arrangement.diskBytes = offset
	for _, entry := range cold {
		entry.ref = pending[entry.key]
	}
	_ = arrangement.persistIndexLocked()
	return nil
}

// Snapshot returns all live entries sorted by key with independent values.
func (arrangement *SpillableArrangement) Snapshot() ([]SpillableArrangementEntry, error) {
	if arrangement == nil {
		return nil, ErrSpillableArrangementNil
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	if err := arrangement.ensureOpenLocked(); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(arrangement.entries))
	for key := range arrangement.entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	rows := make([]SpillableArrangementEntry, 0, len(keys))
	for _, key := range keys {
		entry := arrangement.entries[key]
		var value []byte
		var err error
		if entry.valueHot {
			value = append([]byte(nil), entry.value...)
		} else {
			value, err = arrangement.readColdValueLocked(entry)
			if err != nil {
				return nil, err
			}
		}
		rows = append(rows, SpillableArrangementEntry{Key: key, Value: value})
	}
	return rows, nil
}

// Stats returns current memory and segment counters. It remains readable after
// Close so operators can retain a final accounting snapshot.
func (arrangement *SpillableArrangement) Stats() SpillableArrangementStats {
	if arrangement == nil {
		return SpillableArrangementStats{}
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	return SpillableArrangementStats{
		Entries:      len(arrangement.entries),
		ColdEntries:  arrangement.coldEntries,
		HotBytes:     arrangement.hotBytes,
		DiskBytes:    arrangement.diskBytes,
		SpillRecords: arrangement.spillRecords,
	}
}

// SpillPath returns the local segment path. Call Flush before treating the
// path as durable, then pass it to OpenSpillableArrangement after restart.
func (arrangement *SpillableArrangement) SpillPath() string {
	if arrangement == nil {
		return ""
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	return arrangement.spillPath
}

// SpillIndexPath returns the advisory persisted index path. Backups that
// retain the segment may include this file; OpenSpillableArrangement safely
// rebuilds the index from records when it is absent or invalid.
func (arrangement *SpillableArrangement) SpillIndexPath() string {
	if arrangement == nil {
		return ""
	}
	arrangement.mu.RLock()
	defer arrangement.mu.RUnlock()
	return spillableArrangementIndexPath(arrangement.spillPath)
}

// Close closes the spill segment. An arrangement created without Directory
// owns and removes its temporary directory; caller-provided directories are
// left intact for inspection and backup.
func (arrangement *SpillableArrangement) Close() error {
	if arrangement == nil {
		return ErrSpillableArrangementNil
	}
	arrangement.mu.Lock()
	if arrangement.closed {
		arrangement.mu.Unlock()
		return nil
	}
	arrangement.closed = true
	fileErr := arrangement.file.Close()
	ownedDirectory := arrangement.ownedDirectory
	directory := arrangement.directory
	arrangement.mu.Unlock()
	if ownedDirectory {
		if err := os.RemoveAll(directory); fileErr == nil {
			fileErr = err
		}
	}
	return fileErr
}

func (arrangement *SpillableArrangement) ensureOpenLocked() error {
	if arrangement.closed || arrangement.file == nil {
		return ErrSpillableArrangementClosed
	}
	return nil
}

func (arrangement *SpillableArrangement) validateKey(key string) error {
	if key == "" {
		return ErrSpillableArrangementKeyRequired
	}
	if int64(len(key)) > arrangement.maxKeyBytes {
		return ErrSpillableArrangementKeyTooLarge
	}
	return nil
}

func (arrangement *SpillableArrangement) hotEntriesLocked() []*spillableArrangementEntry {
	entries := make([]*spillableArrangementEntry, 0, len(arrangement.entries))
	for _, entry := range arrangement.entries {
		if entry.valueHot {
			entries = append(entries, entry)
		}
	}
	sort.Slice(entries, func(left, right int) bool { return entries[left].key < entries[right].key })
	return entries
}

func (arrangement *SpillableArrangement) coldEntriesLocked() []*spillableArrangementEntry {
	entries := make([]*spillableArrangementEntry, 0, arrangement.coldEntries)
	for _, entry := range arrangement.entries {
		if !entry.valueHot {
			entries = append(entries, entry)
		}
	}
	return entries
}

func (arrangement *SpillableArrangement) spillToLimitLocked() error {
	plan, requiredBytes := arrangement.spillPlanLocked()
	if len(plan) == 0 {
		return nil
	}
	if arrangement.maxDiskBytes > 0 && requiredBytes > arrangement.maxDiskBytes-arrangement.diskBytes {
		return ErrSpillableArrangementDiskLimit
	}
	return arrangement.spillEntriesLocked(plan)
}

func (arrangement *SpillableArrangement) checkSpillDiskLimitLocked(entries []*spillableArrangementEntry) error {
	var requiredBytes int64
	for _, entry := range entries {
		if !entry.valueHot {
			continue
		}
		recordSize, ok := spillableArrangementRecordSize(entry.key, len(entry.value), arrangement.maxKeyBytes, arrangement.maxValueBytes)
		if !ok || recordSize > math.MaxInt64-requiredBytes {
			return ErrSpillableArrangementDiskLimit
		}
		requiredBytes += recordSize
	}
	if arrangement.maxDiskBytes > 0 && requiredBytes > arrangement.maxDiskBytes-arrangement.diskBytes {
		return ErrSpillableArrangementDiskLimit
	}
	return nil
}

func (arrangement *SpillableArrangement) spillPlanLocked() ([]*spillableArrangementEntry, int64) {
	if arrangement.hotBytes <= arrangement.memoryLimit {
		return nil, 0
	}
	remainingHot := arrangement.hotBytes
	plan := make([]*spillableArrangementEntry, 0)
	var requiredBytes int64
	for index := arrangement.queueHead; index < len(arrangement.queue) && remainingHot > arrangement.memoryLimit; index++ {
		item := arrangement.queue[index]
		entry := arrangement.entries[item.key]
		if entry == nil || !entry.valueHot || entry.gen != item.gen {
			continue
		}
		plan = append(plan, entry)
		remainingHot -= int64(len(entry.value))
		recordSize, ok := spillableArrangementRecordSize(entry.key, len(entry.value), arrangement.maxKeyBytes, arrangement.maxValueBytes)
		if !ok || recordSize > math.MaxInt64-requiredBytes {
			return plan, math.MaxInt64
		}
		requiredBytes += recordSize
	}
	return plan, requiredBytes
}

func (arrangement *SpillableArrangement) spillEntriesLocked(entries []*spillableArrangementEntry) error {
	for _, entry := range entries {
		if !entry.valueHot {
			continue
		}
		valueBytes := int64(len(entry.value))
		ref, err := arrangement.appendRecordLocked(entry.key, entry.value)
		if err != nil {
			return err
		}
		entry.ref = ref
		entry.value = nil
		entry.valueHot = false
		arrangement.hotBytes -= valueBytes
		arrangement.coldEntries++
	}
	arrangement.advanceQueueHeadLocked()
	return nil
}

func (arrangement *SpillableArrangement) appendRecordLocked(key string, value []byte) (spillableArrangementRef, error) {
	record, err := spillableArrangementRecord(key, value, arrangement.maxKeyBytes, arrangement.maxValueBytes)
	if err != nil {
		return spillableArrangementRef{}, err
	}
	offset := arrangement.diskBytes
	if err := writeSpillableArrangementAll(arrangement.file, record); err != nil {
		return spillableArrangementRef{}, err
	}
	arrangement.diskBytes += int64(len(record))
	arrangement.spillRecords++
	return spillableArrangementRef{offset: offset, total: int64(len(record))}, nil
}

func (arrangement *SpillableArrangement) readColdValueLocked(entry *spillableArrangementEntry) ([]byte, error) {
	if entry.ref.total < spillableArrangementHeaderSize || entry.ref.offset < 0 || entry.ref.offset > arrangement.diskBytes-entry.ref.total {
		return nil, ErrSpillableArrangementCorrupt
	}
	record := make([]byte, entry.ref.total)
	if _, err := arrangement.file.ReadAt(record, entry.ref.offset); err != nil {
		return nil, ErrSpillableArrangementCorrupt
	}
	if len(record) < spillableArrangementHeaderSize || !equalSpillableArrangementMagic(record[:4]) {
		return nil, ErrSpillableArrangementCorrupt
	}
	keyLength := int64(binary.LittleEndian.Uint32(record[4:8]))
	valueLength := int64(binary.LittleEndian.Uint64(record[8:16]))
	if keyLength <= 0 || keyLength > arrangement.maxKeyBytes || valueLength < 0 || valueLength > arrangement.maxValueBytes || keyLength > int64(len(record))-spillableArrangementHeaderSize || valueLength != int64(len(record))-spillableArrangementHeaderSize-keyLength {
		return nil, ErrSpillableArrangementCorrupt
	}
	if string(record[spillableArrangementHeaderSize:spillableArrangementHeaderSize+keyLength]) != entry.key {
		return nil, ErrSpillableArrangementCorrupt
	}
	payloadStart := spillableArrangementHeaderSize + keyLength
	value := record[payloadStart : payloadStart+valueLength : payloadStart+valueLength]
	if crc32.ChecksumIEEE(record[spillableArrangementHeaderSize:]) != binary.LittleEndian.Uint32(record[16:20]) {
		return nil, ErrSpillableArrangementCorrupt
	}
	return value, nil
}

func (arrangement *SpillableArrangement) maybeCompactQueueLocked() {
	arrangement.advanceQueueHeadLocked()
	if arrangement.queueHead < 1024 || arrangement.queueHead*2 < len(arrangement.queue) {
		return
	}
	queue := arrangement.queue[:0]
	for index := arrangement.queueHead; index < len(arrangement.queue); index++ {
		item := arrangement.queue[index]
		entry := arrangement.entries[item.key]
		if entry != nil && entry.valueHot && entry.gen == item.gen {
			queue = append(queue, item)
		}
	}
	arrangement.queue = queue
	arrangement.queueHead = 0
}

func (arrangement *SpillableArrangement) advanceQueueHeadLocked() {
	for arrangement.queueHead < len(arrangement.queue) {
		item := arrangement.queue[arrangement.queueHead]
		entry := arrangement.entries[item.key]
		if entry != nil && entry.valueHot && entry.gen == item.gen {
			return
		}
		arrangement.queueHead++
	}
}

func spillableArrangementRecord(key string, value []byte, maxKeyBytes, maxValueBytes int64) ([]byte, error) {
	if key == "" {
		return nil, ErrSpillableArrangementKeyRequired
	}
	if int64(len(key)) > maxKeyBytes {
		return nil, ErrSpillableArrangementKeyTooLarge
	}
	if int64(len(value)) > maxValueBytes {
		return nil, ErrSpillableArrangementValueTooLarge
	}
	if uint64(len(key)) > math.MaxUint32 {
		return nil, ErrSpillableArrangementKeyTooLarge
	}
	recordSize, ok := spillableArrangementRecordSize(key, len(value), maxKeyBytes, maxValueBytes)
	if !ok {
		return nil, ErrSpillableArrangementLimitInvalid
	}
	record := make([]byte, recordSize)
	copy(record[:4], spillableArrangementMagic[:])
	binary.LittleEndian.PutUint32(record[4:8], uint32(len(key)))
	binary.LittleEndian.PutUint64(record[8:16], uint64(len(value)))
	copy(record[spillableArrangementHeaderSize:], key)
	copy(record[spillableArrangementHeaderSize+len(key):], value)
	binary.LittleEndian.PutUint32(record[16:20], crc32.ChecksumIEEE(record[spillableArrangementHeaderSize:]))
	return record, nil
}

func spillableArrangementRecordSize(key string, valueLength int, maxKeyBytes, maxValueBytes int64) (int64, bool) {
	if int64(len(key)) <= 0 || int64(len(key)) > maxKeyBytes || int64(valueLength) < 0 || int64(valueLength) > maxValueBytes {
		return 0, false
	}
	total := int64(spillableArrangementHeaderSize) + int64(len(key))
	if int64(valueLength) > math.MaxInt64-total {
		return 0, false
	}
	return total + int64(valueLength), true
}

func writeSpillableArrangementAll(file *os.File, payload []byte) error {
	for len(payload) > 0 {
		written, err := file.Write(payload)
		if err != nil {
			return err
		}
		if written <= 0 {
			return io.ErrShortWrite
		}
		payload = payload[written:]
	}
	return nil
}

func equalSpillableArrangementMagic(value []byte) bool {
	return len(value) == len(spillableArrangementMagic) && value[0] == spillableArrangementMagic[0] && value[1] == spillableArrangementMagic[1] && value[2] == spillableArrangementMagic[2] && value[3] == spillableArrangementMagic[3]
}
