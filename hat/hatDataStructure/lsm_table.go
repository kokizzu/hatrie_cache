package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"
)

const (
	// DefaultLSMTableMemtableMaxRecords flushes a mutable table after this many
	// distinct keys have accumulated.
	DefaultLSMTableMemtableMaxRecords = 4096
	// DefaultLSMTableMaxRunsBeforeCompaction bounds read amplification by
	// compacting all immutable runs after this many runs exist.
	DefaultLSMTableMaxRunsBeforeCompaction = 8
	// DefaultLSMTableMaxWireBytes bounds one table snapshot.
	DefaultLSMTableMaxWireBytes = 512 << 20
	lsmTableHeaderSize          = 32
	lsmTableVersion             = 1
)

var lsmTableMagic = [4]byte{'H', 'L', 'S', '1'}

var (
	ErrLSMTableNil           = errors.New("hatDataStructure: lsm table is nil")
	ErrLSMTableOptions       = errors.New("hatDataStructure: lsm table options are invalid")
	ErrLSMTableKeyRequired   = errors.New("hatDataStructure: lsm table key is required")
	ErrLSMTableValueTooLarge = errors.New("hatDataStructure: lsm table value is too large")
	ErrLSMTableWireLimit     = errors.New("hatDataStructure: lsm table wire limit exceeded")
	ErrLSMTableCorrupt       = errors.New("hatDataStructure: lsm table is corrupt")
	ErrLSMTableCompaction    = errors.New("hatDataStructure: lsm table compaction limit exceeded")
)

// LSMCompactionMode controls when immutable runs are compacted.
type LSMCompactionMode uint8

const (
	// LSMCompactionImmediate preserves the original synchronous compaction
	// behavior and is the zero-value default.
	LSMCompactionImmediate LSMCompactionMode = iota
	// LSMCompactionDeferred leaves compaction to CompactIfNeeded or an
	// LSMCompactionScheduler owned by the caller.
	LSMCompactionDeferred
)

// LSMCompactionPolicy configures the compaction trigger. Deferred mode uses
// the run threshold by default and can additionally compact when older-run
// wire bytes reach MaxDebtBytes. Either positive threshold is sufficient.
type LSMCompactionPolicy struct {
	Mode         LSMCompactionMode
	MaxDebtRuns  int
	MaxDebtBytes int
}

// LSMTableOptions configures the opt-in Vinyl-style mutable table. The
// memtable is copied into immutable SealedUpsertRun values at the threshold;
// all zero fields use bounded defaults.
type LSMTableOptions struct {
	MemtableMaxRecords      int
	MaxRunsBeforeCompaction int
	RunOptions              SealedUpsertRunOptions
	MaxWireBytes            int
	Compaction              LSMCompactionPolicy
}

type lsmTableConfig struct {
	memtableMaxRecords      int
	maxRunsBeforeCompaction int
	maxWireBytes            int
	runOptions              SealedUpsertRunOptions
	runConfig               sealedUpsertRunConfig
	compaction              LSMCompactionPolicy
}

type lsmTableRecord struct {
	value   []byte
	deleted bool
}

// LSMTableStats describes the current mutable and immutable portions without
// materializing all keys.
type LSMTableStats struct {
	MemtableRecords       int
	MemtableBytes         int
	MemtableTombstones    int
	RunCount              int
	ImmutableBytes        int
	CompactionDebtRuns    int
	CompactionDebtBytes   int
	CompactionCount       uint64
	CompactionInputBytes  uint64
	CompactionOutputBytes uint64
}

// LSMTable is an opt-in bounded log-structured table for byte values. Runs
// are searched newest first, so a tombstone masks older values until the next
// compaction. The default engine remains unchanged; callers choose this
// table when write batching, compact snapshots, and bounded memory matter
// more than plain-map point lookup latency.
type LSMTable struct {
	mu       sync.RWMutex
	config   lsmTableConfig
	memtable map[string]lsmTableRecord
	runs     []*SealedUpsertRun

	memtableBytes      int
	memtableTombstones int

	compactionCount       uint64
	compactionInputBytes  uint64
	compactionOutputBytes uint64
}

// NewLSMTable creates an empty table with copied, validated options.
func NewLSMTable(options LSMTableOptions) (*LSMTable, error) {
	config, err := normalizeLSMTableOptions(options)
	if err != nil {
		return nil, err
	}
	return &LSMTable{
		config:   config,
		memtable: make(map[string]lsmTableRecord),
	}, nil
}

// Put copies value and makes it the newest value for key. A full memtable is
// flushed and may trigger bounded automatic compaction before Put returns.
func (table *LSMTable) Put(key string, value []byte) error {
	if table == nil {
		return ErrLSMTableNil
	}
	if key == "" {
		return ErrLSMTableKeyRequired
	}
	if len(value) > table.config.runConfig.maxValueBytes {
		return ErrLSMTableValueTooLarge
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.memtable == nil {
		table.memtable = make(map[string]lsmTableRecord)
	}
	table.replaceMemtableRecordLocked(key, lsmTableRecord{value: append([]byte(nil), value...)})
	if len(table.memtable) < table.config.memtableMaxRecords {
		return nil
	}
	if err := table.flushMemtableLocked(); err != nil {
		return err
	}
	if table.config.compaction.Mode == LSMCompactionImmediate && len(table.runs) >= table.config.maxRunsBeforeCompaction {
		return table.compactRunsLocked()
	}
	return nil
}

// Delete records a tombstone for key. The tombstone is retained until an
// older run is folded into a newer compacted run.
func (table *LSMTable) Delete(key string) error {
	if table == nil {
		return ErrLSMTableNil
	}
	if key == "" {
		return ErrLSMTableKeyRequired
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if table.memtable == nil {
		table.memtable = make(map[string]lsmTableRecord)
	}
	table.replaceMemtableRecordLocked(key, lsmTableRecord{deleted: true})
	if len(table.memtable) < table.config.memtableMaxRecords {
		return nil
	}
	if err := table.flushMemtableLocked(); err != nil {
		return err
	}
	if table.config.compaction.Mode == LSMCompactionImmediate && len(table.runs) >= table.config.maxRunsBeforeCompaction {
		return table.compactRunsLocked()
	}
	return nil
}

// Get returns an independent value from the newest visible record. Tombstones
// and missing keys both return (nil, false).
func (table *LSMTable) Get(key string) ([]byte, bool) {
	if table == nil || key == "" {
		return nil, false
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	if record, ok := table.memtable[key]; ok {
		if record.deleted {
			return nil, false
		}
		return append([]byte(nil), record.value...), true
	}
	for _, run := range table.runs {
		record, ok := run.Lookup(key)
		if !ok {
			continue
		}
		if record.Deleted {
			return nil, false
		}
		return append([]byte(nil), record.Value...), true
	}
	return nil, false
}

// Flush seals the current memtable without forcing compaction.
func (table *LSMTable) Flush() error {
	if table == nil {
		return ErrLSMTableNil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	return table.flushMemtableLocked()
}

// Compact flushes pending writes and folds all immutable runs into one newer
// run. Obsolete values and tombstones are removed only after their older
// history has been included in the fold.
func (table *LSMTable) Compact() error {
	if table == nil {
		return ErrLSMTableNil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if err := table.flushMemtableLocked(); err != nil {
		return err
	}
	return table.compactRunsLocked()
}

// CompactionDue reports whether the current immutable runs meet the deferred
// compaction policy. Pending memtable records are not included until Flush or
// CompactIfNeeded seals them.
func (table *LSMTable) CompactionDue() bool {
	if table == nil {
		return false
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	return table.compactionDueLocked()
}

// CompactIfNeeded flushes pending writes and performs one compaction when the
// configured deferred policy is due. It returns false when no work was due.
func (table *LSMTable) CompactIfNeeded() (bool, error) {
	if table == nil {
		return false, ErrLSMTableNil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if err := table.flushMemtableLocked(); err != nil {
		return false, err
	}
	if !table.compactionDueLocked() {
		return false, nil
	}
	if err := table.compactRunsLocked(); err != nil {
		return true, err
	}
	return true, nil
}

// Stats returns bounded structural counters for the current table.
func (table *LSMTable) Stats() LSMTableStats {
	if table == nil {
		return LSMTableStats{}
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	stats := LSMTableStats{
		MemtableRecords:       len(table.memtable),
		MemtableBytes:         table.memtableBytes,
		MemtableTombstones:    table.memtableTombstones,
		RunCount:              len(table.runs),
		CompactionDebtRuns:    table.compactionDebtRunsLocked(),
		CompactionDebtBytes:   table.compactionDebtBytesLocked(),
		CompactionCount:       table.compactionCount,
		CompactionInputBytes:  table.compactionInputBytes,
		CompactionOutputBytes: table.compactionOutputBytes,
	}
	for _, run := range table.runs {
		if run != nil {
			stats.ImmutableBytes += run.WireBytes()
		}
	}
	return stats
}

// MarshalBinary flushes pending writes and returns an independent snapshot of
// the immutable runs. Each nested run is independently CRC-protected.
func (table *LSMTable) MarshalBinary() ([]byte, error) {
	if table == nil {
		return nil, ErrLSMTableNil
	}
	table.mu.Lock()
	defer table.mu.Unlock()
	if err := table.flushMemtableLocked(); err != nil {
		return nil, err
	}
	bodyBytes := 0
	for _, run := range table.runs {
		if run == nil || run.WireBytes() > int(^uint32(0)) {
			return nil, ErrLSMTableCorrupt
		}
		if bodyBytes > table.config.maxWireBytes-lsmTableHeaderSize-8-run.WireBytes() {
			return nil, ErrLSMTableWireLimit
		}
		bodyBytes += 8 + run.WireBytes()
	}
	if bodyBytes > table.config.maxWireBytes-lsmTableHeaderSize {
		return nil, ErrLSMTableWireLimit
	}
	data := make([]byte, lsmTableHeaderSize+bodyBytes)
	copy(data[:4], lsmTableMagic[:])
	binary.LittleEndian.PutUint16(data[4:6], lsmTableVersion)
	binary.LittleEndian.PutUint32(data[8:12], uint32(len(table.runs)))
	binary.LittleEndian.PutUint64(data[12:20], uint64(bodyBytes))
	offset := lsmTableHeaderSize
	for _, run := range table.runs {
		wire, err := run.MarshalBinary()
		if err != nil {
			return nil, err
		}
		binary.LittleEndian.PutUint64(data[offset:offset+8], uint64(len(wire)))
		offset += 8
		copy(data[offset:], wire)
		offset += len(wire)
	}
	binary.LittleEndian.PutUint32(data[20:24], crc32.ChecksumIEEE(data[lsmTableHeaderSize:]))
	return data, nil
}

// UnmarshalLSMTable restores a snapshot and copies all encoded bytes.
func UnmarshalLSMTable(data []byte, options LSMTableOptions) (*LSMTable, error) {
	config, err := normalizeLSMTableOptions(options)
	if err != nil {
		return nil, err
	}
	if len(data) > config.maxWireBytes {
		return nil, ErrLSMTableWireLimit
	}
	if len(data) < lsmTableHeaderSize || !equalLSMTableMagic(data[:4]) {
		return nil, ErrLSMTableCorrupt
	}
	if binary.LittleEndian.Uint16(data[4:6]) != lsmTableVersion || binary.LittleEndian.Uint16(data[6:8]) != 0 || binary.LittleEndian.Uint32(data[24:28]) != 0 || binary.LittleEndian.Uint32(data[28:32]) != 0 {
		return nil, ErrLSMTableCorrupt
	}
	runCount := uint64(binary.LittleEndian.Uint32(data[8:12]))
	bodyBytes := binary.LittleEndian.Uint64(data[12:20])
	if bodyBytes != uint64(len(data)-lsmTableHeaderSize) || binary.LittleEndian.Uint32(data[20:24]) != crc32.ChecksumIEEE(data[lsmTableHeaderSize:]) {
		return nil, ErrLSMTableCorrupt
	}
	if runCount > uint64(len(data)-lsmTableHeaderSize)/8 {
		return nil, ErrLSMTableCorrupt
	}
	runs := make([]*SealedUpsertRun, 0, int(runCount))
	offset := lsmTableHeaderSize
	for index := uint64(0); index < runCount; index++ {
		if len(data)-offset < 8 {
			return nil, ErrLSMTableCorrupt
		}
		runBytes := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8
		if runBytes > uint64(len(data)-offset) {
			return nil, ErrLSMTableCorrupt
		}
		run, err := UnmarshalSealedUpsertRun(data[offset:offset+int(runBytes)], config.runOptions)
		if err != nil {
			return nil, ErrLSMTableCorrupt
		}
		runs = append(runs, run)
		offset += int(runBytes)
	}
	if offset != len(data) {
		return nil, ErrLSMTableCorrupt
	}
	return &LSMTable{config: config, memtable: make(map[string]lsmTableRecord), runs: runs}, nil
}

func normalizeLSMTableOptions(options LSMTableOptions) (lsmTableConfig, error) {
	if options.MemtableMaxRecords < 0 || options.MaxRunsBeforeCompaction < 0 || options.MaxWireBytes < 0 || options.Compaction.MaxDebtRuns < 0 || options.Compaction.MaxDebtBytes < 0 {
		return lsmTableConfig{}, ErrLSMTableOptions
	}
	if options.Compaction.Mode != LSMCompactionImmediate && options.Compaction.Mode != LSMCompactionDeferred {
		return lsmTableConfig{}, ErrLSMTableOptions
	}
	runConfig, err := normalizeSealedUpsertRunOptions(options.RunOptions)
	if err != nil {
		return lsmTableConfig{}, ErrLSMTableOptions
	}
	memtableMaxRecords := options.MemtableMaxRecords
	if memtableMaxRecords == 0 {
		memtableMaxRecords = DefaultLSMTableMemtableMaxRecords
	}
	if memtableMaxRecords > runConfig.maxRecords {
		return lsmTableConfig{}, ErrLSMTableOptions
	}
	maxRuns := options.MaxRunsBeforeCompaction
	if maxRuns == 0 {
		maxRuns = DefaultLSMTableMaxRunsBeforeCompaction
	}
	compaction := options.Compaction
	if compaction.Mode == LSMCompactionDeferred && compaction.MaxDebtRuns == 0 {
		compaction.MaxDebtRuns = maxRuns
	}
	maxWireBytes := options.MaxWireBytes
	if maxWireBytes == 0 {
		maxWireBytes = DefaultLSMTableMaxWireBytes
	}
	if maxWireBytes < lsmTableHeaderSize+8 || uint64(maxWireBytes) > uint64(^uint32(0))<<32 {
		return lsmTableConfig{}, ErrLSMTableOptions
	}
	return lsmTableConfig{
		memtableMaxRecords:      memtableMaxRecords,
		maxRunsBeforeCompaction: maxRuns,
		maxWireBytes:            maxWireBytes,
		runOptions:              options.RunOptions,
		runConfig:               runConfig,
		compaction:              compaction,
	}, nil
}

func (table *LSMTable) flushMemtableLocked() error {
	if len(table.memtable) == 0 {
		return nil
	}
	records := make([]UpsertRecord[[]byte], 0, len(table.memtable))
	for key, record := range table.memtable {
		records = append(records, UpsertRecord[[]byte]{
			Key:     key,
			Value:   record.value,
			Deleted: record.deleted,
		})
	}
	run, err := NewSealedUpsertRun(records, table.config.runOptions)
	if err != nil {
		return err
	}
	table.runs = append([]*SealedUpsertRun{run}, table.runs...)
	table.memtable = make(map[string]lsmTableRecord)
	table.memtableBytes = 0
	table.memtableTombstones = 0
	return nil
}

func (table *LSMTable) compactRunsLocked() error {
	if len(table.runs) < 2 {
		return nil
	}
	inputBytes := uint64(0)
	latest := make(map[string]SealedUpsertRecord)
	for _, run := range table.runs {
		if run == nil {
			return ErrLSMTableCorrupt
		}
		inputBytes += uint64(run.WireBytes())
		run.ForEach(func(record SealedUpsertRecord) {
			if _, exists := latest[record.Key]; !exists {
				latest[record.Key] = record
			}
		})
	}
	records := make([]UpsertRecord[[]byte], 0, len(latest))
	for _, record := range latest {
		if record.Deleted {
			continue
		}
		records = append(records, UpsertRecord[[]byte]{Key: record.Key, Value: record.Value})
	}
	if len(records) > table.config.runConfig.maxRecords {
		return ErrLSMTableCompaction
	}
	if len(records) == 0 {
		table.runs = nil
		table.recordCompactionLocked(inputBytes, 0)
		return nil
	}
	run, err := NewSealedUpsertRun(records, table.config.runOptions)
	if err != nil {
		return ErrLSMTableCompaction
	}
	table.runs = []*SealedUpsertRun{run}
	table.recordCompactionLocked(inputBytes, uint64(run.WireBytes()))
	return nil
}

func (table *LSMTable) replaceMemtableRecordLocked(key string, record lsmTableRecord) {
	if previous, exists := table.memtable[key]; exists {
		table.memtableBytes -= len(key) + len(previous.value)
		if previous.deleted {
			table.memtableTombstones--
		}
	}
	table.memtable[key] = record
	table.memtableBytes += len(key) + len(record.value)
	if record.deleted {
		table.memtableTombstones++
	}
}

func (table *LSMTable) compactionDueLocked() bool {
	if table.config.compaction.Mode != LSMCompactionDeferred {
		return false
	}
	debtRuns := table.compactionDebtRunsLocked()
	debtBytes := table.compactionDebtBytesLocked()
	return (table.config.compaction.MaxDebtRuns > 0 && debtRuns >= table.config.compaction.MaxDebtRuns) ||
		(table.config.compaction.MaxDebtBytes > 0 && debtBytes >= table.config.compaction.MaxDebtBytes)
}

func (table *LSMTable) compactionDebtRunsLocked() int {
	if len(table.runs) < 2 {
		return 0
	}
	return len(table.runs) - 1
}

func (table *LSMTable) compactionDebtBytesLocked() int {
	if len(table.runs) < 2 {
		return 0
	}
	debtBytes := 0
	for _, run := range table.runs[1:] {
		if run != nil {
			debtBytes += run.WireBytes()
		}
	}
	return debtBytes
}

func (table *LSMTable) recordCompactionLocked(inputBytes, outputBytes uint64) {
	table.compactionCount++
	table.compactionInputBytes += inputBytes
	table.compactionOutputBytes += outputBytes
}

func equalLSMTableMagic(value []byte) bool {
	return len(value) >= len(lsmTableMagic) && value[0] == lsmTableMagic[0] && value[1] == lsmTableMagic[1] && value[2] == lsmTableMagic[2] && value[3] == lsmTableMagic[3]
}
