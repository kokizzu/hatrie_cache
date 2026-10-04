// Package hatSpace provides a small per-space engine contract. Memtx keeps
// tuples in memory; vinyl retains the hot set in memory and spills cold values
// to the existing checksummed spillable arrangement.
package hatSpace

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"hatrie_cache/hat/hatDataStructure"
)

// EngineKind selects the storage policy for one space.
type EngineKind string

const (
	// EngineMemtx is the default volatile in-memory engine.
	EngineMemtx EngineKind = "memtx"
	// EngineVinyl is a bounded-memory, disk-backed spill engine.
	EngineVinyl EngineKind = "vinyl"
)

var (
	ErrSpaceEngineKindInvalid  = errors.New("hatSpace: invalid engine kind")
	ErrSpaceEngineLimitInvalid = errors.New("hatSpace: engine limit is invalid")
	ErrSpaceEngineMemoryLimit  = errors.New("hatSpace: memory limit exceeded")
	ErrSpaceEngineNil          = errors.New("hatSpace: engine is nil")

	// Storage validation errors are shared with the underlying vinyl engine so
	// callers can use errors.Is across either engine kind.
	ErrSpaceEngineClosed        = hatDataStructure.ErrSpillableArrangementClosed
	ErrSpaceEngineKeyRequired   = hatDataStructure.ErrSpillableArrangementKeyRequired
	ErrSpaceEngineKeyTooLarge   = hatDataStructure.ErrSpillableArrangementKeyTooLarge
	ErrSpaceEngineValueTooLarge = hatDataStructure.ErrSpillableArrangementValueTooLarge
	ErrSpaceEngineDiskLimit     = hatDataStructure.ErrSpillableArrangementDiskLimit
	ErrSpaceEnginePathInvalid   = hatDataStructure.ErrSpillableArrangementPathInvalid
)

// Options configures one selected space engine. A zero Kind uses memtx.
// Path reopens one existing vinyl spill segment; Directory creates a new
// vinyl segment in that directory. Memtx never writes either path.
type Options struct {
	Kind             EngineKind
	Path             string
	Directory        string
	// MemoryLimitBytes bounds logical key-plus-value bytes for memtx and hot
	// value payload bytes for vinyl. Vinyl key/index metadata remains resident.
	MemoryLimitBytes int64
	MaxDiskBytes     int64
	MaxKeyBytes      int64
	MaxValueBytes    int64
}

// Profile describes the deliberate read/write and durability tradeoff of an
// engine without exposing implementation handles.
type Profile struct {
	Kind               EngineKind
	Durable            bool
	MemoryBounded      bool
	SupportsCompaction bool
	ReadAmplification  string
	WriteAmplification string
}

// Entry is an independent key/value snapshot row.
type Entry struct {
	Key   string
	Value []byte
}

// Stats reports engine state and cumulative operation counters.
type Stats struct {
	Kind         EngineKind
	Entries      int
	ColdEntries  int
	HotBytes     int64
	DiskBytes    int64
	SpillRecords uint64
	Sets         uint64
	Gets         uint64
	Hits         uint64
	Deletes      uint64
}

// Engine is the common per-space tuple lifecycle. Set/Get/Snapshot use copy
// isolation, while Flush and Compact make durability and stale-byte cleanup
// explicit for the vinyl engine. Memtx implements them as checked no-ops.
type Engine interface {
	Kind() EngineKind
	Profile() Profile
	Path() string
	Set(string, []byte) error
	Get(string) ([]byte, bool, error)
	Delete(string) bool
	Snapshot() ([]Entry, error)
	Flush() error
	Compact() error
	Stats() Stats
	Close() error
}

// Open creates the requested per-space engine. The default is memtx, keeping
// existing volatile callers free of filesystem work unless vinyl is explicit.
func Open(options Options) (Engine, error) {
	options, err := normalizeOptions(options)
	if err != nil {
		return nil, err
	}
	switch options.Kind {
	case EngineMemtx:
		return newMemtx(options), nil
	case EngineVinyl:
		arrangementOptions := hatDataStructure.SpillableArrangementOptions{
			Directory:        options.Directory,
			MemoryLimitBytes: options.MemoryLimitBytes,
			MaxDiskBytes:     options.MaxDiskBytes,
			MaxKeyBytes:      options.MaxKeyBytes,
			MaxValueBytes:    options.MaxValueBytes,
		}
		var arrangement *hatDataStructure.SpillableArrangement
		if options.Path != "" {
			arrangement, err = hatDataStructure.OpenSpillableArrangement(options.Path, arrangementOptions)
		} else {
			arrangement, err = hatDataStructure.NewSpillableArrangement(arrangementOptions)
		}
		if err != nil {
			return nil, err
		}
		return &vinylEngine{arrangement: arrangement}, nil
	default:
		return nil, ErrSpaceEngineKindInvalid
	}
}

func normalizeOptions(options Options) (Options, error) {
	if options.Kind == "" {
		options.Kind = EngineMemtx
	}
	if options.Kind != EngineMemtx && options.Kind != EngineVinyl {
		return Options{}, ErrSpaceEngineKindInvalid
	}
	if options.MemoryLimitBytes < 0 || options.MaxDiskBytes < 0 || options.MaxKeyBytes < 0 || options.MaxValueBytes < 0 {
		return Options{}, ErrSpaceEngineLimitInvalid
	}
	if options.Kind == EngineMemtx && options.Path != "" {
		return Options{}, ErrSpaceEnginePathInvalid
	}
	return options, nil
}

func profileFor(kind EngineKind) Profile {
	switch kind {
	case EngineVinyl:
		return Profile{
			Kind:               EngineVinyl,
			Durable:            true,
			MemoryBounded:      true,
			SupportsCompaction: true,
			ReadAmplification:  "memtable plus cold spill runs",
			WriteAmplification: "append spill records plus compaction",
		}
	default:
		return Profile{
			Kind:               EngineMemtx,
			Durable:            false,
			MemoryBounded:      true,
			SupportsCompaction: false,
			ReadAmplification:  "single in-memory map lookup",
			WriteAmplification: "single in-memory replacement",
		}
	}
}

type memtxEngine struct {
	mu      sync.RWMutex
	options Options
	values  map[string][]byte
	bytes   int64
	closed  bool
	sets    uint64
	gets    uint64
	hits    uint64
	deletes uint64
}

func newMemtx(options Options) *memtxEngine {
	return &memtxEngine{options: options, values: make(map[string][]byte)}
}

func (engine *memtxEngine) Kind() EngineKind { return EngineMemtx }

func (engine *memtxEngine) Profile() Profile { return profileFor(EngineMemtx) }

func (engine *memtxEngine) Path() string { return "" }

func (engine *memtxEngine) Set(key string, value []byte) error {
	if engine == nil {
		return ErrSpaceEngineNil
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return ErrSpaceEngineClosed
	}
	if err := validateKeyValue(key, value, engine.options); err != nil {
		return err
	}
	previousBytes := int64(0)
	if previous, ok := engine.values[key]; ok {
		previousBytes = int64(len(key) + len(previous))
	}
	nextBytes := engine.bytes - previousBytes + int64(len(key)+len(value))
	if engine.options.MemoryLimitBytes > 0 && nextBytes > engine.options.MemoryLimitBytes {
		return ErrSpaceEngineMemoryLimit
	}
	engine.values[key] = append([]byte(nil), value...)
	engine.bytes = nextBytes
	atomic.AddUint64(&engine.sets, 1)
	return nil
}

func (engine *memtxEngine) Get(key string) ([]byte, bool, error) {
	if engine == nil {
		return nil, false, ErrSpaceEngineNil
	}
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if engine.closed {
		return nil, false, ErrSpaceEngineClosed
	}
	atomic.AddUint64(&engine.gets, 1)
	value, ok := engine.values[key]
	if !ok {
		return nil, false, nil
	}
	atomic.AddUint64(&engine.hits, 1)
	return append([]byte(nil), value...), true, nil
}

func (engine *memtxEngine) Delete(key string) bool {
	if engine == nil {
		return false
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return false
	}
	value, ok := engine.values[key]
	if !ok {
		return false
	}
	delete(engine.values, key)
	engine.bytes -= int64(len(key) + len(value))
	atomic.AddUint64(&engine.deletes, 1)
	return true
}

func (engine *memtxEngine) Snapshot() ([]Entry, error) {
	if engine == nil {
		return nil, ErrSpaceEngineNil
	}
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if engine.closed {
		return nil, ErrSpaceEngineClosed
	}
	keys := make([]string, 0, len(engine.values))
	for key := range engine.values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	entries := make([]Entry, len(keys))
	for index, key := range keys {
		entries[index] = Entry{Key: key, Value: append([]byte(nil), engine.values[key]...)}
	}
	return entries, nil
}

func (engine *memtxEngine) Flush() error {
	if engine == nil {
		return ErrSpaceEngineNil
	}
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if engine.closed {
		return ErrSpaceEngineClosed
	}
	return nil
}

func (engine *memtxEngine) Compact() error { return engine.Flush() }

func (engine *memtxEngine) Stats() Stats {
	if engine == nil {
		return Stats{}
	}
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	return Stats{
		Kind:     EngineMemtx,
		Entries:  len(engine.values),
		HotBytes: engine.bytes,
		Sets:     atomic.LoadUint64(&engine.sets),
		Gets:     atomic.LoadUint64(&engine.gets),
		Hits:     atomic.LoadUint64(&engine.hits),
		Deletes:  atomic.LoadUint64(&engine.deletes),
	}
}

func (engine *memtxEngine) Close() error {
	if engine == nil {
		return ErrSpaceEngineNil
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.closed {
		return nil
	}
	engine.closed = true
	engine.values = nil
	engine.bytes = 0
	return nil
}

type vinylEngine struct {
	arrangement *hatDataStructure.SpillableArrangement
	closed      atomic.Bool
	sets        uint64
	gets        uint64
	hits        uint64
	deletes     uint64
}

func (engine *vinylEngine) Kind() EngineKind { return EngineVinyl }

func (engine *vinylEngine) Profile() Profile { return profileFor(EngineVinyl) }

func (engine *vinylEngine) Path() string {
	if engine == nil || engine.arrangement == nil {
		return ""
	}
	return engine.arrangement.SpillPath()
}

func (engine *vinylEngine) ensureOpen() error {
	if engine == nil {
		return ErrSpaceEngineNil
	}
	if engine.closed.Load() {
		return ErrSpaceEngineClosed
	}
	return nil
}

func (engine *vinylEngine) Set(key string, value []byte) error {
	if err := engine.ensureOpen(); err != nil {
		return err
	}
	if err := validateKeyValue(key, value, Options{Kind: EngineVinyl}); err != nil {
		return err
	}
	if err := engine.arrangement.Set(key, value); err != nil {
		return err
	}
	atomic.AddUint64(&engine.sets, 1)
	return nil
}

func (engine *vinylEngine) Get(key string) ([]byte, bool, error) {
	if err := engine.ensureOpen(); err != nil {
		return nil, false, err
	}
	atomic.AddUint64(&engine.gets, 1)
	value, found, err := engine.arrangement.Get(key)
	if found {
		atomic.AddUint64(&engine.hits, 1)
	}
	return value, found, err
}

func (engine *vinylEngine) Delete(key string) bool {
	if err := engine.ensureOpen(); err != nil {
		return false
	}
	deleted := engine.arrangement.Delete(key)
	if deleted {
		atomic.AddUint64(&engine.deletes, 1)
	}
	return deleted
}

func (engine *vinylEngine) Snapshot() ([]Entry, error) {
	if err := engine.ensureOpen(); err != nil {
		return nil, err
	}
	rows, err := engine.arrangement.Snapshot()
	if err != nil {
		return nil, err
	}
	entries := make([]Entry, len(rows))
	for index, row := range rows {
		entries[index] = Entry{Key: row.Key, Value: append([]byte(nil), row.Value...)}
	}
	return entries, nil
}

func (engine *vinylEngine) Flush() error {
	if err := engine.ensureOpen(); err != nil {
		return err
	}
	return engine.arrangement.Flush()
}

func (engine *vinylEngine) Compact() error {
	if err := engine.ensureOpen(); err != nil {
		return err
	}
	return engine.arrangement.Compact()
}

func (engine *vinylEngine) Stats() Stats {
	if engine == nil || engine.arrangement == nil {
		return Stats{}
	}
	stats := engine.arrangement.Stats()
	return Stats{
		Kind:         EngineVinyl,
		Entries:      stats.Entries,
		ColdEntries:  stats.ColdEntries,
		HotBytes:     stats.HotBytes,
		DiskBytes:    stats.DiskBytes,
		SpillRecords: stats.SpillRecords,
		Sets:         atomic.LoadUint64(&engine.sets),
		Gets:         atomic.LoadUint64(&engine.gets),
		Hits:         atomic.LoadUint64(&engine.hits),
		Deletes:      atomic.LoadUint64(&engine.deletes),
	}
}

func (engine *vinylEngine) Close() error {
	if err := engine.ensureOpen(); err != nil {
		if errors.Is(err, ErrSpaceEngineClosed) {
			return nil
		}
		return err
	}
	engine.closed.Store(true)
	return engine.arrangement.Close()
}

func validateKeyValue(key string, value []byte, options Options) error {
	if strings.TrimSpace(key) == "" {
		return ErrSpaceEngineKeyRequired
	}
	if options.MaxKeyBytes > 0 && int64(len(key)) > options.MaxKeyBytes {
		return ErrSpaceEngineKeyTooLarge
	}
	if options.MaxValueBytes > 0 && int64(len(value)) > options.MaxValueBytes {
		return ErrSpaceEngineValueTooLarge
	}
	return nil
}
