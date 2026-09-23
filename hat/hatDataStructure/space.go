package hatDataStructure

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sort"
	"sync"
)

const (
	// DefaultSpaceMemtxMaxRecords bounds the default in-memory space.
	DefaultSpaceMemtxMaxRecords = 1 << 20
	// DefaultSpaceMemtxMaxValueBytes bounds one memtx value by default.
	DefaultSpaceMemtxMaxValueBytes = 64 << 20
	// DefaultSpaceMaxSnapshotBytes bounds one engine-neutral space snapshot.
	DefaultSpaceMaxSnapshotBytes = 512 << 20
	spaceSnapshotHeaderSize      = 32
	spaceSnapshotVersion         = 1
)

var spaceSnapshotMagic = [4]byte{'H', 'S', 'P', '1'}

var (
	ErrSpaceNil               = errors.New("hatDataStructure: space is nil")
	ErrSpaceEngineInvalid     = errors.New("hatDataStructure: space engine is invalid")
	ErrSpaceOptionsInvalid    = errors.New("hatDataStructure: space options are invalid")
	ErrSpaceKeyRequired       = errors.New("hatDataStructure: space key is required")
	ErrSpaceValueTooLarge     = errors.New("hatDataStructure: space value is too large")
	ErrSpaceFull              = errors.New("hatDataStructure: space is full")
	ErrSpaceSnapshotCorrupt   = errors.New("hatDataStructure: space snapshot is corrupt")
	ErrSpaceSnapshotWireLimit = errors.New("hatDataStructure: space snapshot wire limit exceeded")
	ErrSpaceEngineMismatch    = errors.New("hatDataStructure: space snapshot engine mismatch")
)

// SpaceEngine selects the storage engine for one space. The engine is fixed
// at construction time, matching Tarantool's per-space engine policy.
type SpaceEngine string

const (
	// SpaceEngineMemtx stores values in a bounded resident map and is the zero
	// value default for compatibility and lowest point-read latency.
	SpaceEngineMemtx SpaceEngine = "memtx"
	// SpaceEngineVinyl stores values in the existing bounded LSM engine. The
	// caller owns persistence and backup scheduling through snapshots.
	SpaceEngineVinyl SpaceEngine = "vinyl"
)

// MemtxSpaceOptions configures the resident map used by a memtx space.
type MemtxSpaceOptions struct {
	MaxRecords    int
	MaxValueBytes int
}

// SpaceOptions configures one immutable-at-creation storage policy.
// Engine == "" selects SpaceEngineMemtx.
type SpaceOptions struct {
	Engine SpaceEngine
	Memtx  MemtxSpaceOptions
	Vinyl  LSMTableOptions
}

type normalizedSpaceOptions struct {
	engine SpaceEngine
	memtx  MemtxSpaceOptions
	vinyl  LSMTableOptions
}

// SpaceStats reports engine-specific structural counters without materializing
// the complete key set.
type SpaceStats struct {
	Engine        SpaceEngine
	MemtxRecords  int
	MemtxCapacity int
	Vinyl         LSMTableStats
}

// Space is a common key/value surface over a selected memtx or Vinyl-style
// engine. The selected engine does not change at runtime; create another
// space and migrate through snapshots when an engine change is required.
type Space struct {
	mu           sync.RWMutex
	engine       SpaceEngine
	memtxOptions MemtxSpaceOptions
	memtx        map[string][]byte
	vinyl        *LSMTable
}

// NewSpace creates one independently configured storage space.
func NewSpace(options SpaceOptions) (*Space, error) {
	normalized, err := normalizeSpaceOptions(options)
	if err != nil {
		return nil, err
	}
	space := &Space{
		engine:       normalized.engine,
		memtxOptions: normalized.memtx,
	}
	if normalized.engine == SpaceEngineMemtx {
		space.memtx = make(map[string][]byte)
		return space, nil
	}
	space.vinyl, err = NewLSMTable(normalized.vinyl)
	if err != nil {
		return nil, err
	}
	return space, nil
}

// Engine reports the immutable engine selected for the space.
func (space *Space) Engine() SpaceEngine {
	if space == nil {
		return ""
	}
	return space.engine
}

// Put inserts or replaces a copied value.
func (space *Space) Put(key string, value []byte) error {
	if space == nil {
		return ErrSpaceNil
	}
	if key == "" {
		return ErrSpaceKeyRequired
	}
	if space.engine == SpaceEngineVinyl {
		return space.vinyl.Put(key, value)
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if len(value) > space.memtxOptions.MaxValueBytes {
		return ErrSpaceValueTooLarge
	}
	if _, exists := space.memtx[key]; !exists && len(space.memtx) >= space.memtxOptions.MaxRecords {
		return ErrSpaceFull
	}
	space.memtx[key] = append([]byte(nil), value...)
	return nil
}

// Get returns an independent copy of the current value.
func (space *Space) Get(key string) ([]byte, bool) {
	if space == nil || key == "" {
		return nil, false
	}
	if space.engine == SpaceEngineVinyl {
		return space.vinyl.Get(key)
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	value, ok := space.memtx[key]
	if !ok {
		return nil, false
	}
	return append([]byte(nil), value...), true
}

// Delete removes key. Deleting a missing key is a no-op for both engines.
func (space *Space) Delete(key string) error {
	if space == nil {
		return ErrSpaceNil
	}
	if key == "" {
		return ErrSpaceKeyRequired
	}
	if space.engine == SpaceEngineVinyl {
		if _, exists := space.vinyl.Get(key); !exists {
			return nil
		}
		return space.vinyl.Delete(key)
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	delete(space.memtx, key)
	return nil
}

// Flush seals pending Vinyl writes. It is a no-op for memtx spaces.
func (space *Space) Flush() error {
	if space == nil {
		return ErrSpaceNil
	}
	if space.engine == SpaceEngineVinyl {
		return space.vinyl.Flush()
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	return nil
}

// Compact folds Vinyl runs. It is a no-op for memtx spaces.
func (space *Space) Compact() error {
	if space == nil {
		return ErrSpaceNil
	}
	if space.engine == SpaceEngineVinyl {
		return space.vinyl.Compact()
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	return nil
}

// Stats reports bounded structural state for the selected engine.
func (space *Space) Stats() SpaceStats {
	if space == nil {
		return SpaceStats{}
	}
	if space.engine == SpaceEngineVinyl {
		return SpaceStats{Engine: space.engine, Vinyl: space.vinyl.Stats()}
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	stats := SpaceStats{Engine: space.engine}
	stats.MemtxRecords = len(space.memtx)
	stats.MemtxCapacity = space.memtxOptions.MaxRecords
	return stats
}

// MarshalBinary returns a CRC-protected engine-neutral snapshot. The payload
// for Vinyl is the existing LSM snapshot; memtx uses deterministic sorted
// key/value records so backups do not depend on map iteration order.
func (space *Space) MarshalBinary() ([]byte, error) {
	if space == nil {
		return nil, ErrSpaceNil
	}
	var body []byte
	var count uint32
	if space.engine == SpaceEngineVinyl {
		var err error
		body, err = space.vinyl.MarshalBinary()
		if err != nil {
			return nil, err
		}
	} else {
		space.mu.Lock()
		defer space.mu.Unlock()
		if uint64(len(space.memtx)) > uint64(^uint32(0)) {
			return nil, ErrSpaceSnapshotWireLimit
		}
		count = uint32(len(space.memtx))
		keys := make([]string, 0, len(space.memtx))
		for key := range space.memtx {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value := space.memtx[key]
			if uint64(len(key)) > uint64(^uint32(0)) || uint64(len(value)) > uint64(^uint32(0)) || len(body) > DefaultSpaceMaxSnapshotBytes-spaceSnapshotHeaderSize-8-len(key)-len(value) {
				return nil, ErrSpaceSnapshotWireLimit
			}
			record := make([]byte, 8+len(key)+len(value))
			binary.LittleEndian.PutUint32(record[0:4], uint32(len(key)))
			binary.LittleEndian.PutUint32(record[4:8], uint32(len(value)))
			copy(record[8:], key)
			copy(record[8+len(key):], value)
			body = append(body, record...)
		}
	}
	if len(body) > DefaultSpaceMaxSnapshotBytes-spaceSnapshotHeaderSize {
		return nil, ErrSpaceSnapshotWireLimit
	}
	data := make([]byte, spaceSnapshotHeaderSize+len(body))
	copy(data[:4], spaceSnapshotMagic[:])
	binary.LittleEndian.PutUint16(data[4:6], spaceSnapshotVersion)
	data[6] = spaceEngineCode(space.engine)
	binary.LittleEndian.PutUint32(data[8:12], count)
	binary.LittleEndian.PutUint64(data[12:20], uint64(len(body)))
	binary.LittleEndian.PutUint32(data[20:24], crc32.ChecksumIEEE(body))
	copy(data[spaceSnapshotHeaderSize:], body)
	return data, nil
}

// UnmarshalSpace restores a space snapshot and validates its selected engine.
// A non-empty options.Engine must match the snapshot engine.
func UnmarshalSpace(data []byte, options SpaceOptions) (*Space, error) {
	if len(data) < spaceSnapshotHeaderSize || !equalSpaceSnapshotMagic(data[:4]) {
		return nil, ErrSpaceSnapshotCorrupt
	}
	if binary.LittleEndian.Uint16(data[4:6]) != spaceSnapshotVersion || data[6] == 0 || data[7] != 0 || binary.LittleEndian.Uint32(data[24:28]) != 0 || binary.LittleEndian.Uint32(data[28:32]) != 0 {
		return nil, ErrSpaceSnapshotCorrupt
	}
	engine, ok := spaceEngineFromCode(data[6])
	if !ok {
		return nil, ErrSpaceSnapshotCorrupt
	}
	if options.Engine != "" && options.Engine != engine {
		return nil, ErrSpaceEngineMismatch
	}
	bodyBytes := binary.LittleEndian.Uint64(data[12:20])
	if bodyBytes > uint64(DefaultSpaceMaxSnapshotBytes-spaceSnapshotHeaderSize) || bodyBytes != uint64(len(data)-spaceSnapshotHeaderSize) || binary.LittleEndian.Uint32(data[20:24]) != crc32.ChecksumIEEE(data[spaceSnapshotHeaderSize:]) {
		return nil, ErrSpaceSnapshotCorrupt
	}
	options.Engine = engine
	space, err := NewSpace(options)
	if err != nil {
		return nil, err
	}
	body := data[spaceSnapshotHeaderSize:]
	if engine == SpaceEngineVinyl {
		table, err := UnmarshalLSMTable(body, options.Vinyl)
		if err != nil {
			return nil, err
		}
		space.vinyl = table
		return space, nil
	}
	count := binary.LittleEndian.Uint32(data[8:12])
	if uint64(count) > uint64(space.memtxOptions.MaxRecords) {
		return nil, ErrSpaceFull
	}
	offset := 0
	for index := uint32(0); index < count; index++ {
		if len(body)-offset < 8 {
			return nil, ErrSpaceSnapshotCorrupt
		}
		keyBytes := uint64(binary.LittleEndian.Uint32(body[offset : offset+4]))
		valueBytes := uint64(binary.LittleEndian.Uint32(body[offset+4 : offset+8]))
		offset += 8
		remaining := uint64(len(body) - offset)
		if keyBytes < 1 || keyBytes > remaining {
			return nil, ErrSpaceSnapshotCorrupt
		}
		remaining -= keyBytes
		if valueBytes > uint64(space.memtxOptions.MaxValueBytes) || valueBytes > remaining {
			return nil, ErrSpaceSnapshotCorrupt
		}
		keyLength := int(keyBytes)
		valueLength := int(valueBytes)
		key := string(body[offset : offset+keyLength])
		offset += keyLength
		if _, exists := space.memtx[key]; exists {
			return nil, ErrSpaceSnapshotCorrupt
		}
		space.memtx[key] = append([]byte(nil), body[offset:offset+valueLength]...)
		offset += valueLength
	}
	if offset != len(body) {
		return nil, ErrSpaceSnapshotCorrupt
	}
	return space, nil
}

func normalizeSpaceOptions(options SpaceOptions) (normalizedSpaceOptions, error) {
	engine := options.Engine
	if engine == "" {
		engine = SpaceEngineMemtx
	}
	if engine != SpaceEngineMemtx && engine != SpaceEngineVinyl {
		return normalizedSpaceOptions{}, ErrSpaceEngineInvalid
	}
	memtx := options.Memtx
	if memtx.MaxRecords == 0 {
		memtx.MaxRecords = DefaultSpaceMemtxMaxRecords
	}
	if memtx.MaxValueBytes == 0 {
		memtx.MaxValueBytes = DefaultSpaceMemtxMaxValueBytes
	}
	if memtx.MaxRecords < 1 || memtx.MaxValueBytes < 1 {
		return normalizedSpaceOptions{}, ErrSpaceOptionsInvalid
	}
	return normalizedSpaceOptions{engine: engine, memtx: memtx, vinyl: options.Vinyl}, nil
}

func spaceEngineCode(engine SpaceEngine) byte {
	if engine == SpaceEngineVinyl {
		return 2
	}
	return 1
}

func spaceEngineFromCode(code byte) (SpaceEngine, bool) {
	switch code {
	case 1:
		return SpaceEngineMemtx, true
	case 2:
		return SpaceEngineVinyl, true
	default:
		return "", false
	}
}

func equalSpaceSnapshotMagic(value []byte) bool {
	return len(value) == len(spaceSnapshotMagic) && value[0] == spaceSnapshotMagic[0] && value[1] == spaceSnapshotMagic[1] && value[2] == spaceSnapshotMagic[2] && value[3] == spaceSnapshotMagic[3]
}
