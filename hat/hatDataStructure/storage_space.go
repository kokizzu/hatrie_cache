package hatDataStructure

import (
	"errors"
	"sort"
	"strings"
	"sync"
)

// StorageSpaceMode selects the physical policy for one independent space.
// Memtx is the zero-value/default and keeps all values in process memory. The
// on-disk policy uses SpillableArrangement with a bounded hot-value cache.
type StorageSpaceMode string

const (
	StorageSpaceMemtx       StorageSpaceMode = "memtx"
	StorageSpaceOnDisk      StorageSpaceMode = "on-disk"
	DefaultStorageSpaceMode                  = StorageSpaceMemtx
)

var (
	ErrStorageSpaceNil             = errors.New("storage space is nil")
	ErrStorageSpaceNameRequired    = errors.New("storage space name is required")
	ErrStorageSpaceModeInvalid     = errors.New("storage space mode is invalid")
	ErrStorageSpacePolicyConflict  = errors.New("storage space policy options conflict")
	ErrStorageSpaceClosed          = errors.New("storage space is closed")
	ErrStorageSpaceKeyRequired     = errors.New("storage space key is required")
	ErrStorageSpaceCapacityInvalid = errors.New("storage space capacity is invalid")
)

// StorageSpaceOptions configures one named space. SpillPath reopens an
// existing on-disk segment; Directory controls where a new segment is placed.
// On-disk writes become durable when Flush is called.
type StorageSpaceOptions struct {
	Name             string           `json:"name"`
	Mode             StorageSpaceMode `json:"mode,omitempty"`
	Capacity         int              `json:"capacity,omitempty"`
	Directory        string           `json:"directory,omitempty"`
	SpillPath        string           `json:"spill_path,omitempty"`
	MemoryLimitBytes int64            `json:"memory_limit_bytes,omitempty"`
	MaxDiskBytes     int64            `json:"max_disk_bytes,omitempty"`
	MaxKeyBytes      int64            `json:"max_key_bytes,omitempty"`
	MaxValueBytes    int64            `json:"max_value_bytes,omitempty"`
}

// StorageSpaceEntry is one independent snapshot row.
type StorageSpaceEntry struct {
	Key   string
	Value []byte
}

// StorageSpaceStats reports policy-specific resident and disk usage.
type StorageSpaceStats struct {
	Name         string           `json:"name"`
	Mode         StorageSpaceMode `json:"mode"`
	Entries      int              `json:"entries"`
	ColdEntries  int              `json:"cold_entries"`
	HotBytes     int64            `json:"hot_bytes"`
	DiskBytes    int64            `json:"disk_bytes"`
	SpillRecords uint64           `json:"spill_records"`
}

// StorageSpace is one independent key/value space with a selectable storage
// policy. It is safe for concurrent access and returns cloned values.
type StorageSpace struct {
	mu sync.RWMutex

	name   string
	mode   StorageSpaceMode
	closed bool

	memtx      map[string][]byte
	memtxBytes int64
	disk       *SpillableArrangement
}

// NewStorageSpace creates a new space or reopens an existing SpillPath.
func NewStorageSpace(options StorageSpaceOptions) (*StorageSpace, error) {
	name := strings.TrimSpace(options.Name)
	if name == "" {
		return nil, ErrStorageSpaceNameRequired
	}
	if options.Capacity < 0 {
		return nil, ErrStorageSpaceCapacityInvalid
	}
	mode := options.Mode
	if mode == "" {
		mode = DefaultStorageSpaceMode
	}
	if mode != StorageSpaceMemtx && mode != StorageSpaceOnDisk {
		return nil, ErrStorageSpaceModeInvalid
	}
	if mode == StorageSpaceMemtx {
		if options.SpillPath != "" {
			return nil, ErrStorageSpacePolicyConflict
		}
		return &StorageSpace{
			name:  name,
			mode:  mode,
			memtx: make(map[string][]byte, options.Capacity),
		}, nil
	}
	spillOptions := SpillableArrangementOptions{
		Directory:        options.Directory,
		MemoryLimitBytes: options.MemoryLimitBytes,
		MaxDiskBytes:     options.MaxDiskBytes,
		MaxKeyBytes:      options.MaxKeyBytes,
		MaxValueBytes:    options.MaxValueBytes,
	}
	var (
		disk *SpillableArrangement
		err  error
	)
	if options.SpillPath != "" {
		disk, err = OpenSpillableArrangement(options.SpillPath, spillOptions)
	} else {
		disk, err = NewSpillableArrangement(spillOptions)
	}
	if err != nil {
		return nil, err
	}
	return &StorageSpace{name: name, mode: mode, disk: disk}, nil
}

// Name returns the configured logical space name.
func (space *StorageSpace) Name() string {
	if space == nil {
		return ""
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	return space.name
}

// Mode returns the selected physical policy.
func (space *StorageSpace) Mode() StorageSpaceMode {
	if space == nil {
		return ""
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	return space.mode
}

// Set inserts or replaces a value. The input bytes are cloned before return.
func (space *StorageSpace) Set(key string, value []byte) error {
	if space == nil {
		return ErrStorageSpaceNil
	}
	if key == "" {
		return ErrStorageSpaceKeyRequired
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.closed {
		return ErrStorageSpaceClosed
	}
	if space.mode == StorageSpaceOnDisk {
		return space.disk.Set(key, value)
	}
	previous, exists := space.memtx[key]
	if exists {
		space.memtxBytes -= int64(len(previous))
	}
	cloned := append([]byte(nil), value...)
	space.memtx[key] = cloned
	space.memtxBytes += int64(len(cloned))
	return nil
}

// Get returns an independent copy of key's value.
func (space *StorageSpace) Get(key string) ([]byte, bool, error) {
	if space == nil {
		return nil, false, ErrStorageSpaceNil
	}
	space.mu.RLock()
	if space.closed {
		space.mu.RUnlock()
		return nil, false, ErrStorageSpaceClosed
	}
	if space.mode == StorageSpaceOnDisk {
		value, found, err := space.disk.Get(key)
		space.mu.RUnlock()
		return value, found, err
	}
	value, found := space.memtx[key]
	if !found {
		space.mu.RUnlock()
		return nil, false, nil
	}
	cloned := append([]byte(nil), value...)
	space.mu.RUnlock()
	return cloned, true, nil
}

// Delete removes key and reports whether it was present.
func (space *StorageSpace) Delete(key string) (bool, error) {
	if space == nil {
		return false, ErrStorageSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.closed {
		return false, ErrStorageSpaceClosed
	}
	if space.mode == StorageSpaceOnDisk {
		return space.disk.Delete(key), nil
	}
	value, found := space.memtx[key]
	if !found {
		return false, nil
	}
	delete(space.memtx, key)
	space.memtxBytes -= int64(len(value))
	return true, nil
}

// Snapshot returns all current entries ordered by key.
func (space *StorageSpace) Snapshot() ([]StorageSpaceEntry, error) {
	if space == nil {
		return nil, ErrStorageSpaceNil
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	if space.closed {
		return nil, ErrStorageSpaceClosed
	}
	if space.mode == StorageSpaceOnDisk {
		entries, err := space.disk.Snapshot()
		if err != nil {
			return nil, err
		}
		rows := make([]StorageSpaceEntry, len(entries))
		for index := range entries {
			rows[index] = StorageSpaceEntry{Key: entries[index].Key, Value: append([]byte(nil), entries[index].Value...)}
		}
		return rows, nil
	}
	rows := make([]StorageSpaceEntry, 0, len(space.memtx))
	for key, value := range space.memtx {
		rows = append(rows, StorageSpaceEntry{Key: key, Value: append([]byte(nil), value...)})
	}
	sort.Slice(rows, func(left, right int) bool { return rows[left].Key < rows[right].Key })
	return rows, nil
}

// Flush spills all hot on-disk values and syncs the segment. It is a no-op for
// memtx spaces.
func (space *StorageSpace) Flush() error {
	if space == nil {
		return ErrStorageSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.closed {
		return ErrStorageSpaceClosed
	}
	if space.mode == StorageSpaceOnDisk {
		return space.disk.Flush()
	}
	return nil
}

// Sync syncs already-written on-disk records. Call Flush first when all hot
// values must be included. It is a no-op for memtx spaces.
func (space *StorageSpace) Sync() error {
	if space == nil {
		return ErrStorageSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.closed {
		return ErrStorageSpaceClosed
	}
	if space.mode == StorageSpaceOnDisk {
		return space.disk.Sync()
	}
	return nil
}

// Compact reclaims obsolete on-disk records. It is a no-op for memtx spaces.
func (space *StorageSpace) Compact() error {
	if space == nil {
		return ErrStorageSpaceNil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.closed {
		return ErrStorageSpaceClosed
	}
	if space.mode == StorageSpaceOnDisk {
		return space.disk.Compact()
	}
	return nil
}

// Path returns the on-disk segment path, or an empty string for memtx.
func (space *StorageSpace) Path() string {
	if space == nil {
		return ""
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	if space.mode != StorageSpaceOnDisk || space.disk == nil {
		return ""
	}
	return space.disk.SpillPath()
}

// Stats reports current policy-specific usage.
func (space *StorageSpace) Stats() StorageSpaceStats {
	if space == nil {
		return StorageSpaceStats{}
	}
	space.mu.RLock()
	defer space.mu.RUnlock()
	stats := StorageSpaceStats{Name: space.name, Mode: space.mode}
	if space.mode == StorageSpaceOnDisk {
		spillStats := space.disk.Stats()
		stats.Entries = spillStats.Entries
		stats.ColdEntries = spillStats.ColdEntries
		stats.HotBytes = spillStats.HotBytes
		stats.DiskBytes = spillStats.DiskBytes
		stats.SpillRecords = spillStats.SpillRecords
		return stats
	}
	stats.Entries = len(space.memtx)
	stats.HotBytes = space.memtxBytes
	return stats
}

// Close releases the on-disk segment. Close is idempotent.
func (space *StorageSpace) Close() error {
	if space == nil {
		return nil
	}
	space.mu.Lock()
	defer space.mu.Unlock()
	if space.closed {
		return nil
	}
	space.closed = true
	if space.mode == StorageSpaceOnDisk {
		return space.disk.Close()
	}
	return nil
}
