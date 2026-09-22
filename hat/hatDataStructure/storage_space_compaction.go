package hatDataStructure

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
)

var (
	ErrStorageSpaceCompactionSchedulerNil   = errors.New("storage space compaction scheduler is nil")
	ErrStorageSpaceCompactionOptionsInvalid = errors.New("storage space compaction options are invalid")
	ErrStorageSpaceCompactionSpaceInvalid   = errors.New("storage space compaction space is invalid")
	ErrStorageSpaceCompactionSpaceDuplicate = errors.New("storage space compaction space already registered")
	ErrStorageSpaceCompactionContextInvalid = errors.New("storage space compaction context is nil")
)

const (
	DefaultStorageSpaceCompactionMinStaleBytes   int64 = 1 << 20
	DefaultStorageSpaceCompactionMaxSpacesPerRun       = 1
)

// StorageSpaceCompactionSchedulerOptions controls explicit maintenance runs.
// A zero MinStaleRatio disables the ratio trigger; a zero MinStaleBytes or
// MaxSpacesPerRun selects a conservative default.
type StorageSpaceCompactionSchedulerOptions struct {
	MinStaleBytes   int64
	MinStaleRatio   float64
	MaxSpacesPerRun int
}

// StorageSpaceCompactionRun reports one deterministic maintenance pass.
type StorageSpaceCompactionRun struct {
	Considered     int
	Scheduled      int
	Completed      int
	Skipped        int
	Failed         int
	ReclaimedBytes int64
}

type storageSpaceCompactionSchedulerSpace struct {
	name  string
	space *StorageSpace
}

// StorageSpaceCompactionScheduler selects on-disk spaces whose stale segment
// debt exceeds policy and compacts them synchronously. It has no background
// goroutine; callers decide when maintenance is safe to run.
type StorageSpaceCompactionScheduler struct {
	mu      sync.RWMutex
	options StorageSpaceCompactionSchedulerOptions
	spaces  map[string]*StorageSpace
}

// NewStorageSpaceCompactionScheduler creates an explicit compaction policy.
func NewStorageSpaceCompactionScheduler(options StorageSpaceCompactionSchedulerOptions) (*StorageSpaceCompactionScheduler, error) {
	if options.MinStaleBytes < 0 || options.MinStaleRatio < 0 || options.MinStaleRatio > 1 || options.MaxSpacesPerRun < 0 {
		return nil, ErrStorageSpaceCompactionOptionsInvalid
	}
	if options.MinStaleBytes == 0 {
		options.MinStaleBytes = DefaultStorageSpaceCompactionMinStaleBytes
	}
	if options.MaxSpacesPerRun == 0 {
		options.MaxSpacesPerRun = DefaultStorageSpaceCompactionMaxSpacesPerRun
	}
	return &StorageSpaceCompactionScheduler{
		options: options,
		spaces:  make(map[string]*StorageSpace),
	}, nil
}

// Register adds one space by its logical name. Registration is idempotent only
// for the same pointer; a duplicate name with another space is rejected.
func (scheduler *StorageSpaceCompactionScheduler) Register(space *StorageSpace) error {
	if scheduler == nil {
		return ErrStorageSpaceCompactionSchedulerNil
	}
	if space == nil || strings.TrimSpace(space.Name()) == "" {
		return ErrStorageSpaceCompactionSpaceInvalid
	}
	name := space.Name()
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if existing, found := scheduler.spaces[name]; found {
		if existing == space {
			return nil
		}
		return ErrStorageSpaceCompactionSpaceDuplicate
	}
	scheduler.spaces[name] = space
	return nil
}

// Unregister removes a space by name and reports whether it was registered.
func (scheduler *StorageSpaceCompactionScheduler) Unregister(name string) bool {
	if scheduler == nil {
		return false
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return false
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if _, found := scheduler.spaces[name]; !found {
		return false
	}
	delete(scheduler.spaces, name)
	return true
}

// Run compacts up to MaxSpacesPerRun eligible spaces in name order.
func (scheduler *StorageSpaceCompactionScheduler) Run(ctx context.Context) (StorageSpaceCompactionRun, error) {
	if scheduler == nil {
		return StorageSpaceCompactionRun{}, ErrStorageSpaceCompactionSchedulerNil
	}
	if ctx == nil {
		return StorageSpaceCompactionRun{}, ErrStorageSpaceCompactionContextInvalid
	}
	if err := ctx.Err(); err != nil {
		return StorageSpaceCompactionRun{}, err
	}
	scheduler.mu.RLock()
	spaces := make([]storageSpaceCompactionSchedulerSpace, 0, len(scheduler.spaces))
	for name, space := range scheduler.spaces {
		spaces = append(spaces, storageSpaceCompactionSchedulerSpace{name: name, space: space})
	}
	options := scheduler.options
	scheduler.mu.RUnlock()
	sort.Slice(spaces, func(left, right int) bool { return spaces[left].name < spaces[right].name })
	run := StorageSpaceCompactionRun{}
	for index, candidate := range spaces {
		if index >= options.MaxSpacesPerRun {
			break
		}
		if err := ctx.Err(); err != nil {
			return run, err
		}
		run.Considered++
		stats := candidate.space.Stats()
		if !storageSpaceCompactionEligible(stats, options) {
			run.Skipped++
			continue
		}
		run.Scheduled++
		before := stats.DiskBytes
		if err := candidate.space.Compact(); err != nil {
			run.Failed++
			return run, err
		}
		after := candidate.space.Stats().DiskBytes
		if before > after {
			run.ReclaimedBytes += before - after
		}
		run.Completed++
	}
	return run, nil
}

func storageSpaceCompactionEligible(stats StorageSpaceStats, options StorageSpaceCompactionSchedulerOptions) bool {
	if stats.Mode != StorageSpaceOnDisk || stats.StaleDiskBytes <= 0 {
		return false
	}
	if stats.StaleDiskBytes >= options.MinStaleBytes {
		return true
	}
	if options.MinStaleRatio <= 0 || stats.DiskBytes <= 0 {
		return false
	}
	return float64(stats.StaleDiskBytes)/float64(stats.DiskBytes) >= options.MinStaleRatio
}
