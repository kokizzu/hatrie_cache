package hatDataStructure

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

var (
	ErrLSMCompactionSchedulerNil           = errors.New("hatDataStructure: lsm compaction scheduler is nil")
	ErrLSMCompactionSchedulerNameRequired  = errors.New("hatDataStructure: lsm compaction scheduler name is required")
	ErrLSMCompactionSchedulerTableRequired = errors.New("hatDataStructure: lsm compaction scheduler table is required")
	ErrLSMCompactionSchedulerDuplicate     = errors.New("hatDataStructure: lsm compaction scheduler name already registered")
	ErrLSMCompactionSchedulerLimit         = errors.New("hatDataStructure: lsm compaction scheduler limit is invalid")
	ErrLSMCompactionSchedulerCompaction    = errors.New("hatDataStructure: scheduled lsm compaction failed")
)

// LSMCompactionSchedulerOptions configures one caller-owned scheduler pass.
// The scheduler has no background goroutine; callers decide when to invoke
// RunOnce and can use their own timer, worker, or shutdown lifecycle.
type LSMCompactionSchedulerOptions struct {
	MaxCompactionsPerRun int
}

// LSMCompactionScheduleResult reports one RunOnce pass.
type LSMCompactionScheduleResult struct {
	Considered      int
	Compacted       int
	CompactedSpaces []string
}

type lsmCompactionScheduleEntry struct {
	name  string
	table *LSMTable
	debt  int
}

// LSMCompactionScheduler selects deferred tables with the largest older-run
// wire-byte debt first. Registration and scheduling are explicit and local to
// the caller, so merely creating a scheduler cannot start background work.
type LSMCompactionScheduler struct {
	mu                   sync.RWMutex
	maxCompactionsPerRun int
	tables               map[string]*LSMTable
}

// NewLSMCompactionScheduler validates options and creates an idle scheduler.
func NewLSMCompactionScheduler(options LSMCompactionSchedulerOptions) (*LSMCompactionScheduler, error) {
	if options.MaxCompactionsPerRun < 0 {
		return nil, ErrLSMCompactionSchedulerLimit
	}
	if options.MaxCompactionsPerRun == 0 {
		options.MaxCompactionsPerRun = 1
	}
	return &LSMCompactionScheduler{
		maxCompactionsPerRun: options.MaxCompactionsPerRun,
		tables:               make(map[string]*LSMTable),
	}, nil
}

// Register adds or replaces no existing table under name. Duplicate names
// are rejected so an accidental double registration cannot change scheduling
// order silently.
func (scheduler *LSMCompactionScheduler) Register(name string, table *LSMTable) error {
	if scheduler == nil {
		return ErrLSMCompactionSchedulerNil
	}
	if name == "" {
		return ErrLSMCompactionSchedulerNameRequired
	}
	if table == nil {
		return ErrLSMCompactionSchedulerTableRequired
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if _, exists := scheduler.tables[name]; exists {
		return ErrLSMCompactionSchedulerDuplicate
	}
	scheduler.tables[name] = table
	return nil
}

// Unregister removes a table. It returns whether name was registered.
func (scheduler *LSMCompactionScheduler) Unregister(name string) bool {
	if scheduler == nil {
		return false
	}
	scheduler.mu.Lock()
	defer scheduler.mu.Unlock()
	if _, exists := scheduler.tables[name]; !exists {
		return false
	}
	delete(scheduler.tables, name)
	return true
}

// RunOnce performs at most MaxCompactionsPerRun due compactions, selecting
// the largest debt first and using name as a deterministic tie-breaker.
func (scheduler *LSMCompactionScheduler) RunOnce() (LSMCompactionScheduleResult, error) {
	if scheduler == nil {
		return LSMCompactionScheduleResult{}, ErrLSMCompactionSchedulerNil
	}
	scheduler.mu.RLock()
	entries := make([]lsmCompactionScheduleEntry, 0, len(scheduler.tables))
	for name, table := range scheduler.tables {
		entries = append(entries, lsmCompactionScheduleEntry{name: name, table: table, debt: table.Stats().CompactionDebtBytes})
	}
	maxCompactions := scheduler.maxCompactionsPerRun
	scheduler.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].debt != entries[j].debt {
			return entries[i].debt > entries[j].debt
		}
		return entries[i].name < entries[j].name
	})
	result := LSMCompactionScheduleResult{Considered: len(entries)}
	for _, entry := range entries {
		if result.Compacted >= maxCompactions {
			break
		}
		compacted, err := entry.table.CompactIfNeeded()
		if err != nil {
			return result, fmt.Errorf("%w: %s: %v", ErrLSMCompactionSchedulerCompaction, entry.name, err)
		}
		if compacted {
			result.Compacted++
			result.CompactedSpaces = append(result.CompactedSpaces, entry.name)
		}
	}
	return result, nil
}
