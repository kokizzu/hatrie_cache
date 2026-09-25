package hatStorage

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrCompactionControllerNil reports a method call on a nil controller.
	ErrCompactionControllerNil = errors.New("hatriecache: compaction controller is nil")
	// ErrCompactionControllerOptionsInvalid reports an invalid controller bound.
	ErrCompactionControllerOptionsInvalid = errors.New("hatriecache: compaction controller options are invalid")
	// ErrCompactionControllerQueueFull reports that the active-job bound is full.
	ErrCompactionControllerQueueFull = errors.New("hatriecache: compaction controller queue is full")
	// ErrCompactionControllerBackpressure reports that an estimated job would
	// exceed the configured pending-byte budget.
	ErrCompactionControllerBackpressure = errors.New("hatriecache: compaction controller backpressure")
	// ErrCompactionRequestInvalid reports an empty target or missing callback.
	ErrCompactionRequestInvalid = errors.New("hatriecache: compaction request is invalid")
	// ErrCompactionControllerScheduleRejected reports an internal scheduler mismatch.
	ErrCompactionControllerScheduleRejected = errors.New("hatriecache: compaction controller schedule was rejected")
)

const (
	// DefaultCompactionControllerMaxPending bounds active optimize requests.
	DefaultCompactionControllerMaxPending = 64
	// DefaultCompactionControllerHistoryCapacity bounds retained successful jobs.
	DefaultCompactionControllerHistoryCapacity = 64
	maxCompactionControllerErrorBytes          = 512
)

// CompactionControllerOptions bounds the importable optimize-control layer.
// The controller is inactive until a caller creates one and explicitly runs it.
type CompactionControllerOptions struct {
	SchedulerOptions CompactionSchedulerOptions
	MaxPending       int
	MaxPendingBytes  uint64
	HistoryCapacity  int
}

// CompactionRequest describes one caller-owned targeted merge or compaction.
// Target is the stable coalescing key; Run owns the storage-engine operation.
type CompactionRequest struct {
	Target         string
	Priority       int
	EstimatedBytes uint64
	Run            func(context.Context) error
}

// CompactionJobState is the bounded lifecycle state of one optimize request.
type CompactionJobState string

const (
	CompactionJobPending      CompactionJobState = "pending"
	CompactionJobRunning      CompactionJobState = "running"
	CompactionJobRetryPending CompactionJobState = "retry_pending"
	CompactionJobSucceeded    CompactionJobState = "succeeded"
)

// CompactionJob is a copy-safe status snapshot. A retry_pending job remains
// admitted and will be retried by the next Run call after a failed callback.
type CompactionJob struct {
	ID             uint64             `json:"id"`
	Target         string             `json:"target"`
	Priority       int                `json:"priority"`
	EstimatedBytes uint64             `json:"estimated_bytes,omitempty"`
	State          CompactionJobState `json:"state"`
	Attempts       uint32             `json:"attempts"`
	LastError      string             `json:"last_error,omitempty"`
}

type compactionControllerJob struct {
	CompactionJob
}

// CompactionController adds bounded job identity and status history to the
// existing priority/I/O-aware CompactionScheduler. It deliberately has no
// background goroutine; the caller decides when maintenance runs.
type CompactionController struct {
	scheduler       *CompactionScheduler
	maxPending      int
	maxPendingBytes uint64
	historyCapacity int
	mu              sync.Mutex
	nextID          uint64
	active          int
	activeBytes     uint64
	jobs            map[uint64]*compactionControllerJob
	targetIDs       map[string]uint64
	order           []uint64
}

// NewCompactionController validates bounds and creates an opt-in controller.
// Zero bounds select conservative finite defaults.
func NewCompactionController(options CompactionControllerOptions) (*CompactionController, error) {
	if options.MaxPending < 0 || options.HistoryCapacity < 0 {
		return nil, ErrCompactionControllerOptionsInvalid
	}
	if options.MaxPending == 0 {
		options.MaxPending = DefaultCompactionControllerMaxPending
	}
	if options.HistoryCapacity == 0 {
		options.HistoryCapacity = DefaultCompactionControllerHistoryCapacity
	}
	scheduler, err := NewCompactionScheduler(options.SchedulerOptions)
	if err != nil {
		return nil, err
	}
	return &CompactionController{
		scheduler:       scheduler,
		maxPending:      options.MaxPending,
		maxPendingBytes: options.MaxPendingBytes,
		historyCapacity: options.HistoryCapacity,
		jobs:            make(map[uint64]*compactionControllerJob),
		targetIDs:       make(map[string]uint64),
	}, nil
}

// Submit admits one targeted operation. The bool is false when an active job
// with the same target already exists; the returned snapshot is that job.
func (controller *CompactionController) Submit(request CompactionRequest) (CompactionJob, bool, error) {
	if controller == nil {
		return CompactionJob{}, false, ErrCompactionControllerNil
	}
	target := strings.TrimSpace(request.Target)
	if target == "" || request.Run == nil {
		return CompactionJob{}, false, ErrCompactionRequestInvalid
	}

	controller.mu.Lock()
	defer controller.mu.Unlock()
	if id, exists := controller.targetIDs[target]; exists {
		if job := controller.jobs[id]; job != nil {
			return job.CompactionJob, false, nil
		}
		delete(controller.targetIDs, target)
	}
	if controller.active >= controller.maxPending {
		return CompactionJob{}, false, ErrCompactionControllerQueueFull
	}
	if !controller.admitBytesLocked(request.EstimatedBytes) {
		return CompactionJob{}, false, ErrCompactionControllerBackpressure
	}
	controller.nextID++
	job := &compactionControllerJob{
		CompactionJob: CompactionJob{
			ID:             controller.nextID,
			Target:         target,
			Priority:       request.Priority,
			EstimatedBytes: request.EstimatedBytes,
			State:          CompactionJobPending,
		},
	}
	controller.jobs[job.ID] = job
	controller.targetIDs[target] = job.ID
	controller.order = append(controller.order, job.ID)
	controller.active++
	controller.activeBytes = saturatingCompactionBytes(controller.activeBytes, request.EstimatedBytes)
	queued, err := controller.scheduler.ScheduleWithPriorityAndIO(target, request.Priority, request.EstimatedBytes, func(ctx context.Context) error {
		controller.start(job.ID)
		err := request.Run(ctx)
		controller.finish(job.ID, err)
		return err
	})
	if err != nil {
		controller.removeActiveLocked(job.ID)
		return CompactionJob{}, false, err
	}
	if !queued {
		controller.removeActiveLocked(job.ID)
		return CompactionJob{}, false, ErrCompactionControllerScheduleRejected
	}
	return job.CompactionJob, true, nil
}

// Run drains the controller's queued operations using the existing scheduler.
// A canceled context leaves failed operations in retry_pending state so a later
// Run can retry them; no controller-owned worker is left behind.
func (controller *CompactionController) Run(ctx context.Context) (CompactionRun, error) {
	if controller == nil {
		return CompactionRun{}, ErrCompactionControllerNil
	}
	return controller.scheduler.Run(ctx)
}

// Status returns a copy of one job status. Successful jobs remain queryable
// until the bounded history capacity evicts them.
func (controller *CompactionController) Status(id uint64) (CompactionJob, bool) {
	if controller == nil {
		return CompactionJob{}, false
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	job := controller.jobs[id]
	if job == nil {
		return CompactionJob{}, false
	}
	return job.CompactionJob, true
}

// List returns the oldest retained status snapshots first. A non-positive
// limit selects the controller history capacity.
func (controller *CompactionController) List(limit int) []CompactionJob {
	if controller == nil {
		return nil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if limit <= 0 || limit > len(controller.order) {
		limit = controller.historyCapacity
	}
	if limit > len(controller.order) {
		limit = len(controller.order)
	}
	result := make([]CompactionJob, 0, limit)
	for _, id := range controller.order {
		job := controller.jobs[id]
		if job == nil {
			continue
		}
		result = append(result, job.CompactionJob)
		if len(result) == limit {
			break
		}
	}
	return result
}

func (controller *CompactionController) start(id uint64) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if job := controller.jobs[id]; job != nil {
		job.State = CompactionJobRunning
		job.Attempts++
		job.LastError = ""
	}
}

func (controller *CompactionController) finish(id uint64, runErr error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	job := controller.jobs[id]
	if job == nil {
		return
	}
	if runErr != nil {
		job.State = CompactionJobRetryPending
		job.LastError = compactControllerError(runErr)
		return
	}
	job.State = CompactionJobSucceeded
	job.LastError = ""
	delete(controller.targetIDs, job.Target)
	if controller.active > 0 {
		controller.active--
	}
	controller.activeBytes = subtractCompactionBytes(controller.activeBytes, job.EstimatedBytes)
	controller.pruneHistoryLocked()
}

func (controller *CompactionController) removeActiveLocked(id uint64) {
	job := controller.jobs[id]
	if job == nil {
		return
	}
	delete(controller.jobs, id)
	delete(controller.targetIDs, job.Target)
	if controller.active > 0 {
		controller.active--
	}
	controller.activeBytes = subtractCompactionBytes(controller.activeBytes, job.EstimatedBytes)
	for index, current := range controller.order {
		if current == id {
			controller.order = append(controller.order[:index], controller.order[index+1:]...)
			break
		}
	}
}

func (controller *CompactionController) pruneHistoryLocked() {
	terminal := 0
	for _, id := range controller.order {
		job := controller.jobs[id]
		if job != nil && job.State == CompactionJobSucceeded {
			terminal++
		}
	}
	for terminal > controller.historyCapacity {
		removed := false
		for index, id := range controller.order {
			job := controller.jobs[id]
			if job == nil || job.State != CompactionJobSucceeded {
				continue
			}
			delete(controller.jobs, id)
			controller.order = append(controller.order[:index], controller.order[index+1:]...)
			terminal--
			removed = true
			break
		}
		if !removed {
			return
		}
	}
}

func compactControllerError(err error) string {
	message := fmt.Sprint(err)
	if len(message) <= maxCompactionControllerErrorBytes {
		return message
	}
	return message[:maxCompactionControllerErrorBytes]
}

// admitBytesLocked enforces the optional pending-plus-running estimate budget.
// Zero disables the check, and zero-estimate jobs do not consume budget.
// The caller must hold controller.mu.
func (controller *CompactionController) admitBytesLocked(additional uint64) bool {
	if controller.maxPendingBytes == 0 || additional == 0 {
		return true
	}
	return controller.activeBytes <= controller.maxPendingBytes && additional <= controller.maxPendingBytes-controller.activeBytes
}
