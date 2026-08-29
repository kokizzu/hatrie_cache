package hatSql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var ErrOutsideMaintenanceWindow = errors.New("operation is outside a maintenance window")

// MaintenanceWindow permits operations in the half-open UTC interval [Start, End).
type MaintenanceWindow struct {
	Start time.Time
	End   time.Time
}

// MaintenanceCoordinator gates destructive maintenance callbacks behind
// configured windows and prevents a named task from overlapping itself.
type MaintenanceCoordinator struct {
	mu      sync.Mutex
	windows []MaintenanceWindow
	now     func() time.Time
	tasks   map[string]maintenanceTask
}

type maintenanceTask struct {
	running bool
	run     func(context.Context) error
}

func NewMaintenanceCoordinator(windows []MaintenanceWindow, now func() time.Time) (*MaintenanceCoordinator, error) {
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	copyWindows := append([]MaintenanceWindow(nil), windows...)
	for _, window := range copyWindows {
		if window.Start.IsZero() || !window.End.After(window.Start) {
			return nil, fmt.Errorf("maintenance window start and end are required")
		}
	}
	return &MaintenanceCoordinator{windows: copyWindows, now: now, tasks: make(map[string]maintenanceTask)}, nil
}

func (coordinator *MaintenanceCoordinator) AddCompaction(name string, run func(context.Context) error) error {
	return coordinator.add(name, run)
}

func (coordinator *MaintenanceCoordinator) AddIndexRebuild(name string, run func(context.Context) error) error {
	return coordinator.add(name, run)
}

func (coordinator *MaintenanceCoordinator) AddBackup(name string, run func(context.Context) error) error {
	return coordinator.add(name, run)
}

func (coordinator *MaintenanceCoordinator) add(name string, run func(context.Context) error) error {
	if coordinator == nil || run == nil || strings.TrimSpace(name) == "" {
		return fmt.Errorf("maintenance task name and callback are required")
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if _, exists := coordinator.tasks[name]; exists {
		return fmt.Errorf("maintenance task %q already exists", name)
	}
	coordinator.tasks[name] = maintenanceTask{run: run}
	return nil
}

func (coordinator *MaintenanceCoordinator) Run(ctx context.Context, name string) error {
	if coordinator == nil {
		return fmt.Errorf("maintenance coordinator is nil")
	}
	coordinator.mu.Lock()
	if !coordinator.allowedLocked(coordinator.now()) {
		coordinator.mu.Unlock()
		return ErrOutsideMaintenanceWindow
	}
	task, exists := coordinator.tasks[strings.TrimSpace(name)]
	if !exists {
		coordinator.mu.Unlock()
		return fmt.Errorf("maintenance task %q does not exist", name)
	}
	if task.running {
		coordinator.mu.Unlock()
		return ErrJobAlreadyRunning
	}
	task.running = true
	coordinator.tasks[name] = task
	coordinator.mu.Unlock()
	err := task.run(ctx)
	coordinator.mu.Lock()
	task = coordinator.tasks[name]
	task.running = false
	coordinator.tasks[name] = task
	coordinator.mu.Unlock()
	return err
}

func (coordinator *MaintenanceCoordinator) allowedLocked(now time.Time) bool {
	for _, window := range coordinator.windows {
		if !now.Before(window.Start) && now.Before(window.End) {
			return true
		}
	}
	return false
}
