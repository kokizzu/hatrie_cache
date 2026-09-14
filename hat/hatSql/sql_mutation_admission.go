package hatSql

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrSQLMutationAdmissionOptionsInvalid indicates invalid throttling or
	// maintenance-window configuration.
	ErrSQLMutationAdmissionOptionsInvalid = errors.New("SQL mutation admission options invalid")
	// ErrSQLMutationAdmissionNil indicates that an operation used a nil gate.
	ErrSQLMutationAdmissionNil = errors.New("SQL mutation admission is nil")
)

// SQLMutationMaintenanceWindow blocks mutations during the selected daily
// time interval. Weekdays use time.Weekday indexes, with Sunday at index 0.
// End is exclusive; an End before Start represents an interval crossing
// midnight.
type SQLMutationMaintenanceWindow struct {
	Weekdays [7]bool
	Start    time.Duration
	End      time.Duration
}

// SQLMutationAdmissionOptions configures the opt-in mutation gate. A zero
// value permits mutations immediately and has no maintenance windows.
type SQLMutationAdmissionOptions struct {
	// MinInterval reserves one serialized mutation slot per interval. A zero
	// value disables spacing while retaining any configured windows.
	MinInterval time.Duration
	// MaintenanceWindows block admission until each active window ends.
	MaintenanceWindows []SQLMutationMaintenanceWindow
	// Location interprets daily maintenance-window times. UTC is the default.
	Location *time.Location
}

// SQLMutationAdmission spaces callers through one shared gate and blocks
// configured maintenance windows. It is safe for concurrent callers. A
// canceled context releases the caller without executing its mutation, but a
// future interval already reserved by that caller remains reserved.
type SQLMutationAdmission struct {
	mu                 sync.Mutex
	minInterval        time.Duration
	maintenanceWindows []SQLMutationMaintenanceWindow
	location           *time.Location
	enabled            bool
	nextAllowed        time.Time
}

// NewSQLMutationAdmission validates and creates an opt-in mutation gate.
func NewSQLMutationAdmission(options SQLMutationAdmissionOptions) (*SQLMutationAdmission, error) {
	if options.MinInterval < 0 {
		return nil, fmt.Errorf("%w: min interval cannot be negative", ErrSQLMutationAdmissionOptionsInvalid)
	}
	location := options.Location
	if location == nil {
		location = time.UTC
	}
	windows := make([]SQLMutationMaintenanceWindow, len(options.MaintenanceWindows))
	copy(windows, options.MaintenanceWindows)
	for index, window := range windows {
		if window.Start < 0 || window.Start >= 24*time.Hour || window.End < 0 || window.End >= 24*time.Hour || window.Start == window.End {
			return nil, fmt.Errorf("%w: maintenance window %d must have distinct times within one day", ErrSQLMutationAdmissionOptionsInvalid, index)
		}
		weekdays := false
		for _, enabled := range window.Weekdays {
			weekdays = weekdays || enabled
		}
		if !weekdays {
			return nil, fmt.Errorf("%w: maintenance window %d must select a weekday", ErrSQLMutationAdmissionOptionsInvalid, index)
		}
	}
	return &SQLMutationAdmission{
		minInterval:        options.MinInterval,
		maintenanceWindows: windows,
		location:           location,
		enabled:            options.MinInterval > 0 || len(windows) > 0,
	}, nil
}

// Wait blocks until one mutation slot is available or ctx is canceled.
func (admission *SQLMutationAdmission) Wait(ctx context.Context) error {
	if admission == nil {
		return ErrSQLMutationAdmissionNil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !admission.enabled {
		return nil
	}
	for {
		now := time.Now()
		slot, admitted := admission.reserveAt(now)
		if admitted {
			return waitSQLMutationAdmissionUntil(ctx, now, slot)
		}
		if err := waitSQLMutationAdmissionUntil(ctx, now, slot); err != nil {
			return err
		}
	}
}

// reserveAt returns and reserves the next slot at or after now. It is kept
// separate from Wait so admission policy can be tested without sleeping.
func (admission *SQLMutationAdmission) reserveAt(now time.Time) (time.Time, bool) {
	if admission == nil {
		return time.Time{}, false
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	slot := now
	if admission.nextAllowed.After(slot) {
		slot = admission.nextAllowed
	}
	if end, active := admission.maintenanceEndAtLocked(slot); active {
		return end, false
	}
	if admission.minInterval > 0 {
		admission.nextAllowed = slot.Add(admission.minInterval)
	} else {
		admission.nextAllowed = time.Time{}
	}
	return slot, true
}

// maintenanceEndAt returns the latest end among windows active at now.
func (admission *SQLMutationAdmission) maintenanceEndAt(now time.Time) (time.Time, bool) {
	if admission == nil {
		return time.Time{}, false
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	return admission.maintenanceEndAtLocked(now)
}

func (admission *SQLMutationAdmission) maintenanceEndAtLocked(now time.Time) (time.Time, bool) {
	if len(admission.maintenanceWindows) == 0 {
		return time.Time{}, false
	}
	location := admission.location
	if location == nil {
		location = time.UTC
	}
	localNow := now.In(location)
	dayStart := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, location)
	var latest time.Time
	for _, window := range admission.maintenanceWindows {
		for dayOffset := -1; dayOffset <= 0; dayOffset++ {
			startDay := dayStart.AddDate(0, 0, dayOffset)
			if !window.Weekdays[startDay.Weekday()] {
				continue
			}
			start := startDay.Add(window.Start)
			endDay := startDay
			if window.End < window.Start {
				endDay = endDay.AddDate(0, 0, 1)
			}
			end := endDay.Add(window.End)
			if !localNow.Before(start) && localNow.Before(end) && end.After(latest) {
				latest = end
			}
		}
	}
	if latest.IsZero() {
		return time.Time{}, false
	}
	return latest, true
}

func waitSQLMutationAdmissionUntil(ctx context.Context, now, until time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !until.After(now) {
		return nil
	}
	timer := time.NewTimer(until.Sub(now))
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
