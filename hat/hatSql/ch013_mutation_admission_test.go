package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCH013MutationAdmissionValidatesAndReservesIntervals(t *testing.T) {
	if _, err := NewSQLMutationAdmission(SQLMutationAdmissionOptions{MinInterval: -time.Nanosecond}); !errors.Is(err, ErrSQLMutationAdmissionOptionsInvalid) {
		t.Fatalf("negative interval error = %v, want %v", err, ErrSQLMutationAdmissionOptionsInvalid)
	}
	if _, err := NewSQLMutationAdmission(SQLMutationAdmissionOptions{
		MaintenanceWindows: []SQLMutationMaintenanceWindow{{
			Weekdays: [7]bool{true, true, true, true, true, true, true},
			Start:    2 * time.Hour,
			End:      2 * time.Hour,
		}},
	}); !errors.Is(err, ErrSQLMutationAdmissionOptionsInvalid) {
		t.Fatalf("equal window error = %v, want %v", err, ErrSQLMutationAdmissionOptionsInvalid)
	}

	admission, err := NewSQLMutationAdmission(SQLMutationAdmissionOptions{MinInterval: 100 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewSQLMutationAdmission() error = %v", err)
	}
	now := time.Date(2026, time.January, 4, 12, 0, 0, 0, time.UTC)
	first, admitted := admission.reserveAt(now)
	if !admitted || !first.Equal(now) {
		t.Fatalf("first reservation = %v, %v; want immediate admission", first, admitted)
	}
	second, admitted := admission.reserveAt(now)
	if !admitted || !second.Equal(now.Add(100*time.Millisecond)) {
		t.Fatalf("second reservation = %v, %v; want one interval later", second, admitted)
	}
	third, admitted := admission.reserveAt(now)
	if !admitted || !third.Equal(now.Add(200*time.Millisecond)) {
		t.Fatalf("third reservation = %v, %v; want two intervals later", third, admitted)
	}
}

func TestCH013MutationAdmissionBlocksMaintenanceWindowsIncludingOvernight(t *testing.T) {
	admission, err := NewSQLMutationAdmission(SQLMutationAdmissionOptions{
		MaintenanceWindows: []SQLMutationMaintenanceWindow{{
			Weekdays: [7]bool{true, false, false, false, false, false, false},
			Start:    23 * time.Hour,
			End:      time.Hour,
		}},
	})
	if err != nil {
		t.Fatalf("NewSQLMutationAdmission() error = %v", err)
	}
	sundayLate := time.Date(2026, time.January, 4, 23, 30, 0, 0, time.UTC)
	end, active := admission.maintenanceEndAt(sundayLate)
	wantEnd := time.Date(2026, time.January, 5, 1, 0, 0, 0, time.UTC)
	if !active || !end.Equal(wantEnd) {
		t.Fatalf("Sunday maintenance = %v, %v; want %v, true", end, active, wantEnd)
	}
	mondayEarly := time.Date(2026, time.January, 5, 0, 30, 0, 0, time.UTC)
	end, active = admission.maintenanceEndAt(mondayEarly)
	if !active || !end.Equal(wantEnd) {
		t.Fatalf("Monday overnight maintenance = %v, %v; want %v, true", end, active, wantEnd)
	}
	mondayLater := time.Date(2026, time.January, 5, 1, 0, 0, 0, time.UTC)
	if _, active = admission.maintenanceEndAt(mondayLater); active {
		t.Fatal("maintenance window remained active at its exclusive end")
	}
}

func TestCH013MutationAdmissionWaitHonorsCanceledContext(t *testing.T) {
	admission, err := NewSQLMutationAdmission(SQLMutationAdmissionOptions{MinInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewSQLMutationAdmission() error = %v", err)
	}
	if err := admission.Wait(context.Background()); err != nil {
		t.Fatalf("first Wait() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := admission.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Wait() error = %v, want context.Canceled", err)
	}
}
