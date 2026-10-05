package hatSql

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestCHU24TypedTableMemoryReservationAccountsForWorkingBytes(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:         "reservation",
		Columns:      []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
		MemoryBudget: TypedTableMemoryBudgetOptions{MaxBytes: 256},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	if _, err := table.Upsert("row", []TypedTableValue{TypedString("value")}); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	before := table.MemoryUsage()
	reservation, err := table.ReserveMemory(64)
	if err != nil {
		t.Fatalf("ReserveMemory() error = %v", err)
	}
	if reservation == nil {
		t.Fatal("ReserveMemory() returned nil lease for an enabled budget")
	}
	usage := table.MemoryUsage()
	if usage.ReservedBytes != 64 || usage.UsedBytes != before.UsedBytes+64 {
		t.Fatalf("MemoryUsage() = %#v, want 64 reserved and 64 more used than %#v", usage, before)
	}

	if _, err := table.ReserveMemory(usage.AvailableBytes + 1); !errors.Is(err, ErrTypedTableMemoryBudgetExceeded) {
		t.Fatalf("oversized ReserveMemory() error = %v, want ErrTypedTableMemoryBudgetExceeded", err)
	}
	if _, err := table.Upsert("row", []TypedTableValue{TypedString(strings.Repeat("x", 256))}); !errors.Is(err, ErrTypedTableMemoryBudgetExceeded) {
		t.Fatalf("Upsert() with reservation error = %v, want ErrTypedTableMemoryBudgetExceeded", err)
	}

	reservation.Release()
	reservation.Release()
	if usage := table.MemoryUsage(); usage.ReservedBytes != 0 || usage.UsedBytes != before.UsedBytes {
		t.Fatalf("released MemoryUsage() = %#v, want original usage %#v", usage, before)
	}
}

func TestCHU24TypedTableMemoryReservationDisabledPath(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "unlimited",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	reservation, err := table.ReserveMemory(64)
	if err != nil {
		t.Fatalf("disabled ReserveMemory() error = %v", err)
	}
	if reservation != nil {
		reservation.Release()
		t.Fatal("disabled ReserveMemory() returned a lease")
	}
	if usage := table.MemoryUsage(); usage != (TypedTableMemoryUsage{}) {
		t.Fatalf("disabled MemoryUsage() = %#v, want zero", usage)
	}
}

func TestCHU24TypedTableMemoryReservationValidation(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:         "validation",
		Columns:      []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
		MemoryBudget: TypedTableMemoryBudgetOptions{MaxBytes: 64},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}
	if _, err := table.ReserveMemory(-1); !errors.Is(err, ErrTypedTableMemoryReservationInvalid) {
		t.Fatalf("negative ReserveMemory() error = %v, want ErrTypedTableMemoryReservationInvalid", err)
	}
	reservation, err := table.ReserveMemory(0)
	if err != nil || reservation != nil {
		t.Fatalf("zero ReserveMemory() = (%v, %v), want (nil, nil)", reservation, err)
	}
}

func TestCHU24TypedTableMemoryReservationConcurrentAdmission(t *testing.T) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:         "concurrent",
		Columns:      []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
		MemoryBudget: TypedTableMemoryBudgetOptions{MaxBytes: 128},
	})
	if err != nil {
		t.Fatalf("NewTypedTable() error = %v", err)
	}

	const workers = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	leases := make([]*TypedTableMemoryReservation, 0, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := table.ReserveMemory(8)
			if err != nil {
				return
			}
			mu.Lock()
			leases = append(leases, lease)
			mu.Unlock()
		}()
	}
	wg.Wait()
	if got := table.MemoryUsage().ReservedBytes; got > 128 {
		t.Fatalf("concurrent ReservedBytes = %d, want no more than 128", got)
	}
	for _, lease := range leases {
		lease.Release()
	}
	if usage := table.MemoryUsage(); usage.ReservedBytes != 0 {
		t.Fatalf("after concurrent release MemoryUsage() = %#v, want no reservations", usage)
	}
}
