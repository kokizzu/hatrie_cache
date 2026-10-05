package hatSql

import "testing"

func BenchmarkCHU24TypedTableMemoryReservationDisabled(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:    "unlimited",
		Columns: []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
	})
	if err != nil {
		b.Fatalf("NewTypedTable() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		reservation, err := table.ReserveMemory(64)
		if err != nil || reservation != nil {
			b.Fatalf("ReserveMemory() = (%v, %v), want (nil, nil)", reservation, err)
		}
	}
}

func BenchmarkCHU24TypedTableMemoryReservationEnabled(b *testing.B) {
	table, err := NewTypedTable(TypedTableSchema{
		Name:         "bounded",
		Columns:      []TypedTableColumn{{Name: "value", Kind: TypedTableString}},
		MemoryBudget: TypedTableMemoryBudgetOptions{MaxBytes: 64},
	})
	if err != nil {
		b.Fatalf("NewTypedTable() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		reservation, err := table.ReserveMemory(64)
		if err != nil {
			b.Fatalf("ReserveMemory() error = %v", err)
		}
		reservation.Release()
	}
}
