package hatDataStructure

import (
	"strconv"
	"testing"
)

func BenchmarkTT007AppendWithoutSnapshotJoin(b *testing.B) {
	benchmarkTT007Append(b, false)
}

func BenchmarkTT007AppendWithSnapshotJoin(b *testing.B) {
	benchmarkTT007Append(b, true)
}

func benchmarkTT007Append(b *testing.B, withJoin bool) {
	b.Helper()
	newJournal := func() (*TupleFieldOperationJournal, *TupleFieldOperationJournalJoin) {
		journal, err := NewTupleFieldOperationJournal(TupleFieldOperationJournalOptions{
			MaxRecords: 256,
			MaxBytes:   8 << 20,
		})
		if err != nil {
			b.Fatal(err)
		}
		var join *TupleFieldOperationJournalJoin
		if withJoin {
			appendTT007BenchmarkOperation(b, journal, "base")
			join, err = journal.BeginSnapshotJoin(1)
			if err != nil {
				b.Fatal(err)
			}
		}
		return journal, join
	}

	journal, join := newJournal()
	b.ReportAllocs()
	b.ResetTimer()
	for index := 1; index <= b.N; index++ {
		if index > 1 && index%128 == 1 {
			b.StopTimer()
			if join != nil {
				_ = join.Close()
			}
			journal, join = newJournal()
			b.StartTimer()
		}
		operationID := "op-" + strconv.Itoa(index)
		if _, err := journal.Append(TupleFieldOperation{
			OperationID: operationID,
			Updates:     []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte{byte(index)}}},
		}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if join != nil {
		_ = join.Close()
	}
}

func appendTT007BenchmarkOperation(b *testing.B, journal *TupleFieldOperationJournal, operationID string) {
	b.Helper()
	if journal == nil {
		b.Fatal("journal is nil")
	}
	if _, err := journal.Append(TupleFieldOperation{
		OperationID: operationID,
		Updates:     []TupleFieldUpdate{{Index: 0, Kind: TupleFieldSet, Value: []byte{'b'}}},
	}); err != nil {
		b.Fatal(err)
	}
}
