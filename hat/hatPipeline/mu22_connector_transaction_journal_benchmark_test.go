package hatPipeline

import (
	"context"
	"fmt"
	"testing"
)

func BenchmarkMU22ConnectorTransactionBeginCommit(b *testing.B) {
	journal, err := NewConnectorTransactionJournal(context.Background(), ConnectorTransactionJournalOptions{Capacity: 256})
	if err != nil {
		b.Fatal(err)
	}
	intents := make([]ConnectorTransactionIntent, 512)
	for i := range intents {
		intents[i] = ConnectorTransactionIntent{
			ID:          fmt.Sprintf("tx-%04d", i),
			ConnectorID: "orders",
			Generation:  1,
		}
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intent := intents[i&(len(intents)-1)]
		if _, err := journal.Begin(ctx, intent); err != nil {
			b.Fatal(err)
		}
		if _, err := journal.Commit(ctx, intent.ID, intent.Generation); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU22ConnectorTransactionFailRetry(b *testing.B) {
	journal, err := NewConnectorTransactionJournal(context.Background(), ConnectorTransactionJournalOptions{Capacity: 1})
	if err != nil {
		b.Fatal(err)
	}
	intents := []ConnectorTransactionIntent{
		{ID: "tx-retry-0", ConnectorID: "orders", Generation: 1},
		{ID: "tx-retry-1", ConnectorID: "orders", Generation: 1},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		intent := intents[i&1]
		if _, err := journal.Begin(ctx, intent); err != nil {
			b.Fatal(err)
		}
		if _, err := journal.Fail(ctx, intent.ID, intent.Generation, "temporary"); err != nil {
			b.Fatal(err)
		}
		if _, err := journal.Retry(ctx, intent.ID, intent.Generation); err != nil {
			b.Fatal(err)
		}
		if _, err := journal.Commit(ctx, intent.ID, intent.Generation); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU22ConnectorTransactionEncode(b *testing.B) {
	snapshot := mu22ConnectorTransactionBenchmarkSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeConnectorTransactionSnapshot(snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMU22ConnectorTransactionDecode(b *testing.B) {
	encoded, err := EncodeConnectorTransactionSnapshot(mu22ConnectorTransactionBenchmarkSnapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeConnectorTransactionSnapshot(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func mu22ConnectorTransactionBenchmarkSnapshot() ConnectorTransactionSnapshot {
	transactions := make([]ConnectorTransaction, 64)
	for i := range transactions {
		transactions[i] = ConnectorTransaction{
			ID:              fmt.Sprintf("tx-%04d", i),
			ConnectorID:     "orders",
			Generation:      4,
			Sequence:        uint64(i + 1),
			CreatedSequence: uint64(i + 1),
			Attempt:         uint32(i%3 + 1),
			State:           ConnectorTransactionFailed,
			Offset:          []byte(fmt.Sprintf("offset-%04d", i)),
			Frontier:        []byte(fmt.Sprintf("frontier-%04d", i)),
			LastError:       "temporary connector failure",
		}
	}
	return ConnectorTransactionSnapshot{
		Revision:     64,
		Dropped:      3,
		Transactions: transactions,
	}
}
