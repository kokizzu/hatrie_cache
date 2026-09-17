package hatSql

import (
	"strconv"
	"testing"
)

var mu022RetryJournalBenchmarkRecord SQLConnectorTransactionRecord

func BenchmarkMU022AfterRetryJournalBeginComplete(b *testing.B) {
	journal, err := NewSQLConnectorTransactionRetryJournal(SQLConnectorTransactionRetryJournalOptions{Capacity: 1024})
	if err != nil {
		b.Fatal(err)
	}
	envelopes := make([]SQLSourceTransactionEnvelope, 2048)
	for index := range envelopes {
		envelopes[index] = SQLSourceTransactionEnvelope{
			Source: "orders",
			Transaction: SQLSourceTransaction{
				ID:      "tx-" + strconv.Itoa(index),
				Offsets: []SQLSourceOffset{{Source: "orders", Partition: "0", Offset: uint64(index)}},
			},
			Relations: []string{"orders"},
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		envelope := envelopes[index%len(envelopes)]
		record, result, err := journal.Begin(envelope)
		if err != nil || result != SQLConnectorTransactionStarted {
			b.Fatalf("begin result = %q, %v", result, err)
		}
		record, err = journal.Complete(SQLConnectorTransactionCompletion{
			Source:        envelope.Source,
			TransactionID: envelope.Transaction.ID,
			Attempt:       record.Attempts,
			Outcome:       SQLConnectorTransactionCommitted,
		})
		if err != nil {
			b.Fatal(err)
		}
		mu022RetryJournalBenchmarkRecord = record
	}
}
