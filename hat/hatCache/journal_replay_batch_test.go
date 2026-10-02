package hatCache

import (
	"path/filepath"
	"strconv"
	"testing"
)

func TestCommandJournalReplayUsesScalarBatch(t *testing.T) {
	journal, err := OpenCommandJournal(filepath.Join(t.TempDir(), "commands.journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	producer := newTestTrie(t)
	records := make([]CommandJournalRecord, minNativeCommandBatchSize*2)
	for idx := range records {
		records[idx] = CommandJournalRecord{
			Request: CacheCommandRequest{
				Command: "SETINT",
				Key:     "replay:" + strconv.Itoa(idx),
				Value:   strconv.Itoa(idx),
			},
		}
	}
	if applied, response := journal.executeJournalRecordsBatch(producer, records); applied != len(records) || !response.OK {
		t.Fatalf("executeJournalRecordsBatch() = %d/%#v, want all records applied", applied, response)
	}

	replayed := newTestTrie(t)
	if _, err := journal.Replay(replayed, 0); err != nil {
		t.Fatal(err)
	}
	if replayed.journalScalarBatchCalls == 0 {
		t.Fatal("Replay() did not use the scalar batch path")
	}
	for idx, record := range records {
		if got := replayed.GetCounter(record.Request.Key); got != int32(idx) {
			t.Fatalf("GetCounter(%q) = %d, want %d", record.Request.Key, got, idx)
		}
	}
}

func TestCommandJournalReplayFlushesScalarBatchAroundUnsupported(t *testing.T) {
	journal, err := OpenCommandJournal(t.TempDir() + "/journal")
	if err != nil {
		t.Fatal(err)
	}
	defer journal.Close()

	producer := newTestTrie(t)
	records := make([]CommandJournalRecord, 0, minNativeCommandBatchSize*2+1)
	for idx := 0; idx < minNativeCommandBatchSize; idx++ {
		records = append(records, CommandJournalRecord{Request: CacheCommandRequest{
			Command: "SETINT",
			Key:     "before:" + strconv.Itoa(idx),
			Value:   strconv.Itoa(idx),
		}})
	}
	records = append(records, CommandJournalRecord{Request: CacheCommandRequest{
		Command: "SETSTR",
		Key:     "middle",
		Value:   "value",
	}})
	for idx := 0; idx < minNativeCommandBatchSize; idx++ {
		records = append(records, CommandJournalRecord{Request: CacheCommandRequest{
			Command: "SETINT",
			Key:     "after:" + strconv.Itoa(idx),
			Value:   strconv.Itoa(idx + 100),
		}})
	}
	if applied, response := journal.executeJournalRecordsBatch(producer, records); applied != len(records) || !response.OK {
		t.Fatalf("executeJournalRecordsBatch() = %d/%#v, want all records applied", applied, response)
	}

	replayed := newTestTrie(t)
	if _, err := journal.Replay(replayed, 0); err != nil {
		t.Fatal(err)
	}
	if replayed.journalScalarBatchCalls != 2 {
		t.Fatalf("Replay() scalar batch calls = %d, want 2", replayed.journalScalarBatchCalls)
	}
	if got := replayed.GetString("middle"); got != "value" {
		t.Fatalf("GetString(%q) = %q, want value", "middle", got)
	}
	for idx := 0; idx < minNativeCommandBatchSize; idx++ {
		if got := replayed.GetCounter("before:" + strconv.Itoa(idx)); got != int32(idx) {
			t.Fatalf("GetCounter(before:%d) = %d, want %d", idx, got, idx)
		}
		if got := replayed.GetCounter("after:" + strconv.Itoa(idx)); got != int32(idx+100) {
			t.Fatalf("GetCounter(after:%d) = %d, want %d", idx, got, idx+100)
		}
	}
}

func BenchmarkCommandJournalReplayScalarSetInt(b *testing.B) {
	journalPath := filepath.Join(b.TempDir(), "commands.journal")
	journal, err := OpenCommandJournal(journalPath)
	if err != nil {
		b.Fatal(err)
	}
	producer := CreateHatTrie()
	defer producer.Destroy()
	records := make([]CommandJournalRecord, 4096)
	for idx := range records {
		records[idx] = CommandJournalRecord{
			Request: CacheCommandRequest{
				Command: "SETINT",
				Key:     "benchmark:" + strconv.Itoa(idx),
				Value:   strconv.Itoa(idx),
			},
		}
	}
	if applied, response := journal.executeJournalRecordsBatch(producer, records); applied != len(records) || !response.OK {
		journal.Close()
		b.Fatalf("executeJournalRecordsBatch() = %d/%#v, want all records applied", applied, response)
	}
	if err := journal.Close(); err != nil {
		b.Fatal(err)
	}

	journal, err = OpenCommandJournal(journalPath)
	if err != nil {
		b.Fatal(err)
	}
	defer journal.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for idx := 0; idx < b.N; idx++ {
		replayed := CreateHatTrie()
		if _, err := journal.Replay(replayed, 0); err != nil {
			replayed.Destroy()
			b.Fatal(err)
		}
		replayed.Destroy()
	}
}
