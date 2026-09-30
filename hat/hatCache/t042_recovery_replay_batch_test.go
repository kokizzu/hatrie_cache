package hatCache

import (
	"path/filepath"
	"strconv"
	"testing"
)

func TestT042ReplayUsesNativeScalarBatchForContiguousSets(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	writerTrie := CreateHatTrie()
	defer writerTrie.Destroy()
	writer, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(writer) error = %v", err)
	}
	for index := 0; index < minNativeCommandBatchSize; index++ {
		response := writer.ExecuteCommand(writerTrie, CacheCommandRequest{
			Command: "SET",
			Key:     "replay:" + strconv.Itoa(index),
			Value:   "value:" + strconv.Itoa(index),
		})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) response = %#v", index, response)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}

	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	defer replayJournal.Close()
	replayed := CreateHatTrie()
	defer replayed.Destroy()

	sequence, err := replayJournal.Replay(replayed, 0)
	if err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if sequence != uint64(minNativeCommandBatchSize) {
		t.Fatalf("Replay() sequence = %d, want %d", sequence, minNativeCommandBatchSize)
	}
	if got := replayed.journalScalarBatchCalls; got != 1 {
		t.Fatalf("Replay() scalar batch calls = %d, want 1", got)
	}
	for index := 0; index < minNativeCommandBatchSize; index++ {
		key := "replay:" + strconv.Itoa(index)
		if got := replayed.GetString(key); got != "value:"+strconv.Itoa(index) {
			t.Fatalf("GetString(%q) = %q, want value", key, got)
		}
	}
}

func TestT042ReplayFallsBackAtCommandBoundaries(t *testing.T) {
	path := filepath.Join(t.TempDir(), "commands.journal")
	writerTrie := CreateHatTrie()
	defer writerTrie.Destroy()
	writer, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(writer) error = %v", err)
	}
	for index := 0; index < minNativeCommandBatchSize; index++ {
		response := writer.ExecuteCommand(writerTrie, CacheCommandRequest{
			Command: "SET",
			Key:     "boundary:" + strconv.Itoa(index),
			Value:   "value:" + strconv.Itoa(index),
		})
		if !response.OK {
			t.Fatalf("ExecuteCommand(%d) response = %#v", index, response)
		}
	}
	for _, request := range []CacheCommandRequest{
		{Command: "SETINT", Key: "boundary-counter", Value: "7"},
		{Command: "INC", Key: "boundary-counter", Value: "2"},
	} {
		if response := writer.ExecuteCommand(writerTrie, request); !response.OK {
			t.Fatalf("ExecuteCommand(%q) response = %#v", request.Command, response)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("writer.Close() error = %v", err)
	}

	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		t.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	defer replayJournal.Close()
	replayed := CreateHatTrie()
	defer replayed.Destroy()
	if _, err := replayJournal.Replay(replayed, 0); err != nil {
		t.Fatalf("Replay() error = %v", err)
	}
	if got := replayed.journalScalarBatchCalls; got != 1 {
		t.Fatalf("Replay() scalar batch calls = %d, want 1", got)
	}
	if got := replayed.GetCounter("boundary-counter"); got != 9 {
		t.Fatalf("GetCounter(boundary-counter) = %d, want 9", got)
	}
}

const t042ReplayBenchmarkRecords = 4096

func BenchmarkT042Replay(b *testing.B) {
	path := filepath.Join(b.TempDir(), "commands.journal")
	writerTrie := CreateHatTrie()
	writer, err := OpenCommandJournal(path)
	if err != nil {
		b.Fatalf("OpenCommandJournal(writer) error = %v", err)
	}
	for index := 0; index < t042ReplayBenchmarkRecords; index++ {
		response := writer.ExecuteCommand(writerTrie, CacheCommandRequest{
			Command: "SET",
			Key:     "benchmark:" + strconv.Itoa(index),
			Value:   "value:" + strconv.Itoa(index),
		})
		if !response.OK {
			b.Fatalf("ExecuteCommand(%d) response = %#v", index, response)
		}
	}
	if err := writer.Close(); err != nil {
		b.Fatalf("writer.Close() error = %v", err)
	}
	writerTrie.Destroy()
	replayJournal, err := OpenCommandJournal(path)
	if err != nil {
		b.Fatalf("OpenCommandJournal(replay) error = %v", err)
	}
	defer replayJournal.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		replayed := CreateHatTrie()
		sequence, err := replayJournal.Replay(replayed, 0)
		if err != nil || sequence != t042ReplayBenchmarkRecords {
			replayed.Destroy()
			b.Fatalf("Replay() sequence/error = %d/%v", sequence, err)
		}
		replayed.Destroy()
	}
	b.ReportMetric(t042ReplayBenchmarkRecords, "records/op")
}
