package hatReplication

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"
)

func BenchmarkSinkRetryQueueEnqueueNextAck(b *testing.B) {
	queue, err := NewSinkRetryQueue(SinkRetryOptions{Source: "orders", MaxPending: 1024})
	if err != nil {
		b.Fatal(err)
	}
	records := make([]ExactlyOnceUpsertSinkRecord, 1024)
	for index := range records {
		records[index] = retryTestRecord(uint64(index+1), "order-"+strconv.Itoa(index), "value")
	}
	now := time.Unix(500, 0)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		record := records[index%len(records)]
		if _, err := queue.Enqueue(record, now); err != nil {
			b.Fatal(err)
		}
		delivery, ok, err := queue.Next(now)
		if err != nil || !ok {
			b.Fatalf("next: delivery=%+v ok=%v err=%v", delivery, ok, err)
		}
		if err := queue.Ack(delivery.Record.OutputID, delivery.Record.Sequence); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkRetryQueueRetry(b *testing.B) {
	queue, err := NewSinkRetryQueue(SinkRetryOptions{
		Source:    "orders",
		BaseDelay: time.Nanosecond,
		MaxDelay:  time.Microsecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	record := retryTestRecord(1, "order-1", "value")
	now := time.Unix(500, 0)
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := queue.Enqueue(record, now); err != nil {
			b.Fatal(err)
		}
		delivery, ok, err := queue.Next(now)
		if err != nil || !ok {
			b.Fatalf("next: delivery=%+v ok=%v err=%v", delivery, ok, err)
		}
		if err := queue.Retry(record.OutputID, record.Sequence, now); err != nil {
			b.Fatal(err)
		}
		delivery, ok, err = queue.Next(now.Add(time.Nanosecond))
		if err != nil || !ok {
			b.Fatalf("retry next: delivery=%+v ok=%v err=%v", delivery, ok, err)
		}
		if err := queue.Ack(record.OutputID, record.Sequence); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkRetryQueueSnapshotBinary(b *testing.B) {
	_, snapshot := benchmarkSinkRetrySnapshot(b)
	b.SetBytes(int64(len(mustSinkRetryBinary(snapshot))))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := snapshot.MarshalBinary(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkRetryQueueSnapshotJSON(b *testing.B) {
	_, snapshot := benchmarkSinkRetrySnapshot(b)
	b.SetBytes(int64(len(mustSinkRetryJSON(snapshot))))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := json.Marshal(snapshot); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkRetryQueueUnmarshalBinary(b *testing.B) {
	_, snapshot := benchmarkSinkRetrySnapshot(b)
	data := mustSinkRetryBinary(snapshot)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := UnmarshalSinkRetryQueueSnapshot(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSinkRetryQueueUnmarshalJSON(b *testing.B) {
	_, snapshot := benchmarkSinkRetrySnapshot(b)
	data := mustSinkRetryJSON(snapshot)
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		var decoded SinkRetryQueueSnapshot
		if err := json.Unmarshal(data, &decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkSinkRetrySnapshot(b *testing.B) (*SinkRetryQueue, SinkRetryQueueSnapshot) {
	b.Helper()
	queue, err := NewSinkRetryQueue(SinkRetryOptions{Source: "orders", MaxPending: 128, MaxBytes: 1 << 20})
	if err != nil {
		b.Fatal(err)
	}
	now := time.Unix(500, 0)
	for index := 0; index < 128; index++ {
		if _, err := queue.Enqueue(retryTestRecord(uint64(index+1), "order-"+strconv.Itoa(index), "value"), now); err != nil {
			b.Fatal(err)
		}
	}
	return queue, queue.Snapshot()
}

func mustSinkRetryBinary(snapshot SinkRetryQueueSnapshot) []byte {
	data, err := snapshot.MarshalBinary()
	if err != nil {
		panic(err)
	}
	return data
}

func mustSinkRetryJSON(snapshot SinkRetryQueueSnapshot) []byte {
	data, err := json.Marshal(snapshot)
	if err != nil {
		panic(err)
	}
	return data
}
