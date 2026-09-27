package hatPipeline

import (
	"encoding/json"
	"testing"
)

type mz004DurableJobJSON struct {
	ID         uint64 `json:"id"`
	FrontierID string `json:"frontier_id"`
	Boundary   uint64 `json:"boundary"`
	State      uint8  `json:"state"`
}

type mz004DurableJobJSONSnapshot struct {
	Version uint8                 `json:"version"`
	NextID  uint64                `json:"next_id"`
	Jobs    []mz004DurableJobJSON `json:"jobs"`
}

var (
	mz004DurableJobBaselinePayload []byte
	mz004DurableJobBaselineDecoded mz004DurableJobJSONSnapshot
	mz004DurableJobBinaryPayload   []byte
	mz004DurableJobBinaryNextID    uint64
	mz004DurableJobBinaryDecoded   []FrontierCompactionJob
)

func mz004DurableJobBaselineSnapshot() mz004DurableJobJSONSnapshot {
	jobs := make([]mz004DurableJobJSON, 128)
	for index := range jobs {
		jobs[index] = mz004DurableJobJSON{
			ID:         uint64(index + 1),
			FrontierID: "orders-eu",
			Boundary:   uint64(1000 + index),
			State:      uint8(index % 3),
		}
	}
	return mz004DurableJobJSONSnapshot{Version: 1, NextID: 129, Jobs: jobs}
}

func BenchmarkMZ004DurableJobJSONEncode(b *testing.B) {
	snapshot := mz004DurableJobBaselineSnapshot()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		payload, err := json.Marshal(snapshot)
		if err != nil {
			b.Fatal(err)
		}
		mz004DurableJobBaselinePayload = payload
	}
	b.ReportMetric(float64(len(mz004DurableJobBaselinePayload)), "wire-bytes/op")
}

func BenchmarkMZ004DurableJobJSONDecode(b *testing.B) {
	payload, err := json.Marshal(mz004DurableJobBaselineSnapshot())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		var decoded mz004DurableJobJSONSnapshot
		if err := json.Unmarshal(payload, &decoded); err != nil {
			b.Fatal(err)
		}
		mz004DurableJobBaselineDecoded = decoded
	}
}

func mz004DurableJobBinaryFixture() []FrontierCompactionJob {
	jobs := make([]FrontierCompactionJob, 128)
	for index := range jobs {
		jobs[index] = FrontierCompactionJob{
			ID:         uint64(index + 1),
			FrontierID: "orders-eu",
			Boundary:   uint64(1000 + index),
			State:      FrontierCompactionJobState(index%4 + 1),
		}
	}
	return jobs
}

func BenchmarkMZ004DurableJobBinaryEncode(b *testing.B) {
	jobs := mz004DurableJobBinaryFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		payload, err := encodeFrontierCompactionJobSnapshot(129, jobs)
		if err != nil {
			b.Fatal(err)
		}
		mz004DurableJobBinaryPayload = payload
	}
	b.ReportMetric(float64(len(mz004DurableJobBinaryPayload)), "wire-bytes/op")
}

func BenchmarkMZ004DurableJobBinaryDecode(b *testing.B) {
	payload, err := encodeFrontierCompactionJobSnapshot(129, mz004DurableJobBinaryFixture())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for range b.N {
		nextID, jobs, err := decodeFrontierCompactionJobSnapshot(payload, DefaultFrontierCompactionJobLimit)
		if err != nil {
			b.Fatal(err)
		}
		mz004DurableJobBinaryNextID = nextID
		mz004DurableJobBinaryDecoded = jobs
	}
}
