package hatSql

import (
	"fmt"
	"math"
	"os"
	"testing"
	"time"
)

func TestSQLQueryLogProbabilisticSamplingIsBoundedAndReproducible(t *testing.T) {
	path := t.TempDir() + "/queries.ndjson"
	log, err := OpenSQLQueryLogWithOptions(path, SQLQueryLogOptions{
		SampleRate: 0.25,
		SampleSeed: 17,
	})
	if err != nil {
		t.Fatalf("OpenSQLQueryLogWithOptions() error = %v", err)
	}
	defer log.Close()

	for index := 0; index < 1_000; index++ {
		status := sampledQueryLogStatus(index)
		if err := log.Append(status); err != nil {
			t.Fatalf("Append(%d) error = %v", index, err)
		}
	}
	stats := log.SamplingStats()
	if stats.SampleRate != 0.25 || stats.Observed != 1_000 || stats.Accepted+stats.Dropped != stats.Observed {
		t.Fatalf("sampling stats = %#v", stats)
	}
	if stats.Accepted == 0 || stats.Dropped == 0 {
		t.Fatalf("sampling stats = %#v, want both accepted and dropped entries", stats)
	}
	entries, err := log.Read()
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if uint64(len(entries)) != stats.Accepted {
		t.Fatalf("retained entries = %d, accepted = %d", len(entries), stats.Accepted)
	}

	secondPath := t.TempDir() + "/queries.ndjson"
	second, err := OpenSQLQueryLogWithOptions(secondPath, SQLQueryLogOptions{
		SampleRate: 0.25,
		SampleSeed: 17,
	})
	if err != nil {
		t.Fatalf("second OpenSQLQueryLogWithOptions() error = %v", err)
	}
	defer second.Close()
	for index := 0; index < 1_000; index++ {
		if err := second.Append(sampledQueryLogStatus(index)); err != nil {
			t.Fatalf("second Append(%d) error = %v", index, err)
		}
	}
	secondStats := second.SamplingStats()
	if secondStats.Accepted != stats.Accepted || secondStats.Dropped != stats.Dropped {
		t.Fatalf("same-seed stats = %#v, want %#v", secondStats, stats)
	}
}

func TestSQLQueryLogSamplingDefaultsToRetainAllAndValidatesRate(t *testing.T) {
	path := t.TempDir() + "/queries.ndjson"
	log, err := OpenSQLQueryLog(path)
	if err != nil {
		t.Fatalf("OpenSQLQueryLog() error = %v", err)
	}
	if err := log.Append(sampledQueryLogStatus(1)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	if got := log.SamplingStats(); got.SampleRate != 1 || got.Observed != 0 || got.Accepted != 0 || got.Dropped != 0 {
		t.Fatalf("default sampling stats = %#v, want disabled sampling counters", got)
	}
	if err := log.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	for _, rate := range []float64{-0.01, 1.01, math.NaN(), math.Inf(1)} {
		if _, err := OpenSQLQueryLogWithOptions(t.TempDir()+"/invalid.ndjson", SQLQueryLogOptions{SampleRate: rate}); err == nil {
			t.Fatalf("SampleRate=%v accepted, want validation error", rate)
		}
	}
}

func TestSQLQueryLogDroppedSampleSkipsRotationAndWrite(t *testing.T) {
	path := t.TempDir() + "/queries.ndjson"
	log, err := OpenSQLQueryLogWithOptions(path, SQLQueryLogOptions{
		SampleRate:       0.25,
		SampleSeed:       17,
		MaxFileBytes:     1,
		MaxRetainedFiles: 4,
	})
	if err != nil {
		t.Fatalf("OpenSQLQueryLogWithOptions() error = %v", err)
	}
	defer log.Close()

	foundDroppedAfterWrite := false
	for index := 0; index < 1_000; index++ {
		beforeStats := log.SamplingStats()
		beforeBytes := log.fileBytes
		if err := log.Append(sampledQueryLogStatus(index)); err != nil {
			t.Fatalf("Append(%d) error = %v", index, err)
		}
		afterStats := log.SamplingStats()
		if afterStats.Dropped > beforeStats.Dropped && beforeBytes > 1 {
			if log.fileBytes != beforeBytes {
				t.Fatalf("dropped append changed active file bytes from %d to %d", beforeBytes, log.fileBytes)
			}
			foundDroppedAfterWrite = true
			break
		}
	}
	if !foundDroppedAfterWrite {
		t.Fatal("did not observe a dropped sample after a durable write")
	}
}

func sampledQueryLogStatus(index int) SQLQueryStatus {
	started := time.Unix(1_700_000_000+int64(index), 0).UTC()
	return SQLQueryStatus{
		QueryID:    fmt.Sprintf("sampled-%d", index),
		State:      SQLQueryStateSucceeded,
		StartedAt:  started,
		FinishedAt: started.Add(time.Millisecond),
	}
}

func BenchmarkSQLQueryLogSampling(b *testing.B) {
	for _, test := range []struct {
		name string
		rate float64
	}{
		{name: "retain-all", rate: 1},
		{name: "sample-25-percent", rate: 0.25},
	} {
		b.Run(test.name, func(b *testing.B) {
			log, err := OpenSQLQueryLogWithOptions(b.TempDir()+"/queries.ndjson", SQLQueryLogOptions{
				SampleRate: test.rate,
				SampleSeed: 17,
			})
			if err != nil {
				b.Fatalf("OpenSQLQueryLogWithOptions() error = %v", err)
			}
			entry, err := newSQLQueryLogEntry(chu38BenchmarkQueryStatus())
			if err != nil {
				b.Fatalf("newSQLQueryLogEntry() error = %v", err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := log.AppendEntry(entry); err != nil {
					b.Fatalf("AppendEntry() error = %v", err)
				}
			}
			b.StopTimer()
			stats := log.SamplingStats()
			accepted := stats.Accepted
			if stats.Observed == 0 {
				accepted = uint64(b.N)
			}
			b.ReportMetric(float64(accepted)/float64(b.N), "accepted/op")
			b.ReportMetric(float64(stats.Dropped)/float64(b.N), "dropped/op")
			if err := log.Close(); err != nil {
				b.Fatalf("Close() error = %v", err)
			}
		})
	}
}

func BenchmarkCHU38DevNullAppend(b *testing.B) {
	file, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		b.Fatalf("OpenFile() error = %v", err)
	}
	log := &SQLQueryLog{file: file, maxRecordSize: DefaultSQLQueryLogMaxRecordBytes, sampleRate: 1}
	entry, err := newSQLQueryLogEntry(chu38BenchmarkQueryStatus())
	if err != nil {
		_ = file.Close()
		b.Fatalf("newSQLQueryLogEntry() error = %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := log.AppendEntry(entry); err != nil {
			b.Fatalf("AppendEntry() error = %v", err)
		}
	}
	b.StopTimer()
	if err := file.Close(); err != nil {
		b.Fatalf("Close() error = %v", err)
	}
}

func chu38BenchmarkQueryStatus() SQLQueryStatus {
	return SQLQueryStatus{
		QueryID:    "benchmark-query",
		State:      SQLQueryStateSucceeded,
		StartedAt:  time.Unix(100, 0).UTC(),
		FinishedAt: time.Unix(100, int64(time.Millisecond)).UTC(),
	}
}
