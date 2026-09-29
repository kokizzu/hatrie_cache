package hatReplication

import (
	"errors"
	"strconv"
	"testing"
	"time"
)

func TestReplicaLSNMetricsTracksLagAndThroughput(t *testing.T) {
	metrics, err := NewReplicaLSNMetrics(ReplicaLSNMetricsOptions{MaxSpaces: 2})
	if err != nil {
		t.Fatalf("NewReplicaLSNMetrics() error = %v", err)
	}
	start := time.Unix(100, 0)
	first, err := metrics.Observe("orders", 100, 80, start)
	if err != nil {
		t.Fatalf("first Observe() error = %v", err)
	}
	if first.SourceLSN != 100 || first.AppliedLSN != 80 || first.LagLSN != 20 || first.SourceThroughputPerSecond != 0 || first.ApplyThroughputPerSecond != 0 {
		t.Fatalf("first status = %#v", first)
	}

	second, err := metrics.Observe("orders", 140, 100, start.Add(2*time.Second))
	if err != nil {
		t.Fatalf("second Observe() error = %v", err)
	}
	if second.LagLSN != 40 || second.SourceThroughputPerSecond != 20 || second.ApplyThroughputPerSecond != 10 {
		t.Fatalf("second status = %#v, want lag=40 source-rate=20 apply-rate=10", second)
	}

	metrics.Observe("users", 20, 20, start)
	snapshot := metrics.Snapshot()
	if len(snapshot) != 2 || snapshot[0].Space != "orders" || snapshot[1].Space != "users" {
		t.Fatalf("snapshot = %#v, want deterministic space order", snapshot)
	}
	snapshot[0].Space = "mutated"
	if metrics.Snapshot()[0].Space != "orders" {
		t.Fatal("Snapshot() aliases tracker state")
	}
}

func TestReplicaLSNMetricsRejectsInvalidOrRegressedObservations(t *testing.T) {
	metrics, err := NewReplicaLSNMetrics(ReplicaLSNMetricsOptions{MaxSpaces: 1})
	if err != nil {
		t.Fatalf("NewReplicaLSNMetrics() error = %v", err)
	}
	start := time.Unix(100, 0)
	if _, err := metrics.Observe("", 1, 1, start); !errors.Is(err, ErrReplicaLSNMetricsSpaceRequired) {
		t.Fatalf("empty space error = %v, want ErrReplicaLSNMetricsSpaceRequired", err)
	}
	if _, err := metrics.Observe("orders", 10, 8, start); err != nil {
		t.Fatalf("initial Observe() error = %v", err)
	}
	if _, err := metrics.Observe("orders", 9, 8, start.Add(time.Second)); !errors.Is(err, ErrReplicaLSNMetricsRegressed) {
		t.Fatalf("regressed source error = %v, want ErrReplicaLSNMetricsRegressed", err)
	}
	if _, err := metrics.Observe("orders", 10, 7, start.Add(time.Second)); !errors.Is(err, ErrReplicaLSNMetricsRegressed) {
		t.Fatalf("regressed applied error = %v, want ErrReplicaLSNMetricsRegressed", err)
	}
	if _, err := metrics.Observe("orders", 11, 9, start.Add(-time.Second)); !errors.Is(err, ErrReplicaLSNMetricsRegressed) {
		t.Fatalf("regressed time error = %v, want ErrReplicaLSNMetricsRegressed", err)
	}
	if _, err := metrics.Observe("users", 1, 1, start); !errors.Is(err, ErrReplicaLSNMetricsLimit) {
		t.Fatalf("space limit error = %v, want ErrReplicaLSNMetricsLimit", err)
	}
	status := metrics.Snapshot()
	if len(status) != 1 || status[0].SourceLSN != 10 || status[0].AppliedLSN != 8 {
		t.Fatalf("failed observations changed state = %#v", status)
	}
}

func TestReplicaLSNMetricsZeroValueAndDefaults(t *testing.T) {
	var zero ReplicaLSNMetrics
	if _, err := zero.Observe("orders", 1, 1, time.Unix(100, 0)); !errors.Is(err, ErrReplicaLSNMetricsNotInitialized) {
		t.Fatalf("zero-value Observe() error = %v, want ErrReplicaLSNMetricsNotInitialized", err)
	}
	metrics, err := NewReplicaLSNMetrics(ReplicaLSNMetricsOptions{})
	if err != nil {
		t.Fatalf("default NewReplicaLSNMetrics() error = %v", err)
	}
	if _, err := metrics.Observe("orders", 1, 1, time.Time{}); err != nil {
		t.Fatalf("zero-time Observe() error = %v", err)
	}
}

func TestReplicaLSNMetricsValidatesOptionsAndSaturatesLag(t *testing.T) {
	for _, maxSpaces := range []int{-1, MaxReplicaLSNMetricsMaxSpaces + 1} {
		if _, err := NewReplicaLSNMetrics(ReplicaLSNMetricsOptions{MaxSpaces: maxSpaces}); !errors.Is(err, ErrReplicaLSNMetricsInvalid) {
			t.Fatalf("MaxSpaces=%d error = %v, want ErrReplicaLSNMetricsInvalid", maxSpaces, err)
		}
	}
	metrics, err := NewReplicaLSNMetrics(ReplicaLSNMetricsOptions{MaxSpaces: 1})
	if err != nil {
		t.Fatalf("NewReplicaLSNMetrics() error = %v", err)
	}
	status, err := metrics.Observe("orders", 10, 20, time.Unix(100, 0))
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if status.LagLSN != 0 {
		t.Fatalf("status = %#v, want saturated zero lag", status)
	}
}

var replicaLSNBenchmarkSnapshot []ReplicaLSNSnapshot

func BenchmarkReplicaLSNMetricsObserve(b *testing.B) {
	metrics, err := NewReplicaLSNMetrics(ReplicaLSNMetricsOptions{MaxSpaces: 1})
	if err != nil {
		b.Fatal(err)
	}
	start := time.Unix(100, 0)
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := metrics.Observe("orders", uint64(index+1), uint64(index), start.Add(time.Duration(index+1)*time.Second)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReplicaLSNMetricsSnapshot(b *testing.B) {
	metrics, err := NewReplicaLSNMetrics(ReplicaLSNMetricsOptions{MaxSpaces: 256})
	if err != nil {
		b.Fatal(err)
	}
	start := time.Unix(100, 0)
	for index := 0; index < 256; index++ {
		if _, err := metrics.Observe("space-"+strconv.Itoa(index), uint64(index+1), uint64(index), start.Add(time.Duration(index+1)*time.Second)); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		replicaLSNBenchmarkSnapshot = metrics.Snapshot()
	}
}
