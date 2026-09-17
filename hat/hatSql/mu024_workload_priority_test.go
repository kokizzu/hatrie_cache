package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mu024AdmissionResult struct {
	name  string
	lease *SQLClusterAdmissionLease
	err   error
}

func TestSQLClusterAdmissionPriorityOrdersSharedWorkloads(t *testing.T) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: SQLClusterAdmissionPolicy{
			Serving: SQLClusterAdmissionPool{CPUUnits: 1, MaxRunning: 1, MaxQueued: 8},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Close()

	holder, err := admission.Acquire(context.Background(), SQLClusterAdmissionRequest{
		Cluster:       "priority",
		Class:         SQLClusterWorkServing,
		WorkloadClass: SQLClusterWorkloadSource,
	})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan mu024AdmissionResult, 2)
	go func() {
		lease, acquireErr := admission.Acquire(context.Background(), SQLClusterAdmissionRequest{
			Cluster:       "priority",
			Class:         SQLClusterWorkServing,
			WorkloadClass: SQLClusterWorkloadAdHoc,
			Priority:      0,
		})
		results <- mu024AdmissionResult{name: "ad_hoc", lease: lease, err: acquireErr}
	}()
	mu024WaitForQueuedWorkload(t, admission, "priority", 1)

	go func() {
		lease, acquireErr := admission.Acquire(context.Background(), SQLClusterAdmissionRequest{
			Cluster:       "priority",
			Class:         SQLClusterWorkServing,
			WorkloadClass: SQLClusterWorkloadCompute,
			Priority:      MaxSQLClusterAdmissionPriority,
		})
		results <- mu024AdmissionResult{name: "compute", lease: lease, err: acquireErr}
	}()
	mu024WaitForQueuedWorkload(t, admission, "priority", 2)

	holder.Release()
	first := mu024ReceiveAdmissionResult(t, results)
	if first.err != nil {
		t.Fatal(first.err)
	}
	if first.name != "compute" {
		first.lease.Release()
		t.Fatalf("first granted workload = %q, want compute", first.name)
	}
	first.lease.Release()
	second := mu024ReceiveAdmissionResult(t, results)
	if second.err != nil {
		t.Fatal(second.err)
	}
	if second.name != "ad_hoc" {
		second.lease.Release()
		t.Fatalf("second granted workload = %q, want ad_hoc", second.name)
	}
	second.lease.Release()
}

func TestSQLClusterAdmissionPriorityAgesLowerPriorityWithoutStarvation(t *testing.T) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: SQLClusterAdmissionPolicy{
			Serving: SQLClusterAdmissionPool{CPUUnits: 1, MaxRunning: 1, MaxQueued: 4},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Close()

	current, err := admission.Acquire(context.Background(), SQLClusterAdmissionRequest{
		Cluster:       "aging",
		Class:         SQLClusterWorkServing,
		WorkloadClass: SQLClusterWorkloadSource,
	})
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan mu024AdmissionResult, 2)
	lowContext, cancelLow := context.WithCancel(context.Background())
	defer cancelLow()
	go func() {
		lease, acquireErr := admission.Acquire(lowContext, SQLClusterAdmissionRequest{
			Cluster:       "aging",
			Class:         SQLClusterWorkServing,
			WorkloadClass: SQLClusterWorkloadAdHoc,
			Priority:      0,
		})
		results <- mu024AdmissionResult{name: "low", lease: lease, err: acquireErr}
	}()
	mu024WaitForQueuedWorkload(t, admission, "aging", 1)

	go func() {
		lease, acquireErr := admission.Acquire(context.Background(), SQLClusterAdmissionRequest{
			Cluster:       "aging",
			Class:         SQLClusterWorkServing,
			WorkloadClass: SQLClusterWorkloadCompute,
			Priority:      MaxSQLClusterAdmissionPriority,
		})
		results <- mu024AdmissionResult{name: "high", lease: lease, err: acquireErr}
	}()
	mu024WaitForQueuedWorkload(t, admission, "aging", 2)
	current.Release()
	first := mu024ReceiveAdmissionResult(t, results)
	if first.err != nil || first.name != "high" {
		if first.lease != nil {
			first.lease.Release()
		}
		t.Fatalf("initial priority result = %#v, want high lease", first)
	}
	current = first.lease

	lowGranted := false
	for cycle := 0; cycle <= MaxSQLClusterAdmissionPriority; cycle++ {
		highContext, cancelHigh := context.WithCancel(context.Background())
		go func() {
			lease, acquireErr := admission.Acquire(highContext, SQLClusterAdmissionRequest{
				Cluster:       "aging",
				Class:         SQLClusterWorkServing,
				WorkloadClass: SQLClusterWorkloadCompute,
				Priority:      MaxSQLClusterAdmissionPriority,
			})
			results <- mu024AdmissionResult{name: "high", lease: lease, err: acquireErr}
		}()
		mu024WaitForQueuedWorkload(t, admission, "aging", 2)
		current.Release()
		result := mu024ReceiveAdmissionResult(t, results)
		if result.err != nil {
			cancelHigh()
			t.Fatal(result.err)
		}
		if result.name == "low" {
			lowGranted = true
			result.lease.Release()
			cancelHigh()
			break
		}
		current = result.lease
		cancelHigh()
	}
	if !lowGranted {
		current.Release()
		t.Fatalf("lower-priority workload was starved after %d priority grants", MaxSQLClusterAdmissionPriority+1)
	}
	select {
	case result := <-results:
		if result.lease != nil {
			result.lease.Release()
		}
	case <-time.After(time.Second):
	}
}

func TestSQLClusterAdmissionWorkloadDefaultsAndValidation(t *testing.T) {
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: SQLClusterAdmissionPolicy{
			Serving: SQLClusterAdmissionPool{CPUUnits: 1, MaxRunning: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer admission.Close()

	lease, err := admission.Acquire(context.Background(), SQLClusterAdmissionRequest{
		Cluster: "defaults",
		Class:   SQLClusterWorkServing,
	})
	if err != nil {
		t.Fatalf("legacy request rejected: %v", err)
	}
	lease.Release()
	for _, request := range []SQLClusterAdmissionRequest{
		{Cluster: "defaults", WorkloadClass: SQLClusterWorkloadClass("unknown")},
		{Cluster: "defaults", Priority: -1},
		{Cluster: "defaults", Priority: MaxSQLClusterAdmissionPriority + 1},
	} {
		if _, err := admission.Acquire(context.Background(), request); !errors.Is(err, ErrSQLClusterAdmissionInvalid) {
			t.Fatalf("request %#v error = %v, want invalid", request, err)
		}
	}
}

func mu024WaitForQueuedWorkload(t *testing.T, admission *SQLClusterAdmission, cluster string, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		stats, found := admission.Stats(cluster)
		if found && stats.Serving.Queued >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("cluster %q did not reach queue depth %d", cluster, want)
}

func mu024ReceiveAdmissionResult(t *testing.T, results <-chan mu024AdmissionResult) mu024AdmissionResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for admission result")
		return mu024AdmissionResult{}
	}
}
