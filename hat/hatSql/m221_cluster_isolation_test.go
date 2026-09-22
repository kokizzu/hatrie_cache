package hatSql

import (
	"context"
	"testing"
)

func TestSQLClusterAdmissionIsolatesNamedClusterBudgets(t *testing.T) {
	defaultPolicy := SQLClusterAdmissionPolicy{
		Serving: SQLClusterAdmissionPool{
			CPUUnits:    1,
			MemoryBytes: 16,
			MaxRunning:  1,
			MaxQueued:   1,
		},
		Maintenance: SQLClusterAdmissionPool{
			CPUUnits:    1,
			MemoryBytes: 16,
			MaxRunning:  1,
			MaxQueued:   1,
		},
	}
	analyticsPolicy := defaultPolicy
	analyticsPolicy.Serving = SQLClusterAdmissionPool{
		CPUUnits:    2,
		MemoryBytes: 64,
		MaxRunning:  2,
		MaxQueued:   1,
	}
	admission, err := NewSQLClusterAdmission(SQLClusterAdmissionOptions{
		Default: defaultPolicy,
		Clusters: map[string]SQLClusterAdmissionPolicy{
			"analytics": analyticsPolicy,
			"dashboard": defaultPolicy,
		},
	})
	if err != nil {
		t.Fatalf("NewSQLClusterAdmission() error = %v", err)
	}

	analytics, err := admission.Acquire(context.Background(), SQLClusterAdmissionRequest{
		Cluster:     "analytics",
		Class:       SQLClusterWorkServing,
		CPUUnits:    2,
		MemoryBytes: 64,
	})
	if err != nil {
		t.Fatalf("analytics admission error = %v", err)
	}
	defer analytics.Release()

	dashboard, err := admission.Acquire(context.Background(), SQLClusterAdmissionRequest{
		Cluster:     "dashboard",
		Class:       SQLClusterWorkServing,
		CPUUnits:    1,
		MemoryBytes: 16,
	})
	if err != nil {
		t.Fatalf("dashboard admission was blocked by analytics = %v", err)
	}
	defer dashboard.Release()

	analyticsStats, ok := admission.Stats("analytics")
	if !ok || analyticsStats.Serving.Limits.CPUUnits != 2 || analyticsStats.Serving.Limits.MemoryBytes != 64 || analyticsStats.Serving.Running != 1 {
		t.Fatalf("analytics stats = %#v, ok=%v", analyticsStats, ok)
	}
	dashboardStats, ok := admission.Stats("dashboard")
	if !ok || dashboardStats.Serving.Limits.CPUUnits != 1 || dashboardStats.Serving.Limits.MemoryBytes != 16 || dashboardStats.Serving.Running != 1 {
		t.Fatalf("dashboard stats = %#v, ok=%v", dashboardStats, ok)
	}
}
