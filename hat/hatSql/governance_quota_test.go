package hatSql

import (
	"errors"
	"testing"
	"time"
)

func TestNamespaceQueryQuotaEnforcesFixedWindow(t *testing.T) {
	quota := newNamespaceQueryQuota(2, time.Minute)
	start := time.Unix(100, 0)
	if !quota.allow(start) || !quota.allow(start.Add(time.Second)) {
		t.Fatal("quota rejected an allowed request")
	}
	if quota.allow(start.Add(2 * time.Second)) {
		t.Fatal("quota allowed a request beyond the window limit")
	}
	if !quota.allow(start.Add(time.Minute)) {
		t.Fatal("quota did not reset at the next window")
	}
}

func TestNamespaceQueryQuotaPolicy(t *testing.T) {
	if _, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxQueriesPerWindow: -1}, nil); err == nil {
		t.Fatal("negative MaxQueriesPerWindow was accepted")
	}
	if _, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxQueriesPerWindow: 1, QueryWindow: -time.Second}, nil); err == nil {
		t.Fatal("negative QueryWindow was accepted")
	}

	governor, err := NewNamespaceQueryGovernor(
		NamespaceResourceLimits{MaxQueriesPerWindow: 100, QueryWindow: time.Minute},
		map[string]NamespaceResourceLimits{"tenant": {MaxQueriesPerWindow: 2}},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := governor.limitsFor("tenant").MaxQueriesPerWindow; got != 2 {
		t.Fatalf("tenant MaxQueriesPerWindow = %d, want 2", got)
	}
	if got := governor.limitsFor("tenant").QueryWindow; got != time.Minute {
		t.Fatalf("tenant QueryWindow = %s, want 1m", got)
	}

	if !errors.Is(ErrNamespaceQueryRateLimited, ErrNamespaceQueryRateLimited) {
		t.Fatal("rate-limit sentinel is not usable with errors.Is")
	}
}

func TestNamespaceQueryGovernorEnforcesQuotaBeforeExecution(t *testing.T) {
	governor, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{MaxQueriesPerWindow: 1, QueryWindow: time.Minute}, nil)
	if err != nil {
		t.Fatal(err)
	}
	query := "FROM VALUES (1) AS item(value) SELECT value"
	if _, err := governor.Execute(nil, "tenant", query, nil, nil, SQLQueryOptions{}); err != nil {
		t.Fatalf("first query error = %v", err)
	}
	if _, err := governor.Execute(nil, "tenant", query, nil, nil, SQLQueryOptions{}); !errors.Is(err, ErrNamespaceQueryRateLimited) {
		t.Fatalf("second query error = %v, want %v", err, ErrNamespaceQueryRateLimited)
	}
}

func BenchmarkNamespaceQueryQuotaDisabledPath(b *testing.B) {
	governor, err := NewNamespaceQueryGovernor(NamespaceResourceLimits{}, nil)
	if err != nil {
		b.Fatal(err)
	}
	limits := governor.limitsFor("tenant")
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		if quota := governor.quotaFor("tenant", limits); quota != nil {
			b.Fatal("disabled quota returned an active limiter")
		}
	}
}
