package hatCache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestCacheGRPCServerMaintenanceReadOnlyAllowsReadsAndRejectsWrites(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("name", "original")
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{MaintenanceReadOnly: true})
	defer stop()

	read, err := client.Command(context.Background(), &hatriecachev1.CommandRequest{Command: "GETSTR", Key: "name"})
	if err != nil || !read.GetOk() || read.GetValue() != "original" {
		t.Fatalf("maintenance read = %#v/%v, want original", read, err)
	}

	_, err = client.Command(context.Background(), &hatriecachev1.CommandRequest{Command: "SETSTR", Key: "name", Value: "changed"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("maintenance write error = %v, want FailedPrecondition", err)
	}
	if got := ht.GetString("name"); got != "original" {
		t.Fatalf("maintenance write changed value to %q", got)
	}

	_, err = client.Command(context.Background(), &hatriecachev1.CommandRequest{
		Command: "BATCH",
		Batch: []*hatriecachev1.CommandRequest{
			{Command: "GETSTR", Key: "name"},
			{Command: "SETSTR", Key: "name", Value: "batch-change"},
		},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("maintenance batch error = %v, want FailedPrecondition", err)
	}
	if got := ht.GetString("name"); got != "original" {
		t.Fatalf("maintenance batch changed value to %q", got)
	}

	stream, err := client.CommandStream(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&hatriecachev1.CommandRequest{Command: "GETSTR", Key: "name"}); err != nil {
		t.Fatal(err)
	}
	if response, err := stream.Recv(); err != nil || !response.GetOk() || response.GetValue() != "original" {
		t.Fatalf("maintenance stream read = %#v/%v, want original", response, err)
	}
	if err := stream.Send(&hatriecachev1.CommandRequest{Command: "SETSTR", Key: "name", Value: "stream-change"}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("maintenance stream write error = %v, want FailedPrecondition", err)
	}
}

func TestCacheGRPCServerMaintenanceReadOnlyAllowsSnapshot(t *testing.T) {
	ht := newTestTrie(t)
	called := false
	client, stop := newTestGRPCClient(t, ht, CacheGRPCOptions{
		MaintenanceReadOnly: true,
		Snapshot: func() error {
			called = true
			return nil
		},
	})
	defer stop()

	response, err := client.Snapshot(context.Background(), &hatriecachev1.SnapshotRequest{})
	if err != nil || !response.GetOk() {
		t.Fatalf("maintenance snapshot = %#v/%v, want ok", response, err)
	}
	if !called {
		t.Fatal("maintenance snapshot callback was not called")
	}
}

func TestMonitoringMaintenanceReadOnlyAllowsReadsAndBackup(t *testing.T) {
	ht := newTestTrie(t)
	ht.UpsertString("name", "original")
	handler := NewMonitoringHandler(ht, MonitoringOptions{
		MaintenanceReadOnly:  true,
		BackupSnapshotFormat: SnapshotFormatJSON,
	}).Handler()

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"GETSTR","key":"name"}`))
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK || !strings.Contains(resp.Body.String(), "original") {
		t.Fatalf("maintenance read status/body = %d/%s, want original", resp.Code, resp.Body.String())
	}

	resp = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"name","value":"changed"}`))
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusLocked {
		t.Fatalf("maintenance write status = %d, want %d", resp.Code, http.StatusLocked)
	}
	if got := ht.GetString("name"); got != "original" {
		t.Fatalf("maintenance HTTP write changed value to %q", got)
	}

	bundlePath := filepath.Join(t.TempDir(), "maintenance-backup.tar.gz")
	body := `{"path":` + strconv.Quote(bundlePath) + `}`
	resp = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/api/backup", strings.NewReader(body))
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK {
		t.Fatalf("maintenance backup status = %d: %s", resp.Code, resp.Body.String())
	}
}

func TestMaintenanceReadOnlyDefaultsOff(t *testing.T) {
	ht := newTestTrie(t)
	handler := NewMonitoringHandler(ht, MonitoringOptions{}).Handler()

	resp := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/commands", strings.NewReader(`{"command":"SETSTR","key":"name","value":"enabled"}`))
	handler.ServeHTTP(resp, req)
	if resp.Code != http.StatusOK || ht.GetString("name") != "enabled" {
		t.Fatalf("default command status/value = %d/%q, want 200/enabled", resp.Code, ht.GetString("name"))
	}
}

func TestMonitoringMaintenanceReadOnlyExposesMetric(t *testing.T) {
	handler := NewMonitoringHandler(newTestTrie(t), MonitoringOptions{
		NodeName:            "maintenance-node",
		MaintenanceReadOnly: true,
	}).Handler()

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("maintenance metrics status = %d, want 200", resp.Code)
	}
	if !strings.Contains(resp.Body.String(), "hatrie_cache_maintenance_read_only_enabled") {
		t.Fatal("maintenance read-only metric is missing")
	}
}

var benchmarkTT043AdmissionSink bool

//go:noinline
func benchmarkTT043LegacyAdmission(request CacheCommandRequest) bool {
	return commandShouldJournal(request)
}

//go:noinline
func benchmarkTT043MaintenanceAdmission(enabled *bool, request CacheCommandRequest) bool {
	if !commandShouldJournal(request) {
		return false
	}
	return *enabled
}

func BenchmarkTT043PublicCommandAdmission(b *testing.B) {
	request := CacheCommandRequest{Command: "SETSTR", Key: "name", Value: "value"}
	for _, test := range []struct {
		name    string
		enabled bool
		legacy  bool
	}{
		{name: "legacy_baseline", legacy: true},
		{name: "maintenance_off", enabled: false},
		{name: "maintenance_on", enabled: true},
	} {
		b.Run(test.name, func(b *testing.B) {
			enabled := test.enabled
			b.ReportAllocs()
			b.ResetTimer()
			if test.legacy {
				for range b.N {
					benchmarkTT043AdmissionSink = benchmarkTT043LegacyAdmission(request)
				}
			} else {
				for range b.N {
					benchmarkTT043AdmissionSink = benchmarkTT043MaintenanceAdmission(&enabled, request)
				}
			}
		})
	}
}
