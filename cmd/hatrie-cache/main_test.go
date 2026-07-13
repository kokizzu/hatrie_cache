package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	hatriecache "hatrie_cache"
	hatriecachev1 "hatrie_cache/internal/gen/hatriecache/v1"
)

func TestParseConfigDefaultsMonitoringServerOff(t *testing.T) {
	cfg, err := parseConfig(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.monitoringServer {
		t.Fatal("monitoringServer = true, want false")
	}
	if cfg.monitoringAddr != "127.0.0.1:8080" {
		t.Fatalf("monitoringAddr = %q, want default", cfg.monitoringAddr)
	}
}

func TestParseConfigEnablesMonitoringServerExplicitly(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-monitoring-addr", "127.0.0.1:9090",
		"-monitoring-web-dir", "/tmp/web",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if !cfg.monitoringServer {
		t.Fatal("monitoringServer = false, want true")
	}
	if cfg.monitoringAddr != "127.0.0.1:9090" || cfg.monitoringWebDir != "/tmp/web" {
		t.Fatalf("cfg = %#v, want explicit address and web dir", cfg)
	}
}

func TestParseConfigSnapshotFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-snapshot-path", "/tmp/cache.json",
		"-snapshot-interval", "5s",
		"-journal-path", "/tmp/cache.journal",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.snapshotPath != "/tmp/cache.json" || cfg.snapshotInterval != 5*time.Second {
		t.Fatalf("cfg snapshot = %q/%s, want explicit values", cfg.snapshotPath, cfg.snapshotInterval)
	}
	if cfg.journalPath != "/tmp/cache.journal" {
		t.Fatalf("journalPath = %q, want explicit path", cfg.journalPath)
	}
}

func TestParseConfigMonitoringTLSFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-monitoring-tls-cert", "/tmp/cert.pem",
		"-monitoring-tls-key", "/tmp/key.pem",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.monitoringTLSCert != "/tmp/cert.pem" || cfg.monitoringTLSKey != "/tmp/key.pem" {
		t.Fatalf("cfg TLS = %q/%q, want explicit paths", cfg.monitoringTLSCert, cfg.monitoringTLSKey)
	}
	if got := monitoringURL(cfg); got != "https://127.0.0.1:8080" {
		t.Fatalf("monitoringURL() = %q, want https URL", got)
	}
}

func TestParseConfigTopologyFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-node-id", "node-a",
		"-topology-path", "/tmp/topology.json",
		"-election-timeout", "30s",
		"-replication",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.nodeID != "node-a" || cfg.topologyPath != "/tmp/topology.json" || cfg.electionTimeout != 30*time.Second || !cfg.replication {
		t.Fatalf("cfg topology = %#v, want explicit node and path", cfg)
	}
}

func TestParseConfigGRPCFlag(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-grpc-addr", "127.0.0.1:9091",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.grpcAddr != "127.0.0.1:9091" {
		t.Fatalf("grpcAddr = %q, want explicit address", cfg.grpcAddr)
	}
}

func TestParseConfigLevelDBFlags(t *testing.T) {
	cfg, err := parseConfig([]string{
		"-monitoring-server",
		"-db-path", "/tmp/cache.leveldb",
		"-db-sync-interval", "10s",
		"-db-hot-load",
		"-db-hot-load-max-bytes", "2048",
		"-db-hot-load-max-age", "30m",
		"-db-hot-load-min-hits", "42",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseConfig() error = %v", err)
	}
	if cfg.dbPath != "/tmp/cache.leveldb" || cfg.dbSyncInterval != 10*time.Second {
		t.Fatalf("cfg db = %q/%s, want explicit path and interval", cfg.dbPath, cfg.dbSyncInterval)
	}
	if !cfg.dbHotLoad || cfg.dbHotLoadMaxBytes != 2048 || cfg.dbHotLoadMaxAge != 30*time.Minute || cfg.dbHotLoadMinHits != 42 {
		t.Fatalf("cfg hot-load = %#v, want explicit hot-load options", cfg)
	}
	policy := levelDBLoadPolicy(cfg)
	if !policy.HotValuesOnly || policy.MaxValueBytes != 2048 || policy.MaxLastHitAge != 30*time.Minute || policy.MinHits != 42 {
		t.Fatalf("levelDBLoadPolicy() = %#v, want explicit hot-load policy", policy)
	}
}

func TestParseConfigRejectsPartialMonitoringTLSConfig(t *testing.T) {
	if _, err := parseConfig([]string{"-monitoring-tls-cert", "/tmp/cert.pem"}, &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig(partial TLS) error = nil, want error")
	}
	if _, err := parseConfig([]string{"-monitoring-tls-key", "/tmp/key.pem"}, &bytes.Buffer{}); err == nil {
		t.Fatal("parseConfig(partial TLS key) error = nil, want error")
	}
}

func TestMonitoringTLSConfigEnablesHTTP2(t *testing.T) {
	base := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{
			"acme-tls/1",
		},
	}
	cfg := monitoringTLSConfig(base)
	if cfg == base {
		t.Fatal("monitoringTLSConfig returned base config, want clone")
	}
	if !containsString(cfg.NextProtos, "h2") {
		t.Fatalf("NextProtos = %#v, want h2", cfg.NextProtos)
	}
	if !containsString(cfg.NextProtos, "http/1.1") {
		t.Fatalf("NextProtos = %#v, want http/1.1", cfg.NextProtos)
	}
	if !containsString(cfg.NextProtos, "acme-tls/1") {
		t.Fatalf("NextProtos = %#v, want preserved custom protocol", cfg.NextProtos)
	}
	if base.NextProtos[0] != "acme-tls/1" || len(base.NextProtos) != 1 {
		t.Fatalf("base NextProtos mutated: %#v", base.NextProtos)
	}
}

func TestMonitoringServerServesHTTP2OverTLS(t *testing.T) {
	certPath, keyPath := writeTestCertificate(t)

	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen error = %v", err)
	}

	handler := hatriecache.NewMonitoringHandler(ht, hatriecache.MonitoringOptions{}).Handler()
	server := &http.Server{
		Handler:   handler,
		TLSConfig: monitoringTLSConfig(nil),
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ServeTLS(listener, certPath, keyPath)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Fatalf("server shutdown error = %v", err)
		}
		if err := <-errCh; err != nil && err != http.ErrServerClosed {
			t.Fatalf("server error = %v", err)
		}
	})

	transport := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
	}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport}

	resp, err := client.Get("https://" + listener.Addr().String() + "/api/health")
	if err != nil {
		t.Fatalf("GET /api/health error = %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if resp.ProtoMajor != 2 {
		t.Fatalf("protocol = %s, want HTTP/2", resp.Proto)
	}
}

func TestRunDoesNotStartServerByDefault(t *testing.T) {
	stdout := &bytes.Buffer{}
	if err := run(context.Background(), nil, stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	if !strings.Contains(stdout.String(), "monitoring server disabled") {
		t.Fatalf("stdout = %q, want disabled message", stdout.String())
	}
}

func TestRunDoesNotStartMonitoringWhenGRPCBindFails(t *testing.T) {
	monitoringAddr := freeTCPAddr(t)
	blocker, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("blocking listen error = %v", err)
	}
	defer blocker.Close()

	stdout := &bytes.Buffer{}
	err = run(context.Background(), []string{
		"-monitoring-server",
		"-monitoring-addr", monitoringAddr,
		"-grpc-addr", blocker.Addr().String(),
	}, stdout, &bytes.Buffer{})
	if err == nil {
		t.Fatal("run() error = nil, want gRPC bind error")
	}
	if strings.Contains(stdout.String(), "monitoring server listening") {
		t.Fatalf("stdout = %q, want no monitoring startup message", stdout.String())
	}

	listener, err := net.Listen("tcp", monitoringAddr)
	if err != nil {
		t.Fatalf("monitoring address %s remained in use after failed startup: %v", monitoringAddr, err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("monitoring listener Close() error = %v", err)
	}
}

func TestRunStartsHTTPAndGRPCAndStopsOnContextCancel(t *testing.T) {
	monitoringAddr := freeTCPAddr(t)
	grpcAddr := freeTCPAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, []string{
			"-monitoring-server",
			"-monitoring-addr", monitoringAddr,
			"-monitoring-web-dir", "",
			"-grpc-addr", grpcAddr,
		}, &bytes.Buffer{}, &bytes.Buffer{})
	}()

	waitForHTTPHealth(t, "http://"+monitoringAddr+"/api/health")
	waitForGRPCHealth(t, grpcAddr)
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run() after cancel error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run() did not return after context cancellation")
	}
	waitForAddrReusable(t, monitoringAddr)
	waitForAddrReusable(t, grpcAddr)
}

func TestReportServerErrorDoesNotBlockWhenChannelIsFull(t *testing.T) {
	errCh := make(chan error, 1)
	errCh <- errors.New("first")

	done := make(chan struct{})
	go func() {
		reportServerError(errCh, errors.New("second"))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("reportServerError blocked on a full channel")
	}
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free TCP listen error = %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("free TCP listener Close() error = %v", err)
	}
	return addr
}

func waitForHTTPHealth(t *testing.T, url string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("HTTP health endpoint %s did not become ready", url)
}

func waitForGRPCHealth(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithBlock(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		cancel()
		if err == nil {
			client := hatriecachev1.NewCacheServiceClient(conn)
			callCtx, callCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_, callErr := client.Health(callCtx, &hatriecachev1.HealthRequest{})
			callCancel()
			if closeErr := conn.Close(); closeErr != nil {
				t.Fatalf("gRPC connection Close() error = %v", closeErr)
			}
			if callErr == nil {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("gRPC health endpoint %s did not become ready", addr)
}

func waitForAddrReusable(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		listener, err := net.Listen("tcp", addr)
		if err == nil {
			if err := listener.Close(); err != nil {
				t.Fatalf("listener Close() error = %v", err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("address %s was not released", addr)
}

func TestLoadSnapshotIfConfiguredIgnoresMissingSnapshot(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	if _, err := loadSnapshotIfConfigured(ht, filepath.Join(t.TempDir(), "missing.json")); err != nil {
		t.Fatalf("loadSnapshotIfConfigured(missing) error = %v", err)
	}
}

func TestSnapshotLifecycleHelpersLoadAndSave(t *testing.T) {
	source := hatriecache.CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("key", "value")

	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := source.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot() error = %v", err)
	}

	loaded := hatriecache.CreateHatTrie()
	defer loaded.Destroy()
	if _, err := loadSnapshotIfConfigured(loaded, path); err != nil {
		t.Fatalf("loadSnapshotIfConfigured() error = %v", err)
	}
	if got := loaded.GetString("key"); got != "value" {
		t.Fatalf("loaded key = %q, want value", got)
	}

	loaded.UpsertString("second", "value2")
	if err := loaded.SaveSnapshot(path); err != nil {
		t.Fatalf("SaveSnapshot(second) error = %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatalf("snapshot file info = %v/%v, want non-empty file", info, err)
	}
}

func TestLevelDBLifecycleHelpersLoadAndSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cache.leveldb")
	source := hatriecache.CreateHatTrie()
	defer source.Destroy()
	source.UpsertString("key", "value")

	store, err := openLevelDBIfConfigured(path)
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})
	if err := store.Save(source); err != nil {
		t.Fatalf("store.Save() error = %v", err)
	}

	loaded := hatriecache.CreateHatTrie()
	defer loaded.Destroy()
	if err := loadLevelDBIfConfigured(loaded, store, hatriecache.LevelDBLoadPolicy{}); err != nil {
		t.Fatalf("loadLevelDBIfConfigured() error = %v", err)
	}
	if got := loaded.GetString("key"); got != "value" {
		t.Fatalf("loaded key = %q, want value", got)
	}
}

func TestTopologyLifecycleHelperLoadsAndSaves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topology.json")
	store, err := openTopologyIfConfigured(path, "node-a", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("openTopologyIfConfigured() error = %v", err)
	}
	if got := store.Get(); got.Self != "node-a" {
		t.Fatalf("fallback topology = %#v, want node-a", got)
	}

	updated := hatriecache.SingleNodeTopology("node-b", "127.0.0.1:8081")
	if err := store.Set(updated); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	reloaded, err := openTopologyIfConfigured(path, "node-a", "127.0.0.1:8080")
	if err != nil {
		t.Fatalf("openTopologyIfConfigured(reload) error = %v", err)
	}
	if got := reloaded.Get(); got.Self != "node-b" {
		t.Fatalf("reloaded topology = %#v, want node-b", got)
	}
}

func writeTestCertificate(t *testing.T) (string, string) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey error = %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName: "localhost",
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(time.Hour),
		KeyUsage:  x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{
			x509.ExtKeyUsageServerAuth,
		},
		DNSNames:    []string{"localhost"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("CreateCertificate error = %v", err)
	}

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert.pem")
	keyPath := filepath.Join(dir, "key.pem")
	certFile, err := os.OpenFile(certPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("open cert file error = %v", err)
	}
	if err := pem.Encode(certFile, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		certFile.Close()
		t.Fatalf("write cert error = %v", err)
	}
	if err := certFile.Close(); err != nil {
		t.Fatalf("close cert file error = %v", err)
	}

	keyFile, err := os.OpenFile(keyPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatalf("open key file error = %v", err)
	}
	if err := pem.Encode(keyFile, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}); err != nil {
		keyFile.Close()
		t.Fatalf("write key error = %v", err)
	}
	if err := keyFile.Close(); err != nil {
		t.Fatalf("close key file error = %v", err)
	}

	return certPath, keyPath
}

func TestStartSnapshotSaverWritesPeriodically(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	path := filepath.Join(t.TempDir(), "snapshot.json")
	stop := startSnapshotSaver(ctx, ht, nil, path, time.Millisecond, &bytes.Buffer{})
	defer stop()

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(path); err == nil && info.Size() > 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("periodic snapshot was not written")
}

func TestStartSnapshotSaverStopIsIdempotent(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	ctx, cancel := context.WithCancel(context.Background())
	path := filepath.Join(t.TempDir(), "snapshot.json")
	stop := startSnapshotSaver(ctx, ht, nil, path, time.Hour, &bytes.Buffer{})
	cancel()

	stopped := make(chan struct{})
	go func() {
		stop()
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("snapshot saver repeated stop did not return")
	}
}

func TestStartLevelDBSaverWritesPeriodically(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	ht.UpsertString("key", "value")

	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"))
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := startLevelDBSaver(ctx, ht, store, time.Millisecond, &bytes.Buffer{})
	defer stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entry, ok, err := store.Entry("key")
		if err == nil && ok && entry.Type == "string" && entry.String == "value" {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("periodic LevelDB save was not written")
}

func TestStartLevelDBSaverStopIsIdempotent(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	store, err := openLevelDBIfConfigured(filepath.Join(t.TempDir(), "cache.leveldb"))
	if err != nil {
		t.Fatalf("openLevelDBIfConfigured() error = %v", err)
	}
	defer closeLevelDB(store, &bytes.Buffer{})

	ctx, cancel := context.WithCancel(context.Background())
	stop := startLevelDBSaver(ctx, ht, store, time.Hour, &bytes.Buffer{})
	cancel()

	stopped := make(chan struct{})
	go func() {
		stop()
		stop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("LevelDB saver repeated stop did not return")
	}
}

func TestSnapshotCallbackRequiresPath(t *testing.T) {
	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()

	if got := snapshotCallback(ht, nil, ""); got != nil {
		t.Fatal("snapshotCallback(empty) returned callback, want nil")
	}

	path := filepath.Join(t.TempDir(), "snapshot.json")
	callback := snapshotCallback(ht, nil, path)
	if callback == nil {
		t.Fatal("snapshotCallback(path) = nil, want callback")
	}
	if err := callback(); err != nil {
		t.Fatalf("snapshot callback error = %v", err)
	}
	if info, err := os.Stat(path); err != nil || info.Size() == 0 {
		t.Fatalf("snapshot file info = %v/%v, want non-empty file", info, err)
	}
}

func TestJournaledSnapshotHelpersCheckpointAndCompact(t *testing.T) {
	dir := t.TempDir()
	snapshotPath := filepath.Join(dir, "snapshot.json")
	journalPath := filepath.Join(dir, "commands.journal")

	ht := hatriecache.CreateHatTrie()
	defer ht.Destroy()
	journal, err := openJournalIfConfigured(journalPath)
	if err != nil {
		t.Fatalf("openJournalIfConfigured() error = %v", err)
	}
	if got := journal.ExecuteCommand(ht, hatriecache.CacheCommandRequest{Command: "SETINT", Key: "views", Value: "1"}); !got.OK {
		t.Fatalf("journaled SETINT response = %#v, want ok", got)
	}
	if got := journal.ExecuteCommand(ht, hatriecache.CacheCommandRequest{Command: "INC", Key: "views", Value: "2"}); !got.OK {
		t.Fatalf("journaled INC response = %#v, want ok", got)
	}

	if err := saveSnapshotIfConfigured(ht, journal, snapshotPath); err != nil {
		t.Fatalf("saveSnapshotIfConfigured() error = %v", err)
	}
	if info, err := os.Stat(journalPath); err != nil || info.Size() == 0 {
		t.Fatalf("journal file after compact = %v/%v, want checkpoint marker", info, err)
	}

	loaded := hatriecache.CreateHatTrie()
	defer loaded.Destroy()
	metadata, err := loadSnapshotIfConfigured(loaded, snapshotPath)
	if err != nil {
		t.Fatalf("loadSnapshotIfConfigured() error = %v", err)
	}
	if metadata.JournalSequence != 2 {
		t.Fatalf("journal sequence = %d, want 2", metadata.JournalSequence)
	}
	if got := loaded.GetCounter("views"); got != 3 {
		t.Fatalf("loaded views = %d, want 3", got)
	}
}
