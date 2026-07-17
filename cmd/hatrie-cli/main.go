package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"hatrie_cache/internal/jsonwire"

	hatriecache "hatrie_cache"
)

type clientConfig struct {
	addr    string
	timeout time.Duration
	token   string
}

const maxErrorBodyBytes = 1 << 20
const maxResponseDrainBytes = 1 << 20
const truncatedErrorBodySuffix = "\n... response body truncated"
const minCompressedJSONRequestBytes = 16 << 10
const defaultCommandWireFormat = "auto"
const defaultRequestTimeout = 30 * time.Second

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr, http.DefaultClient); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func cliContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func cliHTTPClient(client *http.Client) *http.Client {
	if client == nil {
		return http.DefaultClient
	}
	return client
}

func cliWriter(writer io.Writer) io.Writer {
	if writer == nil {
		return io.Discard
	}
	return writer
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer, client *http.Client) error {
	ctx = cliContext(ctx)
	stdout = cliWriter(stdout)
	stderr = cliWriter(stderr)
	cfg, remaining, err := parseGlobalFlags(args, stderr)
	if err != nil {
		return err
	}
	client = authenticatedHTTPClient(client, cfg.token)
	if cfg.timeout < 0 {
		return errors.New("timeout must be non-negative")
	}
	if cfg.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, cfg.timeout)
		defer cancel()
	}
	if len(remaining) == 0 {
		return errors.New("subcommand is required: health, stats, entries, topology, election, replication, journal, storage, command, snapshot, backup, restore-bundle, restore-rehearsal, cluster, doctor")
	}

	switch remaining[0] {
	case "health":
		return getJSON(ctx, client, cfg.addr, "/api/health", stdout)
	case "stats":
		return getJSON(ctx, client, cfg.addr, "/api/stats", stdout)
	case "entries":
		return runEntries(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "topology":
		return runTopology(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "election":
		return runElection(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "replication":
		return runReplication(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "journal":
		return runJournal(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "storage":
		return runStorage(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "command":
		return runCommand(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "snapshot":
		return postJSON(ctx, client, cfg.addr, "/api/snapshot", []byte("{}"), stdout)
	case "backup":
		return runBackup(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "restore-bundle":
		return runRestoreBundle(remaining[1:], stdout, stderr)
	case "restore-rehearsal":
		return runRestoreRehearsal(ctx, client, remaining[1:], stdout, stderr)
	case "cluster":
		return runCluster(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "doctor":
		return runDoctor(remaining[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown subcommand %q", remaining[0])
	}
}

func parseGlobalFlags(args []string, output io.Writer) (clientConfig, []string, error) {
	cfg := clientConfig{
		addr:    "http://127.0.0.1:8080",
		timeout: defaultRequestTimeout,
		token:   os.Getenv("HATRIE_CACHE_AUTH_TOKEN"),
	}
	flags := flag.NewFlagSet("hatrie-cli", flag.ContinueOnError)
	flags.SetOutput(cliWriter(output))
	flags.StringVar(&cfg.addr, "addr", cfg.addr, "monitoring server base URL")
	flags.DurationVar(&cfg.timeout, "timeout", cfg.timeout, "request timeout; use 0 to disable")
	flags.StringVar(&cfg.token, "token", cfg.token, "bearer token for authenticated monitoring API requests")
	if err := flags.Parse(args); err != nil {
		return clientConfig{}, nil, err
	}
	return cfg, flags.Args(), nil
}

func authenticatedHTTPClient(client *http.Client, token string) *http.Client {
	client = cliHTTPClient(client)
	token = strings.TrimSpace(token)
	if token == "" {
		return client
	}
	clone := *client
	transport := clone.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	clone.Transport = authTokenTransport{token: token, base: transport}
	return &clone
}

type authTokenTransport struct {
	token string
	base  http.RoundTripper
}

func (transport authTokenTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.Header.Get("Authorization") == "" {
		request = request.Clone(request.Context())
		request.Header.Set("Authorization", "Bearer "+transport.token)
	}
	return transport.base.RoundTrip(request)
}

func runEntries(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("entries", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	prefix := flags.String("prefix", "", "key prefix filter")
	limit := flags.Uint64("limit", 0, "maximum entries to fetch")
	afterKey := flags.String("after-key", "", "only return entries after this key")
	if err := flags.Parse(args); err != nil {
		return err
	}

	path := "/api/entries"
	query := url.Values{}
	if *prefix != "" {
		query.Set("prefix", *prefix)
	}
	if *limit > 0 {
		query.Set("limit", strconv.FormatUint(*limit, 10))
	}
	if *afterKey != "" {
		query.Set("after_key", *afterKey)
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return getJSON(ctx, client, addr, path, stdout)
}

func runTopology(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("topology", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	filePath := flags.String("file", "", "topology JSON file to upload")
	key := flags.String("key", "", "cache key to route through the current topology")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *filePath != "" && *key != "" {
		return errors.New("topology -file and -key are mutually exclusive")
	}
	if *filePath != "" {
		file, err := os.Open(*filePath)
		if err != nil {
			return err
		}
		defer file.Close()
		return putJSONReader(ctx, client, addr, "/api/topology", file, stdout)
	}
	path := "/api/topology"
	if *key != "" {
		path += "?key=" + url.QueryEscape(*key)
	}
	return getJSON(ctx, client, addr, path, stdout)
}

func runElection(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("election", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	key := flags.String("key", "", "cache key to route through election")
	heartbeat := flags.String("heartbeat", "", "node id to mark online")
	offline := flags.String("offline", "", "node id to mark offline")
	if err := flags.Parse(args); err != nil {
		return err
	}
	mutating := 0
	for _, value := range []string{*heartbeat, *offline} {
		if value != "" {
			mutating++
		}
	}
	if mutating > 1 || (mutating > 0 && *key != "") {
		return errors.New("election -key, -heartbeat, and -offline are mutually exclusive")
	}
	if *heartbeat != "" {
		return postElectionUpdate(ctx, client, addr, *heartbeat, true, stdout)
	}
	if *offline != "" {
		return postElectionUpdate(ctx, client, addr, *offline, false, stdout)
	}
	path := "/api/election"
	if *key != "" {
		path += "?key=" + url.QueryEscape(*key)
	}
	return getJSON(ctx, client, addr, path, stdout)
}

func postElectionUpdate(ctx context.Context, client *http.Client, addr string, node string, online bool, stdout io.Writer) error {
	body, err := jsonwire.Marshal(struct {
		Node   string `json:"node"`
		Online bool   `json:"online"`
	}{
		Node:   node,
		Online: online,
	})
	if err != nil {
		return err
	}
	return postJSON(ctx, client, addr, "/api/election", body, stdout)
}

func runReplication(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("replication", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	sync := flags.Bool("sync", false, "push local entries to topology replicas")
	prefix := flags.String("prefix", "", "key prefix to sync")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *prefix != "" && !*sync {
		return errors.New("replication -prefix requires -sync")
	}
	if !*sync {
		return getJSON(ctx, client, addr, "/api/replication", stdout)
	}
	body, err := jsonwire.Marshal(struct {
		Prefix string `json:"prefix,omitempty"`
	}{
		Prefix: *prefix,
	})
	if err != nil {
		return err
	}
	return postJSON(ctx, client, addr, "/api/replication", body, stdout)
}

func runJournal(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("journal", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	afterSequence := flags.Uint64("after-sequence", 0, "only return journal entries after this sequence")
	limit := flags.Uint64("limit", 0, "maximum journal entries to fetch or pull")
	pullFrom := flags.String("pull-from", "", "source monitoring server base URL to pull and apply journal entries from")
	untilCurrent := flags.Bool("until-current", false, "keep pulling batches until the source has no more entries")
	maxBatches := flags.Uint64("max-batches", 0, "maximum journal batches to pull with -until-current")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *maxBatches > 0 && !*untilCurrent {
		return errors.New("journal -max-batches requires -until-current")
	}
	if *untilCurrent && strings.TrimSpace(*pullFrom) == "" {
		return errors.New("journal -until-current requires -pull-from")
	}

	if strings.TrimSpace(*pullFrom) != "" {
		body, err := jsonwire.Marshal(struct {
			Source        string `json:"source"`
			AfterSequence uint64 `json:"after_sequence,omitempty"`
			Limit         uint64 `json:"limit,omitempty"`
			UntilCurrent  bool   `json:"until_current,omitempty"`
			MaxBatches    uint64 `json:"max_batches,omitempty"`
		}{
			Source:        strings.TrimSpace(*pullFrom),
			AfterSequence: *afterSequence,
			Limit:         *limit,
			UntilCurrent:  *untilCurrent,
			MaxBatches:    *maxBatches,
		})
		if err != nil {
			return err
		}
		return postJSON(ctx, client, addr, "/api/journal", body, stdout)
	}

	path := "/api/journal"
	query := url.Values{}
	if *afterSequence > 0 {
		query.Set("after_sequence", strconv.FormatUint(*afterSequence, 10))
	}
	if *limit > 0 {
		query.Set("limit", strconv.FormatUint(*limit, 10))
	}
	if encoded := query.Encode(); encoded != "" {
		path += "?" + encoded
	}
	return getJSON(ctx, client, addr, path, stdout)
}

func runStorage(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("storage subcommand is required: status, flush, compact")
	}
	switch args[0] {
	case "status":
		return getJSON(ctx, client, addr, "/api/storage", stdout)
	case "flush":
		return postJSON(ctx, client, addr, "/api/storage/flush", []byte("{}"), stdout)
	case "compact":
		return runStorageCompact(ctx, client, addr, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown storage subcommand %q", args[0])
	}
}

func runStorageCompact(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("storage compact", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	startKey := flags.String("start-key", "", "first cache key to include in the LevelDB compaction range")
	limitKey := flags.String("limit-key", "", "first cache key after the LevelDB compaction range")
	if err := flags.Parse(args); err != nil {
		return err
	}
	body, err := jsonwire.Marshal(struct {
		StartKey string `json:"start_key,omitempty"`
		LimitKey string `json:"limit_key,omitempty"`
	}{
		StartKey: strings.TrimSpace(*startKey),
		LimitKey: strings.TrimSpace(*limitKey),
	})
	if err != nil {
		return err
	}
	return postJSON(ctx, client, addr, "/api/storage/compact", body, stdout)
}

func runBackup(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("backup", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	path := flags.String("path", "", "server-side backup bundle output path")
	snapshotFormat := flags.String("snapshot-format", "", "optional snapshot format override for the backup bundle")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*path) == "" {
		return errors.New("backup -path is required")
	}
	body, err := jsonwire.Marshal(struct {
		Path           string `json:"path"`
		SnapshotFormat string `json:"snapshot_format,omitempty"`
	}{
		Path:           strings.TrimSpace(*path),
		SnapshotFormat: strings.TrimSpace(*snapshotFormat),
	})
	if err != nil {
		return err
	}
	return postJSON(ctx, client, addr, "/api/backup", body, stdout)
}

func runDoctor(args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("doctor", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	path := flags.String("path", "", "backup directory or atomic backup bundle path to verify")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*path) == "" {
		return errors.New("doctor -path is required")
	}
	report, err := hatriecache.VerifyBackupPath(*path)
	if err != nil {
		return err
	}
	data, err := jsonwire.Marshal(report)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func runRestoreBundle(args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("restore-bundle", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	bundlePath := flags.String("bundle", "", "atomic backup bundle path to verify and restore")
	dataDir := flags.String("data-dir", "data", "restore target data directory")
	overwrite := flags.Bool("overwrite", false, "allow restoring into a non-empty data directory")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*bundlePath) == "" {
		return errors.New("restore-bundle -bundle is required")
	}
	report, err := hatriecache.RestoreBackupBundle(*bundlePath, *dataDir, hatriecache.BackupBundleRestoreOptions{Overwrite: *overwrite})
	if err != nil {
		return err
	}
	data, err := jsonwire.Marshal(report)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func runRestoreRehearsal(ctx context.Context, client *http.Client, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("restore-rehearsal", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	path := flags.String("path", "", "backup directory or atomic backup bundle path to rehearse")
	workDir := flags.String("work-dir", "", "optional rehearsal work directory")
	keepWorkDir := flags.Bool("keep-work-dir", false, "keep the temporary rehearsal work directory")
	runtimeCheck := flags.Bool("runtime-check", true, "start a temporary server and validate restored health, stats, and GET checks")
	runtimeServerBin := flags.String("runtime-server-bin", "", "optional hatrie-cache server binary for runtime checks; defaults to building ./cmd/hatrie-cache")
	var runtimeGets repeatedStringFlag
	flags.Var(&runtimeGets, "runtime-get", "key or key=value to GET from the temporary restored server; repeat for multiple keys")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*path) == "" {
		return errors.New("restore-rehearsal -path is required")
	}
	report, err := hatriecache.RehearseRestore(*path, hatriecache.RestoreRehearsalOptions{
		WorkDir:     *workDir,
		KeepWorkDir: *keepWorkDir,
	})
	if err != nil {
		return err
	}
	if *runtimeCheck {
		runtimeReport, err := runRestoreRehearsalRuntimeChecks(ctx, client, report.RestoredDir, runtimeGets, *runtimeServerBin, stderr)
		if err != nil {
			return err
		}
		report.Runtime = &runtimeReport
	}
	data, err := jsonwire.Marshal(report)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

type repeatedStringFlag []string

func (values *repeatedStringFlag) String() string {
	if values == nil {
		return ""
	}
	return strings.Join(*values, ",")
}

func (values *repeatedStringFlag) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("value must be non-empty")
	}
	*values = append(*values, value)
	return nil
}

func runRestoreRehearsalRuntimeChecks(ctx context.Context, client *http.Client, restoredDir string, getSpecs []string, serverBinary string, stderr io.Writer) (hatriecache.RestoreRehearsalRuntimeReport, error) {
	restoredDir = strings.TrimSpace(restoredDir)
	if restoredDir == "" {
		return hatriecache.RestoreRehearsalRuntimeReport{}, errors.New("restore rehearsal runtime restored dir is required")
	}
	serverBinary = strings.TrimSpace(serverBinary)
	var cleanup func()
	if serverBinary == "" {
		var err error
		serverBinary, cleanup, err = buildRestoreRehearsalServerBinary(ctx, stderr)
		if err != nil {
			return hatriecache.RestoreRehearsalRuntimeReport{}, err
		}
		defer cleanup()
	}
	addr, err := freeLocalTCPAddr()
	if err != nil {
		return hatriecache.RestoreRehearsalRuntimeReport{}, err
	}
	baseURL := "http://" + addr
	args := []string{
		"-monitoring-server",
		"-monitoring-addr", addr,
		"-monitoring-web-dir", "",
	}
	hasData := false
	if snapshotPath := firstExistingRestorePath(restoredDir, "snapshot.hc", "snapshot.json"); snapshotPath != "" {
		args = append(args, "-snapshot-path", snapshotPath)
		hasData = true
	}
	if journalPath := filepath.Join(restoredDir, "commands.journal"); fileExistsCLI(journalPath) {
		args = append(args, "-journal-path", journalPath)
		hasData = true
	}
	if dbPath := filepath.Join(restoredDir, "cache.leveldb"); fileExistsCLI(dbPath) {
		args = append(args, "-db-path", dbPath)
		hasData = true
	}
	if !hasData {
		return hatriecache.RestoreRehearsalRuntimeReport{}, errors.New("restore rehearsal runtime found no restored snapshot, journal, or LevelDB data")
	}

	cmd := exec.CommandContext(cliContext(ctx), serverBinary, args...)
	cmd.Stdout = cliWriter(stderr)
	cmd.Stderr = cliWriter(stderr)
	if err := cmd.Start(); err != nil {
		return hatriecache.RestoreRehearsalRuntimeReport{}, err
	}
	defer stopRestoreRehearsalRuntime(cmd)

	client = cliHTTPClient(client)
	health, err := waitForRestoreRehearsalHealth(ctx, client, baseURL)
	if err != nil {
		return hatriecache.RestoreRehearsalRuntimeReport{}, err
	}
	stats, err := getJSONValue[hatriecache.CacheStats](ctx, client, baseURL, "/api/stats")
	if err != nil {
		return hatriecache.RestoreRehearsalRuntimeReport{}, fmt.Errorf("restore rehearsal runtime stats: %w", err)
	}
	checks, err := runRestoreRehearsalGETChecks(ctx, client, baseURL, getSpecs)
	if err != nil {
		return hatriecache.RestoreRehearsalRuntimeReport{}, err
	}
	return hatriecache.RestoreRehearsalRuntimeReport{
		OK:     true,
		Addr:   baseURL,
		Health: &health,
		Stats:  &stats,
		Gets:   checks,
	}, nil
}

func buildRestoreRehearsalServerBinary(ctx context.Context, stderr io.Writer) (string, func(), error) {
	root := findModuleRoot()
	if root == "" {
		return "", nil, errors.New("restore rehearsal runtime could not find go.mod; pass -runtime-server-bin")
	}
	dir, err := os.MkdirTemp("", "hatrie-cache-restore-runtime-bin-*")
	if err != nil {
		return "", nil, err
	}
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	binary := filepath.Join(dir, "hatrie-cache")
	cmd := exec.CommandContext(cliContext(ctx), "go", "build", "-o", binary, "./cmd/hatrie-cache")
	cmd.Dir = root
	cmd.Stdout = cliWriter(stderr)
	cmd.Stderr = cliWriter(stderr)
	if err := cmd.Run(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("build restore rehearsal runtime server: %w", err)
	}
	return binary, cleanup, nil
}

func waitForRestoreRehearsalHealth(ctx context.Context, client *http.Client, baseURL string) (hatriecache.MonitoringHealth, error) {
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := cliContext(ctx).Err(); err != nil {
			return hatriecache.MonitoringHealth{}, err
		}
		health, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, baseURL, "/api/health")
		if err == nil && health.Status == "online" {
			return health, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("health status is %q", health.Status)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if lastErr == nil {
		lastErr = errors.New("timed out")
	}
	return hatriecache.MonitoringHealth{}, fmt.Errorf("restore rehearsal runtime health: %w", lastErr)
}

func runRestoreRehearsalGETChecks(ctx context.Context, client *http.Client, baseURL string, specs []string) ([]hatriecache.RestoreRehearsalGetCheck, error) {
	if len(specs) == 0 {
		specs = []string{"__hatrie_restore_rehearsal_probe__"}
	}
	checks := make([]hatriecache.RestoreRehearsalGetCheck, 0, len(specs))
	for _, spec := range specs {
		key, expected := restoreRehearsalGETSpec(spec)
		response, err := postCommandJSONValue(ctx, client, baseURL, hatriecache.CacheCommandRequest{Command: "GET", Key: key})
		check := hatriecache.RestoreRehearsalGetCheck{Key: key, Expected: expected}
		if err != nil {
			check.Error = err.Error()
			checks = append(checks, check)
			return checks, fmt.Errorf("restore rehearsal runtime GET %q: %w", key, err)
		}
		check.OK = response.OK
		check.Value = response.Value
		if !response.OK {
			check.Error = response.Message
			checks = append(checks, check)
			return checks, fmt.Errorf("restore rehearsal runtime GET %q failed: %s", key, response.Message)
		}
		if expected != nil && response.Value != *expected {
			check.OK = false
			check.Error = fmt.Sprintf("got %q want %q", response.Value, *expected)
			checks = append(checks, check)
			return checks, fmt.Errorf("restore rehearsal runtime GET %q: got %q want %q", key, response.Value, *expected)
		}
		checks = append(checks, check)
	}
	return checks, nil
}

func restoreRehearsalGETSpec(spec string) (string, *string) {
	key, expected, found := strings.Cut(spec, "=")
	key = strings.TrimSpace(key)
	if !found {
		return key, nil
	}
	return key, &expected
}

func postCommandJSONValue(ctx context.Context, client *http.Client, addr string, request hatriecache.CacheCommandRequest) (hatriecache.CacheCommandResponse, error) {
	var response hatriecache.CacheCommandResponse
	body, err := jsonwire.Marshal(request)
	if err != nil {
		return response, err
	}
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodPost, endpoint(addr, "/api/commands"), bytes.NewReader(body))
	if err != nil {
		return response, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if err := doAndDecodeJSON(client, req, &response); err != nil {
		return response, err
	}
	return response, nil
}

func stopRestoreRehearsalRuntime(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	_ = cmd.Process.Signal(os.Interrupt)
	select {
	case <-done:
		return
	case <-time.After(2 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
}

func freeLocalTCPAddr() (string, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		return "", err
	}
	return addr, nil
}

func firstExistingRestorePath(dir string, names ...string) string {
	for _, name := range names {
		path := filepath.Join(dir, name)
		if fileExistsCLI(path) {
			return path
		}
	}
	return ""
}

func fileExistsCLI(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func findModuleRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if fileExistsCLI(filepath.Join(dir, "go.mod")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

type clusterJoinResult struct {
	OK              bool   `json:"ok"`
	Message         string `json:"message"`
	Peer            string `json:"peer"`
	Node            string `json:"node"`
	Address         string `json:"address"`
	TopologyUpdated bool   `json:"topology_updated"`
	TargetUpdated   bool   `json:"target_updated"`
	JournalPulled   bool   `json:"journal_pulled"`
}

type clusterStatusResult struct {
	OK               bool                           `json:"ok"`
	Peer             string                         `json:"peer"`
	Health           *hatriecache.MonitoringHealth  `json:"health,omitempty"`
	Topology         *hatriecache.ClusterTopology   `json:"topology,omitempty"`
	Election         *hatriecache.ElectionStatus    `json:"election,omitempty"`
	Replication      *hatriecache.ReplicationResult `json:"replication,omitempty"`
	ReplicationError string                         `json:"replication_error,omitempty"`
	Nodes            []clusterNodeStatus            `json:"nodes,omitempty"`
	Errors           []string                       `json:"errors,omitempty"`
}

type clusterNodeStatus struct {
	ID                 string                        `json:"id"`
	Role               string                        `json:"role,omitempty"`
	Address            string                        `json:"address,omitempty"`
	OK                 bool                          `json:"ok"`
	Health             *hatriecache.MonitoringHealth `json:"health,omitempty"`
	TopologyConsistent bool                          `json:"topology_consistent,omitempty"`
	TopologyVersion    uint64                        `json:"topology_version,omitempty"`
	TopologyError      string                        `json:"topology_error,omitempty"`
	ElectionOK         bool                          `json:"election_ok,omitempty"`
	ElectionConsistent bool                          `json:"election_consistent,omitempty"`
	ElectionError      string                        `json:"election_error,omitempty"`
	Error              string                        `json:"error,omitempty"`
}

func runCluster(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 {
		return errors.New("cluster subcommand is required: join, status, doctor")
	}
	switch args[0] {
	case "join":
		return runClusterJoin(ctx, client, addr, args[1:], stdout, stderr)
	case "status", "doctor":
		return runClusterStatus(ctx, client, addr, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown cluster subcommand %q", args[0])
	}
}

func runClusterStatus(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster status", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	peer := flags.String("peer", addr, "cluster node monitoring API base URL")
	probeNodes := flags.Bool("probe-nodes", true, "probe health for topology node addresses")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*peer = strings.TrimSpace(*peer)
	if *peer == "" {
		return errors.New("cluster status -peer is required")
	}

	result := clusterStatusResult{OK: true, Peer: *peer}
	health, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, *peer, "/api/health")
	if err != nil {
		return fmt.Errorf("cluster health: %w", err)
	}
	result.Health = &health
	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("cluster topology: %w", err)
	}
	result.Topology = &topology
	election, err := getJSONValue[hatriecache.ElectionStatus](ctx, client, *peer, "/api/election")
	if err != nil {
		return fmt.Errorf("cluster election: %w", err)
	}
	result.Election = &election
	replication, err := getJSONValue[hatriecache.ReplicationResult](ctx, client, *peer, "/api/replication")
	if err != nil {
		result.ReplicationError = err.Error()
	} else {
		result.Replication = &replication
	}
	if *probeNodes {
		result.Nodes = probeClusterNodes(ctx, client, topology, election)
		for _, node := range result.Nodes {
			if !node.OK {
				result.OK = false
				result.Errors = append(result.Errors, clusterNodeErrors(node)...)
			}
		}
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func probeClusterNodes(ctx context.Context, client *http.Client, topology hatriecache.ClusterTopology, election hatriecache.ElectionStatus) []clusterNodeStatus {
	nodes := make([]clusterNodeStatus, 0, len(topology.Nodes))
	for _, node := range topology.Nodes {
		status := clusterNodeStatus{
			ID:      node.ID,
			Role:    node.Role,
			Address: strings.TrimSpace(node.Address),
		}
		if status.Address == "" {
			status.Error = "address is empty"
			nodes = append(nodes, status)
			continue
		}
		health, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, status.Address, "/api/health")
		if err != nil {
			status.Error = err.Error()
			nodes = append(nodes, status)
			continue
		}
		status.OK = true
		status.Health = &health
		nodeTopology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, status.Address, "/api/topology")
		if err != nil {
			status.OK = false
			status.TopologyError = err.Error()
		} else {
			status.TopologyVersion = nodeTopology.Version
			consistent, err := clusterTopologiesConsistent(topology, nodeTopology)
			if err != nil {
				status.OK = false
				status.TopologyError = err.Error()
			} else {
				status.TopologyConsistent = consistent
				if !consistent {
					status.OK = false
					status.TopologyError = "topology differs from peer"
				}
			}
		}
		nodeElection, err := getJSONValue[hatriecache.ElectionStatus](ctx, client, status.Address, "/api/election")
		if err != nil {
			status.OK = false
			status.ElectionError = err.Error()
		} else {
			status.ElectionOK = true
			status.ElectionConsistent = clusterElectionLeadersConsistent(election, nodeElection)
			if !status.ElectionConsistent {
				status.OK = false
				status.ElectionError = "elected leaders differ from peer"
			}
		}
		nodes = append(nodes, status)
	}
	return nodes
}

func clusterElectionLeadersConsistent(peer hatriecache.ElectionStatus, node hatriecache.ElectionStatus) bool {
	peerLeaders := clusterElectionLeaderMap(peer)
	nodeLeaders := clusterElectionLeaderMap(node)
	if len(peerLeaders) != len(nodeLeaders) {
		return false
	}
	for shard, peerLeader := range peerLeaders {
		if nodeLeaders[shard] != peerLeader {
			return false
		}
	}
	return true
}

func clusterElectionLeaderMap(status hatriecache.ElectionStatus) map[uint32]string {
	out := make(map[uint32]string, len(status.Leaders))
	for _, leader := range status.Leaders {
		out[leader.Shard] = leader.Leader
	}
	return out
}

func clusterTopologiesConsistent(peerTopology hatriecache.ClusterTopology, nodeTopology hatriecache.ClusterTopology) (bool, error) {
	peerStore, err := hatriecache.NewTopologyStore(peerTopology)
	if err != nil {
		return false, fmt.Errorf("peer topology is invalid: %w", err)
	}
	nodeStore, err := hatriecache.NewTopologyStore(nodeTopology)
	if err != nil {
		return false, fmt.Errorf("node topology is invalid: %w", err)
	}
	peer := peerStore.Get()
	node := nodeStore.Get()
	peer.Self = ""
	node.Self = ""
	return reflect.DeepEqual(peer, node), nil
}

func clusterNodeErrors(node clusterNodeStatus) []string {
	prefix := "node " + node.ID + ": "
	var out []string
	if node.Error != "" {
		out = append(out, prefix+node.Error)
	}
	if node.TopologyError != "" {
		out = append(out, prefix+"topology: "+node.TopologyError)
	}
	if node.ElectionError != "" {
		out = append(out, prefix+"election: "+node.ElectionError)
	}
	if len(out) == 0 && !node.OK {
		out = append(out, prefix+"unhealthy")
	}
	return out
}

func runClusterJoin(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster join", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	nodeID := flags.String("node", "", "node id to add to the cluster topology")
	nodeAddress := flags.String("address", "", "joining node monitoring API base URL")
	peer := flags.String("peer", addr, "existing cluster node monitoring API base URL")
	role := flags.String("role", "replica", "topology role for the joining node: primary or replica")
	updateTarget := flags.Bool("update-target", true, "upload the updated topology to the joining node")
	pullJournal := flags.Bool("pull-journal", true, "pull the peer journal into the joining node after topology update")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*nodeID = strings.TrimSpace(*nodeID)
	*nodeAddress = strings.TrimSpace(*nodeAddress)
	*peer = strings.TrimSpace(*peer)
	*role = strings.TrimSpace(*role)
	if *nodeID == "" {
		return errors.New("cluster join -node is required")
	}
	if *nodeAddress == "" {
		return errors.New("cluster join -address is required")
	}
	if *peer == "" {
		return errors.New("cluster join -peer is required")
	}

	if _, err := getJSONValue[map[string]interface{}](ctx, client, *peer, "/api/health"); err != nil {
		return fmt.Errorf("peer health: %w", err)
	}
	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("peer topology: %w", err)
	}
	updated, topologyChanged, err := clusterJoinTopology(topology, *nodeID, *nodeAddress, *role)
	if err != nil {
		return err
	}
	if topologyChanged {
		if err := putJSONValueDiscard(ctx, client, *peer, "/api/topology", updated); err != nil {
			return fmt.Errorf("upload peer topology: %w", err)
		}
	}
	targetUpdated := false
	if *updateTarget {
		if err := putJSONValueDiscard(ctx, client, *nodeAddress, "/api/topology", updated); err != nil {
			return fmt.Errorf("upload joining node topology: %w", err)
		}
		targetUpdated = true
	}
	journalPulled := false
	if *pullJournal {
		body, err := jsonwire.Marshal(struct {
			Source       string `json:"source"`
			UntilCurrent bool   `json:"until_current"`
		}{
			Source:       *peer,
			UntilCurrent: true,
		})
		if err != nil {
			return err
		}
		if err := postJSON(ctx, client, *nodeAddress, "/api/journal", body, io.Discard); err != nil {
			return fmt.Errorf("pull joining node journal: %w", err)
		}
		journalPulled = true
	}

	result := clusterJoinResult{
		OK:              true,
		Message:         "cluster join completed",
		Peer:            *peer,
		Node:            *nodeID,
		Address:         *nodeAddress,
		TopologyUpdated: topologyChanged,
		TargetUpdated:   targetUpdated,
		JournalPulled:   journalPulled,
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	if _, err := stdout.Write(append(data, '\n')); err != nil {
		return err
	}
	return nil
}

func clusterJoinTopology(topology hatriecache.ClusterTopology, nodeID string, address string, role string) (hatriecache.ClusterTopology, bool, error) {
	nodeID = strings.TrimSpace(nodeID)
	address = strings.TrimSpace(address)
	role = strings.TrimSpace(role)
	if nodeID == "" {
		return hatriecache.ClusterTopology{}, false, errors.New("cluster join node id is required")
	}
	if address == "" {
		return hatriecache.ClusterTopology{}, false, errors.New("cluster join node address is required")
	}
	if role == "" {
		role = "replica"
	}
	if role != "primary" && role != "replica" {
		return hatriecache.ClusterTopology{}, false, errors.New("cluster join role must be primary or replica")
	}

	changed := false
	found := false
	for idx := range topology.Nodes {
		if strings.TrimSpace(topology.Nodes[idx].ID) != nodeID {
			continue
		}
		found = true
		if topology.Nodes[idx].Address != address {
			topology.Nodes[idx].Address = address
			changed = true
		}
		if topology.Nodes[idx].Role != role {
			topology.Nodes[idx].Role = role
			changed = true
		}
	}
	if !found {
		topology.Nodes = append(topology.Nodes, hatriecache.TopologyNode{ID: nodeID, Address: address, Role: role})
		changed = true
	}
	if topology.Mode == "" || topology.Mode == hatriecache.TopologyModeSharded {
		for idx := range topology.Shards {
			shard := &topology.Shards[idx]
			if shard.Primary == nodeID || stringInSlice(shard.Replicas, nodeID) {
				continue
			}
			shard.Replicas = append(shard.Replicas, nodeID)
			changed = true
		}
	}
	store, err := hatriecache.NewTopologyStore(topology)
	if err != nil {
		return hatriecache.ClusterTopology{}, false, err
	}
	return store.Get(), changed, nil
}

func stringInSlice(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}

func getJSONValue[T any](ctx context.Context, client *http.Client, addr string, path string) (T, error) {
	var out T
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodGet, endpoint(addr, path), nil)
	if err != nil {
		return out, err
	}
	req.Header.Set("Accept", "application/json")
	if err := doAndDecodeJSON(client, req, &out); err != nil {
		return out, err
	}
	return out, nil
}

func putJSONValueDiscard(ctx context.Context, client *http.Client, addr string, path string, value interface{}) error {
	data, err := jsonwire.Marshal(value)
	if err != nil {
		return err
	}
	return putJSONReader(ctx, client, addr, path, bytes.NewReader(data), io.Discard)
}

func doAndDecodeJSON(client *http.Client, req *http.Request, out interface{}) error {
	client = cliHTTPClient(client)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer drainAndCloseResponse(resp.Body)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := readErrorBody(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	decoder := jsonwire.NewDecoder(io.LimitReader(resp.Body, maxErrorBodyBytes))
	decoder.UseNumber()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("invalid trailing JSON")
	}
	return nil
}

func runCommand(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("command", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	command := flags.String("cmd", "", "cache command")
	key := flags.String("key", "", "cache key")
	value := flags.String("value", "", "cache value")
	valuesJSON := flags.String("values", "", "JSON array for commands that accept multiple values")
	batchJSON := flags.String("batch", "", "JSON array of command requests for a public BATCH command")
	subkey := flags.String("subkey", "", "secondary key or command argument")
	pairsJSON := flags.String("pairs", "", "JSON object for map or radix tree fields")
	priority := flags.String("priority", "", "priority for priority queue push")
	ttlSeconds := flags.Int64("ttl-seconds", -1, "optional ttl in seconds")
	unixSeconds := flags.Int64("unix-seconds", -1, "optional absolute expiration as Unix seconds")
	wireFormat := flags.String("wire-format", defaultCommandWireFormat, "command request wire format: auto, protobuf, or json")
	if err := flags.Parse(args); err != nil {
		return err
	}

	request := hatriecache.CacheCommandRequest{
		Command: *command,
		Key:     *key,
		Value:   *value,
		Subkey:  *subkey,
	}
	if *ttlSeconds >= 0 {
		request.TTLSeconds = ttlSeconds
	}
	if *unixSeconds >= 0 {
		request.UnixSeconds = unixSeconds
	}
	if *priority != "" {
		parsed, err := strconv.ParseInt(strings.TrimSpace(*priority), 10, 64)
		if err != nil {
			return fmt.Errorf("priority: %w", err)
		}
		request.Priority = &parsed
	}
	if *valuesJSON != "" {
		values, err := decodeJSONFlag[hatriecache.Slice](*valuesJSON)
		if err != nil {
			return fmt.Errorf("values: %w", err)
		}
		request.Values = values
	}
	if *pairsJSON != "" {
		pairs, err := decodeJSONFlag[hatriecache.Map](*pairsJSON)
		if err != nil {
			return fmt.Errorf("pairs: %w", err)
		}
		request.Pairs = pairs
	}
	if *batchJSON != "" {
		if strings.TrimSpace(*command) != "" && !strings.EqualFold(strings.TrimSpace(*command), "BATCH") {
			return errors.New("batch cannot be combined with non-BATCH -cmd")
		}
		batch, err := decodeJSONFlag[[]hatriecache.CacheCommandRequest](*batchJSON)
		if err != nil {
			return fmt.Errorf("batch: %w", err)
		}
		request.Command = "BATCH"
		request.Batch = batch
	}
	return postCommandValue(ctx, client, addr, request, *wireFormat, stdout)
}

func decodeJSONFlag[T any](value string) (T, error) {
	var out T
	decoder := jsonwire.NewDecoder(strings.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return out, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return out, errors.New("invalid trailing JSON")
	}
	return out, nil
}

func getJSON(ctx context.Context, client *http.Client, addr string, path string, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodGet, endpoint(addr, path), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	return doAndCopy(client, req, stdout)
}

func postJSON(ctx context.Context, client *http.Client, addr string, path string, body []byte, stdout io.Writer) error {
	reader, contentEncoding, err := jsonRequestBody(body)
	if err != nil {
		return err
	}
	return postJSONReaderWithEncoding(ctx, client, addr, path, reader, contentEncoding, stdout)
}

func postJSONValue(ctx context.Context, client *http.Client, addr string, path string, value interface{}, estimatedSize int, stdout io.Writer) error {
	reader, contentEncoding, err := jsonValueRequestBody(value, estimatedSize)
	if err != nil {
		return err
	}
	return postJSONReaderWithEncoding(ctx, client, addr, path, reader, contentEncoding, stdout)
}

func postCommandValue(ctx context.Context, client *http.Client, addr string, request hatriecache.CacheCommandRequest, wireFormat string, stdout io.Writer) error {
	reader, contentType, contentEncoding, err := commandRequestBody(request, wireFormat)
	if err != nil {
		return err
	}
	err = postCommandReader(ctx, client, addr, reader, contentType, contentEncoding, stdout)
	if shouldRetryCommandAsJSON(wireFormat, contentType, err) {
		reader, contentType, contentEncoding, err = hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatJSON, estimatedCommandRequestBytes(request), minCompressedJSONRequestBytes)
		if err != nil {
			return err
		}
		return postCommandReader(ctx, client, addr, reader, contentType, contentEncoding, stdout)
	}
	return err
}

func postCommandReader(ctx context.Context, client *http.Client, addr string, body io.Reader, contentType string, contentEncoding string, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodPost, endpoint(addr, "/api/commands"), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", contentType)
	req.Header.Set("Content-Type", contentType)
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	return doCommandAndCopy(client, req, stdout)
}

func shouldRetryCommandAsJSON(wireFormat string, contentType string, err error) bool {
	if !commandWireFormatAuto(wireFormat) || contentType != "application/x-protobuf" {
		return false
	}
	var httpErr *commandHTTPError
	return errors.As(err, &httpErr) && httpErr.statusCode == http.StatusUnsupportedMediaType
}

func commandWireFormatAuto(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", defaultCommandWireFormat:
		return true
	default:
		return false
	}
}

func postJSONReaderWithEncoding(ctx context.Context, client *http.Client, addr string, path string, body io.Reader, contentEncoding string, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodPost, endpoint(addr, path), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	return doAndCopy(client, req, stdout)
}

func jsonValueRequestBody(value interface{}, estimatedSize int) (io.Reader, string, error) {
	return jsonwire.RequestBody(value, estimatedSize, minCompressedJSONRequestBytes)
}

func jsonRequestBody(data []byte) (io.Reader, string, error) {
	return jsonwire.EncodedRequestBody(data, minCompressedJSONRequestBytes)
}

func commandRequestBody(request hatriecache.CacheCommandRequest, wireFormat string) (io.Reader, string, string, error) {
	estimatedSize := estimatedCommandRequestBytes(request)
	switch strings.ToLower(strings.TrimSpace(wireFormat)) {
	case "", defaultCommandWireFormat:
		body, contentType, contentEncoding, err := hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatProtobuf, estimatedSize, minCompressedJSONRequestBytes)
		if err == nil {
			return body, contentType, contentEncoding, nil
		}
		return hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatJSON, estimatedSize, minCompressedJSONRequestBytes)
	case string(hatriecache.CommandWireFormatJSON):
		return hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatJSON, estimatedSize, minCompressedJSONRequestBytes)
	case string(hatriecache.CommandWireFormatProtobuf), "proto", "pb":
		return hatriecache.CommandRequestBody(request, hatriecache.CommandWireFormatProtobuf, estimatedSize, minCompressedJSONRequestBytes)
	default:
		return nil, "", "", fmt.Errorf("unsupported command wire format %q", wireFormat)
	}
}

func estimatedCommandRequestBytes(request hatriecache.CacheCommandRequest) int {
	estimate := 64 +
		jsonwire.EstimateJSONStringBytes(request.Command) +
		jsonwire.EstimateJSONStringBytes(request.Key) +
		jsonwire.EstimateJSONStringBytes(request.Value) +
		jsonwire.EstimateJSONStringBytes(request.Subkey)
	if estimate >= minCompressedJSONRequestBytes {
		return minCompressedJSONRequestBytes
	}
	for _, value := range request.Values {
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(value, minCompressedJSONRequestBytes), minCompressedJSONRequestBytes)
		if estimate >= minCompressedJSONRequestBytes {
			return minCompressedJSONRequestBytes
		}
	}
	for key, value := range request.Pairs {
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONStringBytes(key)+1, minCompressedJSONRequestBytes)
		if estimate >= minCompressedJSONRequestBytes {
			return minCompressedJSONRequestBytes
		}
		estimate = jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(value, minCompressedJSONRequestBytes), minCompressedJSONRequestBytes)
		if estimate >= minCompressedJSONRequestBytes {
			return minCompressedJSONRequestBytes
		}
	}
	for _, value := range request.Batch {
		estimate = jsonwire.AddEstimate(estimate, estimatedCommandRequestBytes(value)+1, minCompressedJSONRequestBytes)
		if estimate >= minCompressedJSONRequestBytes {
			return minCompressedJSONRequestBytes
		}
	}
	if request.Priority != nil {
		estimate = addEstimatedOptionalCommandInt64(estimate, request.Priority, minCompressedJSONRequestBytes)
	}
	if request.TTLSeconds != nil {
		estimate = addEstimatedOptionalCommandInt64(estimate, request.TTLSeconds, minCompressedJSONRequestBytes)
	}
	if request.UnixSeconds != nil {
		estimate = addEstimatedOptionalCommandInt64(estimate, request.UnixSeconds, minCompressedJSONRequestBytes)
	}
	return estimate
}

func addEstimatedOptionalCommandInt64(estimate int, value *int64, threshold int) int {
	if value == nil {
		return estimate
	}
	return jsonwire.AddEstimate(estimate, jsonwire.EstimateJSONValueBytes(*value, threshold), threshold)
}

func putJSONReader(ctx context.Context, client *http.Client, addr string, path string, body io.Reader, stdout io.Writer) error {
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodPut, endpoint(addr, path), body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	return doAndCopy(client, req, stdout)
}

func doAndCopy(client *http.Client, req *http.Request, stdout io.Writer) error {
	stdout = cliWriter(stdout)
	client = cliHTTPClient(client)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer drainAndCloseResponse(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := readErrorBody(resp.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("server returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	return copyAndEnsureTrailingNewline(stdout, resp.Body)
}

func doCommandAndCopy(client *http.Client, req *http.Request, stdout io.Writer) error {
	stdout = cliWriter(stdout)
	client = cliHTTPClient(client)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer drainAndCloseResponse(resp.Body)

	contentType := resp.Header.Get("Content-Type")
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, err := readErrorBody(resp.Body)
		if err != nil {
			return err
		}
		message := strings.TrimSpace(string(body))
		if decoded, decodeErr := hatriecache.DecodeCommandResponseWire(bytes.NewReader(body), contentType, maxErrorBodyBytes); decodeErr == nil {
			if data, marshalErr := jsonwire.Marshal(decoded); marshalErr == nil {
				message = string(data)
			}
		}
		return &commandHTTPError{status: resp.Status, statusCode: resp.StatusCode, message: message}
	}
	response, err := hatriecache.DecodeCommandResponseWire(resp.Body, contentType, maxErrorBodyBytes)
	if err != nil {
		if errors.Is(err, hatriecache.ErrUnsupportedCommandResponseContentType) {
			return copyAndEnsureTrailingNewline(stdout, resp.Body)
		}
		return err
	}
	data, err := jsonwire.Marshal(response)
	if err != nil {
		return err
	}
	if _, err := stdout.Write(data); err != nil {
		return err
	}
	_, err = stdout.Write([]byte{'\n'})
	return err
}

type commandHTTPError struct {
	status     string
	statusCode int
	message    string
}

func (err *commandHTTPError) Error() string {
	return fmt.Sprintf("server returned %s: %s", err.status, err.message)
}

func endpoint(addr string, path string) string {
	return strings.TrimRight(addr, "/") + path
}

func readErrorBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxErrorBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) <= maxErrorBodyBytes {
		return data, nil
	}
	data = data[:maxErrorBodyBytes]
	data = append(data, truncatedErrorBodySuffix...)
	return data, nil
}

func drainAndCloseResponse(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.CopyN(io.Discard, body, maxResponseDrainBytes)
	_ = body.Close()
}

type trailingNewlineWriter struct {
	writer io.Writer
	wrote  bool
	last   byte
}

func (writer *trailingNewlineWriter) Write(data []byte) (int, error) {
	n, err := writer.writer.Write(data)
	if n > 0 {
		writer.wrote = true
		writer.last = data[n-1]
	}
	return n, err
}

func copyAndEnsureTrailingNewline(stdout io.Writer, body io.Reader) error {
	stdout = cliWriter(stdout)
	writer := &trailingNewlineWriter{writer: stdout}
	if _, err := io.Copy(writer, body); err != nil {
		return err
	}
	if writer.wrote && writer.last != '\n' {
		_, err := stdout.Write([]byte{'\n'})
		return err
	}
	return nil
}
