package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	stdjson "encoding/json"
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
	"sort"
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

type backupAndVerifyResult struct {
	OK             bool                                `json:"ok"`
	Path           string                              `json:"path"`
	Manifest       hatriecache.BackupBundleManifest    `json:"manifest"`
	Verification   hatriecache.BackupDoctorReport      `json:"verification"`
	Rehearsal      *hatriecache.RestoreRehearsalReport `json:"rehearsal,omitempty"`
	CreateMillis   int64                               `json:"create_millis"`
	VerifyMillis   int64                               `json:"verify_millis"`
	RehearseMillis int64                               `json:"rehearse_millis,omitempty"`
	DurationMillis int64                               `json:"duration_millis"`
}

type supportBundleResult struct {
	OK        bool   `json:"ok"`
	Path      string `json:"path"`
	Nodes     int    `json:"nodes"`
	Errors    int    `json:"errors"`
	Bytes     int64  `json:"bytes"`
	SHA256    string `json:"sha256"`
	Generated string `json:"generated_at"`
}

type supportBundleManifest struct {
	Version     int                         `json:"version"`
	GeneratedAt string                      `json:"generated_at"`
	Peer        string                      `json:"peer"`
	Nodes       []supportBundleManifestNode `json:"nodes"`
}

type supportBundleManifestNode struct {
	ID      string                       `json:"id"`
	Address string                       `json:"address"`
	Dir     string                       `json:"dir"`
	Entries []supportBundleManifestEntry `json:"entries,omitempty"`
	Errors  map[string]string            `json:"errors,omitempty"`
}

type supportBundleManifestEntry struct {
	Name   string `json:"name"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
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
		return errors.New("subcommand is required: health, stats, entries, topology, election, replication, journal, storage, command, snapshot, backup, backup-and-verify, restore-bundle, restore-rehearsal, support-bundle, cluster, doctor")
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
	case "backup-and-verify":
		return runBackupAndVerify(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
	case "support-bundle":
		return runSupportBundle(ctx, client, cfg.addr, remaining[1:], stdout, stderr)
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
	checkRedirect := clone.CheckRedirect
	clone.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if len(via) > 0 && !sameHTTPOrigin(request.URL, via[0].URL) {
			request.Header.Del("Authorization")
			request.Header.Del("X-Hatrie-Auth-Token")
			request.Header.Del("X-Hatrie-Replication-Token")
			*request = *request.WithContext(context.WithValue(request.Context(), suppressAutomaticAuthKey{}, true))
		}
		if checkRedirect != nil {
			return checkRedirect(request, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
	return &clone
}

func sameHTTPOrigin(left *url.URL, right *url.URL) bool {
	if left == nil || right == nil || !strings.EqualFold(left.Scheme, right.Scheme) || !strings.EqualFold(left.Hostname(), right.Hostname()) {
		return false
	}
	return effectiveHTTPPort(left) == effectiveHTTPPort(right)
}

func effectiveHTTPPort(value *url.URL) string {
	if port := value.Port(); port != "" {
		return port
	}
	if strings.EqualFold(value.Scheme, "http") {
		return "80"
	}
	if strings.EqualFold(value.Scheme, "https") {
		return "443"
	}
	return ""
}

type authTokenTransport struct {
	token string
	base  http.RoundTripper
}

type suppressAutomaticAuthKey struct{}

func (transport authTokenTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	suppressAuth, _ := request.Context().Value(suppressAutomaticAuthKey{}).(bool)
	if !suppressAuth && request.Header.Get("Authorization") == "" {
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
	wireFormat := flags.String("wire-format", string(hatriecache.DefaultCommandJournalWireFormat), "journal pull wire format: binary or json")
	if err := flags.Parse(args); err != nil {
		return err
	}
	parsedWireFormat, err := hatriecache.ParseCommandJournalWireFormat(*wireFormat)
	if err != nil {
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
			WireFormat    string `json:"wire_format,omitempty"`
		}{
			Source:        strings.TrimSpace(*pullFrom),
			AfterSequence: *afterSequence,
			Limit:         *limit,
			UntilCurrent:  *untilCurrent,
			MaxBatches:    *maxBatches,
			WireFormat:    string(parsedWireFormat),
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
	mode := flags.String("mode", "auto", "backup mode: auto, snapshot, pebble-checkpoint, or pebble-incremental")
	retain := flags.Int("retain", 0, "incremental repository manifests to retain; default 32")
	snapshotFormat := flags.String("snapshot-format", "", "optional snapshot format override for the backup bundle")
	partitionMode := flags.String("partition-mode", "", "optional backup partition mode metadata")
	partitions := flags.String("partitions", "", "comma-separated partition ids covered by the backup")
	partitionNode := flags.String("partition-node", "", "optional node id that produced the partition backup")
	partitionEpoch := flags.Uint64("partition-epoch", 0, "optional topology epoch for partition metadata")
	partitionFingerprint := flags.String("partition-fingerprint", "", "optional topology fingerprint for partition metadata")
	partitionPrefixes := flags.String("partition-prefixes", "", "comma-separated key prefixes covered by the partition backup")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*path) == "" {
		return errors.New("backup -path is required")
	}
	partition := backupPartitionMetadataFromFlags(*partitionMode, *partitions, *partitionNode, *partitionEpoch, *partitionFingerprint, *partitionPrefixes)
	body, err := jsonwire.Marshal(struct {
		Path           string                               `json:"path"`
		Mode           string                               `json:"mode,omitempty"`
		Retain         int                                  `json:"retain,omitempty"`
		SnapshotFormat string                               `json:"snapshot_format,omitempty"`
		Partition      *hatriecache.BackupPartitionMetadata `json:"partition,omitempty"`
	}{
		Path:           strings.TrimSpace(*path),
		Mode:           strings.TrimSpace(*mode),
		Retain:         *retain,
		SnapshotFormat: strings.TrimSpace(*snapshotFormat),
		Partition:      partition,
	})
	if err != nil {
		return err
	}
	return postJSON(ctx, client, addr, "/api/backup", body, stdout)
}

func runBackupAndVerify(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("backup-and-verify", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	path := flags.String("path", "", "server-side backup bundle output path")
	mode := flags.String("mode", "auto", "backup mode: auto, snapshot, pebble-checkpoint, or pebble-incremental")
	retain := flags.Int("retain", 0, "incremental repository manifests to retain; default 32")
	snapshotFormat := flags.String("snapshot-format", "", "optional snapshot format override for the backup bundle")
	rehearse := flags.Bool("rehearse", true, "restore the verified backup into an isolated temporary directory")
	partitionMode := flags.String("partition-mode", "", "optional backup partition mode metadata")
	partitions := flags.String("partitions", "", "comma-separated partition ids covered by the backup")
	partitionNode := flags.String("partition-node", "", "optional node id that produced the partition backup")
	partitionEpoch := flags.Uint64("partition-epoch", 0, "optional topology epoch for partition metadata")
	partitionFingerprint := flags.String("partition-fingerprint", "", "optional topology fingerprint for partition metadata")
	partitionPrefixes := flags.String("partition-prefixes", "", "comma-separated key prefixes covered by the partition backup")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*path = strings.TrimSpace(*path)
	if *path == "" {
		return errors.New("backup-and-verify -path is required")
	}
	partition := backupPartitionMetadataFromFlags(*partitionMode, *partitions, *partitionNode, *partitionEpoch, *partitionFingerprint, *partitionPrefixes)
	backupRequest := struct {
		Path           string                               `json:"path"`
		Mode           string                               `json:"mode,omitempty"`
		Retain         int                                  `json:"retain,omitempty"`
		SnapshotFormat string                               `json:"snapshot_format,omitempty"`
		Partition      *hatriecache.BackupPartitionMetadata `json:"partition,omitempty"`
	}{
		Path:           *path,
		Mode:           strings.TrimSpace(*mode),
		Retain:         *retain,
		SnapshotFormat: strings.TrimSpace(*snapshotFormat),
		Partition:      partition,
	}
	startedAt := time.Now()
	stageStarted := time.Now()
	manifest, err := postJSONAndDecode[hatriecache.BackupBundleManifest](ctx, client, addr, "/api/backup", backupRequest)
	if err != nil {
		return fmt.Errorf("create backup: %w", err)
	}
	createMillis := time.Since(stageStarted).Milliseconds()
	stageStarted = time.Now()
	verification, err := postJSONAndDecode[hatriecache.BackupDoctorReport](ctx, client, addr, "/api/backup/verify", backupPathRequestCLI{Path: *path})
	if err != nil {
		return fmt.Errorf("verify backup: %w", err)
	}
	if !verification.OK {
		return errors.New("verify backup returned a non-ok report")
	}
	verifyMillis := time.Since(stageStarted).Milliseconds()
	var rehearsal *hatriecache.RestoreRehearsalReport
	var rehearseMillis int64
	if *rehearse {
		stageStarted = time.Now()
		rehearsed, err := postJSONAndDecode[hatriecache.RestoreRehearsalReport](ctx, client, addr, "/api/backup/rehearse", backupPathRequestCLI{Path: *path})
		if err != nil {
			return fmt.Errorf("rehearse backup restore: %w", err)
		}
		if !rehearsed.OK {
			return errors.New("rehearse backup restore returned a non-ok report")
		}
		rehearseMillis = time.Since(stageStarted).Milliseconds()
		rehearsal = &rehearsed
	}
	result := backupAndVerifyResult{
		OK:             true,
		Path:           *path,
		Manifest:       manifest,
		Verification:   verification,
		Rehearsal:      rehearsal,
		CreateMillis:   createMillis,
		VerifyMillis:   verifyMillis,
		RehearseMillis: rehearseMillis,
		DurationMillis: time.Since(startedAt).Milliseconds(),
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

type backupPathRequestCLI struct {
	Path string `json:"path"`
}

const maxSupportBundleEndpointBytes = 4 << 20

func runSupportBundle(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) (returnErr error) {
	flags := flag.NewFlagSet("support-bundle", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	peer := flags.String("peer", addr, "cluster node monitoring API base URL")
	outputPath := flags.String("path", "", "output .tar.gz path")
	auditLimit := flags.Int("audit-limit", 128, "recent audit events per node")
	includeMetrics := flags.Bool("metrics", true, "include Prometheus metrics from every node")
	overwrite := flags.Bool("overwrite", false, "replace an existing regular output file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*peer = strings.TrimSpace(*peer)
	*outputPath = strings.TrimSpace(*outputPath)
	if *peer == "" {
		return errors.New("support-bundle -peer is required")
	}
	if *outputPath == "" {
		return errors.New("support-bundle -path is required")
	}
	if *auditLimit < 0 || *auditLimit > 128 {
		return errors.New("support-bundle -audit-limit must be between 0 and 128")
	}
	if info, err := os.Lstat(*outputPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return errors.New("support-bundle output path must be a regular file")
		}
		if !*overwrite {
			return errors.New("support-bundle output already exists; pass -overwrite to replace it")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("cluster topology: %w", err)
	}
	store, err := hatriecache.NewTopologyStore(topology)
	if err != nil {
		return fmt.Errorf("cluster topology: %w", err)
	}
	topology = store.Get()
	peerNode := clusterPeerNodeID(topology, *peer)
	parent := filepath.Dir(*outputPath)
	if err := os.MkdirAll(parent, 0o700); err != nil {
		return err
	}
	temp, err := os.CreateTemp(parent, ".hatrie-support-*.tar.gz")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	defer func() {
		if returnErr != nil {
			temp.Close()
			os.Remove(tempPath)
		}
	}()
	if err := temp.Chmod(0o600); err != nil {
		return err
	}
	bundleHash := sha256.New()
	gzipWriter := gzip.NewWriter(io.MultiWriter(temp, bundleHash))
	tarWriter := tar.NewWriter(gzipWriter)
	generatedAt := time.Now().UTC()
	manifest := supportBundleManifest{
		Version:     1,
		GeneratedAt: generatedAt.Format(time.RFC3339Nano),
		Peer:        *peer,
		Nodes:       make([]supportBundleManifestNode, 0, len(topology.Nodes)),
	}
	errorCount := 0
	jsonEndpoints := []struct {
		Path string
		File string
	}{
		{Path: "/api/health", File: "health.json"},
		{Path: "/api/config", File: "config.json"},
		{Path: "/api/topology", File: "topology.json"},
		{Path: "/api/election", File: "election.json"},
		{Path: "/api/replication", File: "replication.json"},
		{Path: "/api/storage", File: "storage.json"},
		{Path: "/api/audit?limit=" + strconv.Itoa(*auditLimit), File: "audit.json"},
	}
	for idx, node := range topology.Nodes {
		address := strings.TrimSpace(node.Address)
		if node.ID == peerNode {
			address = *peer
		}
		dir := fmt.Sprintf("nodes/%03d-%s", idx+1, supportBundleSlug(node.ID))
		nodeManifest := supportBundleManifestNode{ID: node.ID, Address: address, Dir: dir, Errors: map[string]string{}}
		for _, endpoint := range jsonEndpoints {
			value, fetchErr := getJSONValue[interface{}](ctx, client, address, endpoint.Path)
			if fetchErr != nil {
				nodeManifest.Errors[endpoint.File] = fetchErr.Error()
				errorCount++
				continue
			}
			data, marshalErr := stdjson.MarshalIndent(redactSupportValue(value), "", "  ")
			if marshalErr != nil {
				nodeManifest.Errors[endpoint.File] = marshalErr.Error()
				errorCount++
				continue
			}
			data = append(data, '\n')
			entry, writeErr := writeSupportBundleEntry(tarWriter, dir+"/"+endpoint.File, data, generatedAt)
			if writeErr != nil {
				return writeErr
			}
			nodeManifest.Entries = append(nodeManifest.Entries, entry)
		}
		if *includeMetrics {
			data, fetchErr := getLimitedHTTPBody(ctx, client, address, "/metrics", maxSupportBundleEndpointBytes)
			if fetchErr != nil {
				nodeManifest.Errors["metrics.txt"] = fetchErr.Error()
				errorCount++
			} else {
				entry, writeErr := writeSupportBundleEntry(tarWriter, dir+"/metrics.txt", data, generatedAt)
				if writeErr != nil {
					return writeErr
				}
				nodeManifest.Entries = append(nodeManifest.Entries, entry)
			}
		}
		if len(nodeManifest.Errors) == 0 {
			nodeManifest.Errors = nil
		}
		manifest.Nodes = append(manifest.Nodes, nodeManifest)
	}
	manifestData, err := stdjson.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	manifestData = append(manifestData, '\n')
	if _, err := writeSupportBundleEntry(tarWriter, "manifest.json", manifestData, generatedAt); err != nil {
		return err
	}
	if err := tarWriter.Close(); err != nil {
		return err
	}
	if err := gzipWriter.Close(); err != nil {
		return err
	}
	if err := temp.Sync(); err != nil {
		return err
	}
	if err := temp.Close(); err != nil {
		return err
	}
	if *overwrite {
		if info, err := os.Lstat(*outputPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return errors.New("support-bundle refuses to replace a symlink")
		}
	}
	if err := os.Rename(tempPath, *outputPath); err != nil {
		return err
	}
	if err := os.Chmod(*outputPath, 0o600); err != nil {
		return err
	}
	info, err := os.Stat(*outputPath)
	if err != nil {
		return err
	}
	result := supportBundleResult{
		OK:        errorCount == 0,
		Path:      *outputPath,
		Nodes:     len(topology.Nodes),
		Errors:    errorCount,
		Bytes:     info.Size(),
		SHA256:    hex.EncodeToString(bundleHash.Sum(nil)),
		Generated: manifest.GeneratedAt,
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func supportBundleSlug(value string) string {
	value = strings.TrimSpace(value)
	var builder strings.Builder
	for _, char := range value {
		switch {
		case char >= 'a' && char <= 'z', char >= 'A' && char <= 'Z', char >= '0' && char <= '9', char == '-', char == '_', char == '.':
			builder.WriteRune(char)
		default:
			builder.WriteByte('-')
		}
	}
	if builder.Len() == 0 {
		return "node"
	}
	return builder.String()
}

func redactSupportValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, nested := range typed {
			if supportBundleSecretField(key) {
				out[key] = "[REDACTED]"
			} else {
				out[key] = redactSupportValue(nested)
			}
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for idx := range typed {
			out[idx] = redactSupportValue(typed[idx])
		}
		return out
	default:
		return value
	}
}

func supportBundleSecretField(field string) bool {
	field = strings.ToLower(strings.TrimSpace(field))
	for _, marker := range []string{"authorization", "credential", "password", "private_key", "secret", "token"} {
		if strings.Contains(field, marker) {
			return true
		}
	}
	return false
}

func writeSupportBundleEntry(writer *tar.Writer, name string, data []byte, modified time.Time) (supportBundleManifestEntry, error) {
	header := &tar.Header{Name: name, Mode: 0o600, Size: int64(len(data)), ModTime: modified}
	if err := writer.WriteHeader(header); err != nil {
		return supportBundleManifestEntry{}, err
	}
	if _, err := writer.Write(data); err != nil {
		return supportBundleManifestEntry{}, err
	}
	digest := sha256.Sum256(data)
	return supportBundleManifestEntry{Name: name, Bytes: int64(len(data)), SHA256: hex.EncodeToString(digest[:])}, nil
}

func getLimitedHTTPBody(ctx context.Context, client *http.Client, addr string, path string, limit int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodGet, endpoint(addr, path), nil)
	if err != nil {
		return nil, err
	}
	client = cliHTTPClient(client)
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer drainAndCloseResponse(response.Body)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, readErr := readErrorBody(response.Body)
		if readErr != nil {
			return nil, readErr
		}
		return nil, &commandHTTPError{status: response.Status, statusCode: response.StatusCode, message: strings.TrimSpace(string(body))}
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("response exceeds %d bytes", limit)
	}
	return data, nil
}

func backupPartitionMetadataFromFlags(mode string, partitions string, node string, epoch uint64, fingerprint string, prefixes string) *hatriecache.BackupPartitionMetadata {
	metadata := hatriecache.BackupPartitionMetadata{
		Mode:                strings.TrimSpace(mode),
		Partitions:          splitCLICommaList(partitions),
		NodeID:              strings.TrimSpace(node),
		TopologyEpoch:       epoch,
		TopologyFingerprint: strings.TrimSpace(fingerprint),
		KeyPrefixes:         splitCLICommaList(prefixes),
	}
	if metadata.Mode == "" && len(metadata.Partitions) == 0 && metadata.NodeID == "" && metadata.TopologyEpoch == 0 && metadata.TopologyFingerprint == "" && len(metadata.KeyPrefixes) == 0 {
		return nil
	}
	return &metadata
}

func splitCLICommaList(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
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
	OK              bool     `json:"ok"`
	Message         string   `json:"message"`
	Peer            string   `json:"peer"`
	Node            string   `json:"node"`
	Address         string   `json:"address"`
	TopologyUpdated bool     `json:"topology_updated"`
	TargetUpdated   bool     `json:"target_updated"`
	JournalPulled   bool     `json:"journal_pulled"`
	NodesUpdated    []string `json:"nodes_updated,omitempty"`
}

type clusterAddReplicaResult struct {
	OK               bool     `json:"ok"`
	Message          string   `json:"message"`
	Peer             string   `json:"peer"`
	Node             string   `json:"node"`
	Address          string   `json:"address"`
	Replaced         bool     `json:"replaced"`
	TopologyUpdated  bool     `json:"topology_updated"`
	JournalPulled    bool     `json:"journal_pulled"`
	FinalSync        bool     `json:"final_sync"`
	TopologyVerified bool     `json:"topology_verified"`
	NodesUpdated     []string `json:"nodes_updated,omitempty"`
}

type clusterRemoveResult struct {
	OK              bool     `json:"ok"`
	Message         string   `json:"message"`
	Peer            string   `json:"peer"`
	Node            string   `json:"node"`
	TopologyUpdated bool     `json:"topology_updated"`
	NodesUpdated    []string `json:"nodes_updated,omitempty"`
}

type clusterDecommissionResult struct {
	OK               bool     `json:"ok"`
	Message          string   `json:"message"`
	Peer             string   `json:"peer"`
	Node             string   `json:"node"`
	RemovedAddress   string   `json:"removed_address,omitempty"`
	AlreadyRemoved   bool     `json:"already_removed"`
	FinalSync        bool     `json:"final_sync"`
	MarkedOffline    bool     `json:"marked_offline"`
	TopologyUpdated  bool     `json:"topology_updated"`
	TopologyVerified bool     `json:"topology_verified"`
	NodesUpdated     []string `json:"nodes_updated,omitempty"`
	Cleanup          string   `json:"cleanup"`
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
	TopologyRepair   *clusterTopologyRepairResult   `json:"topology_repair,omitempty"`
}

type clusterTopologyRepairResult struct {
	Applied      bool     `json:"applied"`
	Authority    string   `json:"authority"`
	NodesUpdated []string `json:"nodes_updated,omitempty"`
}

type clusterConfigDiffResult struct {
	OK            bool                    `json:"ok"`
	Peer          string                  `json:"peer"`
	ReferenceNode string                  `json:"reference_node"`
	IgnoredFields []string                `json:"ignored_fields,omitempty"`
	Nodes         []clusterConfigNodeDiff `json:"nodes"`
}

type clusterConfigNodeDiff struct {
	ID          string                    `json:"id"`
	Address     string                    `json:"address,omitempty"`
	OK          bool                      `json:"ok"`
	Differences []clusterConfigDifference `json:"differences,omitempty"`
	Error       string                    `json:"error,omitempty"`
}

type clusterConfigDifference struct {
	Field     string      `json:"field"`
	Reference interface{} `json:"reference,omitempty"`
	Node      interface{} `json:"node,omitempty"`
}

type clusterMaintenanceResult struct {
	OK               bool                         `json:"ok"`
	Message          string                       `json:"message"`
	Peer             string                       `json:"peer"`
	Node             string                       `json:"node"`
	Maintenance      bool                         `json:"maintenance"`
	Reason           string                       `json:"reason,omitempty"`
	Since            string                       `json:"since,omitempty"`
	TopologyUpdated  bool                         `json:"topology_updated"`
	TopologyVerified bool                         `json:"topology_verified"`
	ElectionVerified bool                         `json:"election_verified"`
	NodesUpdated     []string                     `json:"nodes_updated,omitempty"`
	LeaderBefore     []hatriecache.ElectionLeader `json:"leaders_before,omitempty"`
	LeaderAfter      []hatriecache.ElectionLeader `json:"leaders_after,omitempty"`
}

type clusterUpgradePlanResult struct {
	OK             bool                 `json:"ok"`
	Ready          bool                 `json:"ready"`
	Compatible     bool                 `json:"compatible"`
	Peer           string               `json:"peer"`
	TargetVersion  string               `json:"target_version,omitempty"`
	GeneratedAt    string               `json:"generated_at"`
	MaxBackupAge   string               `json:"max_backup_age"`
	RequireBackup  bool                 `json:"require_backup"`
	Nodes          []clusterUpgradeNode `json:"nodes"`
	BaselineCanary *clusterStatusResult `json:"baseline_canary,omitempty"`
}

type clusterUpgradeNode struct {
	Order             int                       `json:"order"`
	ID                string                    `json:"id"`
	Address           string                    `json:"address"`
	Role              string                    `json:"role,omitempty"`
	Primary           bool                      `json:"primary"`
	CurrentVersion    string                    `json:"current_version"`
	APIVersion        int                       `json:"api_version"`
	StorageConfigured bool                      `json:"storage_configured"`
	BackupAt          string                    `json:"backup_at,omitempty"`
	BackupAgeMillis   int64                     `json:"backup_age_millis,omitempty"`
	Ready             bool                      `json:"ready"`
	Blockers          []string                  `json:"blockers,omitempty"`
	ConfigDifferences []clusterConfigDifference `json:"config_differences,omitempty"`
	Commands          []string                  `json:"commands"`
	CanaryChecks      []string                  `json:"canary_checks"`
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
		return errors.New("cluster subcommand is required: add-replica, join, decommission, remove, status, doctor, config-diff, maintenance, upgrade-plan")
	}
	switch args[0] {
	case "add-replica":
		return runClusterAddReplica(ctx, client, addr, args[1:], stdout, stderr)
	case "join":
		return runClusterJoin(ctx, client, addr, args[1:], stdout, stderr)
	case "remove":
		return runClusterRemove(ctx, client, addr, args[1:], stdout, stderr)
	case "decommission":
		return runClusterDecommission(ctx, client, addr, args[1:], stdout, stderr)
	case "status":
		return runClusterStatus(ctx, client, addr, args[1:], false, stdout, stderr)
	case "doctor":
		return runClusterStatus(ctx, client, addr, args[1:], true, stdout, stderr)
	case "config-diff":
		return runClusterConfigDiff(ctx, client, addr, args[1:], stdout, stderr)
	case "maintenance":
		return runClusterMaintenance(ctx, client, addr, args[1:], stdout, stderr)
	case "upgrade-plan":
		return runClusterUpgradePlan(ctx, client, addr, args[1:], stdout, stderr)
	default:
		return fmt.Errorf("unknown cluster subcommand %q", args[0])
	}
}

func runClusterStatus(ctx context.Context, client *http.Client, addr string, args []string, doctor bool, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster status", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	peer := flags.String("peer", addr, "cluster node monitoring API base URL")
	probeNodes := flags.Bool("probe-nodes", true, "probe health for topology node addresses")
	repairTopology := flags.Bool("repair-topology", false, "replace every member topology with the selected peer topology")
	confirm := flags.Bool("yes", false, "confirm topology repair writes")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*peer = strings.TrimSpace(*peer)
	if *peer == "" {
		return errors.New("cluster status -peer is required")
	}
	if *repairTopology && !doctor {
		return errors.New("cluster status does not allow -repair-topology; use cluster doctor")
	}
	if *repairTopology && !*probeNodes {
		return errors.New("cluster doctor -repair-topology requires -probe-nodes=true")
	}
	if *repairTopology && !*confirm {
		return errors.New("cluster doctor -repair-topology requires explicit -yes confirmation")
	}

	result, err := collectClusterStatus(ctx, client, *peer, *probeNodes)
	if err != nil {
		return err
	}
	if *repairTopology {
		if result.Topology == nil {
			return errors.New("cluster doctor topology is unavailable")
		}
		peerNode := clusterPeerNodeID(*result.Topology, *peer)
		nodesUpdated, err := putTopologyToNodes(ctx, client, *result.Topology, *peer, peerNode, nil)
		if err != nil {
			return fmt.Errorf("repair cluster topology: %w", err)
		}
		result, err = collectClusterStatus(ctx, client, *peer, true)
		if err != nil {
			return fmt.Errorf("verify repaired cluster topology: %w", err)
		}
		result.TopologyRepair = &clusterTopologyRepairResult{
			Applied:      true,
			Authority:    *peer,
			NodesUpdated: nodesUpdated,
		}
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func collectClusterStatus(ctx context.Context, client *http.Client, peer string, probeNodes bool) (clusterStatusResult, error) {
	result := clusterStatusResult{OK: true, Peer: peer}
	health, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, peer, "/api/health")
	if err != nil {
		return clusterStatusResult{}, fmt.Errorf("cluster health: %w", err)
	}
	result.Health = &health
	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, peer, "/api/topology")
	if err != nil {
		return clusterStatusResult{}, fmt.Errorf("cluster topology: %w", err)
	}
	result.Topology = &topology
	election, err := getJSONValue[hatriecache.ElectionStatus](ctx, client, peer, "/api/election")
	if err != nil {
		return clusterStatusResult{}, fmt.Errorf("cluster election: %w", err)
	}
	result.Election = &election
	replication, err := getJSONValue[hatriecache.ReplicationResult](ctx, client, peer, "/api/replication")
	if err != nil {
		result.ReplicationError = err.Error()
	} else {
		result.Replication = &replication
	}
	if probeNodes {
		result.Nodes = probeClusterNodes(ctx, client, topology, election)
		for _, node := range result.Nodes {
			if !node.OK {
				result.OK = false
				result.Errors = append(result.Errors, clusterNodeErrors(node)...)
			}
		}
	}
	return result, nil
}

var clusterNodeLocalConfigFields = []string{
	"audit_log_path",
	"config_path",
	"db_path",
	"grpc_addr",
	"grpc_client_ca",
	"grpc_tls_cert",
	"grpc_tls_key",
	"journal_path",
	"journal_pull_state_path",
	"monitoring_addr",
	"monitoring_tls_cert",
	"monitoring_tls_key",
	"monitoring_web_dir",
	"node_id",
	"replication_outbox_path",
	"snapshot_path",
	"topology_path",
}

func runClusterConfigDiff(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster config-diff", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	peer := flags.String("peer", addr, "authoritative cluster node monitoring API base URL")
	includeNodeLocal := flags.Bool("include-node-local", false, "compare node-local paths, listener addresses, and node ids")
	ignore := flags.String("ignore", "", "additional comma-separated config fields to ignore")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*peer = strings.TrimSpace(*peer)
	if *peer == "" {
		return errors.New("cluster config-diff -peer is required")
	}
	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("cluster topology: %w", err)
	}
	store, err := hatriecache.NewTopologyStore(topology)
	if err != nil {
		return fmt.Errorf("cluster topology: %w", err)
	}
	topology = store.Get()
	peerNode := clusterPeerNodeID(topology, *peer)
	reference, err := getJSONValue[map[string]interface{}](ctx, client, *peer, "/api/config")
	if err != nil {
		return fmt.Errorf("reference config: %w", err)
	}
	ignored := splitCLICommaList(*ignore)
	if !*includeNodeLocal {
		ignored = append(ignored, clusterNodeLocalConfigFields...)
	}
	ignored = uniqueSortedStrings(ignored)
	ignoredSet := make(map[string]bool, len(ignored))
	for _, field := range ignored {
		ignoredSet[field] = true
	}

	result := clusterConfigDiffResult{
		OK:            true,
		Peer:          *peer,
		ReferenceNode: peerNode,
		IgnoredFields: ignored,
		Nodes:         make([]clusterConfigNodeDiff, 0, len(topology.Nodes)),
	}
	for _, node := range topology.Nodes {
		address := strings.TrimSpace(node.Address)
		var config map[string]interface{}
		var nodeErr error
		if node.ID == peerNode {
			address = *peer
			config = reference
		} else {
			config, nodeErr = getJSONValue[map[string]interface{}](ctx, client, address, "/api/config")
		}
		nodeResult := clusterConfigNodeDiff{ID: node.ID, Address: address, OK: true}
		if nodeErr != nil {
			nodeResult.OK = false
			nodeResult.Error = nodeErr.Error()
			result.OK = false
		} else {
			nodeResult.Differences = diffClusterConfigs(reference, config, ignoredSet)
			if len(nodeResult.Differences) > 0 {
				nodeResult.OK = false
				result.OK = false
			}
		}
		result.Nodes = append(result.Nodes, nodeResult)
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func diffClusterConfigs(reference map[string]interface{}, node map[string]interface{}, ignored map[string]bool) []clusterConfigDifference {
	fields := make(map[string]bool, len(reference)+len(node))
	for field := range reference {
		fields[field] = true
	}
	for field := range node {
		fields[field] = true
	}
	names := make([]string, 0, len(fields))
	for field := range fields {
		if ignored == nil || !ignored[field] {
			names = append(names, field)
		}
	}
	sort.Strings(names)
	differences := make([]clusterConfigDifference, 0)
	for _, field := range names {
		if reflect.DeepEqual(reference[field], node[field]) {
			continue
		}
		differences = append(differences, clusterConfigDifference{
			Field:     field,
			Reference: reference[field],
			Node:      node[field],
		})
	}
	return differences
}

func uniqueSortedStrings(values []string) []string {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			set[value] = true
		}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func runClusterMaintenance(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	if len(args) == 0 || (args[0] != "begin" && args[0] != "end") {
		return errors.New("cluster maintenance subcommand is required: begin or end")
	}
	operation := args[0]
	flags := flag.NewFlagSet("cluster maintenance "+operation, flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	peer := flags.String("peer", addr, "existing cluster node monitoring API base URL")
	nodeID := flags.String("node", "", "node id entering or leaving maintenance")
	reason := flags.String("reason", "", "operator reason recorded with maintenance state")
	if err := flags.Parse(args[1:]); err != nil {
		return err
	}
	*peer = strings.TrimSpace(*peer)
	*nodeID = strings.TrimSpace(*nodeID)
	*reason = strings.TrimSpace(*reason)
	if *peer == "" {
		return errors.New("cluster maintenance -peer is required")
	}
	if *nodeID == "" {
		return errors.New("cluster maintenance -node is required")
	}
	enable := operation == "begin"

	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("cluster topology: %w", err)
	}
	electionBefore, err := getJSONValue[hatriecache.ElectionStatus](ctx, client, *peer, "/api/election")
	if err != nil {
		return fmt.Errorf("cluster election: %w", err)
	}
	if enable {
		if err := validateClusterMaintenanceBegin(topology, electionBefore, *nodeID); err != nil {
			return err
		}
	}
	updated, topologyChanged, err := clusterMaintenanceTopology(topology, *nodeID, enable, *reason, time.Now().UTC())
	if err != nil {
		return err
	}
	peerNode := clusterPeerNodeID(topology, *peer)
	nodesUpdated, err := putTopologyToNodes(ctx, client, updated, *peer, peerNode, nil)
	if err != nil {
		return fmt.Errorf("upload maintenance topology: %w", err)
	}
	if err := verifyTopologyOnNodes(ctx, client, updated, *peer, peerNode); err != nil {
		return fmt.Errorf("verify maintenance topology: %w", err)
	}
	electionAfter, err := verifyMaintenanceElection(ctx, client, updated, *peer, peerNode, *nodeID, enable)
	if err != nil {
		return fmt.Errorf("verify maintenance election: %w", err)
	}

	var maintained hatriecache.TopologyNode
	for _, node := range updated.Nodes {
		if node.ID == *nodeID {
			maintained = node
			break
		}
	}
	result := clusterMaintenanceResult{
		OK:               true,
		Message:          "cluster maintenance state updated and verified",
		Peer:             *peer,
		Node:             *nodeID,
		Maintenance:      enable,
		Reason:           maintained.MaintenanceReason,
		Since:            maintained.MaintenanceSince,
		TopologyUpdated:  topologyChanged,
		TopologyVerified: true,
		ElectionVerified: true,
		NodesUpdated:     nodesUpdated,
		LeaderBefore:     electionBefore.Leaders,
		LeaderAfter:      electionAfter.Leaders,
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func clusterMaintenanceTopology(topology hatriecache.ClusterTopology, nodeID string, enable bool, reason string, now time.Time) (hatriecache.ClusterTopology, bool, error) {
	store, err := hatriecache.NewTopologyStore(topology)
	if err != nil {
		return hatriecache.ClusterTopology{}, false, err
	}
	updated := store.Get()
	nodeID = strings.TrimSpace(nodeID)
	reason = strings.TrimSpace(reason)
	for idx := range updated.Nodes {
		node := &updated.Nodes[idx]
		if node.ID != nodeID {
			continue
		}
		if enable {
			if node.Maintenance && node.MaintenanceReason == reason {
				return updated, false, nil
			}
			wasMaintenance := node.Maintenance
			node.Maintenance = true
			node.MaintenanceReason = reason
			if !wasMaintenance || node.MaintenanceSince == "" {
				node.MaintenanceSince = now.UTC().Format(time.RFC3339)
			}
		} else {
			if !node.Maintenance && node.MaintenanceReason == "" && node.MaintenanceSince == "" {
				return updated, false, nil
			}
			node.Maintenance = false
			node.MaintenanceReason = ""
			node.MaintenanceSince = ""
		}
		validated, err := hatriecache.NewTopologyStore(updated)
		if err != nil {
			return hatriecache.ClusterTopology{}, false, err
		}
		return validated.Get(), true, nil
	}
	return hatriecache.ClusterTopology{}, false, fmt.Errorf("cluster maintenance node %q is not registered", nodeID)
}

func validateClusterMaintenanceBegin(topology hatriecache.ClusterTopology, election hatriecache.ElectionStatus, nodeID string) error {
	registered := false
	for _, node := range topology.Nodes {
		if node.ID == nodeID {
			registered = true
			break
		}
	}
	if !registered {
		return fmt.Errorf("cluster maintenance node %q is not registered", nodeID)
	}
	online := make(map[string]bool, len(election.Nodes))
	for _, node := range election.Nodes {
		online[node.ID] = node.Online
	}
	for _, leader := range election.Leaders {
		if leader.Leader != nodeID {
			continue
		}
		alternative := ""
		for _, candidate := range leader.Candidates {
			if candidate != nodeID && online[candidate] {
				alternative = candidate
				break
			}
		}
		if alternative == "" {
			return fmt.Errorf("shard %d has no online failover candidate for maintenance node %q", leader.Shard, nodeID)
		}
	}
	return nil
}

func verifyMaintenanceElection(ctx context.Context, client *http.Client, topology hatriecache.ClusterTopology, peer string, peerNode string, nodeID string, maintenance bool) (hatriecache.ElectionStatus, error) {
	var reference hatriecache.ElectionStatus
	for idx, node := range topology.Nodes {
		address := strings.TrimSpace(node.Address)
		if node.ID == peerNode {
			address = strings.TrimSpace(peer)
		}
		status, err := getJSONValue[hatriecache.ElectionStatus](ctx, client, address, "/api/election")
		if err != nil {
			return hatriecache.ElectionStatus{}, fmt.Errorf("node %q: %w", node.ID, err)
		}
		var target *hatriecache.ElectionNodeStatus
		for statusIdx := range status.Nodes {
			if status.Nodes[statusIdx].ID == nodeID {
				target = &status.Nodes[statusIdx]
				break
			}
		}
		if target == nil {
			return hatriecache.ElectionStatus{}, fmt.Errorf("node %q election omits maintenance node %q", node.ID, nodeID)
		}
		if maintenance && (target.Online || target.Reason != "maintenance") {
			return hatriecache.ElectionStatus{}, fmt.Errorf("node %q reports maintenance node online or with reason %q", node.ID, target.Reason)
		}
		if !maintenance && !target.Online {
			return hatriecache.ElectionStatus{}, fmt.Errorf("node %q still reports restored node offline with reason %q", node.ID, target.Reason)
		}
		for _, leader := range status.Leaders {
			if maintenance && leader.Leader == nodeID {
				return hatriecache.ElectionStatus{}, fmt.Errorf("node %q still elects maintenance node for shard %d", node.ID, leader.Shard)
			}
		}
		if idx == 0 {
			reference = status
		} else if !clusterElectionLeadersConsistent(reference, status) {
			return hatriecache.ElectionStatus{}, fmt.Errorf("node %q elected leaders differ after maintenance update", node.ID)
		}
	}
	return reference, nil
}

func runClusterUpgradePlan(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster upgrade-plan", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	peer := flags.String("peer", addr, "reference cluster node monitoring API base URL")
	targetVersion := flags.String("target-version", "", "version intended for the rollout")
	maxBackupAge := flags.Duration("max-backup-age", 24*time.Hour, "maximum age of a successful backup audit event")
	requireBackup := flags.Bool("require-backup", true, "require a recent successful backup on every node")
	runCanary := flags.Bool("run-canary", false, "run the current cluster status/doctor probes as a baseline canary")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*peer = strings.TrimSpace(*peer)
	*targetVersion = strings.TrimSpace(*targetVersion)
	if *peer == "" {
		return errors.New("cluster upgrade-plan -peer is required")
	}
	if *maxBackupAge < 0 {
		return errors.New("cluster upgrade-plan -max-backup-age must be non-negative")
	}
	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("cluster topology: %w", err)
	}
	store, err := hatriecache.NewTopologyStore(topology)
	if err != nil {
		return fmt.Errorf("cluster topology: %w", err)
	}
	topology = store.Get()
	peerNode := clusterPeerNodeID(topology, *peer)
	referenceHealth, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, *peer, "/api/health")
	if err != nil {
		return fmt.Errorf("reference health: %w", err)
	}
	referenceConfig, err := getJSONValue[map[string]interface{}](ctx, client, *peer, "/api/config")
	if err != nil {
		return fmt.Errorf("reference config: %w", err)
	}
	ignored := make(map[string]bool, len(clusterNodeLocalConfigFields))
	for _, field := range clusterNodeLocalConfigFields {
		ignored[field] = true
	}
	primaryNodes := make(map[string]bool)
	for _, node := range topology.Nodes {
		if node.Role == "primary" {
			primaryNodes[node.ID] = true
		}
	}
	for _, shard := range topology.Shards {
		primaryNodes[shard.Primary] = true
	}
	now := time.Now().UTC()
	result := clusterUpgradePlanResult{
		OK:            true,
		Ready:         true,
		Compatible:    true,
		Peer:          *peer,
		TargetVersion: *targetVersion,
		GeneratedAt:   now.Format(time.RFC3339Nano),
		MaxBackupAge:  maxBackupAge.String(),
		RequireBackup: *requireBackup,
		Nodes:         make([]clusterUpgradeNode, 0, len(topology.Nodes)),
	}
	for _, topologyNode := range topology.Nodes {
		address := strings.TrimSpace(topologyNode.Address)
		if topologyNode.ID == peerNode {
			address = *peer
		}
		node := clusterUpgradeNode{
			ID:           topologyNode.ID,
			Address:      address,
			Role:         topologyNode.Role,
			Primary:      primaryNodes[topologyNode.ID],
			Ready:        true,
			Commands:     upgradeNodeCommands(*peer, topologyNode.ID, *targetVersion),
			CanaryChecks: upgradeNodeCanaryChecks(*peer, address),
		}

		health := referenceHealth
		var healthErr error
		if topologyNode.ID != peerNode {
			health, healthErr = getJSONValue[hatriecache.MonitoringHealth](ctx, client, address, "/api/health")
		}
		if healthErr != nil {
			node.Blockers = append(node.Blockers, "health: "+healthErr.Error())
		} else {
			node.CurrentVersion = health.Version
			node.APIVersion = health.APIVersion
			if !strings.EqualFold(strings.TrimSpace(health.Status), "online") {
				node.Blockers = append(node.Blockers, "node is not online")
			}
			if health.Node != "" && health.Node != topologyNode.ID {
				node.Blockers = append(node.Blockers, fmt.Sprintf("endpoint reports node %q", health.Node))
			}
			if referenceHealth.APIVersion != 0 && health.APIVersion != referenceHealth.APIVersion {
				node.Blockers = append(node.Blockers, fmt.Sprintf("monitoring API version %d differs from reference %d", health.APIVersion, referenceHealth.APIVersion))
				result.Compatible = false
			}
			if strings.TrimSpace(health.Version) == "" {
				node.Blockers = append(node.Blockers, "binary version is unavailable")
			}
		}

		var storage struct {
			Configured bool `json:"configured"`
		}
		storage, storageErr := getJSONValue[struct {
			Configured bool `json:"configured"`
		}](ctx, client, address, "/api/storage")
		if storageErr != nil {
			node.Blockers = append(node.Blockers, "storage: "+storageErr.Error())
		} else {
			node.StorageConfigured = storage.Configured
			if !storage.Configured {
				node.Blockers = append(node.Blockers, "persistent storage is not configured")
			}
		}

		config := referenceConfig
		var configErr error
		if topologyNode.ID != peerNode {
			config, configErr = getJSONValue[map[string]interface{}](ctx, client, address, "/api/config")
		}
		if configErr != nil {
			node.Blockers = append(node.Blockers, "config: "+configErr.Error())
		} else {
			node.ConfigDifferences = diffClusterConfigs(referenceConfig, config, ignored)
			if len(node.ConfigDifferences) > 0 {
				node.Blockers = append(node.Blockers, "effective config differs from reference")
				result.Compatible = false
			}
		}

		var audit struct {
			Configured bool                     `json:"configured"`
			Events     []hatriecache.AuditEvent `json:"events"`
		}
		audit, auditErr := getJSONValue[struct {
			Configured bool                     `json:"configured"`
			Events     []hatriecache.AuditEvent `json:"events"`
		}](ctx, client, address, "/api/audit?limit=128")
		if auditErr != nil {
			if *requireBackup {
				node.Blockers = append(node.Blockers, "backup audit: "+auditErr.Error())
			}
		} else if backupAt, ok := latestSuccessfulBackup(audit.Events); ok {
			node.BackupAt = backupAt.Format(time.RFC3339Nano)
			age := now.Sub(backupAt)
			if age < 0 {
				age = 0
			}
			node.BackupAgeMillis = age.Milliseconds()
			if *requireBackup && age > *maxBackupAge {
				node.Blockers = append(node.Blockers, fmt.Sprintf("latest backup is %s old; maximum is %s", age.Round(time.Second), maxBackupAge.String()))
			}
		} else if *requireBackup {
			if !audit.Configured {
				node.Blockers = append(node.Blockers, "audit log is not configured; backup freshness cannot be verified")
			} else {
				node.Blockers = append(node.Blockers, "no successful backup is present in recent audit history")
			}
		}

		if len(node.Blockers) > 0 {
			node.Ready = false
			result.Ready = false
		}
		result.Nodes = append(result.Nodes, node)
	}
	sort.SliceStable(result.Nodes, func(i, j int) bool {
		if result.Nodes[i].Primary != result.Nodes[j].Primary {
			return !result.Nodes[i].Primary
		}
		return result.Nodes[i].ID < result.Nodes[j].ID
	})
	for idx := range result.Nodes {
		result.Nodes[idx].Order = idx + 1
	}
	if *runCanary {
		canary, err := collectClusterStatus(ctx, client, *peer, true)
		if err != nil {
			return fmt.Errorf("baseline canary: %w", err)
		}
		result.BaselineCanary = &canary
		if !canary.OK {
			result.Ready = false
		}
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func latestSuccessfulBackup(events []hatriecache.AuditEvent) (time.Time, bool) {
	var latest time.Time
	for _, event := range events {
		if event.Action != "backup" || !event.OK {
			continue
		}
		occurredAt, err := time.Parse(time.RFC3339Nano, event.Time)
		if err != nil {
			continue
		}
		if occurredAt.After(latest) {
			latest = occurredAt
		}
	}
	return latest, !latest.IsZero()
}

func upgradeNodeCommands(peer string, nodeID string, targetVersion string) []string {
	reason := "upgrade"
	if targetVersion != "" {
		reason += " to " + targetVersion
	}
	return []string{
		"make cli ARGS=" + shellSingleQuote("cluster maintenance begin -peer "+peer+" -node "+nodeID+" -reason "+reason),
		"restart " + nodeID + " with the target binary using the service manager",
		"make cli ARGS=" + shellSingleQuote("cluster maintenance end -peer "+peer+" -node "+nodeID),
	}
}

func upgradeNodeCanaryChecks(peer string, address string) []string {
	return []string{
		"make cli ARGS=" + shellSingleQuote("-addr "+address+" health"),
		"make cli ARGS=" + shellSingleQuote("cluster doctor -peer "+peer),
		"make cli ARGS=" + shellSingleQuote("-addr "+peer+" replication -sync"),
	}
}

func shellSingleQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
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

func runClusterAddReplica(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster add-replica", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	peer := flags.String("peer", addr, "existing cluster node monitoring API base URL")
	nodeAddress := flags.String("address", "", "joining node monitoring API base URL")
	nodeID := flags.String("node", "", "expected joining node id; defaults to the target health identity")
	replace := flags.Bool("replace", false, "allow the same replica id to move to a new address")
	pullJournal := flags.Bool("pull-journal", true, "catch the joining node up before activating membership")
	finalSync := flags.Bool("final-sync", true, "run anti-entropy replication after activating membership")
	allowMemoryOnly := flags.Bool("allow-memory-only", false, "allow a joining node without persistent storage")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*peer = strings.TrimSpace(*peer)
	*nodeAddress = strings.TrimRight(strings.TrimSpace(*nodeAddress), "/")
	*nodeID = strings.TrimSpace(*nodeID)
	if *peer == "" {
		return errors.New("cluster add-replica -peer is required")
	}
	if *nodeAddress == "" {
		return errors.New("cluster add-replica -address is required")
	}

	targetHealth, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, *nodeAddress, "/api/health")
	if err != nil {
		return fmt.Errorf("joining node health: %w", err)
	}
	if !strings.EqualFold(strings.TrimSpace(targetHealth.Status), "online") {
		return fmt.Errorf("joining node is not online: %q", targetHealth.Status)
	}
	targetNodeID := strings.TrimSpace(targetHealth.Node)
	if targetNodeID == "" {
		return errors.New("joining node health did not report a node id")
	}
	if *nodeID != "" && *nodeID != targetNodeID {
		return fmt.Errorf("joining endpoint reports node %q, not requested node %q", targetNodeID, *nodeID)
	}
	*nodeID = targetNodeID

	var targetStorage struct {
		Configured bool `json:"configured"`
	}
	targetStorage, err = getJSONValue[struct {
		Configured bool `json:"configured"`
	}](ctx, client, *nodeAddress, "/api/storage")
	if err != nil {
		return fmt.Errorf("joining node storage: %w", err)
	}
	if !targetStorage.Configured && !*allowMemoryOnly {
		return errors.New("joining node has no persistent storage; configure storage or pass -allow-memory-only")
	}

	peerHealth, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, *peer, "/api/health")
	if err != nil {
		return fmt.Errorf("peer health: %w", err)
	}
	if peerHealth.APIVersion != 0 && targetHealth.APIVersion != 0 && peerHealth.APIVersion != targetHealth.APIVersion {
		return fmt.Errorf("monitoring API version mismatch: peer %d, joining node %d", peerHealth.APIVersion, targetHealth.APIVersion)
	}

	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("peer topology: %w", err)
	}
	if _, _, _, err := clusterAddReplicaTopology(topology, *nodeID, *nodeAddress, *replace); err != nil {
		return err
	}

	journalPulled := false
	if *pullJournal {
		body, err := jsonwire.Marshal(struct {
			Source       string `json:"source"`
			UntilCurrent bool   `json:"until_current"`
		}{Source: *peer, UntilCurrent: true})
		if err != nil {
			return err
		}
		if err := postJSON(ctx, client, *nodeAddress, "/api/journal", body, io.Discard); err != nil {
			return fmt.Errorf("catch up joining node journal: %w", err)
		}
		journalPulled = true
	}

	latest, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("refresh peer topology after catch-up: %w", err)
	}
	updated, topologyChanged, replaced, err := clusterAddReplicaTopology(latest, *nodeID, *nodeAddress, *replace)
	if err != nil {
		return fmt.Errorf("refreshed topology: %w", err)
	}
	peerNode := clusterPeerNodeID(latest, *peer)
	nodesUpdated, err := putTopologyToNodes(ctx, client, updated, *peer, peerNode, nil)
	if err != nil {
		return fmt.Errorf("activate replica topology: %w", err)
	}

	finalSyncRan := false
	if *finalSync {
		if err := postJSON(ctx, client, *peer, "/api/replication", []byte("{}"), io.Discard); err != nil {
			return fmt.Errorf("final anti-entropy sync: %w", err)
		}
		finalSyncRan = true
	}
	if err := verifyTopologyOnNodes(ctx, client, updated, *peer, peerNode); err != nil {
		return fmt.Errorf("verify activated topology: %w", err)
	}

	result := clusterAddReplicaResult{
		OK:               true,
		Message:          "cluster replica added and verified",
		Peer:             *peer,
		Node:             *nodeID,
		Address:          *nodeAddress,
		Replaced:         replaced,
		TopologyUpdated:  topologyChanged,
		JournalPulled:    journalPulled,
		FinalSync:        finalSyncRan,
		TopologyVerified: true,
		NodesUpdated:     nodesUpdated,
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func runClusterJoin(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster join", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	nodeID := flags.String("node", "", "node id to add to the cluster topology")
	nodeAddress := flags.String("address", "", "joining node monitoring API base URL")
	peer := flags.String("peer", addr, "existing cluster node monitoring API base URL")
	role := flags.String("role", "replica", "topology role for the joining node: primary or replica")
	replace := flags.Bool("replace", false, "allow the same node id to move to a new address")
	updateTarget := flags.Bool("update-target", true, "upload the updated topology to the joining node")
	updateNodes := flags.Bool("update-nodes", true, "upload the updated topology to every reachable cluster node")
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
	updated, topologyChanged, err := clusterJoinTopologyWithReplace(topology, *nodeID, *nodeAddress, *role, *replace)
	if err != nil {
		return err
	}
	peerNode := clusterPeerNodeID(topology, *peer)
	nodesUpdated := make([]string, 0, len(updated.Nodes))
	if *updateNodes {
		var err error
		nodesUpdated, err = putTopologyToNodes(ctx, client, updated, *peer, peerNode, func(node hatriecache.TopologyNode) bool {
			return *updateTarget || node.ID != *nodeID
		})
		if err != nil {
			return fmt.Errorf("upload cluster topology: %w", err)
		}
	} else {
		if topologyChanged {
			if err := putJSONValueDiscard(ctx, client, *peer, "/api/topology", topologyForNode(updated, peerNode)); err != nil {
				return fmt.Errorf("upload peer topology: %w", err)
			}
			if peerNode != "" {
				nodesUpdated = append(nodesUpdated, peerNode)
			}
		}
		if *updateTarget {
			if err := putJSONValueDiscard(ctx, client, *nodeAddress, "/api/topology", topologyForNode(updated, *nodeID)); err != nil {
				return fmt.Errorf("upload joining node topology: %w", err)
			}
			nodesUpdated = append(nodesUpdated, *nodeID)
		}
	}
	targetUpdated := stringInSlice(nodesUpdated, *nodeID)
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
		NodesUpdated:    nodesUpdated,
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

func runClusterRemove(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster remove", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	nodeID := flags.String("node", "", "replica node id to remove from the cluster topology")
	peer := flags.String("peer", addr, "existing cluster node monitoring API base URL")
	updateNodes := flags.Bool("update-nodes", true, "upload the updated topology to every remaining cluster node")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*nodeID = strings.TrimSpace(*nodeID)
	*peer = strings.TrimSpace(*peer)
	if *nodeID == "" {
		return errors.New("cluster remove -node is required")
	}
	if *peer == "" {
		return errors.New("cluster remove -peer is required")
	}

	if _, err := getJSONValue[map[string]interface{}](ctx, client, *peer, "/api/health"); err != nil {
		return fmt.Errorf("peer health: %w", err)
	}
	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("peer topology: %w", err)
	}
	updated, topologyChanged, err := clusterRemoveTopology(topology, *nodeID)
	if err != nil {
		return err
	}
	peerNode := clusterPeerNodeID(topology, *peer)

	nodesUpdated := make([]string, 0, len(updated.Nodes))
	if *updateNodes {
		nodesUpdated, err = putTopologyToNodes(ctx, client, updated, *peer, peerNode, nil)
		if err != nil {
			return fmt.Errorf("upload cluster topology: %w", err)
		}
	} else if topologyChanged {
		if peerNode == *nodeID {
			peerNode = ""
		}
		if err := putJSONValueDiscard(ctx, client, *peer, "/api/topology", topologyForNode(updated, peerNode)); err != nil {
			return fmt.Errorf("upload peer topology: %w", err)
		}
		if peerNode != "" {
			nodesUpdated = append(nodesUpdated, peerNode)
		}
	}

	result := clusterRemoveResult{
		OK:              true,
		Message:         "cluster replica removal completed",
		Peer:            *peer,
		Node:            *nodeID,
		TopologyUpdated: topologyChanged,
		NodesUpdated:    nodesUpdated,
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func runClusterDecommission(ctx context.Context, client *http.Client, addr string, args []string, stdout io.Writer, stderr io.Writer) error {
	flags := flag.NewFlagSet("cluster decommission", flag.ContinueOnError)
	flags.SetOutput(cliWriter(stderr))
	nodeID := flags.String("node", "", "replica node id to retire")
	peer := flags.String("peer", addr, "existing cluster node monitoring API base URL")
	minReplicas := flags.Int("min-replicas", 1, "minimum replicas that must remain for every affected shard")
	finalSync := flags.Bool("final-sync", true, "run anti-entropy replication before removal")
	allowUnreachable := flags.Bool("allow-unreachable", false, "allow removal when the retiring endpoint cannot be identity-checked")
	if err := flags.Parse(args); err != nil {
		return err
	}
	*nodeID = strings.TrimSpace(*nodeID)
	*peer = strings.TrimSpace(*peer)
	if *nodeID == "" {
		return errors.New("cluster decommission -node is required")
	}
	if *peer == "" {
		return errors.New("cluster decommission -peer is required")
	}
	if *minReplicas < 0 {
		return errors.New("cluster decommission -min-replicas must be non-negative")
	}

	if _, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, *peer, "/api/health"); err != nil {
		return fmt.Errorf("peer health: %w", err)
	}
	topology, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, *peer, "/api/topology")
	if err != nil {
		return fmt.Errorf("peer topology: %w", err)
	}
	retiringNode, err := validateClusterDecommission(topology, *nodeID, *minReplicas)
	if err != nil {
		return err
	}
	alreadyRemoved := retiringNode.ID == ""
	if !alreadyRemoved {
		health, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, retiringNode.Address, "/api/health")
		if err != nil && !*allowUnreachable {
			return fmt.Errorf("retiring node health: %w", err)
		}
		if err == nil && strings.TrimSpace(health.Node) != *nodeID {
			return fmt.Errorf("retiring endpoint reports node %q, not %q", health.Node, *nodeID)
		}
	}

	updated, topologyChanged, err := clusterRemoveTopology(topology, *nodeID)
	if err != nil {
		return err
	}
	peerNode := clusterPeerNodeID(topology, *peer)
	if err := verifyClusterNodeHealth(ctx, client, updated, *peer, peerNode); err != nil {
		return fmt.Errorf("surviving node health: %w", err)
	}

	finalSyncRan := false
	markedOffline := false
	if !alreadyRemoved {
		if *finalSync {
			if err := postJSON(ctx, client, *peer, "/api/replication", []byte("{}"), io.Discard); err != nil {
				return fmt.Errorf("final anti-entropy sync: %w", err)
			}
			finalSyncRan = true
		}
		if err := postElectionUpdate(ctx, client, *peer, *nodeID, false, io.Discard); err != nil {
			return fmt.Errorf("mark retiring node offline: %w", err)
		}
		markedOffline = true
	}

	nodesUpdated, err := putTopologyToNodes(ctx, client, updated, *peer, peerNode, nil)
	if err != nil {
		return fmt.Errorf("upload decommissioned topology: %w", err)
	}
	if err := verifyTopologyOnNodes(ctx, client, updated, *peer, peerNode); err != nil {
		return fmt.Errorf("verify decommissioned topology: %w", err)
	}

	cleanup := fmt.Sprintf("stop node %s and archive or delete its data directory only after this verified result", *nodeID)
	result := clusterDecommissionResult{
		OK:               true,
		Message:          "cluster replica decommissioned and verified",
		Peer:             *peer,
		Node:             *nodeID,
		RemovedAddress:   retiringNode.Address,
		AlreadyRemoved:   alreadyRemoved,
		FinalSync:        finalSyncRan,
		MarkedOffline:    markedOffline,
		TopologyUpdated:  topologyChanged,
		TopologyVerified: true,
		NodesUpdated:     nodesUpdated,
		Cleanup:          cleanup,
	}
	data, err := jsonwire.Marshal(result)
	if err != nil {
		return err
	}
	_, err = stdout.Write(append(data, '\n'))
	return err
}

func clusterJoinTopology(topology hatriecache.ClusterTopology, nodeID string, address string, role string) (hatriecache.ClusterTopology, bool, error) {
	return clusterJoinTopologyWithReplace(topology, nodeID, address, role, false)
}

func clusterJoinTopologyWithReplace(topology hatriecache.ClusterTopology, nodeID string, address string, role string, replace bool) (hatriecache.ClusterTopology, bool, error) {
	nodeID = strings.TrimSpace(nodeID)
	address = strings.TrimRight(strings.TrimSpace(address), "/")
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
	for _, node := range topology.Nodes {
		nodeAddress := strings.TrimRight(strings.TrimSpace(node.Address), "/")
		if nodeAddress == address && strings.TrimSpace(node.ID) != nodeID {
			return hatriecache.ClusterTopology{}, false, fmt.Errorf("cluster node address %q already belongs to node %q", address, node.ID)
		}
		if strings.TrimSpace(node.ID) == nodeID && nodeAddress != address && !replace {
			return hatriecache.ClusterTopology{}, false, fmt.Errorf("cluster node %q already exists at %q; use -replace to move it", nodeID, node.Address)
		}
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

func clusterAddReplicaTopology(topology hatriecache.ClusterTopology, nodeID string, address string, replace bool) (hatriecache.ClusterTopology, bool, bool, error) {
	store, err := hatriecache.NewTopologyStore(topology)
	if err != nil {
		return hatriecache.ClusterTopology{}, false, false, err
	}
	normalized := store.Get()
	nodeID = strings.TrimSpace(nodeID)
	address = strings.TrimRight(strings.TrimSpace(address), "/")
	if nodeID == "" {
		return hatriecache.ClusterTopology{}, false, false, errors.New("cluster add-replica node id is required")
	}
	if address == "" {
		return hatriecache.ClusterTopology{}, false, false, errors.New("cluster add-replica node address is required")
	}

	replaced := false
	for _, node := range normalized.Nodes {
		nodeAddress := strings.TrimRight(strings.TrimSpace(node.Address), "/")
		if nodeAddress == address && node.ID != nodeID {
			return hatriecache.ClusterTopology{}, false, false, fmt.Errorf("cluster node address %q already belongs to node %q", address, node.ID)
		}
		if node.ID != nodeID {
			continue
		}
		if strings.TrimSpace(node.Role) == "primary" {
			return hatriecache.ClusterTopology{}, false, false, fmt.Errorf("cluster node %q is primary and cannot be added as a replica", nodeID)
		}
		if nodeAddress != address {
			if !replace {
				return hatriecache.ClusterTopology{}, false, false, fmt.Errorf("cluster node %q already exists at %q; use -replace to move it", nodeID, node.Address)
			}
			replaced = true
		}
	}

	updated, changed, err := clusterJoinTopologyWithReplace(normalized, nodeID, address, "replica", replace)
	if err != nil {
		return hatriecache.ClusterTopology{}, false, false, err
	}
	return updated, changed, replaced, nil
}

func verifyTopologyOnNodes(ctx context.Context, client *http.Client, topology hatriecache.ClusterTopology, peer string, peerNode string) error {
	for _, node := range topology.Nodes {
		address := strings.TrimSpace(node.Address)
		if node.ID == peerNode {
			address = strings.TrimSpace(peer)
		}
		if address == "" {
			return fmt.Errorf("node %q has no monitoring address", node.ID)
		}
		observed, err := getJSONValue[hatriecache.ClusterTopology](ctx, client, address, "/api/topology")
		if err != nil {
			return fmt.Errorf("node %q: %w", node.ID, err)
		}
		if observed.Self != node.ID {
			return fmt.Errorf("node %q reports topology self %q", node.ID, observed.Self)
		}
		consistent, err := clusterTopologiesConsistent(topology, observed)
		if err != nil {
			return fmt.Errorf("node %q: %w", node.ID, err)
		}
		if !consistent {
			return fmt.Errorf("node %q topology differs after update", node.ID)
		}
	}
	return nil
}

func verifyClusterNodeHealth(ctx context.Context, client *http.Client, topology hatriecache.ClusterTopology, peer string, peerNode string) error {
	for _, node := range topology.Nodes {
		address := strings.TrimSpace(node.Address)
		if node.ID == peerNode {
			address = strings.TrimSpace(peer)
		}
		health, err := getJSONValue[hatriecache.MonitoringHealth](ctx, client, address, "/api/health")
		if err != nil {
			return fmt.Errorf("node %q: %w", node.ID, err)
		}
		if !strings.EqualFold(strings.TrimSpace(health.Status), "online") {
			return fmt.Errorf("node %q status is %q", node.ID, health.Status)
		}
		if healthNode := strings.TrimSpace(health.Node); healthNode != "" && healthNode != node.ID {
			return fmt.Errorf("node %q endpoint reports node %q", node.ID, healthNode)
		}
	}
	return nil
}

func validateClusterDecommission(topology hatriecache.ClusterTopology, nodeID string, minReplicas int) (hatriecache.TopologyNode, error) {
	if minReplicas < 0 {
		return hatriecache.TopologyNode{}, errors.New("minimum remaining replicas must be non-negative")
	}
	store, err := hatriecache.NewTopologyStore(topology)
	if err != nil {
		return hatriecache.TopologyNode{}, err
	}
	normalized := store.Get()
	nodeID = strings.TrimSpace(nodeID)
	var retiring hatriecache.TopologyNode
	for _, node := range normalized.Nodes {
		if node.ID == nodeID {
			retiring = node
			break
		}
	}
	if retiring.ID == "" {
		return hatriecache.TopologyNode{}, nil
	}
	if retiring.Role == "primary" {
		return hatriecache.TopologyNode{}, fmt.Errorf("cluster decommission refuses primary node %q; promote or reassign it first", nodeID)
	}
	for _, shard := range normalized.Shards {
		if shard.Primary == nodeID {
			return hatriecache.TopologyNode{}, fmt.Errorf("cluster decommission refuses shard primary node %q; reassign shard %d first", nodeID, shard.ID)
		}
		if !stringInSlice(shard.Replicas, nodeID) {
			continue
		}
		remaining := len(shard.Replicas) - 1
		if remaining < minReplicas {
			return hatriecache.TopologyNode{}, fmt.Errorf("shard %d would have %d remaining replicas; require at least %d", shard.ID, remaining, minReplicas)
		}
	}
	if normalized.Mode == hatriecache.TopologyModeFullReplica {
		remaining := 0
		for _, node := range normalized.Nodes {
			if node.ID != nodeID && node.Role != "primary" {
				remaining++
			}
		}
		if remaining < minReplicas {
			return hatriecache.TopologyNode{}, fmt.Errorf("full replica topology would have %d remaining replicas; require at least %d", remaining, minReplicas)
		}
	}
	return retiring, nil
}

func clusterRemoveTopology(topology hatriecache.ClusterTopology, nodeID string) (hatriecache.ClusterTopology, bool, error) {
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return hatriecache.ClusterTopology{}, false, errors.New("cluster remove node id is required")
	}

	found := false
	for _, node := range topology.Nodes {
		if strings.TrimSpace(node.ID) != nodeID {
			continue
		}
		found = true
		if strings.TrimSpace(node.Role) == "primary" {
			return hatriecache.ClusterTopology{}, false, fmt.Errorf("cluster remove refuses primary node %q; promote or reassign it first", nodeID)
		}
	}
	for _, shard := range topology.Shards {
		if strings.TrimSpace(shard.Primary) == nodeID {
			return hatriecache.ClusterTopology{}, false, fmt.Errorf("cluster remove refuses shard primary node %q; reassign shard %d first", nodeID, shard.ID)
		}
	}
	if !found {
		store, err := hatriecache.NewTopologyStore(topology)
		if err != nil {
			return hatriecache.ClusterTopology{}, false, err
		}
		return store.Get(), false, nil
	}

	nodes := make([]hatriecache.TopologyNode, 0, len(topology.Nodes)-1)
	for _, node := range topology.Nodes {
		if strings.TrimSpace(node.ID) != nodeID {
			nodes = append(nodes, node)
		}
	}
	topology.Nodes = nodes
	topology.Shards = append([]hatriecache.TopologyShard(nil), topology.Shards...)
	for idx := range topology.Shards {
		replicas := make([]string, 0, len(topology.Shards[idx].Replicas))
		for _, replica := range topology.Shards[idx].Replicas {
			if strings.TrimSpace(replica) != nodeID {
				replicas = append(replicas, replica)
			}
		}
		topology.Shards[idx].Replicas = replicas
	}
	if strings.TrimSpace(topology.Self) == nodeID {
		topology.Self = ""
	}
	store, err := hatriecache.NewTopologyStore(topology)
	if err != nil {
		return hatriecache.ClusterTopology{}, false, err
	}
	return store.Get(), true, nil
}

func topologyForNode(topology hatriecache.ClusterTopology, nodeID string) hatriecache.ClusterTopology {
	topology.Self = strings.TrimSpace(nodeID)
	return topology
}

func clusterPeerNodeID(topology hatriecache.ClusterTopology, peer string) string {
	if self := strings.TrimSpace(topology.Self); self != "" {
		return self
	}
	peer = strings.TrimRight(strings.TrimSpace(peer), "/")
	for _, node := range topology.Nodes {
		if strings.TrimRight(strings.TrimSpace(node.Address), "/") == peer {
			return strings.TrimSpace(node.ID)
		}
	}
	primary := ""
	for _, node := range topology.Nodes {
		if strings.TrimSpace(node.Role) != "primary" {
			continue
		}
		if primary != "" {
			primary = ""
			break
		}
		primary = strings.TrimSpace(node.ID)
	}
	if primary != "" {
		return primary
	}
	if len(topology.Nodes) > 0 {
		return strings.TrimSpace(topology.Nodes[0].ID)
	}
	return ""
}

func putTopologyToNodes(ctx context.Context, client *http.Client, topology hatriecache.ClusterTopology, peer string, peerNode string, include func(hatriecache.TopologyNode) bool) ([]string, error) {
	updated := make([]string, 0, len(topology.Nodes))
	for _, node := range topology.Nodes {
		if include != nil && !include(node) {
			continue
		}
		address := strings.TrimSpace(node.Address)
		if node.ID == peerNode {
			address = strings.TrimSpace(peer)
		}
		if address == "" {
			return updated, fmt.Errorf("node %q has no monitoring address", node.ID)
		}
		if err := putJSONValueDiscard(ctx, client, address, "/api/topology", topologyForNode(topology, node.ID)); err != nil {
			return updated, fmt.Errorf("node %q: %w", node.ID, err)
		}
		updated = append(updated, node.ID)
	}
	return updated, nil
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

func postJSONAndDecode[T any](ctx context.Context, client *http.Client, addr string, path string, value interface{}) (T, error) {
	var out T
	body, err := jsonwire.Marshal(value)
	if err != nil {
		return out, err
	}
	reader, contentEncoding, err := jsonRequestBody(body)
	if err != nil {
		return out, err
	}
	req, err := http.NewRequestWithContext(cliContext(ctx), http.MethodPost, endpoint(addr, path), reader)
	if err != nil {
		return out, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if contentEncoding != "" {
		req.Header.Set("Content-Encoding", contentEncoding)
	}
	if err := doAndDecodeJSON(client, req, &out); err != nil {
		return out, err
	}
	return out, nil
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
