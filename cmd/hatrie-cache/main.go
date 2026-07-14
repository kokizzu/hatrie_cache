package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	json "github.com/goccy/go-json"

	hatriecache "hatrie_cache"

	"google.golang.org/grpc"
)

const (
	serverShutdownTimeout              = 5 * time.Second
	defaultMonitoringReadHeaderTimeout = 5 * time.Second
	defaultMonitoringIdleTimeout       = 2 * time.Minute
)

type config struct {
	monitoringServer            bool
	monitoringAddr              string
	monitoringTLSCert           string
	monitoringTLSKey            string
	monitoringWebDir            string
	monitoringReadHeaderTimeout time.Duration
	monitoringIdleTimeout       time.Duration
	nodeID                      string
	topologyPath                string
	electionTimeout             time.Duration
	replication                 bool
	replicationAsync            bool
	replicationQueueSize        int
	replicationRetry            time.Duration
	replicationAttempts         uint
	replicationWireFormat       string
	enforceLeaderWrites         bool
	grpcAddr                    string
	dbPath                      string
	dbFormat                    string
	dbSyncInterval              time.Duration
	dbHotLoad                   bool
	dbHotLoadMaxBytes           int64
	dbHotLoadMaxAge             time.Duration
	dbHotLoadMinHits            uint64
	snapshotPath                string
	snapshotInterval            time.Duration
	snapshotFormat              string
	journalPath                 string
	journalPullSource           string
	journalPullStatePath        string
	journalPullInterval         time.Duration
	journalPullTimeout          time.Duration
	journalPullLimit            uint64
	journalPullMaxBatches       uint64
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	cfg, err := parseConfig(args, stderr)
	if err != nil {
		return err
	}
	if !cfg.monitoringServer {
		fmt.Fprintln(stdout, "monitoring server disabled; pass -monitoring-server to start it")
		return nil
	}

	trie := hatriecache.CreateHatTrie()
	defer trie.Destroy()

	dbStore, err := openLevelDBIfConfigured(cfg.dbPath, storageFormat(cfg))
	if err != nil {
		return err
	}
	defer closeLevelDB(dbStore, stderr)
	if err := loadLevelDBIfConfigured(trie, dbStore, levelDBLoadPolicy(cfg)); err != nil {
		return err
	}

	journal, err := openJournalIfConfigured(cfg.journalPath)
	if err != nil {
		return err
	}
	defer closeJournal(journal, stderr)
	snapshotMetadata, err := loadSnapshotIfConfigured(trie, cfg.snapshotPath)
	if err != nil {
		return err
	}
	if journal != nil {
		if _, err := journal.Replay(trie, snapshotMetadata.JournalSequence); err != nil {
			return err
		}
		if cfg.snapshotPath != "" {
			if err := saveSnapshotIfConfigured(trie, journal, cfg.snapshotPath, snapshotFormat(cfg)); err != nil {
				return err
			}
		}
	}
	if cfg.snapshotPath != "" {
		defer func() {
			if err := saveSnapshotIfConfigured(trie, journal, cfg.snapshotPath, snapshotFormat(cfg)); err != nil {
				fmt.Fprintf(stderr, "save snapshot: %v\n", err)
			}
		}()
	}
	if dbStore != nil {
		defer func() {
			if err := dbStore.Save(trie); err != nil {
				fmt.Fprintf(stderr, "save leveldb: %v\n", err)
			}
		}()
	}
	stopDBSync := startLevelDBSaver(ctx, trie, dbStore, cfg.dbSyncInterval, stderr)
	defer stopDBSync()
	stopSnapshots := startSnapshotSaver(ctx, trie, journal, cfg.snapshotPath, cfg.snapshotInterval, snapshotFormat(cfg), stderr)
	defer stopSnapshots()
	if cfg.journalPullSource != "" && journal == nil {
		return errors.New("journal pull requires -journal-path")
	}
	if cfg.journalPullSource != "" && cfg.journalPullStatePath == "" {
		cfg.journalPullStatePath = cfg.journalPath + ".pull_state.json"
	}
	stopJournalPuller := startJournalPuller(ctx, trie, journal, journalPullerConfig{
		Source:     cfg.journalPullSource,
		StatePath:  cfg.journalPullStatePath,
		Interval:   cfg.journalPullInterval,
		Timeout:    cfg.journalPullTimeout,
		Limit:      cfg.journalPullLimit,
		MaxBatches: cfg.journalPullMaxBatches,
	}, stderr)
	defer stopJournalPuller()

	topology, err := openTopologyIfConfigured(cfg.topologyPath, cfg.nodeID, cfg.monitoringAddr)
	if err != nil {
		return err
	}
	election := hatriecache.NewElectionStore(topology, hatriecache.ElectionOptions{Timeout: cfg.electionTimeout})
	stopElectionHeartbeat, err := startElectionHeartbeat(ctx, election, defaultNodeID(cfg.nodeID), cfg.electionTimeout, cfg.nodeID != "", stderr)
	if err != nil {
		return err
	}
	defer stopElectionHeartbeat()
	var replicator *hatriecache.HTTPReplicator
	if cfg.replication {
		replicator = hatriecache.NewHTTPReplicator(hatriecache.HTTPReplicatorOptions{
			Context:            ctx,
			Self:               defaultNodeID(cfg.nodeID),
			Topology:           topology,
			Election:           election,
			AsyncQueueSize:     replicationQueueSize(cfg),
			AsyncRetryInterval: cfg.replicationRetry,
			AsyncMaxAttempts:   cfg.replicationAttempts,
			WireFormat:         replicationWireFormat(cfg),
		})
		defer replicator.Close()
	}

	handler := hatriecache.NewMonitoringHandler(trie, hatriecache.MonitoringOptions{
		NodeName:            defaultNodeID(cfg.nodeID),
		WebDir:              cfg.monitoringWebDir,
		Snapshot:            snapshotCallback(trie, journal, cfg.snapshotPath, snapshotFormat(cfg)),
		Journal:             journal,
		Topology:            topology,
		Election:            election,
		Replicator:          replicator,
		EnforceLeaderWrites: cfg.enforceLeaderWrites,
	}).Handler()
	server := newMonitoringServer(cfg, handler)

	grpcServer, grpcListener, err := newGRPCServer(cfg, trie, journal, snapshotCallback(trie, journal, cfg.snapshotPath, snapshotFormat(cfg)), topology, election, replicator)
	if err != nil {
		return err
	}
	monitoringListener, err := newMonitoringListener(cfg)
	if err != nil {
		if grpcListener != nil {
			_ = grpcListener.Close()
		}
		if grpcServer != nil {
			grpcServer.Stop()
		}
		return err
	}

	errCh := make(chan error, 2)
	go func() {
		reportServerError(errCh, serveMonitoring(server, cfg, monitoringListener))
	}()
	fmt.Fprintf(stdout, "monitoring server listening on %s\n", monitoringURL(cfg))

	if grpcServer != nil {
		go func() {
			reportServerError(errCh, grpcServer.Serve(grpcListener))
		}()
		fmt.Fprintf(stdout, "grpc server listening on %s\n", cfg.grpcAddr)
	}

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), serverShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return err
		}
		stopGRPCServer(grpcServer)
		return nil
	case err := <-errCh:
		_ = server.Close()
		stopGRPCServer(grpcServer)
		if errors.Is(err, http.ErrServerClosed) || errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	}
}

func parseConfig(args []string, output io.Writer) (config, error) {
	cfg := config{
		monitoringAddr:              "127.0.0.1:8080",
		monitoringWebDir:            "svelte-mpa/dist",
		monitoringReadHeaderTimeout: defaultMonitoringReadHeaderTimeout,
		monitoringIdleTimeout:       defaultMonitoringIdleTimeout,
		electionTimeout:             hatriecache.DefaultElectionTimeout,
		journalPullTimeout:          hatriecache.DefaultCommandJournalPullTimeout,
	}
	flags := flag.NewFlagSet("hatrie-cache", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.BoolVar(&cfg.monitoringServer, "monitoring-server", false, "run the grpc/http2/web monitoring server")
	flags.StringVar(&cfg.monitoringAddr, "monitoring-addr", cfg.monitoringAddr, "monitoring server listen address")
	flags.StringVar(&cfg.monitoringTLSCert, "monitoring-tls-cert", "", "TLS certificate path for HTTPS/HTTP2 monitoring")
	flags.StringVar(&cfg.monitoringTLSKey, "monitoring-tls-key", "", "TLS private key path for HTTPS/HTTP2 monitoring")
	flags.StringVar(&cfg.monitoringWebDir, "monitoring-web-dir", cfg.monitoringWebDir, "directory containing built web monitoring assets")
	flags.DurationVar(&cfg.monitoringReadHeaderTimeout, "monitoring-read-header-timeout", cfg.monitoringReadHeaderTimeout, "maximum time to read monitoring HTTP request headers; use 0 to disable")
	flags.DurationVar(&cfg.monitoringIdleTimeout, "monitoring-idle-timeout", cfg.monitoringIdleTimeout, "maximum idle monitoring HTTP keep-alive time; use 0 to disable")
	flags.StringVar(&cfg.nodeID, "node-id", "", "local cluster node id")
	flags.StringVar(&cfg.topologyPath, "topology-path", "", "optional cluster topology JSON path to load and update")
	flags.DurationVar(&cfg.electionTimeout, "election-timeout", cfg.electionTimeout, "node heartbeat timeout for deterministic topology leader election")
	flags.BoolVar(&cfg.replication, "replication", false, "replicate successful leader writes to topology owners over HTTP")
	flags.BoolVar(&cfg.replicationAsync, "replication-async", false, "queue successful leader-write replication in a bounded async worker")
	flags.IntVar(&cfg.replicationQueueSize, "replication-queue-size", 1024, "maximum queued async replication jobs")
	flags.DurationVar(&cfg.replicationRetry, "replication-retry-interval", hatriecache.DefaultReplicationRetryInterval, "delay between async replication retry attempts")
	flags.UintVar(&cfg.replicationAttempts, "replication-max-attempts", 3, "maximum async replication delivery attempts")
	flags.StringVar(&cfg.replicationWireFormat, "replication-wire-format", string(hatriecache.DefaultCommandWireFormat), "HTTP replication command wire format: protobuf or json")
	flags.BoolVar(&cfg.enforceLeaderWrites, "enforce-leader-writes", false, "reject mutating client commands when this node is not the elected key leader")
	flags.StringVar(&cfg.grpcAddr, "grpc-addr", "", "optional native gRPC API listen address")
	flags.StringVar(&cfg.dbPath, "db-path", "", "optional LevelDB path to load on startup and save on shutdown")
	flags.StringVar(&cfg.dbFormat, "db-format", string(hatriecache.DefaultStorageFormat), "LevelDB record storage format: binary or json")
	flags.DurationVar(&cfg.dbSyncInterval, "db-sync-interval", 0, "optional periodic LevelDB save interval")
	flags.BoolVar(&cfg.dbHotLoad, "db-hot-load", false, "load cold LevelDB keys as lazy references and hot small values into memory")
	flags.Int64Var(&cfg.dbHotLoadMaxBytes, "db-hot-load-max-bytes", 1024, "maximum value size for LevelDB hot-load")
	flags.DurationVar(&cfg.dbHotLoadMaxAge, "db-hot-load-max-age", time.Hour, "maximum last-hit age for LevelDB hot-load")
	flags.Uint64Var(&cfg.dbHotLoadMinHits, "db-hot-load-min-hits", 1000, "minimum hits required for LevelDB hot-load")
	flags.StringVar(&cfg.snapshotPath, "snapshot-path", "", "optional snapshot path to load on startup and save on shutdown")
	flags.DurationVar(&cfg.snapshotInterval, "snapshot-interval", 0, "optional periodic snapshot interval")
	flags.StringVar(&cfg.snapshotFormat, "snapshot-format", string(hatriecache.DefaultSnapshotFormat), "snapshot save format: gzip-best-json, gzip-json, or json")
	flags.StringVar(&cfg.journalPath, "journal-path", "", "optional command journal path to replay on startup and append mutating commands")
	flags.StringVar(&cfg.journalPullSource, "journal-pull-source", "", "optional source monitoring URL to pull journal catch-up batches from")
	flags.StringVar(&cfg.journalPullStatePath, "journal-pull-state-path", "", "optional JSON path for persisted journal pull source sequence")
	flags.DurationVar(&cfg.journalPullInterval, "journal-pull-interval", 0, "optional interval for repeated journal pull catch-up")
	flags.DurationVar(&cfg.journalPullTimeout, "journal-pull-timeout", cfg.journalPullTimeout, "HTTP timeout for each journal pull request; use 0 to disable")
	flags.Uint64Var(&cfg.journalPullLimit, "journal-pull-limit", 0, "maximum entries per journal pull batch")
	flags.Uint64Var(&cfg.journalPullMaxBatches, "journal-pull-max-batches", 0, "maximum batches per journal pull attempt")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if (cfg.monitoringTLSCert == "") != (cfg.monitoringTLSKey == "") {
		return config{}, errors.New("monitoring TLS requires both -monitoring-tls-cert and -monitoring-tls-key")
	}
	if cfg.monitoringReadHeaderTimeout < 0 {
		return config{}, errors.New("monitoring read header timeout must be non-negative")
	}
	if cfg.monitoringIdleTimeout < 0 {
		return config{}, errors.New("monitoring idle timeout must be non-negative")
	}
	if cfg.dbHotLoadMaxBytes < 0 {
		return config{}, errors.New("db hot-load max bytes must be non-negative")
	}
	if cfg.dbHotLoadMaxAge < 0 {
		return config{}, errors.New("db hot-load max age must be non-negative")
	}
	if cfg.replicationQueueSize < 0 {
		return config{}, errors.New("replication queue size must be non-negative")
	}
	if cfg.replicationAsync && cfg.replicationQueueSize == 0 {
		return config{}, errors.New("replication async requires positive queue size")
	}
	if cfg.replicationAsync && cfg.replicationAttempts == 0 {
		return config{}, errors.New("replication async requires positive max attempts")
	}
	if cfg.replicationRetry < 0 {
		return config{}, errors.New("replication retry interval must be non-negative")
	}
	if cfg.journalPullTimeout < 0 {
		return config{}, errors.New("journal pull timeout must be non-negative")
	}
	if _, err := hatriecache.ParseCommandWireFormat(cfg.replicationWireFormat); err != nil {
		return config{}, err
	}
	if _, err := hatriecache.ParseStorageFormat(cfg.dbFormat); err != nil {
		return config{}, err
	}
	if _, err := hatriecache.ParseSnapshotFormat(cfg.snapshotFormat); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func replicationQueueSize(cfg config) int {
	if !cfg.replicationAsync {
		return 0
	}
	return cfg.replicationQueueSize
}

func replicationWireFormat(cfg config) hatriecache.CommandWireFormat {
	format, err := hatriecache.ParseCommandWireFormat(cfg.replicationWireFormat)
	if err != nil {
		return hatriecache.DefaultCommandWireFormat
	}
	return format
}

func snapshotFormat(cfg config) hatriecache.SnapshotFormat {
	format, err := hatriecache.ParseSnapshotFormat(cfg.snapshotFormat)
	if err != nil {
		return hatriecache.DefaultSnapshotFormat
	}
	return format
}

func storageFormat(cfg config) hatriecache.StorageFormat {
	format, err := hatriecache.ParseStorageFormat(cfg.dbFormat)
	if err != nil {
		return hatriecache.DefaultStorageFormat
	}
	return format
}

func newMonitoringServer(cfg config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              cfg.monitoringAddr,
		Handler:           handler,
		TLSConfig:         monitoringTLSConfig(nil),
		ReadHeaderTimeout: cfg.monitoringReadHeaderTimeout,
		IdleTimeout:       cfg.monitoringIdleTimeout,
	}
}

func newMonitoringListener(cfg config) (net.Listener, error) {
	return net.Listen("tcp", cfg.monitoringAddr)
}

func serveMonitoring(server *http.Server, cfg config, listener net.Listener) error {
	if cfg.monitoringTLSCert == "" {
		return server.Serve(listener)
	}
	return server.ServeTLS(listener, cfg.monitoringTLSCert, cfg.monitoringTLSKey)
}

func reportServerError(errCh chan<- error, err error) {
	select {
	case errCh <- err:
	default:
	}
}

func monitoringURL(cfg config) string {
	scheme := "http"
	if cfg.monitoringTLSCert != "" {
		scheme = "https"
	}
	return scheme + "://" + cfg.monitoringAddr
}

func monitoringTLSConfig(base *tls.Config) *tls.Config {
	var cfg tls.Config
	if base != nil {
		cfg = *base.Clone()
	}
	cfg.NextProtos = withHTTP2Proto(cfg.NextProtos)
	return &cfg
}

func withHTTP2Proto(nextProtos []string) []string {
	out := make([]string, 0, len(nextProtos)+2)
	if !containsString(nextProtos, "h2") {
		out = append(out, "h2")
	}
	if !containsString(nextProtos, "http/1.1") {
		out = append(out, "http/1.1")
	}
	for _, proto := range nextProtos {
		if proto != "h2" && proto != "http/1.1" {
			out = append(out, proto)
		}
	}
	return out
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func openTopologyIfConfigured(path string, nodeID string, address string) (*hatriecache.TopologyStore, error) {
	return hatriecache.OpenTopologyStore(path, hatriecache.SingleNodeTopology(defaultNodeID(nodeID), address))
}

func defaultNodeID(nodeID string) string {
	if nodeID = strings.TrimSpace(nodeID); nodeID != "" {
		return nodeID
	}
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		return hostname
	}
	return "local"
}

func openLevelDBIfConfigured(path string, format hatriecache.StorageFormat) (*hatriecache.LevelDBStore, error) {
	if path == "" {
		return nil, nil
	}
	return hatriecache.OpenLevelDBStoreWithFormat(path, format)
}

func levelDBLoadPolicy(cfg config) hatriecache.LevelDBLoadPolicy {
	if !cfg.dbHotLoad {
		return hatriecache.LevelDBLoadPolicy{}
	}
	return hatriecache.LevelDBLoadPolicy{
		HotValuesOnly: true,
		MaxValueBytes: cfg.dbHotLoadMaxBytes,
		MaxLastHitAge: cfg.dbHotLoadMaxAge,
		MinHits:       cfg.dbHotLoadMinHits,
	}
}

func loadLevelDBIfConfigured(trie *hatriecache.HatTrie, store *hatriecache.LevelDBStore, policy hatriecache.LevelDBLoadPolicy) error {
	if store == nil {
		return nil
	}
	_, err := store.LoadWithPolicy(trie, policy)
	return err
}

func closeLevelDB(store *hatriecache.LevelDBStore, stderr io.Writer) {
	if store == nil {
		return
	}
	if err := store.Close(); err != nil {
		fmt.Fprintf(stderr, "close leveldb: %v\n", err)
	}
}

func newGRPCServer(cfg config, trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, snapshot func() error, topology *hatriecache.TopologyStore, election *hatriecache.ElectionStore, replicator *hatriecache.HTTPReplicator) (*grpc.Server, net.Listener, error) {
	if cfg.grpcAddr == "" {
		return nil, nil, nil
	}
	listener, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return nil, nil, err
	}
	server := grpc.NewServer()
	hatriecache.RegisterCacheGRPCServer(server, hatriecache.NewCacheGRPCServer(trie, hatriecache.CacheGRPCOptions{
		NodeName:            defaultNodeID(cfg.nodeID),
		Snapshot:            snapshot,
		Journal:             journal,
		Topology:            topology,
		Election:            election,
		Replicator:          replicator,
		EnforceLeaderWrites: cfg.enforceLeaderWrites,
	}))
	return server, listener, nil
}

func stopGRPCServer(server *grpc.Server) {
	if server == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(serverShutdownTimeout):
		server.Stop()
		<-done
	}
}

func startLevelDBSaver(ctx context.Context, trie *hatriecache.HatTrie, store *hatriecache.LevelDBStore, interval time.Duration, stderr io.Writer) func() {
	if store == nil || interval <= 0 {
		return func() {}
	}

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	save := func() {
		if err := store.Save(trie); err != nil {
			fmt.Fprintf(stderr, "save leveldb: %v\n", err)
		}
	}
	go func() {
		defer close(stopped)
		defer ticker.Stop()
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		default:
		}
		save()
		for {
			select {
			case <-ticker.C:
				save()
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return periodicStopper(done, stopped)
}

func openJournalIfConfigured(path string) (*hatriecache.CommandJournal, error) {
	if path == "" {
		return nil, nil
	}
	return hatriecache.OpenCommandJournal(path)
}

func closeJournal(journal *hatriecache.CommandJournal, stderr io.Writer) {
	if journal == nil {
		return
	}
	if err := journal.Close(); err != nil {
		fmt.Fprintf(stderr, "close journal: %v\n", err)
	}
}

func loadSnapshotIfConfigured(trie *hatriecache.HatTrie, path string) (hatriecache.SnapshotMetadata, error) {
	if path == "" {
		return hatriecache.SnapshotMetadata{}, nil
	}
	metadata, err := trie.LoadSnapshotWithMetadata(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return hatriecache.SnapshotMetadata{}, err
	}
	return metadata, nil
}

func saveSnapshotIfConfigured(trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, path string, format hatriecache.SnapshotFormat) error {
	if path == "" {
		return nil
	}
	if journal != nil {
		return journal.SaveSnapshotWithFormat(trie, path, format)
	}
	return trie.SaveSnapshotWithFormat(path, format)
}

func startSnapshotSaver(ctx context.Context, trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, path string, interval time.Duration, format hatriecache.SnapshotFormat, stderr io.Writer) func() {
	if path == "" || interval <= 0 {
		return func() {}
	}

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	save := func() {
		if err := saveSnapshotIfConfigured(trie, journal, path, format); err != nil {
			fmt.Fprintf(stderr, "save snapshot: %v\n", err)
		}
	}
	go func() {
		defer close(stopped)
		defer ticker.Stop()
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		default:
		}
		save()
		for {
			select {
			case <-ticker.C:
				save()
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return periodicStopper(done, stopped)
}

func startElectionHeartbeat(ctx context.Context, election *hatriecache.ElectionStore, nodeID string, timeout time.Duration, required bool, stderr io.Writer) (func(), error) {
	if election == nil {
		return func() {}, nil
	}
	nodeID = strings.TrimSpace(nodeID)
	if nodeID == "" {
		return func() {}, nil
	}
	heartbeat := func() error {
		return election.Heartbeat(nodeID)
	}
	if err := heartbeat(); err != nil {
		if required {
			return nil, err
		}
		fmt.Fprintf(stderr, "election heartbeat: %v\n", err)
		return func() {}, nil
	}

	interval := electionHeartbeatInterval(timeout)
	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := heartbeat(); err != nil {
					fmt.Fprintf(stderr, "election heartbeat: %v\n", err)
				}
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return periodicStopper(done, stopped), nil
}

func electionHeartbeatInterval(timeout time.Duration) time.Duration {
	if timeout <= 0 {
		timeout = hatriecache.DefaultElectionTimeout
	}
	interval := timeout / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	minimum := 10 * time.Millisecond
	if timeout < 30*time.Millisecond {
		minimum = time.Millisecond
	}
	if interval < minimum {
		interval = minimum
	}
	if interval > 5*time.Second {
		interval = 5 * time.Second
	}
	return interval
}

type journalPullerConfig struct {
	Source     string
	StatePath  string
	Interval   time.Duration
	Timeout    time.Duration
	Limit      uint64
	MaxBatches uint64
}

type journalPullState struct {
	Source         string    `json:"source"`
	AppliedThrough uint64    `json:"applied_through"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func startJournalPuller(ctx context.Context, trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, cfg journalPullerConfig, stderr io.Writer) func() {
	if cfg.Source == "" || journal == nil {
		return func() {}
	}

	pullCtx, cancelPull := context.WithCancel(ctx)
	done := make(chan struct{})
	stopped := make(chan struct{})
	pull := func() {
		result, err := pullJournalOnce(pullCtx, trie, journal, cfg)
		if err != nil {
			fmt.Fprintf(stderr, "pull journal: %v\n", err)
			return
		}
		if result.HasMore {
			fmt.Fprintf(stderr, "pull journal: source still has more entries after %d batches\n", result.Batches)
		}
	}
	go func() {
		defer close(stopped)
		defer cancelPull()
		select {
		case <-pullCtx.Done():
			return
		case <-done:
			return
		default:
		}
		pull()
		if cfg.Interval <= 0 {
			return
		}
		ticker := time.NewTicker(cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				pull()
			case <-pullCtx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return cancelingPeriodicStopper(cancelPull, done, stopped)
}

func pullJournalOnce(ctx context.Context, trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, cfg journalPullerConfig) (hatriecache.CommandJournalPullResult, error) {
	afterSequence, err := loadJournalPullState(cfg.StatePath, cfg.Source)
	if err != nil {
		return hatriecache.CommandJournalPullResult{}, err
	}
	result, err := hatriecache.PullCommandJournal(ctx, trie, journal, hatriecache.CommandJournalPullOptions{
		Source:        cfg.Source,
		AfterSequence: afterSequence,
		Limit:         cfg.Limit,
		UntilCurrent:  true,
		MaxBatches:    cfg.MaxBatches,
		Timeout:       cfg.Timeout,
	})
	if err != nil {
		if result.AppliedThrough > afterSequence {
			if saveErr := saveJournalPullState(cfg.StatePath, cfg.Source, result.AppliedThrough); saveErr != nil {
				return result, errors.Join(err, saveErr)
			}
		}
		return result, err
	}
	if result.AppliedThrough >= afterSequence {
		if err := saveJournalPullState(cfg.StatePath, cfg.Source, result.AppliedThrough); err != nil {
			return result, err
		}
	}
	return result, nil
}

func loadJournalPullState(path string, source string) (uint64, error) {
	if path == "" {
		return 0, nil
	}
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()

	state, err := decodeJournalPullStateJSONReader(file)
	if err != nil {
		return 0, err
	}
	if state.Source != "" && state.Source != source {
		return 0, fmt.Errorf("journal pull state source %q does not match %q", state.Source, source)
	}
	return state.AppliedThrough, nil
}

func decodeJournalPullStateJSONReader(reader io.Reader) (journalPullState, error) {
	var state journalPullState
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&state); err != nil {
		return journalPullState{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return journalPullState{}, errors.New("invalid journal pull state JSON")
	}
	return state, nil
}

func saveJournalPullState(path string, source string, appliedThrough uint64) error {
	if path == "" {
		return nil
	}
	state := journalPullState{
		Source:         source,
		AppliedThrough: appliedThrough,
		UpdatedAt:      time.Now().UTC(),
	}
	return writeJSONFileAtomic(path, state)
}

func writeJSONFileAtomic(path string, value interface{}) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
	}
	encoder := json.NewEncoder(tmp)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return syncJSONDirectory(dir)
}

func syncJSONDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

func snapshotCallback(trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, path string, format hatriecache.SnapshotFormat) func() error {
	if path == "" {
		return nil
	}
	return func() error {
		return saveSnapshotIfConfigured(trie, journal, path, format)
	}
}

func periodicStopper(done chan struct{}, stopped chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			close(done)
			<-stopped
		})
	}
}

func cancelingPeriodicStopper(cancel context.CancelFunc, done chan struct{}, stopped chan struct{}) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			close(done)
			<-stopped
		})
	}
}
