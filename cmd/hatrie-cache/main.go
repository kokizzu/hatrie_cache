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
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	hatriecache "hatrie_cache"
)

const serverShutdownTimeout = 5 * time.Second

type config struct {
	monitoringServer  bool
	monitoringAddr    string
	monitoringTLSCert string
	monitoringTLSKey  string
	monitoringWebDir  string
	nodeID            string
	topologyPath      string
	electionTimeout   time.Duration
	replication       bool
	grpcAddr          string
	dbPath            string
	dbSyncInterval    time.Duration
	dbHotLoad         bool
	dbHotLoadMaxBytes int64
	dbHotLoadMaxAge   time.Duration
	dbHotLoadMinHits  uint64
	snapshotPath      string
	snapshotInterval  time.Duration
	journalPath       string
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

	dbStore, err := openLevelDBIfConfigured(cfg.dbPath)
	if err != nil {
		return err
	}
	defer closeLevelDB(dbStore, stderr)
	if err := loadLevelDBIfConfigured(trie, dbStore, levelDBLoadPolicy(cfg)); err != nil {
		return err
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
			if err := saveSnapshotIfConfigured(trie, journal, cfg.snapshotPath); err != nil {
				return err
			}
		}
	}
	if cfg.snapshotPath != "" {
		defer func() {
			if err := saveSnapshotIfConfigured(trie, journal, cfg.snapshotPath); err != nil {
				fmt.Fprintf(stderr, "save snapshot: %v\n", err)
			}
		}()
	}
	stopSnapshots := startSnapshotSaver(ctx, trie, journal, cfg.snapshotPath, cfg.snapshotInterval, stderr)
	defer stopSnapshots()

	topology, err := openTopologyIfConfigured(cfg.topologyPath, cfg.nodeID, cfg.monitoringAddr)
	if err != nil {
		return err
	}
	election := hatriecache.NewElectionStore(topology, hatriecache.ElectionOptions{Timeout: cfg.electionTimeout})
	if err := election.Heartbeat(defaultNodeID(cfg.nodeID)); err != nil {
		if cfg.nodeID != "" {
			return err
		}
	}
	var replicator *hatriecache.HTTPReplicator
	if cfg.replication {
		replicator = hatriecache.NewHTTPReplicator(hatriecache.HTTPReplicatorOptions{
			Self:     defaultNodeID(cfg.nodeID),
			Topology: topology,
			Election: election,
		})
	}

	handler := hatriecache.NewMonitoringHandler(trie, hatriecache.MonitoringOptions{
		NodeName:   cfg.nodeID,
		WebDir:     cfg.monitoringWebDir,
		Snapshot:   snapshotCallback(trie, journal, cfg.snapshotPath),
		Journal:    journal,
		Topology:   topology,
		Election:   election,
		Replicator: replicator,
	}).Handler()
	server := &http.Server{
		Addr:      cfg.monitoringAddr,
		Handler:   handler,
		TLSConfig: monitoringTLSConfig(nil),
	}

	grpcServer, grpcListener, err := newGRPCServer(cfg, trie, journal, snapshotCallback(trie, journal, cfg.snapshotPath))
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
		monitoringAddr:   "127.0.0.1:8080",
		monitoringWebDir: "svelte-mpa/dist",
		electionTimeout:  hatriecache.DefaultElectionTimeout,
	}
	flags := flag.NewFlagSet("hatrie-cache", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.BoolVar(&cfg.monitoringServer, "monitoring-server", false, "run the grpc/http2/web monitoring server")
	flags.StringVar(&cfg.monitoringAddr, "monitoring-addr", cfg.monitoringAddr, "monitoring server listen address")
	flags.StringVar(&cfg.monitoringTLSCert, "monitoring-tls-cert", "", "TLS certificate path for HTTPS/HTTP2 monitoring")
	flags.StringVar(&cfg.monitoringTLSKey, "monitoring-tls-key", "", "TLS private key path for HTTPS/HTTP2 monitoring")
	flags.StringVar(&cfg.monitoringWebDir, "monitoring-web-dir", cfg.monitoringWebDir, "directory containing built web monitoring assets")
	flags.StringVar(&cfg.nodeID, "node-id", "", "local cluster node id")
	flags.StringVar(&cfg.topologyPath, "topology-path", "", "optional cluster topology JSON path to load and update")
	flags.DurationVar(&cfg.electionTimeout, "election-timeout", cfg.electionTimeout, "node heartbeat timeout for deterministic topology leader election")
	flags.BoolVar(&cfg.replication, "replication", false, "replicate successful leader writes to topology owners over HTTP")
	flags.StringVar(&cfg.grpcAddr, "grpc-addr", "", "optional native gRPC API listen address")
	flags.StringVar(&cfg.dbPath, "db-path", "", "optional LevelDB path to load on startup and save on shutdown")
	flags.DurationVar(&cfg.dbSyncInterval, "db-sync-interval", 0, "optional periodic LevelDB save interval")
	flags.BoolVar(&cfg.dbHotLoad, "db-hot-load", false, "load cold LevelDB keys as lazy references and hot small values into memory")
	flags.Int64Var(&cfg.dbHotLoadMaxBytes, "db-hot-load-max-bytes", 1024, "maximum value size for LevelDB hot-load")
	flags.DurationVar(&cfg.dbHotLoadMaxAge, "db-hot-load-max-age", time.Hour, "maximum last-hit age for LevelDB hot-load")
	flags.Uint64Var(&cfg.dbHotLoadMinHits, "db-hot-load-min-hits", 1000, "minimum hits required for LevelDB hot-load")
	flags.StringVar(&cfg.snapshotPath, "snapshot-path", "", "optional JSON snapshot path to load on startup and save on shutdown")
	flags.DurationVar(&cfg.snapshotInterval, "snapshot-interval", 0, "optional periodic snapshot interval")
	flags.StringVar(&cfg.journalPath, "journal-path", "", "optional command journal path to replay on startup and append mutating commands")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if (cfg.monitoringTLSCert == "") != (cfg.monitoringTLSKey == "") {
		return config{}, errors.New("monitoring TLS requires both -monitoring-tls-cert and -monitoring-tls-key")
	}
	if cfg.dbHotLoadMaxBytes < 0 {
		return config{}, errors.New("db hot-load max bytes must be non-negative")
	}
	if cfg.dbHotLoadMaxAge < 0 {
		return config{}, errors.New("db hot-load max age must be non-negative")
	}
	return cfg, nil
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

func openLevelDBIfConfigured(path string) (*hatriecache.LevelDBStore, error) {
	if path == "" {
		return nil, nil
	}
	return hatriecache.OpenLevelDBStore(path)
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

func newGRPCServer(cfg config, trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, snapshot func() error) (*grpc.Server, net.Listener, error) {
	if cfg.grpcAddr == "" {
		return nil, nil, nil
	}
	listener, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return nil, nil, err
	}
	server := grpc.NewServer()
	hatriecache.RegisterCacheGRPCServer(server, hatriecache.NewCacheGRPCServer(trie, hatriecache.CacheGRPCOptions{
		Snapshot: snapshot,
		Journal:  journal,
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

func saveSnapshotIfConfigured(trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, path string) error {
	if path == "" {
		return nil
	}
	if journal != nil {
		return journal.SaveSnapshot(trie, path)
	}
	return trie.SaveSnapshot(path)
}

func startSnapshotSaver(ctx context.Context, trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, path string, interval time.Duration, stderr io.Writer) func() {
	if path == "" || interval <= 0 {
		return func() {}
	}

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	save := func() {
		if err := saveSnapshotIfConfigured(trie, journal, path); err != nil {
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

func snapshotCallback(trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, path string) func() error {
	if path == "" {
		return nil
	}
	return func() error {
		return saveSnapshotIfConfigured(trie, journal, path)
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
