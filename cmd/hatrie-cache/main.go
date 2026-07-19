package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	stdjson "encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	json "github.com/goccy/go-json"

	hatriecache "hatrie_cache"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

const (
	serverShutdownTimeout              = 5 * time.Second
	defaultMonitoringReadHeaderTimeout = 5 * time.Second
	defaultMonitoringIdleTimeout       = 2 * time.Minute
	destroyedHatTriePanic              = "hatriecache: use of destroyed HatTrie"
	replicationModeJournal             = "journal"
	replicationModeCommand             = "command"
	replicationModeDual                = "dual"
	configProfileDev                   = "dev"
	configProfileProduction            = "production"
	configProfileBench                 = "bench"
)

var errHatTrieDestroyed = errors.New("hatrie-cache: trie is destroyed")

type config struct {
	configPath                  string
	configProfile               string
	checkConfig                 bool
	printConfig                 bool
	monitoringServer            bool
	monitoringAddr              string
	monitoringTLSCert           string
	monitoringTLSKey            string
	monitoringAuthToken         string
	auditLogPath                string
	writeProtection             bool
	rateLimit                   int
	keyStatsMode                string
	keyStatsCapacity            int
	monitoringWebDir            string
	monitoringReadHeaderTimeout time.Duration
	monitoringIdleTimeout       time.Duration
	nodeID                      string
	topologyPath                string
	electionTimeout             time.Duration
	replication                 bool
	replicationMode             string
	replicationAsync            bool
	replicationQueueSize        int
	replicationRetry            time.Duration
	replicationAttempts         uint
	replicationDeadLetterLimit  int
	replicationOutboxPath       string
	replicationOutboxFormat     string
	replicationOutboxCodec      string
	replicationOutboxBatch      time.Duration
	replicationCircuitFailures  int
	replicationCircuitCooldown  time.Duration
	replicationWireFormat       string
	replicationTransport        string
	replicationGRPCWindow       int
	replicationHTTPFallback     bool
	replicationAuthToken        string
	replicationBatchMaxBytes    int
	replicationMaxTargets       int
	replicationSyncInterval     time.Duration
	replicationSyncPrefix       string
	enforceLeaderWrites         bool
	grpcAddr                    string
	grpcTLSCert                 string
	grpcTLSKey                  string
	grpcClientCA                string
	dbPath                      string
	dbFormat                    string
	dbSyncInterval              time.Duration
	dbCompareBeforeWrite        string
	dbCompactInterval           time.Duration
	dbCompactStartKey           string
	dbCompactLimitKey           string
	dbHotLoad                   bool
	dbHotLoadMaxBytes           int64
	dbHotLoadMaxAge             time.Duration
	dbHotLoadMinHits            uint64
	dbMemoryCapBytes            int64
	dbRSSCapBytes               int64
	dbMemoryEvictInterval       time.Duration
	dbMemoryEvictMinValueBytes  int64
	snapshotPath                string
	snapshotInterval            time.Duration
	snapshotFormat              string
	journalPath                 string
	journalFormat               string
	journalGroupCommitWindow    time.Duration
	journalGroupCommitMaxBatch  int
	journalPullSource           string
	journalPullStatePath        string
	journalPullInterval         time.Duration
	journalPullTimeout          time.Duration
	journalPullLimit            uint64
	journalPullMaxBatches       uint64
	journalPullFullSyncFallback bool
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func serverContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func diagnosticWriter(writer io.Writer) io.Writer {
	if writer == nil {
		return io.Discard
	}
	return writer
}

func run(ctx context.Context, args []string, stdout io.Writer, stderr io.Writer) error {
	ctx = serverContext(ctx)
	stdout = diagnosticWriter(stdout)
	stderr = diagnosticWriter(stderr)
	cfg, err := parseConfig(args, stderr)
	if err != nil {
		return err
	}
	if cfg.checkConfig {
		if err := validateConfigReferences(cfg); err != nil {
			return err
		}
		if cfg.printConfig {
			return writeRedactedConfig(stdout, cfg)
		}
		fmt.Fprintln(stdout, "configuration ok")
		return nil
	}
	if cfg.printConfig {
		return writeRedactedConfig(stdout, cfg)
	}
	if !cfg.monitoringServer {
		fmt.Fprintln(stdout, "monitoring server disabled; pass -monitoring-server to start it")
		return nil
	}
	auditLog, err := openAuditLogIfConfigured(cfg.auditLogPath)
	if err != nil {
		return err
	}
	defer closeAuditLog(auditLog, stderr)
	rateLimiter := hatriecache.NewRateLimiter(cfg.rateLimit, time.Second)
	apiMetrics := hatriecache.NewAPIMetrics()
	replicationSafety := hatriecache.NewReplicationSafetyStore()

	trie := hatriecache.CreateHatTrie()
	defer trie.Destroy()
	if err := trie.ConfigureKeyStats(hatriecache.KeyStatsMode(cfg.keyStatsMode), cfg.keyStatsCapacity); err != nil {
		return err
	}

	dbStore, err := openLevelDBIfConfigured(cfg.dbPath, storageFormat(cfg))
	if err != nil {
		return err
	}
	defer closeLevelDB(dbStore, stderr)
	var levelDBDirtyTracker *hatriecache.LevelDBDirtyTracker
	if dbStore != nil {
		levelDBDirtyTracker = hatriecache.NewLevelDBDirtyTracker()
	}
	if err := loadLevelDBIfConfigured(trie, dbStore, levelDBLoadPolicy(cfg)); err != nil {
		return err
	}

	journal, err := openJournalIfConfiguredWithOptions(cfg.journalPath, journalOptions(cfg))
	if err != nil {
		return err
	}
	defer closeJournal(journal, stderr)
	if cfg.journalPullSource != "" && journal == nil {
		return errors.New("journal pull requires -journal-path")
	}
	if cfg.journalPullSource != "" && cfg.journalPullStatePath == "" {
		cfg.journalPullStatePath = cfg.journalPath + ".pull_state.json"
	}
	if cfg.journalPullSource != "" {
		if _, err := loadJournalPullState(cfg.journalPullStatePath, cfg.journalPullSource); err != nil {
			return err
		}
	}
	pullSnapshotPath := journalPullSnapshotPath(cfg.journalPullStatePath, cfg.journalPullSource)
	snapshotMetadata, err := loadStartupSnapshot(trie, journal, cfg.snapshotPath, pullSnapshotPath)
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
	stopDBSync := startLevelDBSaver(ctx, trie, dbStore, levelDBDirtyTracker, cfg.dbSyncInterval, levelDBSaveOptions(cfg), stderr)
	defer stopDBSync()
	stopSnapshots := startSnapshotSaver(ctx, trie, journal, cfg.snapshotPath, cfg.snapshotInterval, snapshotFormat(cfg), stderr)
	defer stopSnapshots()
	var persistFullSync func() error
	if dbStore != nil {
		persistFullSync = func() error { return dbStore.Save(trie) }
	}
	stopJournalPuller := startJournalPuller(ctx, trie, journal, journalPullerConfig{
		Source:           cfg.journalPullSource,
		StatePath:        cfg.journalPullStatePath,
		Interval:         cfg.journalPullInterval,
		Timeout:          cfg.journalPullTimeout,
		Limit:            cfg.journalPullLimit,
		MaxBatches:       cfg.journalPullMaxBatches,
		Dirty:            levelDBDirtyTracker,
		FullSyncFallback: cfg.journalPullFullSyncFallback,
		AuthToken:        cfg.replicationAuthToken,
		SnapshotPath:     pullSnapshotPath,
		PersistFullSync:  persistFullSync,
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
	if cfg.replication && replicationModeUsesCommandFanout(cfg.replicationMode) {
		replicationOutbox, err := openReplicationOutboxIfConfigured(cfg.replicationOutboxPath, cfg.replicationOutboxFormat, cfg.replicationOutboxCodec, cfg.replicationOutboxBatch)
		if err != nil {
			return err
		}
		defer closeReplicationOutbox(replicationOutbox, stderr)
		replicator = hatriecache.NewHTTPReplicator(hatriecache.HTTPReplicatorOptions{
			Context:                  ctx,
			Self:                     defaultNodeID(cfg.nodeID),
			Topology:                 topology,
			Election:                 election,
			AsyncQueueSize:           replicationQueueSize(cfg),
			AsyncRetryInterval:       cfg.replicationRetry,
			AsyncMaxAttempts:         cfg.replicationAttempts,
			AsyncDeadLetterLimit:     cfg.replicationDeadLetterLimit,
			AsyncOutbox:              replicationOutbox,
			CircuitBreakerFailures:   cfg.replicationCircuitFailures,
			CircuitBreakerCooldown:   cfg.replicationCircuitCooldown,
			WireFormat:               replicationWireFormat(cfg),
			Transport:                replicationTransport(cfg),
			GRPCStreamWindow:         cfg.replicationGRPCWindow,
			DisableHTTPFallback:      !cfg.replicationHTTPFallback,
			AuthToken:                cfg.replicationAuthToken,
			ReplicationBatchMaxBytes: replicationBatchLimit(cfg),
			MaxInFlightTargets:       cfg.replicationMaxTargets,
		})
		defer replicator.Close()
	}
	stopReplicationSyncer := startReplicationSyncer(ctx, trie, replicator, cfg.replicationSyncInterval, cfg.replicationSyncPrefix, stderr)
	defer stopReplicationSyncer()

	monitoringHandler := hatriecache.NewMonitoringHandler(trie, hatriecache.MonitoringOptions{
		NodeName:             defaultNodeID(cfg.nodeID),
		WebDir:               cfg.monitoringWebDir,
		AuthToken:            cfg.monitoringAuthToken,
		ReplicationAuthToken: cfg.replicationAuthToken,
		AuditLog:             auditLog,
		WriteProtected:       cfg.writeProtection,
		RateLimiter:          rateLimiter,
		Metrics:              apiMetrics,
		Snapshot:             snapshotCallback(trie, journal, cfg.snapshotPath, snapshotFormat(cfg)),
		LevelDBStore:         dbStore,
		LevelDBDirtyTracker:  levelDBDirtyTracker,
		BackupSnapshotFormat: snapshotFormat(cfg),
		Journal:              journal,
		Topology:             topology,
		Election:             election,
		Replicator:           replicator,
		ReplicationSafety:    replicationSafety,
		EnforceLeaderWrites:  cfg.enforceLeaderWrites,
		RuntimeConfig:        redactedConfig(cfg),
	})
	stopDBCompactor := startLevelDBCompactor(ctx, dbStore, cfg.dbCompactInterval, levelDBCompactorOptions{
		StartKey: cfg.dbCompactStartKey,
		LimitKey: cfg.dbCompactLimitKey,
	}, monitoringHandler.RecordStorageCompact, stderr)
	defer stopDBCompactor()
	stopDBMemoryGovernor := startLevelDBMemoryGovernor(ctx, trie, dbStore, cfg.dbMemoryEvictInterval, levelDBMemoryGovernorOptions{
		Spill: hatriecache.LevelDBSpillOptions{
			MaxHotBytes:   cfg.dbMemoryCapBytes,
			MinValueBytes: cfg.dbMemoryEvictMinValueBytes,
		},
		RSSCapBytes: uint64(cfg.dbRSSCapBytes),
		RSS:         processRSSBytes,
	}, monitoringHandler.RecordStorageSpill, stderr)
	defer stopDBMemoryGovernor()
	handler := monitoringHandler.Handler()
	server := newMonitoringServer(cfg, handler)

	grpcServer, grpcListener, err := newGRPCServer(cfg, trie, journal, snapshotCallback(trie, journal, cfg.snapshotPath, snapshotFormat(cfg)), topology, election, replicator, auditLog, rateLimiter, apiMetrics, replicationSafety, levelDBDirtyTracker)
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
		keyStatsMode:                string(hatriecache.DefaultKeyStatsMode),
		keyStatsCapacity:            hatriecache.DefaultKeyStatsCapacity,
		electionTimeout:             hatriecache.DefaultElectionTimeout,
		replicationMode:             replicationModeJournal,
		replicationWireFormat:       string(hatriecache.DefaultCommandWireFormat),
		replicationTransport:        string(hatriecache.ReplicationTransportHTTP),
		replicationGRPCWindow:       hatriecache.DefaultReplicationGRPCStreamWindow,
		replicationHTTPFallback:     true,
		replicationBatchMaxBytes:    hatriecache.DefaultReplicationBatchMaxBytes,
		replicationMaxTargets:       hatriecache.DefaultReplicationMaxInFlightTargets,
		journalPullTimeout:          hatriecache.DefaultCommandJournalPullTimeout,
		journalPullFullSyncFallback: true,
		dbFormat:                    string(hatriecache.DefaultStorageFormat),
		dbCompareBeforeWrite:        string(hatriecache.DefaultLevelDBCompareBeforeWriteMode),
		snapshotFormat:              string(hatriecache.DefaultSnapshotFormat),
		journalFormat:               string(hatriecache.DefaultCommandJournalFormat),
		journalGroupCommitWindow:    hatriecache.DefaultJournalGroupCommitWindow,
		journalGroupCommitMaxBatch:  hatriecache.DefaultJournalGroupCommitMaxBatch,
	}
	configPath, err := configPathFromArgs(args)
	if err != nil {
		return config{}, err
	}
	configProfile, err := configProfileFromArgs(args)
	if err != nil {
		return config{}, err
	}
	if configPath != "" {
		fileProfile, err := configProfileFromFile(configPath)
		if err != nil {
			return config{}, err
		}
		if configProfile == "" {
			configProfile = fileProfile
		}
	}
	if configProfile != "" {
		cfg, err = applyConfigProfileDefaults(cfg, configProfile)
		if err != nil {
			return config{}, err
		}
	}
	cfg.configPath = configPath
	flags := flag.NewFlagSet("hatrie-cache", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&cfg.configPath, "config", cfg.configPath, "optional JSON config file path")
	flags.StringVar(&cfg.configPath, "config-file", cfg.configPath, "optional JSON config file path")
	flags.StringVar(&cfg.configProfile, "profile", cfg.configProfile, "optional sane config profile: dev, production, or bench")
	flags.StringVar(&cfg.configProfile, "config-profile", cfg.configProfile, "optional sane config profile: dev, production, or bench")
	flags.BoolVar(&cfg.checkConfig, "check-config", false, "validate configuration and exit without starting listeners")
	flags.BoolVar(&cfg.printConfig, "print-config", false, "print effective redacted configuration and exit without starting listeners")
	flags.BoolVar(&cfg.monitoringServer, "monitoring-server", cfg.monitoringServer, "run the grpc/http2/web monitoring server")
	flags.StringVar(&cfg.monitoringAddr, "monitoring-addr", cfg.monitoringAddr, "monitoring server listen address")
	flags.StringVar(&cfg.monitoringTLSCert, "monitoring-tls-cert", "", "TLS certificate path for HTTPS/HTTP2 monitoring")
	flags.StringVar(&cfg.monitoringTLSKey, "monitoring-tls-key", "", "TLS private key path for HTTPS/HTTP2 monitoring")
	flags.StringVar(&cfg.monitoringAuthToken, "monitoring-auth-token", "", "optional bearer token required for monitoring API endpoints")
	flags.StringVar(&cfg.auditLogPath, "audit-log-path", "", "optional JSONL audit log path for dangerous monitoring API actions")
	flags.BoolVar(&cfg.writeProtection, "write-protection", cfg.writeProtection, "reject dangerous monitoring API writes")
	flags.IntVar(&cfg.rateLimit, "rate-limit", cfg.rateLimit, "maximum dangerous monitoring API actions per caller per second; use 0 to disable")
	flags.StringVar(&cfg.keyStatsMode, "key-stats-mode", cfg.keyStatsMode, "per-key telemetry retention: bounded, full, or off")
	flags.IntVar(&cfg.keyStatsCapacity, "key-stats-capacity", cfg.keyStatsCapacity, "maximum keys with retained telemetry in bounded mode")
	flags.StringVar(&cfg.monitoringWebDir, "monitoring-web-dir", cfg.monitoringWebDir, "directory containing built web monitoring assets")
	flags.DurationVar(&cfg.monitoringReadHeaderTimeout, "monitoring-read-header-timeout", cfg.monitoringReadHeaderTimeout, "maximum time to read monitoring HTTP request headers; use 0 to disable")
	flags.DurationVar(&cfg.monitoringIdleTimeout, "monitoring-idle-timeout", cfg.monitoringIdleTimeout, "maximum idle monitoring HTTP keep-alive time; use 0 to disable")
	flags.StringVar(&cfg.nodeID, "node-id", "", "local cluster node id")
	flags.StringVar(&cfg.topologyPath, "topology-path", "", "optional cluster topology JSON path to load and update")
	flags.DurationVar(&cfg.electionTimeout, "election-timeout", cfg.electionTimeout, "node heartbeat timeout for deterministic topology leader election")
	flags.BoolVar(&cfg.replication, "replication", cfg.replication, "enable the configured replication mode")
	flags.StringVar(&cfg.replicationMode, "replication-mode", cfg.replicationMode, "replication mode: journal, command, or dual")
	flags.BoolVar(&cfg.replicationAsync, "replication-async", cfg.replicationAsync, "queue successful leader-write replication in a bounded async worker")
	flags.IntVar(&cfg.replicationQueueSize, "replication-queue-size", 1024, "maximum queued async replication jobs")
	flags.DurationVar(&cfg.replicationRetry, "replication-retry-interval", hatriecache.DefaultReplicationRetryInterval, "delay between async replication retry attempts")
	flags.UintVar(&cfg.replicationAttempts, "replication-max-attempts", 3, "maximum async replication delivery attempts")
	flags.IntVar(&cfg.replicationDeadLetterLimit, "replication-dead-letter-limit", hatriecache.DefaultReplicationDeadLetterLimit, "maximum retained async replication dead-letter failures; use 0 to disable")
	flags.StringVar(&cfg.replicationOutboxPath, "replication-outbox-path", "", "optional durable async replication outbox path")
	flags.StringVar(&cfg.replicationOutboxFormat, "replication-outbox-format", "auto", "durable replication outbox backend: auto, json, or leveldb")
	flags.StringVar(&cfg.replicationOutboxCodec, "replication-outbox-codec", string(hatriecache.ReplicationOutboxCodecBinary), "LevelDB replication outbox record codec: binary or json")
	flags.DurationVar(&cfg.replicationOutboxBatch, "replication-outbox-batch-window", hatriecache.DefaultReplicationOutboxBatchWindow, "maximum delay for grouping concurrent durable outbox writes; use 0 to disable")
	flags.IntVar(&cfg.replicationCircuitFailures, "replication-circuit-breaker-failures", hatriecache.DefaultReplicationCircuitBreakerFailures, "consecutive per-target replication failures before opening the circuit breaker; use 0 to disable")
	flags.DurationVar(&cfg.replicationCircuitCooldown, "replication-circuit-breaker-cooldown", hatriecache.DefaultReplicationCircuitBreakerCooldown, "per-target replication circuit breaker cooldown before a half-open probe; use 0 to disable")
	flags.StringVar(&cfg.replicationWireFormat, "replication-wire-format", cfg.replicationWireFormat, "HTTP replication command wire format: protobuf or json")
	flags.StringVar(&cfg.replicationTransport, "replication-transport", cfg.replicationTransport, "live and anti-entropy replication transport: http or grpc-stream")
	flags.IntVar(&cfg.replicationGRPCWindow, "replication-grpc-window", cfg.replicationGRPCWindow, "maximum unacknowledged batches per live gRPC replication target")
	flags.BoolVar(&cfg.replicationHTTPFallback, "replication-http-fallback", cfg.replicationHTTPFallback, "fall back to HTTP when a gRPC replication stream cannot be used")
	flags.StringVar(&cfg.replicationAuthToken, "replication-auth-token", "", "optional bearer token sent on HTTP replication and accepted only for internal replication commands")
	flags.IntVar(&cfg.replicationBatchMaxBytes, "replication-batch-max-bytes", cfg.replicationBatchMaxBytes, "maximum estimated bytes per HTTP replication batch; use 0 to disable batch splitting")
	flags.IntVar(&cfg.replicationMaxTargets, "replication-max-in-flight-targets", cfg.replicationMaxTargets, "maximum concurrent HTTP replication targets; use 1 for serial delivery")
	flags.DurationVar(&cfg.replicationSyncInterval, "replication-sync-interval", 0, "optional periodic anti-entropy replication sync interval; use 0 to disable")
	flags.StringVar(&cfg.replicationSyncPrefix, "replication-sync-prefix", "", "optional key prefix for periodic anti-entropy replication sync")
	flags.BoolVar(&cfg.enforceLeaderWrites, "enforce-leader-writes", false, "reject mutating client commands when this node is not the elected key leader")
	flags.StringVar(&cfg.grpcAddr, "grpc-addr", "", "optional native gRPC API listen address")
	flags.StringVar(&cfg.grpcTLSCert, "grpc-tls-cert", "", "TLS certificate path for the native gRPC API")
	flags.StringVar(&cfg.grpcTLSKey, "grpc-tls-key", "", "TLS private key path for the native gRPC API")
	flags.StringVar(&cfg.grpcClientCA, "grpc-client-ca", "", "optional client CA PEM path; when set, native gRPC requires mTLS client certificates")
	flags.StringVar(&cfg.dbPath, "db-path", cfg.dbPath, "optional LevelDB path to load on startup and save on shutdown")
	flags.StringVar(&cfg.dbFormat, "db-format", cfg.dbFormat, "LevelDB record storage format: binary or json")
	flags.DurationVar(&cfg.dbSyncInterval, "db-sync-interval", cfg.dbSyncInterval, "optional periodic LevelDB save interval")
	flags.StringVar(&cfg.dbCompareBeforeWrite, "db-compare-before-write", cfg.dbCompareBeforeWrite, "dirty LevelDB save compare mode: auto, always, or never")
	flags.DurationVar(&cfg.dbCompactInterval, "db-compact-interval", cfg.dbCompactInterval, "optional periodic LevelDB compaction interval")
	flags.StringVar(&cfg.dbCompactStartKey, "db-compact-start-key", "", "first cache key to include in periodic LevelDB compaction")
	flags.StringVar(&cfg.dbCompactLimitKey, "db-compact-limit-key", "", "first cache key after the periodic LevelDB compaction range")
	flags.BoolVar(&cfg.dbHotLoad, "db-hot-load", cfg.dbHotLoad, "load cold LevelDB keys as lazy references and hot small values into memory")
	flags.Int64Var(&cfg.dbHotLoadMaxBytes, "db-hot-load-max-bytes", 1024, "maximum value size for LevelDB hot-load")
	flags.DurationVar(&cfg.dbHotLoadMaxAge, "db-hot-load-max-age", time.Hour, "maximum last-hit age for LevelDB hot-load")
	flags.Uint64Var(&cfg.dbHotLoadMinHits, "db-hot-load-min-hits", 1000, "minimum hits required for LevelDB hot-load")
	flags.Int64Var(&cfg.dbMemoryCapBytes, "db-memory-cap-bytes", 0, "estimated hot value bytes cap for periodic LevelDB cold eviction; use 0 to disable")
	flags.Int64Var(&cfg.dbRSSCapBytes, "db-rss-cap-bytes", 0, "process RSS bytes threshold that triggers periodic LevelDB cold eviction; use 0 to disable")
	flags.DurationVar(&cfg.dbMemoryEvictInterval, "db-memory-evict-interval", 0, "periodic LevelDB cold eviction interval; use 0 to disable")
	flags.Int64Var(&cfg.dbMemoryEvictMinValueBytes, "db-memory-evict-min-value-bytes", 1024, "minimum estimated value bytes eligible for LevelDB cold eviction")
	flags.StringVar(&cfg.snapshotPath, "snapshot-path", cfg.snapshotPath, "optional snapshot path to load on startup and save on shutdown")
	flags.DurationVar(&cfg.snapshotInterval, "snapshot-interval", cfg.snapshotInterval, "optional periodic snapshot interval")
	flags.StringVar(&cfg.snapshotFormat, "snapshot-format", cfg.snapshotFormat, "snapshot save format: gzip-best-binary, gzip-binary, binary, gzip-best-json, gzip-json, or json")
	flags.StringVar(&cfg.journalPath, "journal-path", cfg.journalPath, "optional command journal path to replay on startup and append mutating commands")
	flags.StringVar(&cfg.journalFormat, "journal-format", cfg.journalFormat, "command journal write format: binary or json")
	flags.DurationVar(&cfg.journalGroupCommitWindow, "journal-group-commit-window", cfg.journalGroupCommitWindow, "maximum journal group commit wait; zero batches only already queued callers")
	flags.IntVar(&cfg.journalGroupCommitMaxBatch, "journal-group-commit-max-batch", cfg.journalGroupCommitMaxBatch, "maximum commands per durable journal group commit; use 1 for immediate fsync")
	flags.StringVar(&cfg.journalPullSource, "journal-pull-source", "", "optional source monitoring URL to pull journal catch-up batches from")
	flags.StringVar(&cfg.journalPullStatePath, "journal-pull-state-path", "", "optional JSON path for persisted journal pull source sequence")
	flags.DurationVar(&cfg.journalPullInterval, "journal-pull-interval", cfg.journalPullInterval, "optional interval for repeated journal pull catch-up")
	flags.DurationVar(&cfg.journalPullTimeout, "journal-pull-timeout", cfg.journalPullTimeout, "HTTP timeout for each journal pull request; use 0 to disable")
	flags.Uint64Var(&cfg.journalPullLimit, "journal-pull-limit", 0, "maximum entries per journal pull batch")
	flags.Uint64Var(&cfg.journalPullMaxBatches, "journal-pull-max-batches", 0, "maximum batches per journal pull attempt")
	flags.BoolVar(&cfg.journalPullFullSyncFallback, "journal-pull-full-sync-fallback", cfg.journalPullFullSyncFallback, "request an exact full snapshot when journal deltas were compacted")
	if configPath != "" {
		if err := applyConfigFile(configPath, flags); err != nil {
			return config{}, err
		}
	}
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	configProfile, err = parseConfigProfile(cfg.configProfile)
	if err != nil {
		return config{}, err
	}
	cfg.configProfile = configProfile
	if (cfg.monitoringTLSCert == "") != (cfg.monitoringTLSKey == "") {
		return config{}, errors.New("monitoring TLS requires both -monitoring-tls-cert and -monitoring-tls-key")
	}
	if (cfg.grpcTLSCert == "") != (cfg.grpcTLSKey == "") {
		return config{}, errors.New("gRPC TLS requires both -grpc-tls-cert and -grpc-tls-key")
	}
	if cfg.grpcClientCA != "" && cfg.grpcTLSCert == "" {
		return config{}, errors.New("gRPC client CA requires -grpc-tls-cert and -grpc-tls-key")
	}
	if cfg.monitoringReadHeaderTimeout < 0 {
		return config{}, errors.New("monitoring read header timeout must be non-negative")
	}
	if cfg.monitoringIdleTimeout < 0 {
		return config{}, errors.New("monitoring idle timeout must be non-negative")
	}
	if cfg.rateLimit < 0 {
		return config{}, errors.New("rate limit must be non-negative")
	}
	cfg.keyStatsMode = strings.ToLower(strings.TrimSpace(cfg.keyStatsMode))
	switch hatriecache.KeyStatsMode(cfg.keyStatsMode) {
	case hatriecache.KeyStatsModeBounded:
		if cfg.keyStatsCapacity <= 0 {
			return config{}, errors.New("bounded key stats capacity must be positive")
		}
	case hatriecache.KeyStatsModeFull, hatriecache.KeyStatsModeOff:
		cfg.keyStatsCapacity = 0
	default:
		return config{}, errors.New("key stats mode must be bounded, full, or off")
	}
	if cfg.dbHotLoadMaxBytes < 0 {
		return config{}, errors.New("db hot-load max bytes must be non-negative")
	}
	if cfg.dbHotLoadMaxAge < 0 {
		return config{}, errors.New("db hot-load max age must be non-negative")
	}
	if cfg.dbCompactInterval < 0 {
		return config{}, errors.New("db compact interval must be non-negative")
	}
	if cfg.dbMemoryCapBytes < 0 {
		return config{}, errors.New("db memory cap bytes must be non-negative")
	}
	if cfg.dbRSSCapBytes < 0 {
		return config{}, errors.New("db rss cap bytes must be non-negative")
	}
	if cfg.dbMemoryEvictInterval < 0 {
		return config{}, errors.New("db memory evict interval must be non-negative")
	}
	if cfg.dbMemoryEvictMinValueBytes < 0 {
		return config{}, errors.New("db memory evict min value bytes must be non-negative")
	}
	if cfg.dbMemoryEvictInterval > 0 && cfg.dbMemoryCapBytes == 0 && cfg.dbRSSCapBytes == 0 {
		return config{}, errors.New("db memory evict interval requires positive -db-memory-cap-bytes or -db-rss-cap-bytes")
	}
	if cfg.dbMemoryEvictInterval > 0 && strings.TrimSpace(cfg.dbPath) == "" {
		return config{}, errors.New("db memory eviction requires -db-path")
	}
	if cfg.replicationQueueSize < 0 {
		return config{}, errors.New("replication queue size must be non-negative")
	}
	replicationMode, err := parseReplicationMode(cfg.replicationMode)
	if err != nil {
		return config{}, err
	}
	cfg.replicationMode = replicationMode
	if cfg.replication && replicationModeUsesJournal(cfg.replicationMode) && strings.TrimSpace(cfg.journalPath) == "" {
		return config{}, errors.New("journal replication mode requires -journal-path")
	}
	if cfg.replicationAsync && !replicationModeUsesCommandFanout(cfg.replicationMode) {
		return config{}, errors.New("replication async requires -replication-mode command or dual")
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
	if cfg.replicationDeadLetterLimit < 0 {
		return config{}, errors.New("replication dead-letter limit must be non-negative")
	}
	if cfg.replicationOutboxPath != "" && (!cfg.replication || !cfg.replicationAsync || !replicationModeUsesCommandFanout(cfg.replicationMode)) {
		return config{}, errors.New("replication outbox path requires -replication, -replication-mode command or dual, and -replication-async")
	}
	if _, err := parseReplicationOutboxFormat(cfg.replicationOutboxFormat); err != nil {
		return config{}, err
	}
	if _, err := hatriecache.ParseReplicationOutboxCodec(cfg.replicationOutboxCodec); err != nil {
		return config{}, err
	}
	if cfg.replicationOutboxBatch < 0 {
		return config{}, errors.New("replication outbox batch window must be non-negative")
	}
	if cfg.replicationCircuitFailures < 0 {
		return config{}, errors.New("replication circuit breaker failures must be non-negative")
	}
	if cfg.replicationCircuitCooldown < 0 {
		return config{}, errors.New("replication circuit breaker cooldown must be non-negative")
	}
	if cfg.replicationSyncInterval < 0 {
		return config{}, errors.New("replication sync interval must be non-negative")
	}
	if cfg.replicationSyncInterval > 0 && !cfg.replication {
		return config{}, errors.New("replication sync interval requires -replication")
	}
	if cfg.replicationSyncInterval > 0 && !replicationModeUsesCommandFanout(cfg.replicationMode) {
		return config{}, errors.New("replication sync interval requires -replication-mode command or dual")
	}
	if cfg.journalPullTimeout < 0 {
		return config{}, errors.New("journal pull timeout must be non-negative")
	}
	if cfg.journalGroupCommitWindow < 0 {
		return config{}, errors.New("journal group commit window must be non-negative")
	}
	if cfg.journalGroupCommitMaxBatch < 1 {
		return config{}, errors.New("journal group commit max batch must be positive")
	}
	if cfg.journalGroupCommitMaxBatch > hatriecache.MaxJournalGroupCommitBatch {
		return config{}, fmt.Errorf("journal group commit max batch must be <= %d", hatriecache.MaxJournalGroupCommitBatch)
	}
	if _, err := hatriecache.ParseCommandWireFormat(cfg.replicationWireFormat); err != nil {
		return config{}, err
	}
	if _, err := hatriecache.ParseReplicationTransport(cfg.replicationTransport); err != nil {
		return config{}, err
	}
	if cfg.replicationGRPCWindow < 1 || cfg.replicationGRPCWindow > hatriecache.MaxReplicationGRPCStreamWindow {
		return config{}, fmt.Errorf("replication gRPC window must be between 1 and %d", hatriecache.MaxReplicationGRPCStreamWindow)
	}
	if cfg.replicationBatchMaxBytes < 0 {
		return config{}, errors.New("replication batch max bytes must be non-negative")
	}
	if cfg.replicationMaxTargets < 1 {
		return config{}, errors.New("replication max in-flight targets must be positive")
	}
	if _, err := hatriecache.ParseStorageFormat(cfg.dbFormat); err != nil {
		return config{}, err
	}
	if _, err := hatriecache.ParseLevelDBCompareBeforeWriteMode(cfg.dbCompareBeforeWrite); err != nil {
		return config{}, err
	}
	if _, err := hatriecache.ParseSnapshotFormat(cfg.snapshotFormat); err != nil {
		return config{}, err
	}
	if _, err := hatriecache.ParseCommandJournalFormat(cfg.journalFormat); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func applyConfigProfileDefaults(cfg config, profile string) (config, error) {
	profile, err := parseConfigProfile(profile)
	if err != nil {
		return config{}, err
	}
	cfg.configProfile = profile
	switch profile {
	case "":
		return cfg, nil
	case configProfileDev:
		cfg.monitoringServer = true
		cfg.monitoringAddr = "127.0.0.1:8080"
		cfg.rateLimit = 0
		cfg.writeProtection = false
	case configProfileProduction:
		cfg.monitoringServer = true
		cfg.monitoringAddr = "0.0.0.0:8080"
		cfg.rateLimit = 100
		cfg.writeProtection = false
		cfg.dbPath = "data/cache.leveldb"
		cfg.dbFormat = string(hatriecache.DefaultStorageFormat)
		cfg.dbSyncInterval = 30 * time.Second
		cfg.dbCompareBeforeWrite = string(hatriecache.DefaultLevelDBCompareBeforeWriteMode)
		cfg.dbCompactInterval = 10 * time.Minute
		cfg.dbHotLoad = true
		cfg.snapshotPath = "data/snapshot.hc"
		cfg.snapshotInterval = 5 * time.Minute
		cfg.snapshotFormat = string(hatriecache.DefaultSnapshotFormat)
		cfg.journalPath = "data/commands.journal"
		cfg.journalFormat = string(hatriecache.DefaultCommandJournalFormat)
		cfg.replicationWireFormat = string(hatriecache.DefaultCommandWireFormat)
		cfg.replicationTransport = string(hatriecache.ReplicationTransportHTTP)
		cfg.replicationGRPCWindow = hatriecache.DefaultReplicationGRPCStreamWindow
		cfg.replicationHTTPFallback = true
		cfg.replicationBatchMaxBytes = hatriecache.DefaultReplicationBatchMaxBytes
		cfg.replicationMaxTargets = hatriecache.DefaultReplicationMaxInFlightTargets
	case configProfileBench:
		cfg.monitoringServer = true
		cfg.monitoringAddr = "127.0.0.1:8080"
		cfg.rateLimit = 0
		cfg.writeProtection = false
		cfg.replication = false
		cfg.replicationAsync = false
		cfg.dbPath = ""
		cfg.dbSyncInterval = 0
		cfg.dbCompactInterval = 0
		cfg.dbHotLoad = false
		cfg.snapshotPath = ""
		cfg.snapshotInterval = 0
		cfg.journalPath = ""
		cfg.journalPullSource = ""
		cfg.journalPullInterval = 0
	}
	return cfg, nil
}

func parseConfigProfile(value string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "":
		return "", nil
	case configProfileDev, "development":
		return configProfileDev, nil
	case configProfileProduction, "prod":
		return configProfileProduction, nil
	case configProfileBench, "benchmark":
		return configProfileBench, nil
	default:
		return "", errors.New("config profile must be dev, production, or bench")
	}
}

func configPathFromArgs(args []string) (string, error) {
	var path string
outer:
	for idx := 0; idx < len(args); idx++ {
		arg := args[idx]
		if arg == "--" {
			break
		}
		for _, name := range []string{"-config", "--config", "-config-file", "--config-file"} {
			if arg == name {
				if idx+1 >= len(args) {
					return "", fmt.Errorf("%s requires a value", name)
				}
				idx++
				if strings.TrimSpace(args[idx]) == "" {
					return "", fmt.Errorf("%s requires a non-empty value", name)
				}
				path = args[idx]
				continue outer
			}
			if strings.HasPrefix(arg, name+"=") {
				value := strings.TrimSpace(strings.TrimPrefix(arg, name+"="))
				if value == "" {
					return "", fmt.Errorf("%s requires a non-empty value", name)
				}
				path = value
			}
		}
	}
	return path, nil
}

func configProfileFromArgs(args []string) (string, error) {
	var profile string
outer:
	for idx := 0; idx < len(args); idx++ {
		arg := args[idx]
		if arg == "--" {
			break
		}
		for _, name := range []string{"-profile", "--profile", "-config-profile", "--config-profile"} {
			if arg == name {
				if idx+1 >= len(args) {
					return "", fmt.Errorf("%s requires a value", name)
				}
				idx++
				value, err := parseConfigProfile(args[idx])
				if err != nil {
					return "", err
				}
				profile = value
				continue outer
			}
			if strings.HasPrefix(arg, name+"=") {
				value, err := parseConfigProfile(strings.TrimPrefix(arg, name+"="))
				if err != nil {
					return "", err
				}
				profile = value
			}
		}
	}
	return profile, nil
}

func configProfileFromFile(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read config file %s: %w", path, err)
	}
	values := make(map[string]stdjson.RawMessage)
	if err := stdjson.Unmarshal(data, &values); err != nil {
		return "", fmt.Errorf("parse config file %s: %w", path, err)
	}
	for _, key := range []string{"config_profile", "config-profile", "profile"} {
		raw, ok := values[key]
		if !ok {
			continue
		}
		value, err := configFlagValueString(raw)
		if err != nil {
			return "", fmt.Errorf("config file %s: option %q: %w", path, key, err)
		}
		profile, err := parseConfigProfile(value)
		if err != nil {
			return "", fmt.Errorf("config file %s: option %q: %w", path, key, err)
		}
		return profile, nil
	}
	return "", nil
}

func applyConfigFile(path string, flags *flag.FlagSet) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file %s: %w", path, err)
	}
	values := make(map[string]stdjson.RawMessage)
	if err := stdjson.Unmarshal(data, &values); err != nil {
		return fmt.Errorf("parse config file %s: %w", path, err)
	}
	for key, raw := range values {
		name := configOptionName(key)
		if name == "config" || name == "config-file" {
			return fmt.Errorf("config file %s: option %q must be provided on the command line", path, key)
		}
		flagValue := flags.Lookup(name)
		if flagValue == nil {
			return fmt.Errorf("config file %s: unknown option %q", path, key)
		}
		value, err := configFlagValueString(raw)
		if err != nil {
			return fmt.Errorf("config file %s: option %q: %w", path, key, err)
		}
		if err := flagValue.Value.Set(value); err != nil {
			return fmt.Errorf("config file %s: option %q: %w", path, key, err)
		}
	}
	return nil
}

func validateConfigReferences(cfg config) error {
	if cfg.monitoringTLSCert != "" {
		if _, err := tls.LoadX509KeyPair(cfg.monitoringTLSCert, cfg.monitoringTLSKey); err != nil {
			return fmt.Errorf("load monitoring TLS certificate: %w", err)
		}
	}
	if cfg.grpcTLSCert != "" {
		if _, err := grpcTLSConfig(cfg); err != nil {
			return err
		}
	}
	if cfg.topologyPath != "" {
		if _, err := hatriecache.LoadTopology(cfg.topologyPath); err != nil {
			return fmt.Errorf("load topology: %w", err)
		}
	}
	return nil
}

func writeRedactedConfig(writer io.Writer, cfg config) error {
	data, err := stdjson.MarshalIndent(redactedConfig(cfg), "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = writer.Write(data)
	return err
}

func redactedConfig(cfg config) map[string]interface{} {
	return map[string]interface{}{
		"config_path":                          cfg.configPath,
		"config_profile":                       cfg.configProfile,
		"check_config":                         cfg.checkConfig,
		"print_config":                         cfg.printConfig,
		"monitoring_server":                    cfg.monitoringServer,
		"monitoring_addr":                      cfg.monitoringAddr,
		"monitoring_tls_cert":                  cfg.monitoringTLSCert,
		"monitoring_tls_key":                   cfg.monitoringTLSKey,
		"monitoring_auth_token":                redactedSecret(cfg.monitoringAuthToken),
		"audit_log_path":                       cfg.auditLogPath,
		"write_protection":                     cfg.writeProtection,
		"rate_limit":                           cfg.rateLimit,
		"key_stats_mode":                       cfg.keyStatsMode,
		"key_stats_capacity":                   cfg.keyStatsCapacity,
		"monitoring_web_dir":                   cfg.monitoringWebDir,
		"monitoring_read_header_timeout":       cfg.monitoringReadHeaderTimeout.String(),
		"monitoring_idle_timeout":              cfg.monitoringIdleTimeout.String(),
		"node_id":                              cfg.nodeID,
		"topology_path":                        cfg.topologyPath,
		"election_timeout":                     cfg.electionTimeout.String(),
		"replication":                          cfg.replication,
		"replication_mode":                     cfg.replicationMode,
		"replication_async":                    cfg.replicationAsync,
		"replication_queue_size":               cfg.replicationQueueSize,
		"replication_retry_interval":           cfg.replicationRetry.String(),
		"replication_max_attempts":             cfg.replicationAttempts,
		"replication_dead_letter_limit":        cfg.replicationDeadLetterLimit,
		"replication_outbox_path":              cfg.replicationOutboxPath,
		"replication_outbox_format":            cfg.replicationOutboxFormat,
		"replication_outbox_codec":             cfg.replicationOutboxCodec,
		"replication_outbox_batch_window":      cfg.replicationOutboxBatch.String(),
		"replication_circuit_breaker_failures": cfg.replicationCircuitFailures,
		"replication_circuit_breaker_cooldown": cfg.replicationCircuitCooldown.String(),
		"replication_wire_format":              cfg.replicationWireFormat,
		"replication_transport":                cfg.replicationTransport,
		"replication_grpc_window":              cfg.replicationGRPCWindow,
		"replication_http_fallback":            cfg.replicationHTTPFallback,
		"replication_auth_token":               redactedSecret(cfg.replicationAuthToken),
		"replication_batch_max_bytes":          cfg.replicationBatchMaxBytes,
		"replication_max_in_flight_targets":    cfg.replicationMaxTargets,
		"replication_sync_interval":            cfg.replicationSyncInterval.String(),
		"replication_sync_prefix":              cfg.replicationSyncPrefix,
		"enforce_leader_writes":                cfg.enforceLeaderWrites,
		"grpc_addr":                            cfg.grpcAddr,
		"grpc_tls_cert":                        cfg.grpcTLSCert,
		"grpc_tls_key":                         cfg.grpcTLSKey,
		"grpc_client_ca":                       cfg.grpcClientCA,
		"db_path":                              cfg.dbPath,
		"db_format":                            cfg.dbFormat,
		"db_sync_interval":                     cfg.dbSyncInterval.String(),
		"db_compare_before_write":              cfg.dbCompareBeforeWrite,
		"db_compact_interval":                  cfg.dbCompactInterval.String(),
		"db_compact_start_key":                 cfg.dbCompactStartKey,
		"db_compact_limit_key":                 cfg.dbCompactLimitKey,
		"db_hot_load":                          cfg.dbHotLoad,
		"db_hot_load_max_bytes":                cfg.dbHotLoadMaxBytes,
		"db_hot_load_max_age":                  cfg.dbHotLoadMaxAge.String(),
		"db_hot_load_min_hits":                 cfg.dbHotLoadMinHits,
		"db_memory_cap_bytes":                  cfg.dbMemoryCapBytes,
		"db_rss_cap_bytes":                     cfg.dbRSSCapBytes,
		"db_memory_evict_interval":             cfg.dbMemoryEvictInterval.String(),
		"db_memory_evict_min_value_bytes":      cfg.dbMemoryEvictMinValueBytes,
		"snapshot_path":                        cfg.snapshotPath,
		"snapshot_interval":                    cfg.snapshotInterval.String(),
		"snapshot_format":                      cfg.snapshotFormat,
		"journal_path":                         cfg.journalPath,
		"journal_format":                       cfg.journalFormat,
		"journal_group_commit_window":          cfg.journalGroupCommitWindow.String(),
		"journal_group_commit_max_batch":       cfg.journalGroupCommitMaxBatch,
		"journal_pull_source":                  cfg.journalPullSource,
		"journal_pull_state_path":              cfg.journalPullStatePath,
		"journal_pull_interval":                cfg.journalPullInterval.String(),
		"journal_pull_timeout":                 cfg.journalPullTimeout.String(),
		"journal_pull_limit":                   cfg.journalPullLimit,
		"journal_pull_max_batches":             cfg.journalPullMaxBatches,
		"journal_pull_full_sync_fallback":      cfg.journalPullFullSyncFallback,
	}
}

func redactedSecret(value string) string {
	if strings.TrimSpace(value) == "" {
		return ""
	}
	return "<redacted>"
}

func configOptionName(key string) string {
	key = strings.TrimSpace(key)
	key = strings.TrimLeft(key, "-")
	return strings.ReplaceAll(key, "_", "-")
}

func configFlagValueString(raw stdjson.RawMessage) (string, error) {
	decoder := stdjson.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	switch typed := value.(type) {
	case string:
		return typed, nil
	case bool:
		return strconv.FormatBool(typed), nil
	case stdjson.Number:
		return typed.String(), nil
	case nil:
		return "", errors.New("null values are not supported")
	default:
		return "", fmt.Errorf("value must be a string, bool, or number")
	}
}

func replicationQueueSize(cfg config) int {
	if !cfg.replicationAsync {
		return 0
	}
	return cfg.replicationQueueSize
}

func replicationBatchLimit(cfg config) int {
	if cfg.replicationBatchMaxBytes == 0 {
		return -1
	}
	return cfg.replicationBatchMaxBytes
}

func parseReplicationMode(mode string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", replicationModeJournal:
		return replicationModeJournal, nil
	case replicationModeCommand:
		return replicationModeCommand, nil
	case replicationModeDual:
		return replicationModeDual, nil
	default:
		return "", fmt.Errorf("replication mode must be %s, %s, or %s", replicationModeJournal, replicationModeCommand, replicationModeDual)
	}
}

func replicationModeUsesJournal(mode string) bool {
	normalized, err := parseReplicationMode(mode)
	if err != nil {
		return false
	}
	return normalized == replicationModeJournal || normalized == replicationModeDual
}

func replicationModeUsesCommandFanout(mode string) bool {
	normalized, err := parseReplicationMode(mode)
	if err != nil {
		return false
	}
	return normalized == replicationModeCommand || normalized == replicationModeDual
}

func replicationWireFormat(cfg config) hatriecache.CommandWireFormat {
	format, err := hatriecache.ParseCommandWireFormat(cfg.replicationWireFormat)
	if err != nil {
		return hatriecache.DefaultCommandWireFormat
	}
	return format
}

func replicationTransport(cfg config) hatriecache.ReplicationTransport {
	transport, err := hatriecache.ParseReplicationTransport(cfg.replicationTransport)
	if err != nil {
		return hatriecache.ReplicationTransportHTTP
	}
	return transport
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

func levelDBSaveOptions(cfg config) hatriecache.LevelDBSaveOptions {
	mode, err := hatriecache.ParseLevelDBCompareBeforeWriteMode(cfg.dbCompareBeforeWrite)
	if err != nil {
		mode = hatriecache.DefaultLevelDBCompareBeforeWriteMode
	}
	return hatriecache.LevelDBSaveOptions{CompareBeforeWrite: mode}
}

func journalFormat(cfg config) hatriecache.CommandJournalFormat {
	format, err := hatriecache.ParseCommandJournalFormat(cfg.journalFormat)
	if err != nil {
		return hatriecache.DefaultCommandJournalFormat
	}
	return format
}

func journalOptions(cfg config) hatriecache.CommandJournalOptions {
	return hatriecache.CommandJournalOptions{
		Format:              journalFormat(cfg),
		GroupCommitWindow:   cfg.journalGroupCommitWindow,
		GroupCommitMaxBatch: cfg.journalGroupCommitMaxBatch,
	}
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
	stderr = diagnosticWriter(stderr)
	if err := store.Close(); err != nil {
		fmt.Fprintf(stderr, "close leveldb: %v\n", err)
	}
}

func openReplicationOutboxIfConfigured(path string, format string, codec string, batchWindow time.Duration) (*hatriecache.ReplicationOutboxStore, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	outboxFormat, err := parseReplicationOutboxFormat(format)
	if err != nil {
		return nil, err
	}
	if outboxFormat == "auto" {
		outboxFormat = "leveldb"
		if strings.EqualFold(filepath.Ext(path), ".json") {
			outboxFormat = "json"
		}
	}
	var outbox *hatriecache.ReplicationOutboxStore
	switch outboxFormat {
	case "json":
		outbox, err = hatriecache.OpenReplicationOutbox(path)
	case "leveldb":
		parsedCodec, parseErr := hatriecache.ParseReplicationOutboxCodec(codec)
		if parseErr != nil {
			return nil, parseErr
		}
		outbox, err = hatriecache.OpenLevelDBReplicationOutboxWithOptions(path, hatriecache.ReplicationOutboxOptions{
			Codec:       parsedCodec,
			BatchWindow: batchWindow,
		})
	default:
		err = fmt.Errorf("unsupported replication outbox format %q", format)
	}
	if err != nil {
		return nil, fmt.Errorf("open replication outbox: %w", err)
	}
	return outbox, nil
}

func closeReplicationOutbox(outbox *hatriecache.ReplicationOutboxStore, stderr io.Writer) {
	if outbox == nil {
		return
	}
	stderr = diagnosticWriter(stderr)
	if err := outbox.Close(); err != nil {
		fmt.Fprintf(stderr, "close replication outbox: %v\n", err)
	}
}

func parseReplicationOutboxFormat(format string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "", "auto":
		return "auto", nil
	case "json":
		return "json", nil
	case "leveldb":
		return "leveldb", nil
	default:
		return "", fmt.Errorf("replication outbox format must be auto, json, or leveldb")
	}
}

func openAuditLogIfConfigured(path string) (*hatriecache.AuditLogger, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil
	}
	logger, err := hatriecache.OpenAuditLogger(path)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	return logger, nil
}

func closeAuditLog(logger *hatriecache.AuditLogger, stderr io.Writer) {
	if logger == nil {
		return
	}
	stderr = diagnosticWriter(stderr)
	if err := logger.Close(); err != nil {
		fmt.Fprintf(stderr, "close audit log: %v\n", err)
	}
}

func newGRPCServer(cfg config, trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, snapshot func() error, topology *hatriecache.TopologyStore, election *hatriecache.ElectionStore, replicator *hatriecache.HTTPReplicator, auditLog *hatriecache.AuditLogger, rateLimiter *hatriecache.RateLimiter, apiMetrics *hatriecache.APIMetrics, replicationSafety *hatriecache.ReplicationSafetyStore, dirtyTracker *hatriecache.LevelDBDirtyTracker) (*grpc.Server, net.Listener, error) {
	if cfg.grpcAddr == "" {
		return nil, nil, nil
	}
	options, err := grpcServerOptions(cfg)
	if err != nil {
		return nil, nil, err
	}
	listener, err := net.Listen("tcp", cfg.grpcAddr)
	if err != nil {
		return nil, nil, err
	}
	server := grpc.NewServer(options...)
	hatriecache.RegisterCacheGRPCServer(server, hatriecache.NewCacheGRPCServer(trie, hatriecache.CacheGRPCOptions{
		NodeName:             defaultNodeID(cfg.nodeID),
		AuthToken:            cfg.monitoringAuthToken,
		ReplicationAuthToken: cfg.replicationAuthToken,
		AuditLog:             auditLog,
		WriteProtected:       cfg.writeProtection,
		RateLimiter:          rateLimiter,
		Metrics:              apiMetrics,
		Snapshot:             snapshot,
		Journal:              journal,
		DirtyTracker:         dirtyTracker,
		Topology:             topology,
		Election:             election,
		Replicator:           replicator,
		ReplicationSafety:    replicationSafety,
		EnforceLeaderWrites:  cfg.enforceLeaderWrites,
	}))
	return server, listener, nil
}

func grpcServerOptions(cfg config) ([]grpc.ServerOption, error) {
	if cfg.grpcTLSCert == "" {
		return nil, nil
	}
	tlsConfig, err := grpcTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	return []grpc.ServerOption{grpc.Creds(credentials.NewTLS(tlsConfig))}, nil
}

func grpcTLSConfig(cfg config) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.grpcTLSCert, cfg.grpcTLSKey)
	if err != nil {
		return nil, fmt.Errorf("load gRPC TLS certificate: %w", err)
	}
	tlsConfig := &tls.Config{
		MinVersion:   tls.VersionTLS12,
		NextProtos:   withHTTP2Proto(nil),
		Certificates: []tls.Certificate{cert},
	}
	if cfg.grpcClientCA != "" {
		caPEM, err := os.ReadFile(cfg.grpcClientCA)
		if err != nil {
			return nil, fmt.Errorf("read gRPC client CA: %w", err)
		}
		clientCAs := x509.NewCertPool()
		if !clientCAs.AppendCertsFromPEM(caPEM) {
			return nil, errors.New("gRPC client CA does not contain any PEM certificates")
		}
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		tlsConfig.ClientCAs = clientCAs
	}
	return tlsConfig, nil
}

func stopGRPCServer(server *grpc.Server) {
	stopGRPCServerWithTimeout(server, serverShutdownTimeout)
}

func stopGRPCServerWithTimeout(server *grpc.Server, timeout time.Duration) {
	if server == nil {
		return
	}
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()
	if timeout <= 0 {
		server.Stop()
		<-done
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		server.Stop()
		<-done
	}
}

func startLevelDBSaver(ctx context.Context, trie *hatriecache.HatTrie, store *hatriecache.LevelDBStore, dirty *hatriecache.LevelDBDirtyTracker, interval time.Duration, options hatriecache.LevelDBSaveOptions, stderr io.Writer) func() {
	ctx = serverContext(ctx)
	if store == nil || interval <= 0 {
		return func() {}
	}
	stderr = diagnosticWriter(stderr)

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	first := true
	save := func() bool {
		var err error
		if first || dirty == nil {
			err = saveLevelDBIfOpenAndClearDirty(trie, store, dirty)
		} else {
			err = saveDirtyLevelDBIfOpen(trie, store, dirty, options)
		}
		if errors.Is(err, errHatTrieDestroyed) {
			return false
		}
		if err != nil {
			fmt.Fprintf(stderr, "save leveldb: %v\n", err)
			return true
		}
		first = false
		return true
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
		if !save() {
			return
		}
		for {
			select {
			case <-ticker.C:
				if !save() {
					return
				}
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return periodicStopper(done, stopped)
}

func saveLevelDBIfOpen(trie *hatriecache.HatTrie, store *hatriecache.LevelDBStore) (err error) {
	defer recoverDestroyedHatTrie(&err)
	return store.Save(trie)
}

func saveLevelDBIfOpenAndClearDirty(trie *hatriecache.HatTrie, store *hatriecache.LevelDBStore, dirty *hatriecache.LevelDBDirtyTracker) (err error) {
	defer recoverDestroyedHatTrie(&err)
	if dirty == nil {
		return store.Save(trie)
	}
	snapshot := dirty.Snapshot()
	if err := store.Save(trie); err != nil {
		return err
	}
	dirty.Clear(snapshot)
	return nil
}

func saveDirtyLevelDBIfOpen(trie *hatriecache.HatTrie, store *hatriecache.LevelDBStore, dirty *hatriecache.LevelDBDirtyTracker, options hatriecache.LevelDBSaveOptions) (err error) {
	defer recoverDestroyedHatTrie(&err)
	return store.SaveDirtyWithOptions(trie, dirty, options)
}

type levelDBCompactorOptions struct {
	StartKey string
	LimitKey string
}

func startLevelDBCompactor(ctx context.Context, store *hatriecache.LevelDBStore, interval time.Duration, options levelDBCompactorOptions, record func(hatriecache.LevelDBCompactionResult), stderr io.Writer) func() {
	ctx = serverContext(ctx)
	if store == nil || interval <= 0 {
		return func() {}
	}
	stderr = diagnosticWriter(stderr)

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	compact := func() bool {
		result, err := store.Compact(hatriecache.LevelDBCompactionOptions{
			StartKey: strings.TrimSpace(options.StartKey),
			LimitKey: strings.TrimSpace(options.LimitKey),
		})
		if err != nil {
			if errors.Is(err, hatriecache.ErrLevelDBStoreClosed) {
				return false
			}
			fmt.Fprintf(stderr, "compact leveldb: %v\n", err)
			return true
		}
		if record != nil {
			record(result)
		}
		return true
	}
	go func() {
		defer close(stopped)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !compact() {
					return
				}
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return periodicStopper(done, stopped)
}

type levelDBMemoryGovernorOptions struct {
	Spill       hatriecache.LevelDBSpillOptions
	RSSCapBytes uint64
	RSS         func() (uint64, error)
}

func startLevelDBMemoryGovernor(ctx context.Context, trie *hatriecache.HatTrie, store *hatriecache.LevelDBStore, interval time.Duration, options levelDBMemoryGovernorOptions, record func(hatriecache.LevelDBSpillResult), stderr io.Writer) func() {
	ctx = serverContext(ctx)
	if store == nil || interval <= 0 || (options.Spill.MaxHotBytes <= 0 && options.RSSCapBytes == 0) {
		return func() {}
	}
	stderr = diagnosticWriter(stderr)
	if options.RSS == nil {
		options.RSS = processRSSBytes
	}

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	spill := func() bool {
		spillOptions := options.Spill
		if options.RSSCapBytes > 0 {
			rssBytes, err := options.RSS()
			if err != nil {
				fmt.Fprintf(stderr, "read process rss: %v\n", err)
				if spillOptions.MaxHotBytes <= 0 {
					return true
				}
			} else if rssBytes < options.RSSCapBytes && spillOptions.MaxHotBytes <= 0 {
				return true
			}
		}
		result, err := spillColdLevelDBIfOpen(trie, store, spillOptions)
		if errors.Is(err, errHatTrieDestroyed) || errors.Is(err, hatriecache.ErrLevelDBStoreClosed) {
			return false
		}
		if err != nil {
			fmt.Fprintf(stderr, "spill cold leveldb values: %v\n", err)
			return true
		}
		if record != nil {
			record(result)
		}
		return true
	}
	go func() {
		defer close(stopped)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !spill() {
					return
				}
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return periodicStopper(done, stopped)
}

func processRSSBytes() (uint64, error) {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0, err
	}
	return rssBytesFromStatm(string(data), uint64(os.Getpagesize()))
}

func rssBytesFromStatm(data string, pageSize uint64) (uint64, error) {
	fields := strings.Fields(data)
	if len(fields) < 2 {
		return 0, errors.New("statm missing rss pages")
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse statm rss pages: %w", err)
	}
	if pageSize == 0 {
		return 0, errors.New("page size must be positive")
	}
	if pages > ^uint64(0)/pageSize {
		return 0, errors.New("statm rss bytes overflow")
	}
	return pages * pageSize, nil
}

func spillColdLevelDBIfOpen(trie *hatriecache.HatTrie, store *hatriecache.LevelDBStore, options hatriecache.LevelDBSpillOptions) (result hatriecache.LevelDBSpillResult, err error) {
	defer recoverDestroyedHatTrie(&err)
	return store.SpillCold(trie, options)
}

func openJournalIfConfigured(path string, format hatriecache.CommandJournalFormat) (*hatriecache.CommandJournal, error) {
	return openJournalIfConfiguredWithOptions(path, hatriecache.CommandJournalOptions{
		Format:              format,
		GroupCommitWindow:   hatriecache.DefaultJournalGroupCommitWindow,
		GroupCommitMaxBatch: hatriecache.DefaultJournalGroupCommitMaxBatch,
	})
}

func openJournalIfConfiguredWithOptions(path string, options hatriecache.CommandJournalOptions) (*hatriecache.CommandJournal, error) {
	if path == "" {
		return nil, nil
	}
	return hatriecache.OpenCommandJournalWithOptions(path, options)
}

func closeJournal(journal *hatriecache.CommandJournal, stderr io.Writer) {
	if journal == nil {
		return
	}
	stderr = diagnosticWriter(stderr)
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

func loadStartupSnapshot(trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, configuredPath string, pullPath string) (hatriecache.SnapshotMetadata, error) {
	selectedPath := ""
	selectedPull := false
	var selectedMetadata hatriecache.SnapshotMetadata
	for _, candidate := range []struct {
		path string
		pull bool
	}{{path: configuredPath}, {path: pullPath, pull: true}} {
		if candidate.path == "" {
			continue
		}
		metadata, err := hatriecache.ReadSnapshotMetadata(candidate.path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return hatriecache.SnapshotMetadata{}, err
		}
		if selectedPath == "" || metadata.JournalSequence > selectedMetadata.JournalSequence {
			selectedPath = candidate.path
			selectedPull = candidate.pull
			selectedMetadata = metadata
		}
	}
	if selectedPath == "" {
		return hatriecache.SnapshotMetadata{}, nil
	}
	if selectedPull {
		if journal == nil {
			return hatriecache.SnapshotMetadata{}, errors.New("journal pull snapshot requires a journal")
		}
		return journal.ReplaceWithSnapshot(trie, selectedPath)
	}
	return trie.LoadSnapshotWithMetadata(selectedPath)
}

func journalPullSnapshotPath(statePath string, source string) string {
	statePath = strings.TrimSpace(statePath)
	source = strings.TrimSpace(source)
	if statePath == "" || source == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(source))
	return fmt.Sprintf("%s.%x.snapshot.hc", statePath, sum[:8])
}

func saveSnapshotIfConfigured(trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, path string, format hatriecache.SnapshotFormat) (err error) {
	defer recoverDestroyedHatTrie(&err)
	if path == "" {
		return nil
	}
	if journal != nil {
		return journal.SaveSnapshotWithFormat(trie, path, format)
	}
	return trie.SaveSnapshotWithFormat(path, format)
}

func startSnapshotSaver(ctx context.Context, trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, path string, interval time.Duration, format hatriecache.SnapshotFormat, stderr io.Writer) func() {
	ctx = serverContext(ctx)
	if path == "" || interval <= 0 {
		return func() {}
	}
	stderr = diagnosticWriter(stderr)

	ticker := time.NewTicker(interval)
	done := make(chan struct{})
	stopped := make(chan struct{})
	save := func() bool {
		if err := saveSnapshotIfConfigured(trie, journal, path, format); errors.Is(err, errHatTrieDestroyed) {
			return false
		} else if err != nil {
			fmt.Fprintf(stderr, "save snapshot: %v\n", err)
		}
		return true
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
		if !save() {
			return
		}
		for {
			select {
			case <-ticker.C:
				if !save() {
					return
				}
			case <-ctx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return periodicStopper(done, stopped)
}

func recoverDestroyedHatTrie(err *error) {
	recovered := recover()
	if recovered == nil {
		return
	}
	if message, ok := recovered.(string); ok && message == destroyedHatTriePanic {
		*err = errHatTrieDestroyed
		return
	}
	panic(recovered)
}

func startElectionHeartbeat(ctx context.Context, election *hatriecache.ElectionStore, nodeID string, timeout time.Duration, required bool, stderr io.Writer) (func(), error) {
	ctx = serverContext(ctx)
	if election == nil {
		return func() {}, nil
	}
	stderr = diagnosticWriter(stderr)
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

func startReplicationSyncer(ctx context.Context, trie *hatriecache.HatTrie, replicator *hatriecache.HTTPReplicator, interval time.Duration, prefix string, stderr io.Writer) func() {
	ctx = serverContext(ctx)
	if replicator == nil || interval <= 0 {
		return func() {}
	}
	stderr = diagnosticWriter(stderr)

	syncCtx, cancelSync := context.WithCancel(ctx)
	done := make(chan struct{})
	stopped := make(chan struct{})
	syncOnce := func() {
		result := replicator.SyncAll(syncCtx, trie, prefix)
		if replicationSyncResultNeedsLog(result) {
			fmt.Fprintf(stderr, "replication sync: %s\n", replicationSyncResultLogMessage(result))
		}
	}
	go func() {
		defer close(stopped)
		defer cancelSync()
		select {
		case <-syncCtx.Done():
			return
		case <-done:
			return
		default:
		}
		syncOnce()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				syncOnce()
			case <-syncCtx.Done():
				return
			case <-done:
				return
			}
		}
	}()
	return cancelingPeriodicStopper(cancelSync, done, stopped)
}

func replicationSyncResultNeedsLog(result hatriecache.ReplicationResult) bool {
	if result.Skipped {
		return result.Reason != "" && result.Reason != "no entries to sync"
	}
	for _, target := range result.Targets {
		if !target.OK {
			return true
		}
	}
	return false
}

func replicationSyncResultLogMessage(result hatriecache.ReplicationResult) string {
	if result.Skipped {
		return result.Reason
	}
	failures := 0
	for _, target := range result.Targets {
		if !target.OK {
			failures++
		}
	}
	return fmt.Sprintf("%d/%d target deliveries failed", failures, len(result.Targets))
}

type journalPullerConfig struct {
	Source           string
	StatePath        string
	Interval         time.Duration
	Timeout          time.Duration
	Limit            uint64
	MaxBatches       uint64
	Dirty            *hatriecache.LevelDBDirtyTracker
	FullSyncFallback bool
	AuthToken        string
	Client           *http.Client
	SnapshotPath     string
	PersistFullSync  func() error
}

type journalPullState struct {
	Source         string    `json:"source"`
	AppliedThrough uint64    `json:"applied_through"`
	UpdatedAt      time.Time `json:"updated_at"`
}

func startJournalPuller(ctx context.Context, trie *hatriecache.HatTrie, journal *hatriecache.CommandJournal, cfg journalPullerConfig, stderr io.Writer) func() {
	ctx = serverContext(ctx)
	if cfg.Source == "" || journal == nil {
		return func() {}
	}
	stderr = diagnosticWriter(stderr)

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
		DirtyTracker:  cfg.Dirty,
		AuthToken:     cfg.AuthToken,
		Client:        cfg.Client,
	})
	if err != nil {
		if cfg.FullSyncFallback && errors.Is(err, hatriecache.ErrCommandJournalCompacted) {
			metadata, syncErr := hatriecache.PullCommandJournalSnapshot(ctx, cfg.Source, cfg.AuthToken, cfg.Client, cfg.SnapshotPath, result.LastSequence)
			if syncErr != nil {
				return result, errors.Join(err, fmt.Errorf("full sync fallback: %w", syncErr))
			}
			if metadata.JournalSequence < result.LastSequence {
				return result, errors.Join(err, fmt.Errorf("full sync fallback snapshot sequence %d is older than journal sequence %d", metadata.JournalSequence, result.LastSequence))
			}
			restored, restoreErr := journal.ReplaceWithSnapshot(trie, cfg.SnapshotPath)
			if restoreErr != nil {
				return result, errors.Join(err, fmt.Errorf("full sync fallback restore: %w", restoreErr))
			}
			if restored.JournalSequence != metadata.JournalSequence {
				return result, errors.Join(err, errors.New("full sync fallback snapshot changed during restore"))
			}
			if cfg.PersistFullSync != nil {
				if persistErr := cfg.PersistFullSync(); persistErr != nil {
					return result, errors.Join(err, fmt.Errorf("full sync fallback persistence: %w", persistErr))
				}
			}
			result.FullSyncFallback = true
			result.LastSequence = metadata.JournalSequence
			result.AppliedThrough = metadata.JournalSequence
			if saveErr := saveJournalPullState(cfg.StatePath, cfg.Source, result.AppliedThrough); saveErr != nil {
				return result, saveErr
			}
			return result, nil
		}
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
