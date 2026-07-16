package hatriecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"hatrie_cache/internal/jsonwire"
)

const (
	maxCommandJournalTailResponseBytes         = 1 << 20
	maxCommandJournalErrorResponseBytes        = 1 << 20
	maxMonitoringJSONRequestBytes              = 1 << 20
	maxMonitoringEntriesLimit                  = 100000
	truncatedCommandJournalErrorResponseSuffix = "\n... journal source error body truncated"
)

var errCommandJournalTailResponseTooLarge = errors.New("hatriecache: journal source response is too large")
var errMonitoringEntriesLimitReached = errors.New("hatriecache: monitoring entries limit reached")

type MonitoringOptions struct {
	NodeName             string
	WebDir               string
	AuthToken            string
	AuditLog             *AuditLogger
	WriteProtected       bool
	RateLimiter          *RateLimiter
	Metrics              *APIMetrics
	StartAt              time.Time
	Snapshot             func() error
	LevelDBStore         *LevelDBStore
	BackupSnapshotFormat SnapshotFormat
	Journal              *CommandJournal
	Topology             *TopologyStore
	Election             *ElectionStore
	Replicator           *HTTPReplicator
	ReplicationSafety    *ReplicationSafetyStore
	EnforceLeaderWrites  bool
	RuntimeConfig        map[string]interface{}
}

type MonitoringHandler struct {
	trie      *HatTrie
	options   MonitoringOptions
	storageMu sync.Mutex
	storage   monitoringStorageState
}

type commandExecutionOptions struct {
	NodeName            string
	Journal             *CommandJournal
	Topology            *TopologyStore
	Election            *ElectionStore
	Replicator          *HTTPReplicator
	ReplicationSafety   *ReplicationSafetyStore
	EnforceLeaderWrites bool
}

type MonitoringHealth struct {
	Status          string `json:"status"`
	Node            string `json:"node"`
	UptimeSeconds   int64  `json:"uptime_seconds"`
	MemoryBytes     uint64 `json:"memory_bytes"`
	DiskSpillBytes  uint64 `json:"disk_spill_bytes"`
	CleanersRunning int    `json:"cleaners_running"`
}

type MonitoringEntry struct {
	Key          string `json:"key"`
	Type         string `json:"type"`
	TTLMillis    *int64 `json:"ttl_ms"`
	OnDisk       bool   `json:"on_disk"`
	SizeBytes    int64  `json:"size_bytes"`
	ValuePreview string `json:"value_preview"`
}

type MonitoringEntriesResponse struct {
	Entries      []MonitoringEntry `json:"entries"`
	Limit        uint64            `json:"limit,omitempty"`
	HasMore      bool              `json:"has_more,omitempty"`
	AfterKey     string            `json:"after_key,omitempty"`
	NextAfterKey string            `json:"next_after_key,omitempty"`

	afterKeySet     bool
	nextAfterKeySet bool
}

func (response MonitoringEntriesResponse) MarshalJSON() ([]byte, error) {
	type monitoringEntriesResponseJSON struct {
		Entries      []MonitoringEntry `json:"entries"`
		Limit        uint64            `json:"limit,omitempty"`
		HasMore      bool              `json:"has_more,omitempty"`
		AfterKey     *string           `json:"after_key,omitempty"`
		NextAfterKey *string           `json:"next_after_key,omitempty"`
	}
	out := monitoringEntriesResponseJSON{
		Entries: response.Entries,
		Limit:   response.Limit,
		HasMore: response.HasMore,
	}
	if response.afterKeySet || response.AfterKey != "" {
		out.AfterKey = &response.AfterKey
	}
	if response.nextAfterKeySet || response.NextAfterKey != "" {
		out.NextAfterKey = &response.NextAfterKey
	}
	return jsonwire.Marshal(out)
}

type replicationSyncRequest struct {
	Prefix string `json:"prefix,omitempty"`
}

type journalPullRequest struct {
	Source        string `json:"source"`
	AfterSequence uint64 `json:"after_sequence,omitempty"`
	Limit         uint64 `json:"limit,omitempty"`
	UntilCurrent  bool   `json:"until_current,omitempty"`
	MaxBatches    uint64 `json:"max_batches,omitempty"`
}

type backupBundleRequest struct {
	Path           string `json:"path"`
	SnapshotFormat string `json:"snapshot_format,omitempty"`
}

type storageStatus struct {
	LevelDBConfigured bool                     `json:"leveldb_configured"`
	Store             string                   `json:"store,omitempty"`
	Path              string                   `json:"path,omitempty"`
	Format            string                   `json:"format,omitempty"`
	SizeBytes         int64                    `json:"size_bytes,omitempty"`
	Error             string                   `json:"error,omitempty"`
	Properties        LevelDBProperties        `json:"properties,omitempty"`
	Operation         storageOperationStatus   `json:"operation"`
	LastFlush         *LevelDBFlushResult      `json:"last_flush,omitempty"`
	LastCompact       *LevelDBCompactionResult `json:"last_compact,omitempty"`
}

type storageCompactRequest struct {
	StartKey string `json:"start_key,omitempty"`
	LimitKey string `json:"limit_key,omitempty"`
}

type monitoringStorageState struct {
	action      string
	startedAt   time.Time
	lastFlush   *LevelDBFlushResult
	lastCompact *LevelDBCompactionResult
}

type storageOperationStatus struct {
	Running   bool       `json:"running"`
	Action    string     `json:"action,omitempty"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	AgeMillis int64      `json:"age_millis,omitempty"`
}

func NewMonitoringHandler(trie *HatTrie, options MonitoringOptions) *MonitoringHandler {
	if options.StartAt.IsZero() {
		options.StartAt = time.Now()
	}
	if options.NodeName == "" {
		if hostname, err := os.Hostname(); err == nil && hostname != "" {
			options.NodeName = hostname
		} else {
			options.NodeName = "local"
		}
	}
	if options.Topology == nil {
		topology, err := NewTopologyStore(SingleNodeTopology(options.NodeName, ""))
		if err == nil {
			options.Topology = topology
		}
	}
	if options.Metrics == nil {
		options.Metrics = NewAPIMetrics()
	}
	if options.Election == nil && options.Topology != nil {
		options.Election = NewElectionStore(options.Topology, ElectionOptions{})
		_ = options.Election.Heartbeat(options.NodeName)
	}
	if options.ReplicationSafety == nil {
		options.ReplicationSafety = NewReplicationSafetyStore()
	}
	return &MonitoringHandler{trie: trie, options: options}
}

func (handler *MonitoringHandler) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", handler.handleHealth)
	mux.HandleFunc("/api/config", handler.handleConfig)
	mux.HandleFunc("/api/stats", handler.handleStats)
	mux.HandleFunc("/api/entries", handler.handleEntries)
	mux.HandleFunc("/api/commands", handler.handleCommands)
	mux.HandleFunc("/api/snapshot", handler.handleSnapshot)
	mux.HandleFunc("/api/backup", handler.handleBackup)
	mux.HandleFunc("/api/storage", handler.handleStorage)
	mux.HandleFunc("/api/storage/flush", handler.handleStorageFlush)
	mux.HandleFunc("/api/storage/compact", handler.handleStorageCompact)
	mux.HandleFunc("/api/topology", handler.handleTopology)
	mux.HandleFunc("/api/election", handler.handleElection)
	mux.HandleFunc("/api/replication", handler.handleReplication)
	mux.HandleFunc("/api/journal", handler.handleJournal)
	mux.HandleFunc("/metrics", handler.handleMetrics)
	if handler.options.WebDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(handler.options.WebDir)))
	}
	var out http.Handler = mux
	if strings.TrimSpace(handler.options.AuthToken) != "" {
		out = monitoringAuthHandler(strings.TrimSpace(handler.options.AuthToken), out)
	}
	return gzipHTTPHandler(out)
}

func monitoringAuthHandler(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !monitoringPathRequiresAuth(r.URL.Path) || monitoringRequestAuthorized(r, token) {
			next.ServeHTTP(w, r)
			return
		}
		w.Header().Set("WWW-Authenticate", `Bearer realm="hatrie-cache"`)
		writeJSONStatus(w, http.StatusUnauthorized, commandError("unauthorized"))
	})
}

func monitoringPathRequiresAuth(path string) bool {
	return strings.HasPrefix(path, "/api/") || path == "/metrics"
}

func monitoringRequestAuthorized(r *http.Request, token string) bool {
	token = normalizeAuthToken(token)
	if token == "" {
		return true
	}
	if authTokenMatches(r.Header.Get("X-Hatrie-Auth-Token"), token) {
		return true
	}
	return authTokenMatches(authBearerToken(r.Header.Get("Authorization")), token)
}

func (handler *MonitoringHandler) auditHTTP(r *http.Request, event AuditEvent) {
	if handler.options.AuditLog == nil {
		return
	}
	event.Node = handler.options.NodeName
	event.Protocol = "http"
	event.RemoteAddr = r.RemoteAddr
	event.Method = r.Method
	if r.URL != nil {
		event.Path = r.URL.Path
	}
	handler.options.Metrics.RecordAuditResult(handler.options.AuditLog.Log(event))
}

func (handler *MonitoringHandler) rejectDangerousHTTP(w http.ResponseWriter, r *http.Request, action string, details map[string]interface{}) bool {
	if handler.options.WriteProtected {
		handler.options.Metrics.RecordWriteProtectionRejection()
		handler.auditHTTP(r, AuditEvent{Action: action, OK: false, Status: http.StatusForbidden, Message: "writes are disabled", Details: details})
		writeJSONStatus(w, http.StatusForbidden, commandError("writes are disabled"))
		return true
	}
	if handler.options.RateLimiter != nil && !handler.options.RateLimiter.Allow(monitoringRateLimitKey(r)) {
		handler.options.Metrics.RecordRateLimitRejection()
		handler.auditHTTP(r, AuditEvent{Action: action, OK: false, Status: http.StatusTooManyRequests, Message: "rate limit exceeded", Details: details})
		writeJSONStatus(w, http.StatusTooManyRequests, commandError("rate limit exceeded"))
		return true
	}
	return false
}

func (handler *MonitoringHandler) rejectDangerousCommandHTTP(w http.ResponseWriter, r *http.Request, request CacheCommandRequest, format CommandWireFormat) bool {
	if !commandShouldJournal(request) {
		return false
	}
	if handler.options.WriteProtected {
		handler.options.Metrics.RecordWriteProtectionRejection()
		response := commandError("writes are disabled")
		handler.auditCommandHTTP(r, request, response, false, http.StatusForbidden)
		writeCommandResponseWire(w, r, http.StatusForbidden, response, format)
		return true
	}
	if handler.options.RateLimiter != nil && !handler.options.RateLimiter.Allow(monitoringRateLimitKey(r)) {
		handler.options.Metrics.RecordRateLimitRejection()
		response := commandError("rate limit exceeded")
		handler.auditCommandHTTP(r, request, response, false, http.StatusTooManyRequests)
		writeCommandResponseWire(w, r, http.StatusTooManyRequests, response, format)
		return true
	}
	return false
}

func monitoringRateLimitKey(r *http.Request) string {
	if r == nil {
		return "http"
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil && host != "" {
		return host
	}
	if r.RemoteAddr != "" {
		return r.RemoteAddr
	}
	return "http"
}

func (handler *MonitoringHandler) requireTrie(w http.ResponseWriter) bool {
	if handler.trie != nil {
		return true
	}
	writeJSONStatus(w, http.StatusServiceUnavailable, commandError("trie is not configured"))
	return false
}

func (handler *MonitoringHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if !handler.requireTrie(w) {
		return
	}

	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	writeJSON(w, MonitoringHealth{
		Status:          "online",
		Node:            handler.options.NodeName,
		UptimeSeconds:   int64(time.Since(handler.options.StartAt).Seconds()),
		MemoryBytes:     mem.Alloc,
		DiskSpillBytes:  handler.trie.diskSpillBytes(),
		CleanersRunning: 0,
	})
}

func (handler *MonitoringHandler) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.options.RuntimeConfig == nil {
		writeJSON(w, map[string]interface{}{})
		return
	}
	writeJSON(w, cloneRuntimeConfig(handler.options.RuntimeConfig))
}

func cloneRuntimeConfig(config map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(config))
	for key, value := range config {
		out[key] = value
	}
	return out
}

func (handler *MonitoringHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if !handler.requireTrie(w) {
		return
	}
	writeJSON(w, handler.trie.Stats())
}

func (handler *MonitoringHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if !handler.requireTrie(w) {
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = io.WriteString(w, handler.prometheusMetrics())
}

func (handler *MonitoringHandler) prometheusMetrics() string {
	var builder strings.Builder
	node := prometheusLabelValue(handler.options.NodeName)
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	stats := handler.trie.Stats()

	writePrometheusHelp(&builder, "hatrie_cache_up", "Whether this HAT-trie cache process is serving metrics.")
	writePrometheusType(&builder, "hatrie_cache_up", "gauge")
	fmt.Fprintf(&builder, "hatrie_cache_up{node=\"%s\"} 1\n", node)
	writePrometheusHelp(&builder, "hatrie_cache_uptime_seconds", "Process uptime in seconds.")
	writePrometheusType(&builder, "hatrie_cache_uptime_seconds", "gauge")
	fmt.Fprintf(&builder, "hatrie_cache_uptime_seconds{node=\"%s\"} %d\n", node, int64(time.Since(handler.options.StartAt).Seconds()))
	writePrometheusHelp(&builder, "hatrie_cache_keys", "Current number of keys in the cache.")
	writePrometheusType(&builder, "hatrie_cache_keys", "gauge")
	fmt.Fprintf(&builder, "hatrie_cache_keys{node=\"%s\"} %d\n", node, handler.trie.Size())
	writePrometheusHelp(&builder, "hatrie_cache_memory_bytes", "Go heap bytes currently allocated.")
	writePrometheusType(&builder, "hatrie_cache_memory_bytes", "gauge")
	fmt.Fprintf(&builder, "hatrie_cache_memory_bytes{node=\"%s\"} %d\n", node, mem.Alloc)
	writePrometheusHelp(&builder, "hatrie_cache_disk_spill_bytes", "Bytes currently spilled to disk by the cache.")
	writePrometheusType(&builder, "hatrie_cache_disk_spill_bytes", "gauge")
	fmt.Fprintf(&builder, "hatrie_cache_disk_spill_bytes{node=\"%s\"} %d\n", node, handler.trie.diskSpillBytes())

	writePrometheusCounter(&builder, "hatrie_cache_reads_total", "Total cache read operations.", node, stats.Reads)
	writePrometheusCounter(&builder, "hatrie_cache_hits_total", "Total cache hit operations.", node, stats.Hits)
	writePrometheusCounter(&builder, "hatrie_cache_misses_total", "Total cache miss operations.", node, stats.Misses)
	writePrometheusCounter(&builder, "hatrie_cache_writes_total", "Total cache write operations.", node, stats.Writes)
	writePrometheusCounter(&builder, "hatrie_cache_deletes_total", "Total cache delete operations.", node, stats.Deletes)
	writePrometheusCounter(&builder, "hatrie_cache_expirations_total", "Total cache expiration operations.", node, stats.Expirations)
	apiMetrics := handler.options.Metrics.Snapshot()
	writePrometheusCounter(&builder, "hatrie_cache_audit_events_total", "Total dangerous API audit events written.", node, apiMetrics.AuditEventsTotal)
	writePrometheusCounter(&builder, "hatrie_cache_audit_errors_total", "Total dangerous API audit log write errors.", node, apiMetrics.AuditErrorsTotal)
	writePrometheusCounter(&builder, "hatrie_cache_write_protection_rejections_total", "Total dangerous API actions rejected by write protection.", node, apiMetrics.WriteProtectionRejectionsTotal)
	writePrometheusCounter(&builder, "hatrie_cache_rate_limit_rejections_total", "Total dangerous API actions rejected by rate limiting.", node, apiMetrics.RateLimitRejectionsTotal)
	writePrometheusGauge(&builder, "hatrie_cache_write_protection_enabled", "Whether dangerous API writes are currently blocked by write protection.", node, boolGauge(handler.options.WriteProtected))
	writePrometheusGauge(&builder, "hatrie_cache_rate_limit_per_second", "Configured dangerous API action rate limit per caller per second; zero means disabled.", node, uint64(handler.options.RateLimiter.Limit()))

	if handler.options.Journal != nil {
		writePrometheusHelp(&builder, "hatrie_cache_journal_sequence", "Latest local command journal sequence.")
		writePrometheusType(&builder, "hatrie_cache_journal_sequence", "gauge")
		fmt.Fprintf(&builder, "hatrie_cache_journal_sequence{node=\"%s\"} %d\n", node, handler.options.Journal.Sequence())
	}
	if handler.options.Replicator != nil {
		result := handler.options.Replicator.LastResult()
		if result.Queue != nil {
			writePrometheusHelp(&builder, "hatrie_cache_replication_queue_depth", "Current async replication queue depth.")
			writePrometheusType(&builder, "hatrie_cache_replication_queue_depth", "gauge")
			fmt.Fprintf(&builder, "hatrie_cache_replication_queue_depth{node=\"%s\"} %d\n", node, result.Queue.Depth)
			writePrometheusCounter(&builder, "hatrie_cache_replication_queue_dropped_total", "Total dropped async replication jobs.", node, result.Queue.Dropped)
			writePrometheusCounter(&builder, "hatrie_cache_replication_attempts_total", "Total async replication target delivery attempts.", node, result.Queue.Attempts)
			writePrometheusCounter(&builder, "hatrie_cache_replication_successes_total", "Total successful async replication target deliveries.", node, result.Queue.Successes)
			writePrometheusCounter(&builder, "hatrie_cache_replication_failures_total", "Total failed async replication target deliveries.", node, result.Queue.Failures)
		}
	}
	return builder.String()
}

func writePrometheusCounter(builder *strings.Builder, name string, help string, node string, value uint64) {
	writePrometheusHelp(builder, name, help)
	writePrometheusType(builder, name, "counter")
	fmt.Fprintf(builder, "%s{node=\"%s\"} %d\n", name, node, value)
}

func writePrometheusGauge(builder *strings.Builder, name string, help string, node string, value uint64) {
	writePrometheusHelp(builder, name, help)
	writePrometheusType(builder, name, "gauge")
	fmt.Fprintf(builder, "%s{node=\"%s\"} %d\n", name, node, value)
}

func boolGauge(value bool) uint64 {
	if value {
		return 1
	}
	return 0
}

func writePrometheusHelp(builder *strings.Builder, name string, help string) {
	fmt.Fprintf(builder, "# HELP %s %s\n", name, prometheusHelpText(help))
}

func writePrometheusType(builder *strings.Builder, name string, metricType string) {
	fmt.Fprintf(builder, "# TYPE %s %s\n", name, metricType)
}

func prometheusLabelValue(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	value = strings.ReplaceAll(value, "\"", "\\\"")
	return value
}

func prometheusHelpText(value string) string {
	value = strings.ReplaceAll(value, "\\", "\\\\")
	value = strings.ReplaceAll(value, "\n", "\\n")
	return value
}

func (handler *MonitoringHandler) handleEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if !handler.requireTrie(w) {
		return
	}
	values := r.URL.Query()
	prefix := values.Get("prefix")
	limit, err := monitoringEntriesLimit(values.Get("limit"))
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
		return
	}
	afterKey, hasAfterKey, err := monitoringEntriesAfterKey(prefix, values.Get("after_key"), values.Has("after_key"))
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
		return
	}
	writeJSON(w, handler.trie.monitoringEntriesPage(prefix, afterKey, hasAfterKey, limit))
}

func (handler *MonitoringHandler) handleCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if !handler.requireTrie(w) {
		_ = r.Body.Close()
		return
	}

	requestFormat, ok := monitoringCommandRequestFormat(w, r)
	if !ok {
		return
	}
	if _, ok := commandWireFormatFromAccept(r.Header.Get("Accept"), requestFormat); !ok {
		_ = r.Body.Close()
		w.Header().Add("Vary", "Accept")
		http.Error(w, "no acceptable command response content type", http.StatusNotAcceptable)
		return
	}
	request, closeBody, ok := monitoringCommandRequestFromFormat(w, r, requestFormat)
	if !ok {
		return
	}
	defer closeBody()
	if requestContextDone(w, r) {
		return
	}
	if handler.rejectDangerousCommandHTTP(w, r, request, requestFormat) {
		return
	}
	response, rejected := executeCacheCommand(r.Context(), handler.trie, request, commandExecutionOptions{
		NodeName:            handler.options.NodeName,
		Journal:             handler.options.Journal,
		Topology:            handler.options.Topology,
		Election:            handler.options.Election,
		Replicator:          handler.options.Replicator,
		ReplicationSafety:   handler.options.ReplicationSafety,
		EnforceLeaderWrites: handler.options.EnforceLeaderWrites,
	})
	if rejected {
		handler.auditCommandHTTP(r, request, response, false, http.StatusConflict)
		writeCommandResponseWire(w, r, http.StatusConflict, response, requestFormat)
		return
	}
	handler.auditCommandHTTP(r, request, response, response.OK, http.StatusOK)
	writeCommandResponseWire(w, r, http.StatusOK, response, requestFormat)
}

func (handler *MonitoringHandler) auditCommandHTTP(r *http.Request, request CacheCommandRequest, response CacheCommandResponse, ok bool, status int) {
	if !commandShouldJournal(request) {
		return
	}
	handler.auditHTTP(r, AuditEvent{
		Action:  "command",
		Command: normalizedCommand(request.Command),
		Key:     strings.TrimSpace(request.Key),
		OK:      ok,
		Status:  status,
		Message: response.Message,
	})
}

func executeCacheCommand(ctx context.Context, trie *HatTrie, request CacheCommandRequest, options commandExecutionOptions) (CacheCommandResponse, bool) {
	if trie == nil {
		return commandError("trie is not configured"), false
	}
	replicationToken, response, handled, rejected := checkReplicationSafety(request, options.Topology, options.ReplicationSafety)
	if handled {
		return response, rejected
	}
	if response, rejected := rejectNonLeaderWrite(request, options.NodeName, options.Election, options.EnforceLeaderWrites); rejected {
		return response, true
	}
	if options.Journal != nil {
		response = options.Journal.ExecuteCommand(trie, request)
	} else {
		response = trie.ExecuteCommand(request)
	}
	if response.OK {
		options.ReplicationSafety.Commit(replicationToken)
	}
	if options.Replicator != nil {
		options.Replicator.ReplicateCommand(ctx, trie, request, response)
	}
	return response, false
}

func checkReplicationSafety(request CacheCommandRequest, topology *TopologyStore, safety *ReplicationSafetyStore) (replicationSafetyToken, CacheCommandResponse, bool, bool) {
	switch normalizedCommand(request.Command) {
	case "INTERNALSET", "INTERNALDEL":
	default:
		return replicationSafetyToken{}, CacheCommandResponse{}, false, false
	}
	source, sequence, fingerprint := replicationSafetyMetadata(request)
	if fingerprint != "" && topology != nil && topology.VerifiesReplicationFingerprint() {
		localFingerprint := topology.Fingerprint()
		if localFingerprint != "" && localFingerprint != fingerprint {
			return replicationSafetyToken{}, commandError("replication topology fingerprint mismatch"), true, true
		}
	}
	token, duplicate := safety.Check(source, sequence)
	if duplicate {
		return token, CacheCommandResponse{OK: true, Message: "duplicate replication command"}, true, false
	}
	return token, CacheCommandResponse{}, false, false
}

func rejectNonLeaderWrite(request CacheCommandRequest, nodeName string, election *ElectionStore, enforce bool) (CacheCommandResponse, bool) {
	if !enforce || !commandRequiresLeader(request) {
		return CacheCommandResponse{}, false
	}
	if election == nil {
		return commandError("election store is not configured"), true
	}
	route, ok := election.LeaderForKey(strings.TrimSpace(request.Key))
	if !ok {
		return commandError("topology cannot route key"), true
	}
	if !route.Leader.Available || route.Leader.Leader == "" {
		return commandError("no elected leader for key"), true
	}
	if route.Leader.Leader != strings.TrimSpace(nodeName) {
		return commandError("local node is not elected leader for key; leader is " + route.Leader.Leader), true
	}
	return CacheCommandResponse{}, false
}

func commandRequiresLeader(request CacheCommandRequest) bool {
	if !commandShouldJournal(request) {
		return false
	}
	switch normalizedCommand(request.Command) {
	case "INTERNALSET", "INTERNALDEL":
		return false
	default:
		return true
	}
}

func (handler *MonitoringHandler) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.rejectDangerousHTTP(w, r, "snapshot", nil) {
		return
	}
	if handler.options.Snapshot == nil {
		handler.auditHTTP(r, AuditEvent{Action: "snapshot", OK: false, Status: http.StatusConflict, Message: "snapshot path is not configured"})
		writeJSONStatus(w, http.StatusConflict, commandError("snapshot path is not configured"))
		return
	}
	if err := handler.options.Snapshot(); err != nil {
		handler.auditHTTP(r, AuditEvent{Action: "snapshot", OK: false, Status: http.StatusInternalServerError, Message: err.Error()})
		writeJSONStatus(w, http.StatusInternalServerError, commandError(err.Error()))
		return
	}
	handler.auditHTTP(r, AuditEvent{Action: "snapshot", OK: true, Status: http.StatusOK, Message: "snapshot saved"})
	writeJSON(w, CacheCommandResponse{OK: true, Message: "snapshot saved"})
}

func (handler *MonitoringHandler) handleBackup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if !handler.requireTrie(w) {
		return
	}
	decoder, closeBody, bodyTooLarge, ok := monitoringJSONDecoder(w, r)
	if !ok {
		return
	}
	defer closeBody()
	decoder.DisallowUnknownFields()
	var request backupBundleRequest
	if err := decoder.Decode(&request); err != nil {
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid backup request")
		return
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid backup request")
		return
	}
	if writeMonitoringRequestTooLarge(w, bodyTooLarge()) {
		return
	}
	request.Path = strings.TrimSpace(request.Path)
	if request.Path == "" {
		writeJSONStatus(w, http.StatusBadRequest, commandError("backup path is required"))
		return
	}
	if handler.rejectDangerousHTTP(w, r, "backup", map[string]interface{}{"path": request.Path}) {
		return
	}
	format := handler.options.BackupSnapshotFormat
	if request.SnapshotFormat != "" {
		parsed, err := ParseSnapshotFormat(request.SnapshotFormat)
		if err != nil {
			writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
			return
		}
		format = parsed
	}
	manifest, err := CreateBackupBundle(request.Path, handler.trie, handler.options.Journal, BackupBundleOptions{SnapshotFormat: format})
	if err != nil {
		handler.auditHTTP(r, AuditEvent{Action: "backup", OK: false, Status: http.StatusInternalServerError, Message: err.Error(), Details: map[string]interface{}{"path": request.Path}})
		writeJSONStatus(w, http.StatusInternalServerError, commandError(err.Error()))
		return
	}
	handler.auditHTTP(r, AuditEvent{Action: "backup", OK: true, Status: http.StatusOK, Details: map[string]interface{}{"path": request.Path, "snapshot_format": manifest.SnapshotFormat, "journal_sequence": manifest.JournalSequence}})
	writeJSON(w, manifest)
}

func (handler *MonitoringHandler) handleStorage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	writeJSON(w, handler.storageStatus())
}

func (handler *MonitoringHandler) storageStatus() storageStatus {
	status := storageStatus{
		LevelDBConfigured: handler.options.LevelDBStore != nil,
		Operation:         handler.storageOperationStatus(time.Now().UTC()),
	}
	handler.storageMu.Lock()
	if handler.storage.lastFlush != nil {
		flush := *handler.storage.lastFlush
		status.LastFlush = &flush
	}
	if handler.storage.lastCompact != nil {
		compact := *handler.storage.lastCompact
		status.LastCompact = &compact
	}
	handler.storageMu.Unlock()

	store := handler.options.LevelDBStore
	if store == nil {
		return status
	}
	status.Store = "leveldb"
	status.Path = store.path
	status.Format = string(store.format)
	db, unlock, err := store.lockDB()
	if err != nil {
		status.Error = err.Error()
		return status
	}
	defer unlock()
	status.SizeBytes, err = directorySizeBytes(store.path)
	if err != nil {
		status.Error = err.Error()
	}
	status.Properties = levelDBProperties(db)
	return status
}

func (handler *MonitoringHandler) storageOperationStatus(now time.Time) storageOperationStatus {
	handler.storageMu.Lock()
	defer handler.storageMu.Unlock()
	if handler.storage.action == "" {
		return storageOperationStatus{}
	}
	startedAt := handler.storage.startedAt
	return storageOperationStatus{
		Running:   true,
		Action:    handler.storage.action,
		StartedAt: &startedAt,
		AgeMillis: now.Sub(startedAt).Milliseconds(),
	}
}

func (handler *MonitoringHandler) beginStorageOperation(action string) bool {
	handler.storageMu.Lock()
	defer handler.storageMu.Unlock()
	if handler.storage.action != "" {
		return false
	}
	handler.storage.action = action
	handler.storage.startedAt = time.Now().UTC()
	return true
}

func (handler *MonitoringHandler) finishStorageOperation() {
	handler.storageMu.Lock()
	defer handler.storageMu.Unlock()
	handler.storage.action = ""
	handler.storage.startedAt = time.Time{}
}

func (handler *MonitoringHandler) recordStorageFlush(result LevelDBFlushResult) {
	handler.storageMu.Lock()
	defer handler.storageMu.Unlock()
	handler.storage.lastFlush = &result
}

func (handler *MonitoringHandler) recordStorageCompact(result LevelDBCompactionResult) {
	handler.storageMu.Lock()
	defer handler.storageMu.Unlock()
	handler.storage.lastCompact = &result
}

func (handler *MonitoringHandler) handleStorageFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.options.LevelDBStore == nil {
		handler.auditHTTP(r, AuditEvent{Action: "storage.flush", OK: false, Status: http.StatusConflict, Message: "leveldb store is not configured"})
		writeJSONStatus(w, http.StatusConflict, commandError("leveldb store is not configured"))
		return
	}
	decoder, closeBody, bodyTooLarge, ok := monitoringJSONDecoder(w, r)
	if !ok {
		return
	}
	defer closeBody()
	decoder.DisallowUnknownFields()
	var request struct{}
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid storage flush request")
		return
	} else if err == nil {
		var extra struct{}
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid storage flush request")
			return
		}
	}
	if writeMonitoringRequestTooLarge(w, bodyTooLarge()) {
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.rejectDangerousHTTP(w, r, "storage.flush", map[string]interface{}{"store": "leveldb"}) {
		return
	}
	if !handler.beginStorageOperation("flush") {
		handler.auditHTTP(r, AuditEvent{Action: "storage.flush", OK: false, Status: http.StatusConflict, Message: "storage operation is already running", Details: map[string]interface{}{"store": "leveldb"}})
		writeJSONStatus(w, http.StatusConflict, commandError("storage operation is already running"))
		return
	}
	defer handler.finishStorageOperation()
	result, err := handler.options.LevelDBStore.Flush(handler.trie)
	if err != nil {
		handler.auditHTTP(r, AuditEvent{Action: "storage.flush", OK: false, Status: http.StatusInternalServerError, Message: err.Error(), Details: map[string]interface{}{"store": "leveldb"}})
		writeJSONStatus(w, http.StatusInternalServerError, commandError(err.Error()))
		return
	}
	handler.recordStorageFlush(result)
	handler.auditHTTP(r, AuditEvent{Action: "storage.flush", OK: true, Status: http.StatusOK, Details: map[string]interface{}{"store": result.Store, "keys": result.Keys, "duration_millis": result.DurationMillis}})
	writeJSON(w, result)
}

func (handler *MonitoringHandler) handleStorageCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.options.LevelDBStore == nil {
		handler.auditHTTP(r, AuditEvent{Action: "storage.compact", OK: false, Status: http.StatusConflict, Message: "leveldb store is not configured"})
		writeJSONStatus(w, http.StatusConflict, commandError("leveldb store is not configured"))
		return
	}
	decoder, closeBody, bodyTooLarge, ok := monitoringJSONDecoder(w, r)
	if !ok {
		return
	}
	defer closeBody()
	decoder.DisallowUnknownFields()
	var request storageCompactRequest
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid storage compact request")
		return
	} else if err == nil {
		var extra struct{}
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid storage compact request")
			return
		}
	}
	if writeMonitoringRequestTooLarge(w, bodyTooLarge()) {
		return
	}
	request.StartKey = strings.TrimSpace(request.StartKey)
	request.LimitKey = strings.TrimSpace(request.LimitKey)
	details := map[string]interface{}{"store": "leveldb", "start_key": request.StartKey, "limit_key": request.LimitKey}
	if requestContextDone(w, r) {
		return
	}
	if handler.rejectDangerousHTTP(w, r, "storage.compact", details) {
		return
	}
	if !handler.beginStorageOperation("compact") {
		handler.auditHTTP(r, AuditEvent{Action: "storage.compact", OK: false, Status: http.StatusConflict, Message: "storage operation is already running", Details: details})
		writeJSONStatus(w, http.StatusConflict, commandError("storage operation is already running"))
		return
	}
	defer handler.finishStorageOperation()
	result, err := handler.options.LevelDBStore.Compact(LevelDBCompactionOptions{StartKey: request.StartKey, LimitKey: request.LimitKey})
	if err != nil {
		handler.auditHTTP(r, AuditEvent{Action: "storage.compact", OK: false, Status: http.StatusInternalServerError, Message: err.Error(), Details: details})
		writeJSONStatus(w, http.StatusInternalServerError, commandError(err.Error()))
		return
	}
	handler.recordStorageCompact(result)
	handler.auditHTTP(r, AuditEvent{Action: "storage.compact", OK: true, Status: http.StatusOK, Details: map[string]interface{}{"store": result.Store, "start_key": result.StartKey, "limit_key": result.LimitKey, "duration_millis": result.DurationMillis}})
	writeJSON(w, result)
}

func (handler *MonitoringHandler) handleTopology(w http.ResponseWriter, r *http.Request) {
	if handler.options.Topology == nil {
		writeJSONStatus(w, http.StatusConflict, commandError("topology store is not configured"))
		return
	}

	switch r.Method {
	case http.MethodGet:
		if requestContextDone(w, r) {
			return
		}
		key := r.URL.Query().Get("key")
		if key != "" {
			route, ok := handler.options.Topology.Route(key)
			if !ok {
				writeJSONStatus(w, http.StatusConflict, commandError("topology has no shards"))
				return
			}
			writeJSON(w, route)
			return
		}
		writeJSON(w, handler.options.Topology.Get())
	case http.MethodPut:
		if requestContextDone(w, r) {
			return
		}
		var topology ClusterTopology
		decoder, closeBody, bodyTooLarge, ok := monitoringJSONDecoder(w, r)
		if !ok {
			return
		}
		defer closeBody()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&topology); err != nil {
			writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid topology request")
			return
		}
		var extra struct{}
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid topology request")
			return
		}
		if writeMonitoringRequestTooLarge(w, bodyTooLarge()) {
			return
		}
		if requestContextDone(w, r) {
			return
		}
		if handler.rejectDangerousHTTP(w, r, "topology.update", map[string]interface{}{"mode": topology.Mode, "version": topology.Version}) {
			return
		}
		if err := handler.options.Topology.Set(topology); err != nil {
			handler.auditHTTP(r, AuditEvent{Action: "topology.update", OK: false, Status: http.StatusBadRequest, Message: err.Error(), Details: map[string]interface{}{"mode": topology.Mode, "version": topology.Version}})
			writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
			return
		}
		handler.auditHTTP(r, AuditEvent{Action: "topology.update", OK: true, Status: http.StatusOK, Details: map[string]interface{}{"mode": topology.Mode, "version": topology.Version, "nodes": len(topology.Nodes), "shards": len(topology.Shards)}})
		writeJSON(w, handler.options.Topology.Get())
	default:
		writeMethodNotAllowed(w)
	}
}

type electionUpdateRequest struct {
	Node   string `json:"node"`
	Online *bool  `json:"online,omitempty"`
}

func (handler *MonitoringHandler) handleElection(w http.ResponseWriter, r *http.Request) {
	if handler.options.Election == nil {
		writeJSONStatus(w, http.StatusConflict, commandError("election store is not configured"))
		return
	}

	switch r.Method {
	case http.MethodGet:
		if requestContextDone(w, r) {
			return
		}
		key := r.URL.Query().Get("key")
		if key != "" {
			route, ok := handler.options.Election.LeaderForKey(key)
			if !ok {
				writeJSONStatus(w, http.StatusConflict, commandError("topology cannot route key"))
				return
			}
			writeJSON(w, route)
			return
		}
		writeJSON(w, handler.options.Election.Status())
	case http.MethodPost:
		if requestContextDone(w, r) {
			return
		}
		var request electionUpdateRequest
		decoder, closeBody, bodyTooLarge, ok := monitoringJSONDecoder(w, r)
		if !ok {
			return
		}
		defer closeBody()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid election request")
			return
		}
		var extra struct{}
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid election request")
			return
		}
		if writeMonitoringRequestTooLarge(w, bodyTooLarge()) {
			return
		}
		if requestContextDone(w, r) {
			return
		}
		details := map[string]interface{}{"node": request.Node, "online": request.Online == nil || *request.Online}
		if handler.rejectDangerousHTTP(w, r, "election.update", details) {
			return
		}
		var err error
		if request.Online == nil || *request.Online {
			err = handler.options.Election.Heartbeat(request.Node)
		} else {
			err = handler.options.Election.MarkOffline(request.Node)
		}
		if err != nil {
			handler.auditHTTP(r, AuditEvent{Action: "election.update", OK: false, Status: http.StatusBadRequest, Message: err.Error(), Details: details})
			writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
			return
		}
		handler.auditHTTP(r, AuditEvent{Action: "election.update", OK: true, Status: http.StatusOK, Details: details})
		writeJSON(w, handler.options.Election.Status())
	default:
		writeMethodNotAllowed(w)
	}
}

func (handler *MonitoringHandler) handleReplication(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.options.Replicator == nil {
		writeJSONStatus(w, http.StatusConflict, commandError("replication is not configured"))
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, handler.options.Replicator.LastResult())
		return
	}

	var request replicationSyncRequest
	decoder, closeBody, bodyTooLarge, ok := monitoringJSONDecoder(w, r)
	if !ok {
		return
	}
	defer closeBody()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid replication request")
		return
	} else if err == nil {
		var extra struct{}
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid replication request")
			return
		}
	}
	if writeMonitoringRequestTooLarge(w, bodyTooLarge()) {
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.rejectDangerousHTTP(w, r, "replication.sync", map[string]interface{}{"prefix": request.Prefix}) {
		return
	}
	result := handler.options.Replicator.SyncAll(r.Context(), handler.trie, request.Prefix)
	handler.auditHTTP(r, AuditEvent{Action: "replication.sync", OK: !result.Skipped, Status: http.StatusOK, Message: result.Reason, Details: map[string]interface{}{"prefix": request.Prefix, "entries": result.Entries}})
	writeJSON(w, result)
}

func (handler *MonitoringHandler) handleJournal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	if handler.options.Journal == nil {
		writeJSONStatus(w, http.StatusConflict, commandError("journal is not configured"))
		return
	}
	if r.Method == http.MethodPost {
		handler.handleJournalPull(w, r)
		return
	}
	limit, err := commandJournalTailLimit(r.URL.Query().Get("limit"))
	if err != nil {
		writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
		return
	}
	afterSequence := uint64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("after_sequence")); raw != "" {
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			writeJSONStatus(w, http.StatusBadRequest, commandError("after_sequence must be an unsigned integer"))
			return
		}
		afterSequence = value
	}
	tail, err := handler.options.Journal.Tail(afterSequence, limit)
	if err != nil {
		if errors.Is(err, ErrCommandJournalCompacted) {
			writeJSONStatus(w, http.StatusConflict, commandError(err.Error()))
			return
		}
		writeJSONStatus(w, http.StatusInternalServerError, commandError(err.Error()))
		return
	}
	writeJSON(w, tail)
}

func (handler *MonitoringHandler) handleJournalPull(w http.ResponseWriter, r *http.Request) {
	var request journalPullRequest
	decoder, closeBody, bodyTooLarge, ok := monitoringJSONDecoder(w, r)
	if !ok {
		return
	}
	defer closeBody()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid journal pull request")
		return
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid journal pull request")
		return
	}
	if writeMonitoringRequestTooLarge(w, bodyTooLarge()) {
		return
	}
	source := strings.TrimSpace(request.Source)
	if requestContextDone(w, r) {
		return
	}
	if handler.rejectDangerousHTTP(w, r, "journal.pull", map[string]interface{}{"source": source, "after_sequence": request.AfterSequence}) {
		return
	}

	result, err := PullCommandJournal(r.Context(), handler.trie, handler.options.Journal, CommandJournalPullOptions{
		Source:        source,
		AfterSequence: request.AfterSequence,
		Limit:         request.Limit,
		UntilCurrent:  request.UntilCurrent,
		MaxBatches:    request.MaxBatches,
		Timeout:       DefaultCommandJournalPullTimeout,
	})
	if err != nil {
		handler.auditHTTP(r, AuditEvent{Action: "journal.pull", OK: false, Status: http.StatusBadGateway, Message: err.Error(), Details: map[string]interface{}{"source": source, "after_sequence": request.AfterSequence}})
		var pullErr *CommandJournalPullError
		if errors.As(err, &pullErr) && pullErr.Status > 0 {
			writeJSONStatus(w, pullErr.Status, commandError(pullErr.Error()))
			return
		}
		writeJSONStatus(w, http.StatusBadGateway, commandError(err.Error()))
		return
	}
	handler.auditHTTP(r, AuditEvent{Action: "journal.pull", OK: true, Status: http.StatusOK, Details: map[string]interface{}{"source": source, "after_sequence": request.AfterSequence, "applied": result.Applied, "applied_through": result.AppliedThrough}})
	writeJSON(w, result)
}

func fetchCommandJournalTail(ctx context.Context, client *http.Client, endpoint string) (CommandJournalTail, int, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if client == nil {
		client = http.DefaultClient
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return CommandJournalTail{}, 0, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return CommandJournalTail{}, 0, err
	}
	defer drainAndClose(resp.Body)

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, readErr := readCommandJournalErrorResponseBody(resp.Body)
		if readErr != nil {
			return CommandJournalTail{}, resp.StatusCode, readErr
		}
		return CommandJournalTail{}, resp.StatusCode, fmt.Errorf("journal source returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
	}
	tail, err := decodeCommandJournalTailResponse(resp.Body)
	if err != nil {
		return CommandJournalTail{}, resp.StatusCode, err
	}
	return tail, resp.StatusCode, nil
}

func decodeCommandJournalTailResponse(body io.Reader) (CommandJournalTail, error) {
	limited := &io.LimitedReader{R: body, N: maxCommandJournalTailResponseBytes + 1}
	decoder := jsonwire.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	var tail CommandJournalTail
	if err := decoder.Decode(&tail); err != nil {
		if limitedReaderExceeded(limited) {
			return CommandJournalTail{}, errCommandJournalTailResponseTooLarge
		}
		return CommandJournalTail{}, err
	}
	if limited.N <= 0 {
		return CommandJournalTail{}, errCommandJournalTailResponseTooLarge
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if limitedReaderExceeded(limited) {
			return CommandJournalTail{}, errCommandJournalTailResponseTooLarge
		}
		return CommandJournalTail{}, errors.New("journal source returned invalid trailing JSON")
	}
	if limitedReaderExceeded(limited) {
		return CommandJournalTail{}, errCommandJournalTailResponseTooLarge
	}
	if tail.Entries == nil {
		tail.Entries = []CommandJournalRecord{}
	}
	return tail, nil
}

func readCommandJournalErrorResponseBody(body io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, maxCommandJournalErrorResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) <= maxCommandJournalErrorResponseBytes {
		return data, nil
	}
	data = data[:maxCommandJournalErrorResponseBytes]
	data = append(data, truncatedCommandJournalErrorResponseSuffix...)
	return data, nil
}

func commandJournalEndpoint(source string, afterSequence uint64, limit int) (string, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		return "", errors.New("journal source is required")
	}
	if !strings.Contains(source, "://") {
		source = "http://" + source
	}
	parsed, err := url.Parse(source)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", errors.New("journal source is invalid")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/api/journal"
	query := parsed.Query()
	if afterSequence > 0 {
		query.Set("after_sequence", strconv.FormatUint(afterSequence, 10))
	} else {
		query.Del("after_sequence")
	}
	if limit > 0 {
		query.Set("limit", strconv.Itoa(limit))
	} else {
		query.Del("limit")
	}
	parsed.RawQuery = query.Encode()
	parsed.Fragment = ""
	return parsed.String(), nil
}

func commandJournalTailLimit(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DefaultCommandJournalTailLimit, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, errors.New("limit must be a positive unsigned integer")
	}
	return normalizeCommandJournalTailLimit(value)
}

func monitoringEntriesLimit(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || value == 0 {
		return 0, errors.New("limit must be a positive unsigned integer")
	}
	if value > maxMonitoringEntriesLimit {
		return 0, fmt.Errorf("limit must be <= %d", maxMonitoringEntriesLimit)
	}
	return int(value), nil
}

func monitoringEntriesAfterKey(prefix string, raw string, present bool) (string, bool, error) {
	if !present {
		return "", false, nil
	}
	if err := validateKey(raw); err != nil {
		return "", false, err
	}
	if prefix != "" && !strings.HasPrefix(raw, prefix) {
		return "", false, errors.New("after_key must match prefix")
	}
	return raw, true, nil
}

func normalizeCommandJournalTailLimit(value uint64) (int, error) {
	if value == 0 {
		return DefaultCommandJournalTailLimit, nil
	}
	if value > uint64(MaxCommandJournalTailLimit) {
		return 0, fmt.Errorf("limit must be <= %d", MaxCommandJournalTailLimit)
	}
	return int(value), nil
}

func normalizeCommandJournalPullBatches(untilCurrent bool, value uint64) (int, error) {
	if !untilCurrent {
		if value > 0 {
			return 0, errors.New("max_batches requires until_current")
		}
		return 1, nil
	}
	if value == 0 {
		return DefaultCommandJournalPullBatches, nil
	}
	if value > uint64(MaxCommandJournalPullBatches) {
		return 0, fmt.Errorf("max_batches must be <= %d", MaxCommandJournalPullBatches)
	}
	return int(value), nil
}

func (ht *HatTrie) diskSpillBytes() uint64 {
	ht.mu.RLock()
	defer ht.mu.RUnlock()

	if ht.disks == nil {
		return 0
	}
	var total uint64
	for _, path := range ht.disks.paths {
		info, err := os.Stat(path)
		if err == nil {
			total += uint64(info.Size())
		}
	}
	return total
}

func (ht *HatTrie) monitoringEntries(prefix string) []MonitoringEntry {
	return ht.monitoringEntriesLimited(prefix, 0).Entries
}

func (ht *HatTrie) monitoringEntriesLimited(prefix string, limit int) MonitoringEntriesResponse {
	return ht.monitoringEntriesPage(prefix, "", false, limit)
}

func (ht *HatTrie) monitoringEntriesPage(prefix string, afterKey string, hasAfterKey bool, limit int) MonitoringEntriesResponse {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	now := time.Time{}
	if len(ht.expires) > 0 {
		now = ht.currentTime()
	}
	response := MonitoringEntriesResponse{}
	if limit > 0 {
		response.Limit = uint64(limit)
	}
	if hasAfterKey {
		response.AfterKey = afterKey
		response.afterKeySet = true
	}
	err := ht.scanEntriesWithPrefixAtLockedChecked(prefix, true, now, func(entry Entry) error {
		if hasAfterKey && entry.Key <= afterKey {
			return nil
		}
		if limit > 0 && len(response.Entries) >= limit {
			response.HasMore = true
			return errMonitoringEntriesLimitReached
		}
		ttl := ttlMillis(ht.expires[entry.Key], now)
		size, preview := ht.monitoringPreviewLocked(entry.Value)
		response.Entries = append(response.Entries, MonitoringEntry{
			Key:          entry.Key,
			Type:         monitoringType(entry.Value),
			TTLMillis:    ttl,
			OnDisk:       entry.Value.OnDisk(),
			SizeBytes:    size,
			ValuePreview: preview,
		})
		return nil
	})
	if err != nil && !errors.Is(err, errMonitoringEntriesLimitReached) {
		return MonitoringEntriesResponse{Entries: []MonitoringEntry{}, Limit: response.Limit}
	}
	if response.Entries == nil {
		response.Entries = []MonitoringEntry{}
	}
	if response.HasMore && len(response.Entries) > 0 {
		response.NextAfterKey = response.Entries[len(response.Entries)-1].Key
		response.nextAfterKeySet = true
	}
	return response
}

func ttlMillis(expiresAt time.Time, now time.Time) *int64 {
	if expiresAt.IsZero() {
		return nil
	}
	remaining := expiresAt.Sub(now).Milliseconds()
	if remaining < 0 {
		remaining = 0
	}
	return &remaining
}

func (ht *HatTrie) monitoringPreviewLocked(hval HatValue) (int64, string) {
	switch hval.Type() {
	case DATAVALUE_TYPE_COUNTER:
		return 4, strconv.FormatInt(int64(hval.Index), 10)
	case DATAVALUE_TYPE_RAW_STRING:
		if int(hval.Index) >= len(ht.raws.array) || hval.Index < 0 {
			return 0, ""
		}
		value := ht.raws.stringValue(hval.Index)
		return int64(len(value)), truncatePreview(value)
	case DATAVALUE_TYPE_RAW_BYTES:
		if hval.OnDisk() {
			if int(hval.Index) >= len(ht.disks.paths) || hval.Index < 0 {
				return 0, ""
			}
			path := ht.disks.paths[hval.Index]
			info, err := os.Stat(path)
			if err != nil {
				return 0, "missing disk value"
			}
			return info.Size(), strconv.FormatInt(info.Size(), 10) + " bytes"
		}
		if int(hval.Index) >= len(ht.raws.array) || hval.Index < 0 {
			return 0, ""
		}
		size := len(ht.raws.array[hval.Index])
		return int64(size), strconv.Itoa(size) + " bytes"
	case DATAVALUE_TYPE_MAP:
		if int(hval.Index) >= len(ht.maps.array) || hval.Index < 0 {
			return 0, ""
		}
		value := ht.maps.array[hval.Index]
		return int64(len(value)), strconv.Itoa(len(value)) + " fields"
	case DATAVALUE_TYPE_SLICE:
		if int(hval.Index) >= len(ht.slices.array) || hval.Index < 0 {
			return 0, ""
		}
		value := ht.slices.array[hval.Index]
		return int64(value.Len()), strconv.Itoa(value.Len()) + " items"
	case DATAVALUE_TYPE_LEVELDB_REF:
		ref, ok := ht.dbrefs.Get(hval.Index)
		if !ok {
			return 0, "missing cold value"
		}
		return 0, "cold " + ref.Type + " value"
	case DATAVALUE_TYPE_SET:
		if int(hval.Index) >= len(ht.sets.array) || hval.Index < 0 {
			return 0, ""
		}
		value := ht.sets.array[hval.Index]
		return int64(value.Len()), strconv.Itoa(value.Len()) + " members"
	case DATAVALUE_TYPE_PRIORITY_QUEUE:
		if int(hval.Index) >= len(ht.priorityQueues.array) || hval.Index < 0 {
			return 0, ""
		}
		value := ht.priorityQueues.array[hval.Index]
		return int64(value.Len()), strconv.Itoa(value.Len()) + " priority items"
	case DATAVALUE_TYPE_BLOOM_FILTER:
		if int(hval.Index) >= len(ht.bloomFilters.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.bloomFilters.array[hval.Index].Info()
		return int64(info.BitBytes), strconv.FormatUint(info.BitCount, 10) + " bits, " + strconv.Itoa(int(info.HashCount)) + " hashes"
	case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
		if int(hval.Index) >= len(ht.countMinSketches.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.countMinSketches.array[hval.Index].Info()
		return int64(info.CounterBytes), strconv.FormatUint(info.Width, 10) + "x" + strconv.Itoa(int(info.Depth)) + " counters, " + strconv.FormatUint(info.TotalCount, 10) + " total"
	case DATAVALUE_TYPE_HYPERLOGLOG:
		if int(hval.Index) >= len(ht.hyperLogLogs.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.hyperLogLogs.array[hval.Index].Info()
		return int64(info.RegisterBytes), strconv.Itoa(int(info.Precision)) + " precision, " + strconv.FormatUint(info.Estimate, 10) + " estimated"
	case DATAVALUE_TYPE_TOP_K:
		if int(hval.Index) >= len(ht.topKs.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.topKs.array[hval.Index].Info()
		return ht.topKs.array[hval.Index].EncodedSize(), strconv.FormatUint(info.Tracked, 10) + "/" + strconv.FormatUint(info.Capacity, 10) + " tracked, " + strconv.FormatUint(info.Total, 10) + " total"
	case DATAVALUE_TYPE_CUCKOO_FILTER:
		if int(hval.Index) >= len(ht.cuckooFilters.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.cuckooFilters.array[hval.Index].Info()
		return int64(info.FingerprintBytes), strconv.FormatUint(info.Count, 10) + "/" + strconv.FormatUint(info.Capacity, 10) + " slots, " + strconv.Itoa(int(info.FingerprintBits)) + "-bit fingerprints"
	case DATAVALUE_TYPE_ROARING_BITMAP:
		if int(hval.Index) >= len(ht.roaringBitmaps.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.roaringBitmaps.array[hval.Index].Info()
		return int64(info.EncodedBytes), strconv.FormatUint(info.Cardinality, 10) + " integers, " + strconv.FormatUint(info.Containers, 10) + " containers"
	case DATAVALUE_TYPE_SPARSE_BITSET:
		if int(hval.Index) >= len(ht.sparseBitsets.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.sparseBitsets.array[hval.Index].Info()
		return int64(info.EncodedBytes), strconv.FormatUint(info.Cardinality, 10) + " integers, " + strconv.FormatUint(info.Containers, 10) + " containers"
	case DATAVALUE_TYPE_QUANTILE_SKETCH:
		if int(hval.Index) >= len(ht.quantileSketches.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.quantileSketches.array[hval.Index].Info()
		return info.EncodedBytes, strconv.FormatUint(info.Count, 10) + " samples, " + strconv.FormatUint(info.SummarySize, 10) + " summary points"
	case DATAVALUE_TYPE_FENWICK_TREE:
		if int(hval.Index) >= len(ht.fenwickTrees.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.fenwickTrees.array[hval.Index].Info()
		return int64(info.TreeBytes), strconv.FormatUint(info.Size, 10) + " counters, " + strconv.FormatInt(info.Total, 10) + " total"
	case DATAVALUE_TYPE_RESERVOIR_SAMPLE:
		if int(hval.Index) >= len(ht.reservoirSamples.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.reservoirSamples.array[hval.Index].Info()
		return info.EncodedBytes, strconv.FormatUint(info.Tracked, 10) + "/" + strconv.FormatUint(info.Capacity, 10) + " sampled, " + strconv.FormatUint(info.Seen, 10) + " seen"
	case DATAVALUE_TYPE_XOR_FILTER:
		if int(hval.Index) >= len(ht.xorFilters.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.xorFilters.array[hval.Index].Info()
		if info.Built {
			return int64(info.FingerprintBytes), strconv.FormatUint(info.Items, 10) + " items, " + strconv.FormatUint(info.FingerprintBytes, 10) + " fingerprint bytes"
		}
		return ht.xorFilters.array[hval.Index].EncodedSize(), strconv.FormatUint(info.Staged, 10) + " staged items"
	case DATAVALUE_TYPE_RADIX_TREE:
		if int(hval.Index) >= len(ht.radixTrees.array) || hval.Index < 0 {
			return 0, ""
		}
		info := ht.radixTrees.array[hval.Index].Info()
		return int64(info.EncodedBytes), strconv.FormatUint(info.Items, 10) + " items, " + strconv.FormatUint(info.Nodes, 10) + " nodes"
	default:
		return 0, ""
	}
}

func monitoringType(hval HatValue) string {
	switch hval.Type() {
	case DATAVALUE_TYPE_COUNTER:
		return "counter"
	case DATAVALUE_TYPE_RAW_STRING:
		return "string"
	case DATAVALUE_TYPE_RAW_BYTES:
		return "bytes"
	case DATAVALUE_TYPE_MAP:
		return "map"
	case DATAVALUE_TYPE_SLICE:
		return "slice"
	case DATAVALUE_TYPE_LEVELDB_REF:
		return "leveldb_ref"
	case DATAVALUE_TYPE_SET:
		return "set"
	case DATAVALUE_TYPE_PRIORITY_QUEUE:
		return "priority_queue"
	case DATAVALUE_TYPE_BLOOM_FILTER:
		return "bloom_filter"
	case DATAVALUE_TYPE_COUNT_MIN_SKETCH:
		return "count_min_sketch"
	case DATAVALUE_TYPE_HYPERLOGLOG:
		return "hyperloglog"
	case DATAVALUE_TYPE_TOP_K:
		return "top_k"
	case DATAVALUE_TYPE_CUCKOO_FILTER:
		return "cuckoo_filter"
	case DATAVALUE_TYPE_ROARING_BITMAP:
		return "roaring_bitmap"
	case DATAVALUE_TYPE_QUANTILE_SKETCH:
		return "quantile_sketch"
	case DATAVALUE_TYPE_FENWICK_TREE:
		return "fenwick_tree"
	case DATAVALUE_TYPE_SPARSE_BITSET:
		return "sparse_bitset"
	case DATAVALUE_TYPE_RESERVOIR_SAMPLE:
		return "reservoir_sample"
	case DATAVALUE_TYPE_XOR_FILTER:
		return "xor_filter"
	case DATAVALUE_TYPE_RADIX_TREE:
		return "radix_tree"
	default:
		return "unknown"
	}
}

func truncatePreview(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 80 {
		return value
	}
	limit := 77
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit] + "..."
}

func writeJSON(w http.ResponseWriter, value interface{}) {
	writeJSONStatus(w, http.StatusOK, value)
}

func writeJSONStatus(w http.ResponseWriter, status int, value interface{}) {
	data, err := jsonwire.Marshal(value)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data = append(data, '\n')
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func monitoringJSONDecoder(w http.ResponseWriter, r *http.Request) (*jsonwire.Decoder, func(), func() bool, bool) {
	body, closeBody, ok := limitedEncodedRequestBody(w, r, maxMonitoringJSONRequestBytes)
	if !ok {
		return nil, nil, nil, false
	}
	return jsonwire.NewDecoder(body), closeBody, func() bool {
		return trackedRequestBodyTooLarge(body)
	}, true
}

func monitoringCommandRequestFormat(w http.ResponseWriter, r *http.Request) (CommandWireFormat, bool) {
	format, ok := commandWireFormatFromContentType(r.Header.Get("Content-Type"))
	if !ok {
		_ = r.Body.Close()
		http.Error(w, "unsupported command request content type", http.StatusUnsupportedMediaType)
		return "", false
	}
	return format, true
}

func monitoringCommandRequest(w http.ResponseWriter, r *http.Request) (CacheCommandRequest, CommandWireFormat, func(), bool) {
	format, ok := monitoringCommandRequestFormat(w, r)
	if !ok {
		return CacheCommandRequest{}, "", nil, false
	}
	request, closeBody, ok := monitoringCommandRequestFromFormat(w, r, format)
	if !ok {
		return CacheCommandRequest{}, "", nil, false
	}
	return request, format, closeBody, true
}

func monitoringCommandRequestFromFormat(w http.ResponseWriter, r *http.Request, format CommandWireFormat) (CacheCommandRequest, func(), bool) {
	body, closeBody, ok := limitedEncodedRequestBody(w, r, maxMonitoringJSONRequestBytes)
	if !ok {
		return CacheCommandRequest{}, nil, false
	}
	bodyTooLarge := func() bool {
		return trackedRequestBodyTooLarge(body)
	}
	if format == CommandWireFormatProtobuf {
		request, err := decodeCommandRequestProto(body, maxMonitoringJSONRequestBytes)
		if err != nil {
			closeBody()
			writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid command request")
			return CacheCommandRequest{}, nil, false
		}
		return request, closeBody, true
	}

	decoder := jsonwire.NewDecoder(body)
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	var request CacheCommandRequest
	if err := decoder.Decode(&request); err != nil {
		closeBody()
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid command request")
		return CacheCommandRequest{}, nil, false
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		closeBody()
		writeInvalidMonitoringRequest(w, err, bodyTooLarge(), "invalid command request")
		return CacheCommandRequest{}, nil, false
	}
	if writeMonitoringRequestTooLarge(w, bodyTooLarge()) {
		closeBody()
		return CacheCommandRequest{}, nil, false
	}
	return request, closeBody, true
}

func writeInvalidMonitoringRequest(w http.ResponseWriter, err error, requestTooLarge bool, message string) {
	var maxBytesErr *http.MaxBytesError
	if requestTooLarge || errors.As(err, &maxBytesErr) || errors.Is(err, errReplicationResponseTooLarge) {
		http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, message, http.StatusBadRequest)
}

func writeMonitoringRequestTooLarge(w http.ResponseWriter, requestTooLarge bool) bool {
	if !requestTooLarge {
		return false
	}
	http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
	return true
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func requestContextDone(w http.ResponseWriter, r *http.Request) bool {
	if err := r.Context().Err(); err != nil {
		writeJSONStatus(w, http.StatusRequestTimeout, commandError(err.Error()))
		return true
	}
	return false
}
