package hatriecache

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
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
	NodeName            string
	WebDir              string
	StartAt             time.Time
	Snapshot            func() error
	Journal             *CommandJournal
	Topology            *TopologyStore
	Election            *ElectionStore
	Replicator          *HTTPReplicator
	EnforceLeaderWrites bool
}

type MonitoringHandler struct {
	trie    *HatTrie
	options MonitoringOptions
}

type commandExecutionOptions struct {
	NodeName            string
	Journal             *CommandJournal
	Election            *ElectionStore
	Replicator          *HTTPReplicator
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
	if options.Election == nil && options.Topology != nil {
		options.Election = NewElectionStore(options.Topology, ElectionOptions{})
		_ = options.Election.Heartbeat(options.NodeName)
	}
	return &MonitoringHandler{trie: trie, options: options}
}

func (handler *MonitoringHandler) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", handler.handleHealth)
	mux.HandleFunc("/api/stats", handler.handleStats)
	mux.HandleFunc("/api/entries", handler.handleEntries)
	mux.HandleFunc("/api/commands", handler.handleCommands)
	mux.HandleFunc("/api/snapshot", handler.handleSnapshot)
	mux.HandleFunc("/api/topology", handler.handleTopology)
	mux.HandleFunc("/api/election", handler.handleElection)
	mux.HandleFunc("/api/replication", handler.handleReplication)
	mux.HandleFunc("/api/journal", handler.handleJournal)
	if handler.options.WebDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(handler.options.WebDir)))
	}
	return gzipHTTPHandler(mux)
}

func (handler *MonitoringHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
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

func (handler *MonitoringHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}
	writeJSON(w, handler.trie.Stats())
}

func (handler *MonitoringHandler) handleEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
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
	response, rejected := executeCacheCommand(r.Context(), handler.trie, request, commandExecutionOptions{
		NodeName:            handler.options.NodeName,
		Journal:             handler.options.Journal,
		Election:            handler.options.Election,
		Replicator:          handler.options.Replicator,
		EnforceLeaderWrites: handler.options.EnforceLeaderWrites,
	})
	if rejected {
		writeCommandResponseWire(w, r, http.StatusConflict, response, requestFormat)
		return
	}
	writeCommandResponseWire(w, r, http.StatusOK, response, requestFormat)
}

func executeCacheCommand(ctx context.Context, trie *HatTrie, request CacheCommandRequest, options commandExecutionOptions) (CacheCommandResponse, bool) {
	if response, rejected := rejectNonLeaderWrite(request, options.NodeName, options.Election, options.EnforceLeaderWrites); rejected {
		return response, true
	}
	var response CacheCommandResponse
	if options.Journal != nil {
		response = options.Journal.ExecuteCommand(trie, request)
	} else {
		response = trie.ExecuteCommand(request)
	}
	if options.Replicator != nil {
		options.Replicator.ReplicateCommand(ctx, trie, request, response)
	}
	return response, false
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
	if handler.options.Snapshot == nil {
		writeJSONStatus(w, http.StatusConflict, commandError("snapshot path is not configured"))
		return
	}
	if err := handler.options.Snapshot(); err != nil {
		writeJSONStatus(w, http.StatusInternalServerError, commandError(err.Error()))
		return
	}
	writeJSON(w, CacheCommandResponse{OK: true, Message: "snapshot saved"})
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
		decoder, closeBody, ok := monitoringJSONDecoder(w, r)
		if !ok {
			return
		}
		defer closeBody()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&topology); err != nil {
			http.Error(w, "invalid topology request", http.StatusBadRequest)
			return
		}
		var extra struct{}
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			http.Error(w, "invalid topology request", http.StatusBadRequest)
			return
		}
		if requestContextDone(w, r) {
			return
		}
		if err := handler.options.Topology.Set(topology); err != nil {
			writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
			return
		}
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
		decoder, closeBody, ok := monitoringJSONDecoder(w, r)
		if !ok {
			return
		}
		defer closeBody()
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil {
			http.Error(w, "invalid election request", http.StatusBadRequest)
			return
		}
		var extra struct{}
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			http.Error(w, "invalid election request", http.StatusBadRequest)
			return
		}
		if requestContextDone(w, r) {
			return
		}
		var err error
		if request.Online == nil || *request.Online {
			err = handler.options.Election.Heartbeat(request.Node)
		} else {
			err = handler.options.Election.MarkOffline(request.Node)
		}
		if err != nil {
			writeJSONStatus(w, http.StatusBadRequest, commandError(err.Error()))
			return
		}
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
	decoder, closeBody, ok := monitoringJSONDecoder(w, r)
	if !ok {
		return
	}
	defer closeBody()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		http.Error(w, "invalid replication request", http.StatusBadRequest)
		return
	} else if err == nil {
		var extra struct{}
		if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
			http.Error(w, "invalid replication request", http.StatusBadRequest)
			return
		}
	}
	if requestContextDone(w, r) {
		return
	}
	writeJSON(w, handler.options.Replicator.SyncAll(r.Context(), handler.trie, request.Prefix))
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
	decoder, closeBody, ok := monitoringJSONDecoder(w, r)
	if !ok {
		return
	}
	defer closeBody()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid journal pull request", http.StatusBadRequest)
		return
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		http.Error(w, "invalid journal pull request", http.StatusBadRequest)
		return
	}
	source := strings.TrimSpace(request.Source)
	if requestContextDone(w, r) {
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
		var pullErr *CommandJournalPullError
		if errors.As(err, &pullErr) && pullErr.Status > 0 {
			writeJSONStatus(w, pullErr.Status, commandError(pullErr.Error()))
			return
		}
		writeJSONStatus(w, http.StatusBadGateway, commandError(err.Error()))
		return
	}
	writeJSON(w, result)
}

func fetchCommandJournalTail(ctx context.Context, client *http.Client, endpoint string) (CommandJournalTail, int, error) {
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
		value := string(ht.raws.array[hval.Index])
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

func monitoringJSONDecoder(w http.ResponseWriter, r *http.Request) (*jsonwire.Decoder, func(), bool) {
	body, closeBody, ok := limitedEncodedRequestBody(w, r, maxMonitoringJSONRequestBytes)
	if !ok {
		return nil, nil, false
	}
	return jsonwire.NewDecoder(body), closeBody, true
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
	if format == CommandWireFormatProtobuf {
		request, err := decodeCommandRequestProto(body, maxMonitoringJSONRequestBytes)
		if err != nil {
			closeBody()
			http.Error(w, "invalid command request", http.StatusBadRequest)
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
		http.Error(w, "invalid command request", http.StatusBadRequest)
		return CacheCommandRequest{}, nil, false
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		closeBody()
		http.Error(w, "invalid command request", http.StatusBadRequest)
		return CacheCommandRequest{}, nil, false
	}
	return request, closeBody, true
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
