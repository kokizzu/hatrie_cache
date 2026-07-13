package hatriecache

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type MonitoringOptions struct {
	NodeName   string
	WebDir     string
	StartAt    time.Time
	Snapshot   func() error
	Journal    *CommandJournal
	Topology   *TopologyStore
	Election   *ElectionStore
	Replicator *HTTPReplicator
}

type MonitoringHandler struct {
	trie    *HatTrie
	options MonitoringOptions
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
	Entries []MonitoringEntry `json:"entries"`
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
	if handler.options.WebDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(handler.options.WebDir)))
	}
	return mux
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
	prefix := r.URL.Query().Get("prefix")
	writeJSON(w, MonitoringEntriesResponse{Entries: handler.trie.monitoringEntries(prefix)})
}

func (handler *MonitoringHandler) handleCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeMethodNotAllowed(w)
		return
	}
	if requestContextDone(w, r) {
		return
	}

	var request CacheCommandRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&request); err != nil {
		http.Error(w, "invalid command request", http.StatusBadRequest)
		return
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		http.Error(w, "invalid command request", http.StatusBadRequest)
		return
	}
	if requestContextDone(w, r) {
		return
	}

	var response CacheCommandResponse
	if handler.options.Journal != nil {
		response = handler.options.Journal.ExecuteCommand(handler.trie, request)
	} else {
		response = handler.trie.ExecuteCommand(request)
	}
	if handler.options.Replicator != nil {
		handler.options.Replicator.ReplicateCommand(r.Context(), handler.trie, request, response)
	}
	writeJSON(w, response)
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
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
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
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
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
	if r.Method != http.MethodGet {
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
	writeJSON(w, handler.options.Replicator.LastResult())
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
	ht.mu.Lock()
	defer ht.mu.Unlock()

	entries := ht.entriesWithPrefixLocked(prefix, true)
	out := make([]MonitoringEntry, 0, len(entries))
	now := ht.currentTime()
	for _, entry := range entries {
		ttl := ttlMillis(ht.expires[entry.Key], now)
		size, preview := ht.monitoringPreviewLocked(entry.Value)
		out = append(out, MonitoringEntry{
			Key:          entry.Key,
			Type:         monitoringType(entry.Value),
			TTLMillis:    ttl,
			OnDisk:       entry.Value.OnDisk(),
			SizeBytes:    size,
			ValuePreview: preview,
		})
	}
	return out
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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
