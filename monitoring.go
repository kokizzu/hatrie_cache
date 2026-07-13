package hatriecache

import (
	"encoding/json"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type MonitoringOptions struct {
	NodeName string
	WebDir   string
	StartAt  time.Time
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
	return &MonitoringHandler{trie: trie, options: options}
}

func (handler *MonitoringHandler) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/health", handler.handleHealth)
	mux.HandleFunc("/api/stats", handler.handleStats)
	mux.HandleFunc("/api/entries", handler.handleEntries)
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
	writeJSON(w, handler.trie.Stats())
}

func (handler *MonitoringHandler) handleEntries(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeMethodNotAllowed(w)
		return
	}
	prefix := r.URL.Query().Get("prefix")
	writeJSON(w, MonitoringEntriesResponse{Entries: handler.trie.monitoringEntries(prefix)})
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
		return int64(len(value)), strconv.Itoa(len(value)) + " items"
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
	default:
		return "unknown"
	}
}

func truncatePreview(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 80 {
		return value
	}
	return value[:77] + "..."
}

func writeJSON(w http.ResponseWriter, value interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeMethodNotAllowed(w http.ResponseWriter) {
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}
