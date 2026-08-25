// Package hatMonitoring provides portable monitoring endpoint models and an
// authenticated HTTP client for hatrie-cache operators.
package hatMonitoring

import (
	"encoding/json"
	"fmt"
	"strings"
)

// APIVersion is the current monitoring endpoint contract version.
const APIVersion = 1

// MaxEntriesLimit is the largest permitted page size for entry inspection.
const MaxEntriesLimit = 100000

// Health reports cache-node liveness and resource summaries.
type Health struct {
	Status          string `json:"status"`
	Node            string `json:"node"`
	APIVersion      int    `json:"api_version"`
	Version         string `json:"version"`
	UptimeSeconds   int64  `json:"uptime_seconds"`
	MemoryBytes     uint64 `json:"memory_bytes"`
	DiskSpillBytes  uint64 `json:"disk_spill_bytes"`
	CleanersRunning int    `json:"cleaners_running"`
	LocalPartitions int    `json:"local_partitions"`
}

// Entry summarizes one cache key without exposing its complete value.
type Entry struct {
	Key          string `json:"key"`
	Type         string `json:"type"`
	TTLMillis    *int64 `json:"ttl_ms"`
	OnDisk       bool   `json:"on_disk"`
	SizeBytes    int64  `json:"size_bytes"`
	ValuePreview string `json:"value_preview"`
}

// EntriesRequest controls a lexicographic entry page.
type EntriesRequest struct {
	Prefix   string
	AfterKey string
	Limit    int
}

// Validate normalizes and validates an entries request.
func (request EntriesRequest) Validate() (EntriesRequest, error) {
	request.Prefix = strings.TrimSpace(request.Prefix)
	request.AfterKey = strings.TrimSpace(request.AfterKey)
	if request.Limit < 0 || request.Limit > MaxEntriesLimit {
		return EntriesRequest{}, fmt.Errorf("entries limit must be between 0 and %d", MaxEntriesLimit)
	}
	return request, nil
}

// EntriesResponse is a stable, cursor-addressable entry page.
type EntriesResponse struct {
	Entries             []Entry `json:"entries"`
	Limit               uint64  `json:"limit,omitempty"`
	HasMore             bool    `json:"has_more,omitempty"`
	AfterKey            string  `json:"after_key,omitempty"`
	NextAfterKey        string  `json:"next_after_key,omitempty"`
	AfterKeyPresent     bool    `json:"-"`
	NextAfterKeyPresent bool    `json:"-"`
}

// MarshalJSON preserves explicitly supplied empty cursors.
func (response EntriesResponse) MarshalJSON() ([]byte, error) {
	type encoded struct {
		Entries      []Entry `json:"entries"`
		Limit        uint64  `json:"limit,omitempty"`
		HasMore      bool    `json:"has_more,omitempty"`
		AfterKey     *string `json:"after_key,omitempty"`
		NextAfterKey *string `json:"next_after_key,omitempty"`
	}
	value := encoded{Entries: response.Entries, Limit: response.Limit, HasMore: response.HasMore}
	if response.AfterKeyPresent || response.AfterKey != "" {
		value.AfterKey = &response.AfterKey
	}
	if response.NextAfterKeyPresent || response.NextAfterKey != "" {
		value.NextAfterKey = &response.NextAfterKey
	}
	return json.Marshal(value)
}
