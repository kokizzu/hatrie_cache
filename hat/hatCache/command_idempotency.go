package hatCache

import (
	"crypto/sha256"
	stdjson "encoding/json"
	"fmt"
	"strings"
)

// MaxCommandJournalIdempotencyKeyBytes bounds the memory and hashing cost of a
// caller-supplied idempotency key.
const MaxCommandJournalIdempotencyKeyBytes = 256
const commandIdempotencyFingerprintSize = sha256.Size

// CommandJournalIdempotencyStats describes the bounded idempotency ledger and
// its process-lifetime duplicate/eviction counters. DuplicateEstimatedBytes
// is the canonical fingerprint-payload byte count for requests skipped by an
// exact idempotency hit; it is intended for operational accounting, not wire
// framing.
type CommandJournalIdempotencyStats struct {
	Enabled                 bool   `json:"enabled"`
	Capacity                int    `json:"capacity"`
	Entries                 int    `json:"entries"`
	OldestSequence          uint64 `json:"oldest_sequence,omitempty"`
	NewestSequence          uint64 `json:"newest_sequence,omitempty"`
	Evictions               uint64 `json:"evictions"`
	Duplicates              uint64 `json:"duplicates"`
	DuplicateEstimatedBytes uint64 `json:"duplicate_estimated_bytes"`
}

type commandIdempotencyCheck struct {
	enabled               bool
	key                   string
	fingerprint           [sha256.Size]byte
	estimatedRequestBytes uint64
}

type commandIdempotencyRecord struct {
	fingerprint [sha256.Size]byte
	response    CacheCommandResponse
	sequence    uint64
}

type commandIdempotencyPending struct {
	check    commandIdempotencyCheck
	response CacheCommandResponse
	sequence uint64
}

type commandIdempotencyState struct {
	capacity                int
	entries                 map[string]commandIdempotencyRecord
	order                   []string
	orderIndex              int
	evictions               uint64
	duplicates              uint64
	duplicateEstimatedBytes uint64
}

type commandIdempotencyFingerprintRequest struct {
	Command        string                                 `json:"command"`
	Atomic         bool                                   `json:"atomic,omitempty"`
	Key            string                                 `json:"key,omitempty"`
	Value          string                                 `json:"value,omitempty"`
	Values         []any                                  `json:"values,omitempty"`
	Batch          []commandIdempotencyFingerprintRequest `json:"batch,omitempty"`
	Subkey         string                                 `json:"subkey,omitempty"`
	Pairs          map[string]any                         `json:"pairs,omitempty"`
	Priority       *int64                                 `json:"priority,omitempty"`
	TTLSeconds     *int64                                 `json:"ttl_seconds,omitempty"`
	UnixSeconds    *int64                                 `json:"unix_seconds,omitempty"`
	IdempotencyKey string                                 `json:"idempotency_key,omitempty"`
	BinaryValue    []byte                                 `json:"binary_value,omitempty"`
}

func newCommandIdempotencyState(capacity int) commandIdempotencyState {
	state := commandIdempotencyState{capacity: capacity}
	if capacity > 0 {
		state.entries = make(map[string]commandIdempotencyRecord, capacity)
	}
	return state
}

func (state *commandIdempotencyState) enabled() bool {
	return state != nil && state.capacity > 0
}

func newCommandIdempotencyCheck(request CacheCommandRequest) (commandIdempotencyCheck, error) {
	key := strings.TrimSpace(request.IdempotencyKey)
	if key == "" {
		return commandIdempotencyCheck{}, nil
	}
	if err := validateCommandIdempotencyKey(key); err != nil {
		return commandIdempotencyCheck{}, err
	}
	payload := commandIdempotencyFingerprintRequestFrom(request, true)
	data, err := stdjson.Marshal(payload)
	if err != nil {
		return commandIdempotencyCheck{}, fmt.Errorf("hatriecache: cannot fingerprint idempotent command: %w", err)
	}
	return commandIdempotencyCheck{
		enabled:               true,
		key:                   key,
		fingerprint:           sha256.Sum256(data),
		estimatedRequestBytes: uint64(len(data)),
	}, nil
}

func validateCommandIdempotencyKey(key string) error {
	if len([]byte(strings.TrimSpace(key))) > MaxCommandJournalIdempotencyKeyBytes {
		return fmt.Errorf("hatriecache: idempotency key must be <= %d bytes", MaxCommandJournalIdempotencyKeyBytes)
	}
	return nil
}

func commandIdempotencyFingerprintRequestFrom(request CacheCommandRequest, omitKey bool) commandIdempotencyFingerprintRequest {
	payload := commandIdempotencyFingerprintRequest{
		Command:     strings.ToUpper(strings.TrimSpace(request.Command)),
		Atomic:      request.Atomic,
		Key:         strings.TrimSpace(request.Key),
		Value:       request.Value,
		Subkey:      request.Subkey,
		Pairs:       request.Pairs,
		Priority:    request.Priority,
		TTLSeconds:  request.TTLSeconds,
		UnixSeconds: request.UnixSeconds,
		BinaryValue: request.BinaryValue,
	}
	if !omitKey {
		payload.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	}
	if request.Values != nil {
		payload.Values = append([]any(nil), request.Values...)
	}
	if request.Batch != nil {
		payload.Batch = make([]commandIdempotencyFingerprintRequest, len(request.Batch))
		for index, nested := range request.Batch {
			payload.Batch[index] = commandIdempotencyFingerprintRequestFrom(nested, false)
		}
	}
	return payload
}

func commandIdempotencyFingerprintData(check commandIdempotencyCheck) []byte {
	if !check.enabled {
		return nil
	}
	return append([]byte(nil), check.fingerprint[:]...)
}

func (state *commandIdempotencyState) lookup(check commandIdempotencyCheck) (CacheCommandResponse, bool, error) {
	if state == nil || !state.enabled() || !check.enabled {
		return CacheCommandResponse{}, false, nil
	}
	record, ok := state.entries[check.key]
	if !ok {
		return CacheCommandResponse{}, false, nil
	}
	if record.fingerprint != check.fingerprint {
		return CacheCommandResponse{}, false, fmt.Errorf("hatriecache: idempotency key was reused with a different command")
	}
	state.duplicates++
	state.duplicateEstimatedBytes += check.estimatedRequestBytes
	return cloneCacheCommandResponse(record.response), true, nil
}

func (state *commandIdempotencyState) remember(check commandIdempotencyCheck, response CacheCommandResponse, sequence uint64) {
	if state == nil || !state.enabled() || !check.enabled {
		return
	}
	if record, ok := state.entries[check.key]; ok {
		record.fingerprint = check.fingerprint
		record.response = cloneCacheCommandResponse(response)
		record.sequence = sequence
		state.entries[check.key] = record
		return
	}
	if len(state.entries) >= state.capacity {
		for state.orderIndex < len(state.order) {
			oldest := state.order[state.orderIndex]
			state.orderIndex++
			if _, ok := state.entries[oldest]; !ok {
				continue
			}
			delete(state.entries, oldest)
			state.evictions++
			break
		}
	}
	state.entries[check.key] = commandIdempotencyRecord{
		fingerprint: check.fingerprint,
		response:    cloneCacheCommandResponse(response),
		sequence:    sequence,
	}
	state.order = append(state.order, check.key)
	if state.orderIndex > 1024 && state.orderIndex*2 >= len(state.order) {
		state.order = append([]string(nil), state.order[state.orderIndex:]...)
		state.orderIndex = 0
	}
}

func (state *commandIdempotencyState) stats() CommandJournalIdempotencyStats {
	if state == nil {
		return CommandJournalIdempotencyStats{}
	}
	stats := CommandJournalIdempotencyStats{
		Enabled:                 state.enabled(),
		Capacity:                state.capacity,
		Entries:                 len(state.entries),
		Evictions:               state.evictions,
		Duplicates:              state.duplicates,
		DuplicateEstimatedBytes: state.duplicateEstimatedBytes,
	}
	for _, record := range state.entries {
		if record.sequence == 0 {
			continue
		}
		if stats.OldestSequence == 0 || record.sequence < stats.OldestSequence {
			stats.OldestSequence = record.sequence
		}
		if record.sequence > stats.NewestSequence {
			stats.NewestSequence = record.sequence
		}
	}
	return stats
}

func lookupPendingCommandIdempotency(pending []commandIdempotencyPending, check commandIdempotencyCheck) (CacheCommandResponse, bool, error) {
	if !check.enabled {
		return CacheCommandResponse{}, false, nil
	}
	for _, record := range pending {
		if !record.check.enabled || record.check.key != check.key {
			continue
		}
		if record.check.fingerprint != check.fingerprint {
			return CacheCommandResponse{}, false, fmt.Errorf("hatriecache: idempotency key was reused with a different command")
		}
		return cloneCacheCommandResponse(record.response), true, nil
	}
	return CacheCommandResponse{}, false, nil
}

func cloneCacheCommandResponse(response CacheCommandResponse) CacheCommandResponse {
	if len(response.Responses) == 0 {
		return response
	}
	nestedResponses := response.Responses
	response.Responses = make([]CacheCommandResponse, len(nestedResponses))
	for index, nested := range nestedResponses {
		response.Responses[index] = cloneCacheCommandResponse(nested)
	}
	return response
}

func recoveredCommandIdempotencyResponse() CacheCommandResponse {
	return CacheCommandResponse{OK: true, Message: "idempotency key already applied"}
}
