package hatReplication

import (
	"fmt"
	"strings"
	"time"
)

// Result describes the outcome of one replication attempt.
type Result struct {
	Command         string                 `json:"command,omitempty"`
	Key             string                 `json:"key,omitempty"`
	Entries         int                    `json:"entries,omitempty"`
	Queued          bool                   `json:"queued,omitempty"`
	Skipped         bool                   `json:"skipped"`
	Reason          string                 `json:"reason,omitempty"`
	Health          string                 `json:"health"`
	HealthScore     int                    `json:"health_score"`
	HealthReason    string                 `json:"health_reason,omitempty"`
	DeadLetterCount int                    `json:"dead_letter_count,omitempty"`
	DeadLetters     []DeadLetter           `json:"dead_letters,omitempty"`
	CircuitBreakers []CircuitBreakerTarget `json:"circuit_breakers,omitempty"`
	StartedAt       *time.Time             `json:"started_at,omitempty"`
	FinishedAt      *time.Time             `json:"finished_at,omitempty"`
	DurationMillis  int64                  `json:"duration_millis,omitempty"`
	Queue           *QueueStats            `json:"queue,omitempty"`
	Targets         []TargetResult         `json:"targets,omitempty"`
}

// DeadLetter records a replication job that exhausted delivery attempts.
type DeadLetter struct {
	ID       uint64         `json:"id"`
	Command  string         `json:"command,omitempty"`
	Key      string         `json:"key,omitempty"`
	FailedAt *time.Time     `json:"failed_at,omitempty"`
	Attempts uint           `json:"attempts"`
	Reason   string         `json:"reason,omitempty"`
	Targets  []TargetResult `json:"targets,omitempty"`
}

// CircuitBreakerTarget records transport health for one replication target.
type CircuitBreakerTarget struct {
	Node              string     `json:"node"`
	State             string     `json:"state"`
	Failures          int        `json:"failures"`
	OpenedAt          *time.Time `json:"opened_at,omitempty"`
	OpenUntil         *time.Time `json:"open_until,omitempty"`
	LastFailureAt     *time.Time `json:"last_failure_at,omitempty"`
	LastSuccessAt     *time.Time `json:"last_success_at,omitempty"`
	LastFailureReason string     `json:"last_failure_reason,omitempty"`
}

// QueueStats reports bounded asynchronous replication outbox health.
type QueueStats struct {
	Enabled               bool              `json:"enabled"`
	Paused                bool              `json:"paused"`
	Depth                 int               `json:"depth"`
	Capacity              int               `json:"capacity"`
	Enqueued              uint64            `json:"enqueued"`
	Dropped               uint64            `json:"dropped"`
	Attempts              uint64            `json:"attempts"`
	Successes             uint64            `json:"successes"`
	Failures              uint64            `json:"failures"`
	Retried               uint64            `json:"retried"`
	OldestQueuedAt        *time.Time        `json:"oldest_queued_at,omitempty"`
	OldestQueuedAgeMillis int64             `json:"oldest_queued_age_millis,omitempty"`
	OldestQueuedKey       string            `json:"oldest_queued_key,omitempty"`
	OldestQueuedTargets   []string          `json:"oldest_queued_targets,omitempty"`
	DurableBacklog        bool              `json:"durable_backlog,omitempty"`
	InFlightStartedAt     *time.Time        `json:"in_flight_started_at,omitempty"`
	InFlightAgeMillis     int64             `json:"in_flight_age_millis,omitempty"`
	InFlightKey           string            `json:"in_flight_key,omitempty"`
	LastRetryAt           *time.Time        `json:"last_retry_at,omitempty"`
	LastRetryAgeMillis    int64             `json:"last_retry_age_millis,omitempty"`
	LastRetryKey          string            `json:"last_retry_key,omitempty"`
	DroppedByTarget       map[string]uint64 `json:"dropped_by_target,omitempty"`
	FailuresByTarget      map[string]uint64 `json:"failures_by_target,omitempty"`
	Closed                bool              `json:"closed"`
}

// TargetResult records the outcome for one remote node.
type TargetResult struct {
	Node             string     `json:"node"`
	Key              string     `json:"key,omitempty"`
	Address          string     `json:"address,omitempty"`
	OK               bool       `json:"ok"`
	Status           int        `json:"status,omitempty"`
	Error            string     `json:"error,omitempty"`
	CircuitOpen      bool       `json:"circuit_open,omitempty"`
	CircuitState     string     `json:"circuit_state,omitempty"`
	CircuitOpenUntil *time.Time `json:"circuit_open_until,omitempty"`
	// UnsupportedTypedReplication is internal transport capability state. It
	// is excluded from operator responses but remains exported for root-package
	// transport adapters.
	UnsupportedTypedReplication bool `json:"-"`
}

// OutboxCodec selects the durable representation used by a replication outbox.
type OutboxCodec string

const (
	OutboxCodecBinary OutboxCodec = "binary"
	OutboxCodecJSON   OutboxCodec = "json"
)

// ParseOutboxCodec validates and canonicalizes a replication outbox codec.
func ParseOutboxCodec(value string) (OutboxCodec, error) {
	switch OutboxCodec(strings.ToLower(strings.TrimSpace(value))) {
	case "", OutboxCodecBinary:
		return OutboxCodecBinary, nil
	case OutboxCodecJSON:
		return OutboxCodecJSON, nil
	default:
		return "", fmt.Errorf("hatriecache: unsupported replication outbox codec %q", value)
	}
}
