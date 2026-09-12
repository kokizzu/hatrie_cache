package hatTopology

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
)

var (
	// ErrConfigWatchNil indicates a method call on a nil log.
	ErrConfigWatchNil = errors.New("hatTopology: config watch log is nil")
	// ErrConfigWatchContextRequired indicates that a context was omitted.
	ErrConfigWatchContextRequired = errors.New("hatTopology: config watch context is required")
	// ErrConfigWatchAuthorizerRequired indicates that no authorizer was
	// configured. Configuration events are never anonymous.
	ErrConfigWatchAuthorizerRequired = errors.New("hatTopology: config watch authorizer is required")
	// ErrConfigWatchHistoryLimitInvalid indicates an invalid history bound.
	ErrConfigWatchHistoryLimitInvalid = errors.New("hatTopology: config watch history limit is invalid")
	// ErrConfigWatchKeyLimitInvalid indicates an invalid key-size bound.
	ErrConfigWatchKeyLimitInvalid = errors.New("hatTopology: config watch key limit is invalid")
	// ErrConfigWatchValueLimitInvalid indicates an invalid value-size bound.
	ErrConfigWatchValueLimitInvalid = errors.New("hatTopology: config watch value limit is invalid")
	// ErrConfigWatchMemoryBudgetInvalid indicates that configured history and
	// value bounds could retain more than the global budget.
	ErrConfigWatchMemoryBudgetInvalid = errors.New("hatTopology: config watch memory budget is invalid")
	// ErrConfigWatchPrincipalInvalid indicates a missing or oversized identity.
	ErrConfigWatchPrincipalInvalid = errors.New("hatTopology: config watch principal is invalid")
	// ErrConfigWatchKeyInvalid indicates a missing or oversized key.
	ErrConfigWatchKeyInvalid = errors.New("hatTopology: config watch key is invalid")
	// ErrConfigWatchSourceInvalid indicates a missing or oversized source node.
	ErrConfigWatchSourceInvalid = errors.New("hatTopology: config watch source is invalid")
	// ErrConfigWatchValueInvalid indicates an oversized configuration value.
	ErrConfigWatchValueInvalid = errors.New("hatTopology: config watch value is invalid")
	// ErrConfigWatchDeleteValueInvalid indicates a delete event with a value.
	ErrConfigWatchDeleteValueInvalid = errors.New("hatTopology: deleted config event cannot contain a value")
	// ErrConfigWatchReadLimitInvalid indicates an invalid read batch bound.
	ErrConfigWatchReadLimitInvalid = errors.New("hatTopology: config watch read limit is invalid")
	// ErrConfigWatchVersionStale indicates a version that is not newer than the
	// latest accepted event.
	ErrConfigWatchVersionStale = errors.New("hatTopology: config watch version is stale")
	// ErrConfigWatchHistoryGap indicates that the requested cursor predates the
	// retained history and needs a fresh snapshot.
	ErrConfigWatchHistoryGap = errors.New("hatTopology: config watch history gap")
)

const (
	// DefaultConfigWatchHistoryLimit bounds retained events for a new log.
	DefaultConfigWatchHistoryLimit = 256
	// MaxConfigWatchHistoryLimit prevents an untrusted configuration from
	// reserving an unbounded event ring.
	MaxConfigWatchHistoryLimit = 1 << 16
	// DefaultConfigWatchMaxKeyBytes bounds one configuration path.
	DefaultConfigWatchMaxKeyBytes = 256
	// MaxConfigWatchKeyBytes bounds one configuration path even when configured.
	MaxConfigWatchKeyBytes = 4096
	// DefaultConfigWatchMaxValueBytes keeps the default retained value budget
	// small enough for an embedded process.
	DefaultConfigWatchMaxValueBytes = 64 << 10
	// MaxConfigWatchValueBytes bounds one value retained in the history ring.
	MaxConfigWatchValueBytes = 16 << 20
	// MaxConfigWatchRetainedValueBytes bounds the configured worst-case value
	// retention before any event is published.
	MaxConfigWatchRetainedValueBytes = 64 << 20
	// DefaultConfigWatchReadLimit bounds one replay or wait response.
	DefaultConfigWatchReadLimit = 64
	// MaxConfigWatchReadLimit prevents one caller from requesting an unbounded
	// response batch.
	MaxConfigWatchReadLimit      = 4096
	maxConfigWatchPrincipalBytes = 128
	maxConfigWatchSourceBytes    = 128
)

// ConfigWatchAction identifies the operation being authorized.
type ConfigWatchAction uint8

const (
	ConfigWatchPublish ConfigWatchAction = iota + 1
	ConfigWatchRead
)

// String returns the stable action name.
func (action ConfigWatchAction) String() string {
	switch action {
	case ConfigWatchPublish:
		return "publish"
	case ConfigWatchRead:
		return "read"
	default:
		return "unknown"
	}
}

// ConfigWatchAuthorization is passed to the configured authorizer. Read
// authorization receives the requested key as empty because a read returns
// the whole ordered change stream; a transport may add a key-scoped log when
// that is required by its policy.
type ConfigWatchAuthorization struct {
	Principal string
	Action    ConfigWatchAction
	Key       string
}

// ConfigWatchAuthorizer authenticates and authorizes one log operation. It is
// called without the log mutex, and must be safe for concurrent callers.
type ConfigWatchAuthorizer func(context.Context, ConfigWatchAuthorization) error

// ConfigWatchOptions bounds the retained configuration history. The
// authorizer is required even though this package does not provide a network
// transport.
type ConfigWatchOptions struct {
	HistoryLimit  int
	MaxKeyBytes   int
	MaxValueBytes int
	Authorizer    ConfigWatchAuthorizer
}

// ConfigWatchEvent is one versioned configuration mutation. Version zero on
// Publish is assigned as the next local version; distributed publishers
// should provide a globally ordered version from their consensus/fencing
// layer. Value is copied on publish and read.
type ConfigWatchEvent struct {
	Version uint64 `json:"version"`
	Source  string `json:"source"`
	Key     string `json:"key"`
	Value   []byte `json:"value,omitempty"`
	Deleted bool   `json:"deleted,omitempty"`
}

// ConfigWatchRequest selects a replay or wait cursor.
type ConfigWatchRequest struct {
	Principal    string
	AfterVersion uint64
	Limit        int
}

// ConfigWatchStats is a point-in-time retention snapshot.
type ConfigWatchStats struct {
	CurrentVersion  uint64
	EarliestVersion uint64
	Retained        int
	HistoryLimit    int
}

// ConfigWatchGapError tells a reconnecting client which retained version it
// must use after obtaining a fresh configuration snapshot.
type ConfigWatchGapError struct {
	AfterVersion    uint64
	EarliestVersion uint64
	CurrentVersion  uint64
}

func (err *ConfigWatchGapError) Error() string {
	if err == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s: after=%d earliest=%d current=%d", ErrConfigWatchHistoryGap, err.AfterVersion, err.EarliestVersion, err.CurrentVersion)
}

func (err *ConfigWatchGapError) Unwrap() error {
	if err == nil {
		return nil
	}
	return ErrConfigWatchHistoryGap
}

// ConfigWatchLog stores a bounded, authenticated configuration change stream.
// It has no network or cluster membership side effects and is safe for
// concurrent use.
type ConfigWatchLog struct {
	mu             sync.Mutex
	history        []ConfigWatchEvent
	historyStart   int
	historySize    int
	historyDropped bool
	current        uint64
	historyLimit   int
	maxKeyBytes    int
	maxValueBytes  int
	authorizer     ConfigWatchAuthorizer
	notify         chan struct{}
}

// NewConfigWatchLog creates a bounded authenticated configuration log.
func NewConfigWatchLog(options ConfigWatchOptions) (*ConfigWatchLog, error) {
	historyLimit := options.HistoryLimit
	if historyLimit == 0 {
		historyLimit = DefaultConfigWatchHistoryLimit
	}
	if historyLimit < 1 || historyLimit > MaxConfigWatchHistoryLimit {
		return nil, ErrConfigWatchHistoryLimitInvalid
	}
	maxKeyBytes := options.MaxKeyBytes
	if maxKeyBytes == 0 {
		maxKeyBytes = DefaultConfigWatchMaxKeyBytes
	}
	if maxKeyBytes < 1 || maxKeyBytes > MaxConfigWatchKeyBytes {
		return nil, ErrConfigWatchKeyLimitInvalid
	}
	maxValueBytes := options.MaxValueBytes
	if maxValueBytes == 0 {
		maxValueBytes = DefaultConfigWatchMaxValueBytes
	}
	if maxValueBytes < 1 || maxValueBytes > MaxConfigWatchValueBytes {
		return nil, ErrConfigWatchValueLimitInvalid
	}
	if historyLimit > MaxConfigWatchRetainedValueBytes/maxValueBytes {
		return nil, ErrConfigWatchMemoryBudgetInvalid
	}
	if options.Authorizer == nil {
		return nil, ErrConfigWatchAuthorizerRequired
	}
	return &ConfigWatchLog{
		history:       make([]ConfigWatchEvent, historyLimit),
		historyLimit:  historyLimit,
		maxKeyBytes:   maxKeyBytes,
		maxValueBytes: maxValueBytes,
		authorizer:    options.Authorizer,
		notify:        make(chan struct{}),
	}, nil
}

func (log *ConfigWatchLog) authorize(ctx context.Context, principal string, action ConfigWatchAction, key string) (string, error) {
	if log == nil {
		return "", ErrConfigWatchNil
	}
	if ctx == nil {
		return "", ErrConfigWatchContextRequired
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	principal = strings.TrimSpace(principal)
	if principal == "" || len(principal) > maxConfigWatchPrincipalBytes {
		return "", ErrConfigWatchPrincipalInvalid
	}
	if log.authorizer == nil {
		return "", ErrConfigWatchAuthorizerRequired
	}
	if err := log.authorizer(ctx, ConfigWatchAuthorization{Principal: principal, Action: action, Key: key}); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	return principal, nil
}

func (log *ConfigWatchLog) validateEvent(event ConfigWatchEvent) (ConfigWatchEvent, error) {
	event.Source = strings.TrimSpace(event.Source)
	if event.Source == "" || len(event.Source) > maxConfigWatchSourceBytes {
		return ConfigWatchEvent{}, ErrConfigWatchSourceInvalid
	}
	event.Key = strings.TrimSpace(event.Key)
	if event.Key == "" || len(event.Key) > log.maxKeyBytes {
		return ConfigWatchEvent{}, ErrConfigWatchKeyInvalid
	}
	if len(event.Value) > log.maxValueBytes {
		return ConfigWatchEvent{}, ErrConfigWatchValueInvalid
	}
	if event.Deleted && len(event.Value) != 0 {
		return ConfigWatchEvent{}, ErrConfigWatchDeleteValueInvalid
	}
	if event.Value != nil {
		event.Value = append([]byte(nil), event.Value...)
	}
	return event, nil
}

// Publish authenticates and appends one event. Versions must increase; zero
// is assigned locally and is not a distributed ordering mechanism.
func (log *ConfigWatchLog) Publish(ctx context.Context, principal string, event ConfigWatchEvent) error {
	if log == nil {
		return ErrConfigWatchNil
	}
	principal, err := log.authorize(ctx, principal, ConfigWatchPublish, strings.TrimSpace(event.Key))
	if err != nil {
		return err
	}
	_ = principal
	event, err = log.validateEvent(event)
	if err != nil {
		return err
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if event.Version == 0 {
		if log.current == ^uint64(0) {
			return ErrConfigWatchVersionStale
		}
		event.Version = log.current + 1
	}
	if event.Version <= log.current {
		return fmt.Errorf("%w: got=%d current=%d", ErrConfigWatchVersionStale, event.Version, log.current)
	}
	index := (log.historyStart + log.historySize) % log.historyLimit
	if log.historySize == log.historyLimit {
		index = log.historyStart
		log.historyStart = (log.historyStart + 1) % log.historyLimit
		log.historyDropped = true
	} else {
		log.historySize++
	}
	log.history[index] = event
	log.current = event.Version
	previousNotify := log.notify
	log.notify = make(chan struct{})
	close(previousNotify)
	return nil
}

func (log *ConfigWatchLog) readLimit(limit int) (int, error) {
	if limit == 0 {
		return DefaultConfigWatchReadLimit, nil
	}
	if limit < 1 || limit > MaxConfigWatchReadLimit {
		return 0, ErrConfigWatchReadLimitInvalid
	}
	return limit, nil
}

// Read returns retained events after the requested cursor. The returned
// cursor is the last delivered version, so callers can safely use it when a
// response is smaller than the current history.
func (log *ConfigWatchLog) Read(ctx context.Context, request ConfigWatchRequest) ([]ConfigWatchEvent, uint64, error) {
	principal, err := log.authorize(ctx, request.Principal, ConfigWatchRead, "")
	if err != nil {
		return nil, request.AfterVersion, err
	}
	_ = principal
	limit, err := log.readLimit(request.Limit)
	if err != nil {
		return nil, request.AfterVersion, err
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	if log.historySize == 0 || request.AfterVersion >= log.current {
		return nil, request.AfterVersion, nil
	}
	earliest := log.history[log.historyStart].Version
	if log.historyDropped && earliest > 0 && request.AfterVersion < earliest-1 {
		return nil, request.AfterVersion, &ConfigWatchGapError{
			AfterVersion:    request.AfterVersion,
			EarliestVersion: earliest,
			CurrentVersion:  log.current,
		}
	}
	events := make([]ConfigWatchEvent, 0, min(limit, log.historySize))
	for offset := 0; offset < log.historySize && len(events) < limit; offset++ {
		event := log.history[(log.historyStart+offset)%log.historyLimit]
		if event.Version <= request.AfterVersion {
			continue
		}
		events = append(events, cloneConfigWatchEvent(event))
	}
	next := request.AfterVersion
	if len(events) > 0 {
		next = events[len(events)-1].Version
	}
	return events, next, nil
}

// Wait returns retained events immediately when available, or blocks until a
// newer event is published or ctx is canceled. It uses one shared notification
// channel per log, so idle clients do not reserve one goroutine each.
func (log *ConfigWatchLog) Wait(ctx context.Context, request ConfigWatchRequest) ([]ConfigWatchEvent, uint64, error) {
	for {
		events, cursor, err := log.Read(ctx, request)
		if err != nil || len(events) > 0 {
			return events, cursor, err
		}
		request.AfterVersion = cursor
		log.mu.Lock()
		notify := log.notify
		log.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, cursor, ctx.Err()
		case <-notify:
		}
	}
}

// Stats returns current retention bounds.
func (log *ConfigWatchLog) Stats() ConfigWatchStats {
	if log == nil {
		return ConfigWatchStats{}
	}
	log.mu.Lock()
	defer log.mu.Unlock()
	stats := ConfigWatchStats{
		CurrentVersion: log.current,
		Retained:       log.historySize,
		HistoryLimit:   log.historyLimit,
	}
	if log.historySize > 0 {
		stats.EarliestVersion = log.history[log.historyStart].Version
	}
	return stats
}

func cloneConfigWatchEvent(event ConfigWatchEvent) ConfigWatchEvent {
	if event.Value != nil {
		event.Value = append([]byte(nil), event.Value...)
	}
	return event
}
