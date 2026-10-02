package hatTopology

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"hatrie_cache/hat/hatPeer"
)

const (
	// ConfigWatchPeerCommand is the reserved compact-peer command used by the
	// authenticated configuration watch adapter.
	ConfigWatchPeerCommand = "_hat.config.watch.v1"
	// DefaultConfigWatchPeerBuffer bounds events retained for one remote
	// watcher when its consumer is temporarily slower than the peer.
	DefaultConfigWatchPeerBuffer = 64
	// MaxConfigWatchPeerBuffer prevents one caller from reserving unbounded
	// memory for remote events.
	MaxConfigWatchPeerBuffer = 1 << 16
	// DefaultConfigWatchPeerRetryDelay is the first reconnect delay.
	DefaultConfigWatchPeerRetryDelay = 25 * time.Millisecond
	// DefaultConfigWatchPeerRetryMaxDelay caps reconnect backoff.
	DefaultConfigWatchPeerRetryMaxDelay = 2 * time.Second
)

var (
	ErrConfigWatchPeerContextRequired  = errors.New("hatTopology: config watch peer context is required")
	ErrConfigWatchPeerDialRequired     = errors.New("hatTopology: config watch peer dialer is required")
	ErrConfigWatchPeerOptionsInvalid   = errors.New("hatTopology: config watch peer options are invalid")
	ErrConfigWatchPeerPrincipalInvalid = errors.New("hatTopology: config watch peer principal is invalid")
	ErrConfigWatchPeerClosed           = errors.New("hatTopology: config watch peer is closed")
	ErrConfigWatchPeerRemote           = errors.New("hatTopology: remote config watch request failed")
	ErrConfigWatchPeerProtocol         = errors.New("hatTopology: config watch peer response is invalid")
)

type configWatchPeerOperation string

const (
	configWatchPeerRead configWatchPeerOperation = "read"
	configWatchPeerWait configWatchPeerOperation = "wait"
)

type configWatchPeerRequest struct {
	Operation    configWatchPeerOperation `json:"operation"`
	Principal    string                   `json:"principal"`
	Prefix       string                   `json:"prefix,omitempty"`
	AfterVersion uint64                   `json:"after_version"`
	Limit        int                      `json:"limit,omitempty"`
}

type configWatchPeerResponse struct {
	Events          []ConfigWatchEvent `json:"events,omitempty"`
	NextVersion     uint64             `json:"next_version"`
	ErrorCode       string             `json:"error_code,omitempty"`
	Error           string             `json:"error,omitempty"`
	AfterVersion    uint64             `json:"after_version,omitempty"`
	EarliestVersion uint64             `json:"earliest_version,omitempty"`
	CurrentVersion  uint64             `json:"current_version,omitempty"`
}

// ConfigWatchPeerHandlerOptions binds an optional authenticated principal to
// one session. A bound handler ignores the principal in the wire request;
// construct one only after the transport has authenticated that peer.
type ConfigWatchPeerHandlerOptions struct {
	Log       *ConfigWatchLog
	Principal string
}

// NewConfigWatchPeerHandler adapts one ConfigWatchLog to a
// CompactPeerSession handler. Use NewConfigWatchPeerHandlerForPrincipal for a
// remote connection after its transport identity has been authenticated.
func NewConfigWatchPeerHandler(log *ConfigWatchLog) hatPeer.CompactPeerHandler {
	return NewConfigWatchPeerHandlerWithOptions(ConfigWatchPeerHandlerOptions{Log: log})
}

// NewConfigWatchPeerHandlerWithOptions constructs a handler with optional
// principal binding for one already-authenticated compact session.
func NewConfigWatchPeerHandlerWithOptions(options ConfigWatchPeerHandlerOptions) hatPeer.CompactPeerHandler {
	return newConfigWatchPeerHandler(options)
}

// NewConfigWatchPeerHandlerForPrincipal binds all requests on one peer
// session to principal. This prevents a caller from spoofing a different
// ConfigWatchAuthorizer identity in the JSON request.
func NewConfigWatchPeerHandlerForPrincipal(log *ConfigWatchLog, principal string) (hatPeer.CompactPeerHandler, error) {
	principal = strings.TrimSpace(principal)
	if principal == "" || len(principal) > maxConfigWatchPrincipalBytes {
		return nil, ErrConfigWatchPeerPrincipalInvalid
	}
	return NewConfigWatchPeerHandlerWithOptions(ConfigWatchPeerHandlerOptions{Log: log, Principal: principal}), nil
}

func newConfigWatchPeerHandler(options ConfigWatchPeerHandlerOptions) hatPeer.CompactPeerHandler {
	boundPrincipal := strings.TrimSpace(options.Principal)
	log := options.Log
	return func(ctx context.Context, frame hatPeer.CompactFrame) (hatPeer.CompactFrame, error) {
		response := configWatchPeerResponse{}
		if string(frame.Command) != ConfigWatchPeerCommand {
			response.ErrorCode = "invalid"
			response.Error = "unknown configuration watch command"
			return configWatchPeerFrame(frame, response)
		}
		var request configWatchPeerRequest
		if err := json.Unmarshal(frame.Payload, &request); err != nil {
			response.ErrorCode = "invalid"
			response.Error = "invalid configuration watch request"
			return configWatchPeerFrame(frame, response)
		}
		if request.Operation != configWatchPeerRead && request.Operation != configWatchPeerWait {
			response.ErrorCode = "invalid"
			response.Error = "unsupported configuration watch operation"
			return configWatchPeerFrame(frame, response)
		}
		if boundPrincipal != "" {
			request.Principal = boundPrincipal
		}
		if log == nil {
			response.ErrorCode = "remote"
			response.Error = "configuration watch log is unavailable"
			return configWatchPeerFrame(frame, response)
		}
		watchRequest := ConfigWatchRequest{
			Principal:    request.Principal,
			Prefix:       request.Prefix,
			AfterVersion: request.AfterVersion,
			Limit:        request.Limit,
		}
		var (
			events []ConfigWatchEvent
			cursor uint64
			err    error
		)
		if request.Operation == configWatchPeerRead {
			events, cursor, err = log.Read(ctx, watchRequest)
		} else {
			events, cursor, err = log.Wait(ctx, watchRequest)
		}
		if err != nil {
			response = configWatchPeerError(err)
		} else {
			response.Events = events
			response.NextVersion = cursor
		}
		return configWatchPeerFrame(frame, response)
	}
}

func configWatchPeerFrame(request hatPeer.CompactFrame, response configWatchPeerResponse) (hatPeer.CompactFrame, error) {
	payload, err := json.Marshal(response)
	if err != nil {
		return hatPeer.CompactFrame{}, err
	}
	return hatPeer.CompactFrame{
		Command: append([]byte(nil), request.Command...),
		Payload: payload,
	}, nil
}

func configWatchPeerError(err error) configWatchPeerResponse {
	response := configWatchPeerResponse{ErrorCode: "remote", Error: "remote configuration watch request failed"}
	var gap *ConfigWatchGapError
	switch {
	case errors.As(err, &gap):
		response.ErrorCode = "history_gap"
		response.Error = "configuration watch history gap"
		response.AfterVersion = gap.AfterVersion
		response.EarliestVersion = gap.EarliestVersion
		response.CurrentVersion = gap.CurrentVersion
	case errors.Is(err, ErrConfigWatchPrincipalInvalid), errors.Is(err, ErrConfigWatchAuthorizerRequired):
		response.ErrorCode = "forbidden"
		response.Error = "configuration watch request denied"
	case errors.Is(err, ErrConfigWatchKeyInvalid), errors.Is(err, ErrConfigWatchValueInvalid), errors.Is(err, ErrConfigWatchReadLimitInvalid):
		response.ErrorCode = "invalid"
		response.Error = "invalid configuration watch request"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		response.ErrorCode = "canceled"
		response.Error = "configuration watch request canceled"
	}
	return response
}

// ConfigWatchPeerDial establishes one compact peer session. A dialer may
// return a new session after a transport failure; the watcher reuses its
// version cursor on every reconnect.
type ConfigWatchPeerDial func(context.Context) (*hatPeer.CompactPeerSession, error)

// ConfigWatchPeerOptions configures one reconnecting remote prefix watcher.
// Authentication and authorization are delegated to the remote log's
// ConfigWatchAuthorizer through Principal.
type ConfigWatchPeerOptions struct {
	Principal     string
	Prefix        string
	AfterVersion  uint64
	Limit         int
	Buffer        int
	RetryDelay    time.Duration
	RetryMaxDelay time.Duration
	Dial          ConfigWatchPeerDial
}

// ConfigWatchPeerWatcher receives ordered remote configuration events. A
// transport failure is retried with bounded backoff; a history gap or policy
// error terminates the watcher because replay requires operator-controlled
// snapshot/reconciliation rather than silently skipping versions.
type ConfigWatchPeerWatcher struct {
	events    chan ConfigWatchEvent
	context   context.Context
	cancel    context.CancelFunc
	options   ConfigWatchPeerOptions
	done      chan struct{}
	closeOnce sync.Once
	stateMu   sync.Mutex
	err       error
	closing   bool
}

// NewConfigWatchPeerWatcher starts a bounded reconnecting watch loop.
func NewConfigWatchPeerWatcher(ctx context.Context, options ConfigWatchPeerOptions) (*ConfigWatchPeerWatcher, error) {
	if ctx == nil {
		return nil, ErrConfigWatchPeerContextRequired
	}
	if options.Dial == nil {
		return nil, ErrConfigWatchPeerDialRequired
	}
	options.Principal = strings.TrimSpace(options.Principal)
	if options.Principal == "" {
		return nil, ErrConfigWatchPeerOptionsInvalid
	}
	options.Prefix = strings.TrimSpace(options.Prefix)
	if len(options.Prefix) > MaxConfigWatchKeyBytes {
		return nil, ErrConfigWatchPeerOptionsInvalid
	}
	if options.Limit == 0 {
		options.Limit = DefaultConfigWatchReadLimit
	}
	if options.Limit < 1 || options.Limit > MaxConfigWatchReadLimit {
		return nil, ErrConfigWatchPeerOptionsInvalid
	}
	if options.Buffer == 0 {
		options.Buffer = DefaultConfigWatchPeerBuffer
	}
	if options.Buffer < 1 || options.Buffer > MaxConfigWatchPeerBuffer {
		return nil, ErrConfigWatchPeerOptionsInvalid
	}
	if options.RetryDelay == 0 {
		options.RetryDelay = DefaultConfigWatchPeerRetryDelay
	}
	if options.RetryMaxDelay == 0 {
		options.RetryMaxDelay = DefaultConfigWatchPeerRetryMaxDelay
	}
	if options.RetryDelay < 0 || options.RetryMaxDelay < options.RetryDelay {
		return nil, ErrConfigWatchPeerOptionsInvalid
	}
	watchContext, cancel := context.WithCancel(ctx)
	watcher := &ConfigWatchPeerWatcher{
		events:  make(chan ConfigWatchEvent, options.Buffer),
		context: watchContext,
		cancel:  cancel,
		options: options,
		done:    make(chan struct{}),
	}
	go watcher.run()
	return watcher, nil
}

// Events returns the ordered event stream.
func (watcher *ConfigWatchPeerWatcher) Events() <-chan ConfigWatchEvent {
	if watcher == nil {
		return nil
	}
	return watcher.events
}

// Err returns the terminal error after Events is closed. An explicit Close
// reports nil; a parent-context cancellation reports that context error.
func (watcher *ConfigWatchPeerWatcher) Err() error {
	if watcher == nil {
		return ErrConfigWatchPeerClosed
	}
	watcher.stateMu.Lock()
	err := watcher.err
	watcher.stateMu.Unlock()
	return err
}

// Close stops the watcher and closes its event stream after the reconnect loop
// has released the current peer session.
func (watcher *ConfigWatchPeerWatcher) Close() {
	if watcher == nil {
		return
	}
	watcher.closeOnce.Do(func() {
		watcher.stateMu.Lock()
		watcher.closing = true
		watcher.stateMu.Unlock()
		watcher.cancel()
	})
	<-watcher.done
}

func (watcher *ConfigWatchPeerWatcher) run() {
	defer close(watcher.events)
	defer close(watcher.done)
	cursor := watcher.options.AfterVersion
	backoff := watcher.options.RetryDelay
	for {
		if watcher.context.Err() != nil {
			watcher.finishContextError()
			return
		}
		session, err := watcher.options.Dial(watcher.context)
		if err != nil || session == nil {
			if err == nil {
				err = hatPeer.ErrCompactPeerConnectionRequired
			}
			if !watcher.waitRetry(backoff) {
				watcher.finishContextError()
				return
			}
			backoff = nextConfigWatchPeerBackoff(backoff, watcher.options.RetryMaxDelay)
			continue
		}
		backoff = watcher.options.RetryDelay
		for {
			if watcher.context.Err() != nil {
				_ = session.Close()
				watcher.finishContextError()
				return
			}
			payload, marshalErr := encodeConfigWatchPeerRequest(configWatchPeerRequest{
				Operation:    configWatchPeerWait,
				Principal:    watcher.options.Principal,
				Prefix:       watcher.options.Prefix,
				AfterVersion: cursor,
				Limit:        watcher.options.Limit,
			})
			if marshalErr != nil {
				_ = session.Close()
				watcher.finishError(marshalErr)
				return
			}
			frame, callErr := session.Call(watcher.context, []byte(ConfigWatchPeerCommand), payload)
			if callErr != nil {
				_ = session.Close()
				if watcher.context.Err() != nil {
					watcher.finishContextError()
					return
				}
				if !watcher.waitRetry(backoff) {
					watcher.finishContextError()
					return
				}
				backoff = nextConfigWatchPeerBackoff(backoff, watcher.options.RetryMaxDelay)
				break
			}
			response, decodeErr := decodeConfigWatchPeerResponse(frame.Payload)
			if decodeErr != nil {
				_ = session.Close()
				watcher.finishError(decodeErr)
				return
			}
			if response.ErrorCode != "" {
				_ = session.Close()
				watcher.finishError(configWatchPeerResponseError(response))
				return
			}
			if response.NextVersion < cursor {
				_ = session.Close()
				watcher.finishError(ErrConfigWatchPeerProtocol)
				return
			}
			lastVersion := cursor
			for _, event := range response.Events {
				if event.Version <= lastVersion || (watcher.options.Prefix != "" && !strings.HasPrefix(event.Key, watcher.options.Prefix)) {
					_ = session.Close()
					watcher.finishError(ErrConfigWatchPeerProtocol)
					return
				}
				select {
				case watcher.events <- event:
					lastVersion = event.Version
				case <-watcher.context.Done():
					_ = session.Close()
					watcher.finishContextError()
					return
				}
			}
			if response.NextVersion < lastVersion {
				_ = session.Close()
				watcher.finishError(ErrConfigWatchPeerProtocol)
				return
			}
			cursor = response.NextVersion
		}
	}
}

func encodeConfigWatchPeerRequest(request configWatchPeerRequest) ([]byte, error) {
	return json.Marshal(request)
}

func decodeConfigWatchPeerResponse(payload []byte) (configWatchPeerResponse, error) {
	var response configWatchPeerResponse
	if err := json.Unmarshal(payload, &response); err != nil {
		return configWatchPeerResponse{}, fmt.Errorf("%w: %v", ErrConfigWatchPeerProtocol, err)
	}
	return response, nil
}

func configWatchPeerResponseError(response configWatchPeerResponse) error {
	if response.ErrorCode == "history_gap" {
		return &ConfigWatchGapError{AfterVersion: response.AfterVersion, EarliestVersion: response.EarliestVersion, CurrentVersion: response.CurrentVersion}
	}
	if response.ErrorCode == "canceled" {
		return context.Canceled
	}
	return fmt.Errorf("%w: %s", ErrConfigWatchPeerRemote, response.Error)
}

func (watcher *ConfigWatchPeerWatcher) waitRetry(delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-watcher.context.Done():
		return false
	}
}

func nextConfigWatchPeerBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}

func (watcher *ConfigWatchPeerWatcher) finishContextError() {
	watcher.stateMu.Lock()
	defer watcher.stateMu.Unlock()
	if watcher.closing {
		watcher.err = nil
		return
	}
	watcher.err = watcher.context.Err()
}

func (watcher *ConfigWatchPeerWatcher) finishError(err error) {
	watcher.stateMu.Lock()
	watcher.err = err
	watcher.stateMu.Unlock()
	watcher.cancel()
}
