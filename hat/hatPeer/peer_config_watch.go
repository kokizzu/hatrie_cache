package hatPeer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrPeerConfigWatchTransportRequired  = errors.New("hatPeer: config watch transport is required")
	ErrPeerConfigWatchHandlerRequired    = errors.New("hatPeer: config watch handler is required")
	ErrPeerConfigWatchPrefixRequired     = errors.New("hatPeer: config watch prefix is required")
	ErrPeerConfigWatchOptionsInvalid     = errors.New("hatPeer: config watch options are invalid")
	ErrPeerConfigWatchBatchInvalid       = errors.New("hatPeer: config watch batch is invalid")
	ErrPeerConfigWatchReconnectExhausted = errors.New("hatPeer: config watch reconnect attempts exhausted")
)

const (
	DefaultPeerConfigWatchBatchSize        = 64
	MaxPeerConfigWatchBatchSize            = 1024
	DefaultPeerConfigWatchReconnectInitial = 10 * time.Millisecond
	DefaultPeerConfigWatchReconnectMax     = time.Second
)

// PeerConfigWatchEvent is one immutable remote configuration change. Value is
// copied before Handler is called, so a handler can reuse or mutate its copy.
type PeerConfigWatchEvent struct {
	Cursor  uint64
	Key     string
	Value   []byte
	Deleted bool
}

// PeerConfigWatchRequest is the cursor and prefix sent to a peer on each poll.
// A transport may implement the call as a long poll over CompactPeerSession,
// HTTP, or another authenticated peer protocol.
type PeerConfigWatchRequest struct {
	Prefix string
	Cursor uint64
	Limit  int
}

// PeerConfigWatchBatch is one transport response. NextCursor may advance with
// no events when the remote log has compacted irrelevant entries.
type PeerConfigWatchBatch struct {
	Events     []PeerConfigWatchEvent
	NextCursor uint64
}

// PeerConfigWatchTransport performs one bounded remote watch poll. It should
// honor ctx and return a transport error when the peer connection is lost.
type PeerConfigWatchTransport interface {
	Watch(context.Context, PeerConfigWatchRequest) (PeerConfigWatchBatch, error)
}

// PeerConfigWatchHandler consumes one ordered configuration event. Returning
// an error stops the watcher without advancing past the rejected event.
type PeerConfigWatchHandler func(context.Context, PeerConfigWatchEvent) error

// PeerConfigWatchOptions configures a reconnecting prefix watcher. A zero
// BatchSize or reconnect duration selects bounded defaults. MaxReconnects zero
// means retry until the context is canceled.
type PeerConfigWatchOptions struct {
	Prefix           string
	Cursor           uint64
	BatchSize        int
	ReconnectInitial time.Duration
	ReconnectMax     time.Duration
	MaxReconnects    int
	Sleep            func(context.Context, time.Duration) error
	Handler          PeerConfigWatchHandler
}

// PeerConfigWatcher replays ordered remote configuration changes across peer
// reconnects. Run is single-consumer; a watcher must not be run concurrently.
type PeerConfigWatcher struct {
	transport PeerConfigWatchTransport
	prefix    string
	cursor    uint64
	batchSize int
	initial   time.Duration
	maximum   time.Duration
	maxRetry  int
	sleep     func(context.Context, time.Duration) error
	handler   PeerConfigWatchHandler
}

// NewPeerConfigWatcher validates a reconnecting peer configuration watcher.
func NewPeerConfigWatcher(transport PeerConfigWatchTransport, options PeerConfigWatchOptions) (*PeerConfigWatcher, error) {
	if transport == nil {
		return nil, ErrPeerConfigWatchTransportRequired
	}
	if options.Handler == nil {
		return nil, ErrPeerConfigWatchHandlerRequired
	}
	prefix := strings.TrimSpace(options.Prefix)
	if prefix == "" {
		return nil, ErrPeerConfigWatchPrefixRequired
	}
	batchSize := options.BatchSize
	if batchSize == 0 {
		batchSize = DefaultPeerConfigWatchBatchSize
	}
	if batchSize < 1 || batchSize > MaxPeerConfigWatchBatchSize {
		return nil, fmt.Errorf("%w: batch size must be between 1 and %d", ErrPeerConfigWatchOptionsInvalid, MaxPeerConfigWatchBatchSize)
	}
	initial := options.ReconnectInitial
	if initial == 0 {
		initial = DefaultPeerConfigWatchReconnectInitial
	}
	if initial < 0 {
		return nil, fmt.Errorf("%w: reconnect initial delay must not be negative", ErrPeerConfigWatchOptionsInvalid)
	}
	maximum := options.ReconnectMax
	if maximum == 0 {
		maximum = DefaultPeerConfigWatchReconnectMax
	}
	if maximum < initial {
		return nil, fmt.Errorf("%w: reconnect maximum delay must be at least the initial delay", ErrPeerConfigWatchOptionsInvalid)
	}
	if options.MaxReconnects < 0 {
		return nil, fmt.Errorf("%w: max reconnects must not be negative", ErrPeerConfigWatchOptionsInvalid)
	}
	sleep := options.Sleep
	if sleep == nil {
		sleep = sleepPeerConfigWatch
	}
	return &PeerConfigWatcher{
		transport: transport,
		prefix:    prefix,
		cursor:    options.Cursor,
		batchSize: batchSize,
		initial:   initial,
		maximum:   maximum,
		maxRetry:  options.MaxReconnects,
		sleep:     sleep,
		handler:   options.Handler,
	}, nil
}

// Cursor returns the last successfully delivered or skipped remote cursor.
func (watcher *PeerConfigWatcher) Cursor() uint64 {
	if watcher == nil {
		return 0
	}
	return watcher.cursor
}

// Run polls the peer until ctx is canceled, a handler rejects an event, or a
// non-retryable batch validation error occurs. Cursor state survives transport
// errors and is sent again after reconnect.
func (watcher *PeerConfigWatcher) Run(ctx context.Context) error {
	if watcher == nil {
		return ErrPeerConfigWatchTransportRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	delay := watcher.initial
	reconnects := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		batch, err := watcher.transport.Watch(ctx, PeerConfigWatchRequest{
			Prefix: watcher.prefix,
			Cursor: watcher.cursor,
			Limit:  watcher.batchSize,
		})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if watcher.maxRetry > 0 && reconnects >= watcher.maxRetry {
				return fmt.Errorf("%w: %v", ErrPeerConfigWatchReconnectExhausted, err)
			}
			if err := watcher.sleep(ctx, delay); err != nil {
				return err
			}
			reconnects++
			if delay < watcher.maximum {
				delay *= 2
				if delay > watcher.maximum {
					delay = watcher.maximum
				}
			}
			continue
		}
		reconnects = 0
		delay = watcher.initial
		if err := watcher.applyBatch(ctx, batch); err != nil {
			return err
		}
	}
}

func (watcher *PeerConfigWatcher) applyBatch(ctx context.Context, batch PeerConfigWatchBatch) error {
	if len(batch.Events) > watcher.batchSize || batch.NextCursor < watcher.cursor {
		return ErrPeerConfigWatchBatchInvalid
	}
	cursor := watcher.cursor
	for _, event := range batch.Events {
		if event.Cursor <= cursor || event.Cursor > batch.NextCursor || !strings.HasPrefix(event.Key, watcher.prefix) {
			return ErrPeerConfigWatchBatchInvalid
		}
		copyEvent := event
		copyEvent.Value = append([]byte(nil), event.Value...)
		if err := watcher.handler(ctx, copyEvent); err != nil {
			return err
		}
		cursor = event.Cursor
	}
	if batch.NextCursor < cursor {
		return ErrPeerConfigWatchBatchInvalid
	}
	watcher.cursor = batch.NextCursor
	return nil
}

func sleepPeerConfigWatch(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
