package hatPeer

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

var (
	// ErrCompactPeerWatchOptionsInvalid indicates an invalid watch request or bound.
	ErrCompactPeerWatchOptionsInvalid = errors.New("hatPeer: compact peer watch options are invalid")
	// ErrCompactPeerWatchProviderRequired indicates that a server has no source adapter.
	ErrCompactPeerWatchProviderRequired = errors.New("hatPeer: compact peer watch provider is required")
	// ErrCompactPeerWatchClosed indicates that a watch endpoint is closed.
	ErrCompactPeerWatchClosed = errors.New("hatPeer: compact peer watch is closed")
	// ErrCompactPeerWatchLimit indicates that a watch bound has been reached.
	ErrCompactPeerWatchLimit = errors.New("hatPeer: compact peer watch limit reached")
	// ErrCompactPeerWatchProtocol indicates a malformed watch envelope.
	ErrCompactPeerWatchProtocol = errors.New("hatPeer: compact peer watch protocol is invalid")
	// ErrCompactPeerWatchNotFound indicates that an event or registration used an unknown ID.
	ErrCompactPeerWatchNotFound = errors.New("hatPeer: compact peer watch was not found")
)

const (
	// DefaultCompactPeerWatchBuffer bounds one remote watch's pending events.
	DefaultCompactPeerWatchBuffer = 64
	// MaxCompactPeerWatchBuffer prevents a remote registration from reserving
	// unbounded memory on either endpoint.
	MaxCompactPeerWatchBuffer = 1 << 20
	// DefaultCompactPeerWatchMaxWatches bounds active watches on one endpoint.
	DefaultCompactPeerWatchMaxWatches = 64
	maxCompactPeerWatchMaxWatches     = 1 << 16
	maxCompactPeerWatchFilterBytes    = 4096
	maxCompactPeerWatchOperationBytes = 64
	compactPeerWatchWireVersion       = 1
	compactPeerWatchFlagKey           = 1 << 0
	compactPeerWatchFlagPrefix        = 1 << 1
	compactPeerWatchFlagCoalesce      = 1 << 2
)

var (
	compactPeerWatchRegisterCommand   = []byte("_hat.peer.watch.register.v1")
	compactPeerWatchUnregisterCommand = []byte("_hat.peer.watch.unregister.v1")
	compactPeerWatchEventCommand      = []byte("_hat.peer.watch.event.v1")
)

// CompactPeerWatchRequest selects one exact key or key prefix. LastEpoch is
// supplied by CompactPeerWatchClient during reattachment and is passed to the
// provider so it can report whether an event gap is possible.
type CompactPeerWatchRequest struct {
	Key            string
	Prefix         string
	Buffer         int
	Coalesce       bool
	CoalesceWindow time.Duration
	LastEpoch      uint64
}

// CompactPeerWatchEvent is delivered in mutation order for one remote watch.
// Gap is an explicit recovery signal: the provider could not replay every
// mutation after LastEpoch and the consumer should refresh its configuration.
type CompactPeerWatchEvent struct {
	Key       string
	Operation string
	Epoch     uint64
	Gap       bool
}

// CompactPeerWatchSource adapts a local bounded watcher to the peer protocol.
// Events must be ordered for one source. Close must be idempotent.
type CompactPeerWatchSource interface {
	Events() <-chan CompactPeerWatchEvent
	Close() error
}

// CompactPeerWatchProvider opens one local source for a remote registration.
// Authentication and authorization remain owned by the compact peer handshake
// and the application that constructs the session.
type CompactPeerWatchProvider interface {
	OpenCompactPeerWatch(context.Context, CompactPeerWatchRequest) (CompactPeerWatchSource, error)
}

// CompactPeerWatchServerOptions configures a bounded watch server over one
// already-authenticated compact peer connection.
type CompactPeerWatchServerOptions struct {
	Session    CompactPeerSessionOptions
	Provider   CompactPeerWatchProvider
	MaxWatches int
}

// CompactPeerWatchServer serves registration requests and forwards ordered
// source events as request/response calls, which gives event delivery bounded
// backpressure and uses the session's existing framing and authentication.
type CompactPeerWatchServer struct {
	session  *CompactPeerSession
	provider CompactPeerWatchProvider
	ordinary CompactPeerHandler
	context  context.Context
	cancel   context.CancelFunc
	max      int

	mu        sync.Mutex
	closed    bool
	watches   map[uint64]*compactPeerWatchServerSubscription
	closeOnce sync.Once
}

type compactPeerWatchServerSubscription struct {
	id     uint64
	source CompactPeerWatchSource
}

// NewCompactPeerWatchServer starts a bounded server on conn. The caller is
// responsible for performing the existing CompactPeer handshake before this
// constructor when a network connection requires authentication.
func NewCompactPeerWatchServer(conn net.Conn, options CompactPeerWatchServerOptions) (*CompactPeerWatchServer, error) {
	if options.Provider == nil {
		return nil, ErrCompactPeerWatchProviderRequired
	}
	maxWatches := options.MaxWatches
	if maxWatches == 0 {
		maxWatches = DefaultCompactPeerWatchMaxWatches
	}
	if maxWatches < 1 || maxWatches > maxCompactPeerWatchMaxWatches {
		return nil, ErrCompactPeerWatchOptionsInvalid
	}
	parent := options.Session.Context
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	server := &CompactPeerWatchServer{
		provider: options.Provider,
		ordinary: options.Session.Handler,
		context:  ctx,
		cancel:   cancel,
		max:      maxWatches,
		watches:  make(map[uint64]*compactPeerWatchServerSubscription),
	}
	sessionOptions := options.Session
	sessionOptions.Context = ctx
	sessionOptions.Handler = server.handleRequest
	session, err := NewCompactPeerSession(conn, sessionOptions)
	if err != nil {
		cancel()
		return nil, err
	}
	server.session = session
	return server, nil
}

// Close terminates the session and all local source subscriptions.
func (server *CompactPeerWatchServer) Close() error {
	if server == nil {
		return ErrCompactPeerWatchClosed
	}
	server.closeOnce.Do(func() {
		server.cancel()
		_ = server.session.Close()
		server.mu.Lock()
		server.closed = true
		subscriptions := make([]*compactPeerWatchServerSubscription, 0, len(server.watches))
		for _, subscription := range server.watches {
			subscriptions = append(subscriptions, subscription)
		}
		server.watches = make(map[uint64]*compactPeerWatchServerSubscription)
		server.mu.Unlock()
		for _, subscription := range subscriptions {
			_ = subscription.source.Close()
		}
	})
	return nil
}

// Done returns a channel closed when the underlying session terminates.
func (server *CompactPeerWatchServer) Done() <-chan struct{} {
	if server == nil || server.session == nil {
		return nil
	}
	return server.session.Done()
}

// Err returns the terminal error from the underlying session.
func (server *CompactPeerWatchServer) Err() error {
	if server == nil || server.session == nil {
		return ErrCompactPeerWatchClosed
	}
	return server.session.Err()
}

func (server *CompactPeerWatchServer) handleRequest(ctx context.Context, frame CompactFrame) (CompactFrame, error) {
	switch {
	case bytes.Equal(frame.Command, compactPeerWatchRegisterCommand):
		requestID, request, err := decodeCompactPeerWatchRegistration(frame.Payload)
		if err != nil {
			return CompactFrame{}, err
		}
		if err := server.register(ctx, requestID, request); err != nil {
			return CompactFrame{}, err
		}
		return CompactFrame{}, nil
	case bytes.Equal(frame.Command, compactPeerWatchUnregisterCommand):
		requestID, err := decodeCompactPeerWatchID(frame.Payload)
		if err != nil {
			return CompactFrame{}, err
		}
		server.unregister(requestID)
		return CompactFrame{}, nil
	default:
		if server.ordinary == nil {
			return CompactFrame{}, ErrCompactPeerHandlerRequired
		}
		return server.ordinary(ctx, frame)
	}
}

func (server *CompactPeerWatchServer) register(ctx context.Context, id uint64, request CompactPeerWatchRequest) error {
	if id == 0 {
		return ErrCompactPeerWatchProtocol
	}
	normalized, err := normalizeCompactPeerWatchRequest(request)
	if err != nil {
		return err
	}
	server.mu.Lock()
	if server.closed {
		server.mu.Unlock()
		return ErrCompactPeerWatchClosed
	}
	if _, exists := server.watches[id]; exists {
		server.mu.Unlock()
		return ErrCompactPeerWatchProtocol
	}
	if len(server.watches) >= server.max {
		server.mu.Unlock()
		return ErrCompactPeerWatchLimit
	}
	server.mu.Unlock()

	source, err := server.provider.OpenCompactPeerWatch(ctx, normalized)
	if err != nil {
		return err
	}
	if source == nil || source.Events() == nil {
		if source != nil {
			_ = source.Close()
		}
		return ErrCompactPeerWatchOptionsInvalid
	}
	subscription := &compactPeerWatchServerSubscription{id: id, source: source}
	server.mu.Lock()
	if server.closed {
		server.mu.Unlock()
		_ = source.Close()
		return ErrCompactPeerWatchClosed
	}
	if _, exists := server.watches[id]; exists {
		server.mu.Unlock()
		_ = source.Close()
		return ErrCompactPeerWatchProtocol
	}
	server.watches[id] = subscription
	server.mu.Unlock()
	go server.forward(subscription)
	return nil
}

func (server *CompactPeerWatchServer) forward(subscription *compactPeerWatchServerSubscription) {
	defer server.detach(subscription)
	for {
		select {
		case event, ok := <-subscription.source.Events():
			if !ok {
				return
			}
			payload, err := encodeCompactPeerWatchEvent(subscription.id, event)
			if err != nil {
				return
			}
			if _, err := server.session.Call(server.context, compactPeerWatchEventCommand, payload); err != nil {
				return
			}
		case <-server.context.Done():
			return
		}
	}
}

func (server *CompactPeerWatchServer) unregister(id uint64) {
	server.mu.Lock()
	subscription := server.watches[id]
	if subscription != nil {
		delete(server.watches, id)
	}
	server.mu.Unlock()
	if subscription != nil {
		_ = subscription.source.Close()
	}
}

func (server *CompactPeerWatchServer) detach(subscription *compactPeerWatchServerSubscription) {
	server.mu.Lock()
	if server.watches[subscription.id] == subscription {
		delete(server.watches, subscription.id)
	}
	server.mu.Unlock()
	_ = subscription.source.Close()
}

// CompactPeerWatchClientOptions configures a client over one authenticated
// compact peer connection. Reconnect accepts a newly authenticated net.Conn
// and re-registers every active watch.
type CompactPeerWatchClientOptions struct {
	Session    CompactPeerSessionOptions
	MaxWatches int
}

// CompactPeerWatchClient owns remote registrations and preserves them across
// explicit Reconnect calls. A reconnect does not hide authentication or dialer
// policy from the caller.
type CompactPeerWatchClient struct {
	mu          sync.RWMutex
	reconnectMu sync.Mutex
	closed      bool
	max         int
	nextID      uint64
	generation  uint64
	session     *CompactPeerSession
	options     CompactPeerSessionOptions
	ordinary    CompactPeerHandler
	watches     map[uint64]*CompactPeerRemoteWatcher
}

// NewCompactPeerWatchClient starts a watch client on conn.
func NewCompactPeerWatchClient(conn net.Conn, options CompactPeerWatchClientOptions) (*CompactPeerWatchClient, error) {
	maxWatches := options.MaxWatches
	if maxWatches == 0 {
		maxWatches = DefaultCompactPeerWatchMaxWatches
	}
	if maxWatches < 1 || maxWatches > maxCompactPeerWatchMaxWatches {
		return nil, ErrCompactPeerWatchOptionsInvalid
	}
	client := &CompactPeerWatchClient{
		max:        maxWatches,
		options:    options.Session,
		ordinary:   options.Session.Handler,
		generation: 1,
		watches:    make(map[uint64]*CompactPeerRemoteWatcher),
	}
	sessionOptions := options.Session
	sessionOptions.Handler = client.handlerForGeneration(client.generation)
	session, err := NewCompactPeerSession(conn, sessionOptions)
	if err != nil {
		return nil, err
	}
	client.session = session
	client.options = sessionOptions
	return client, nil
}

// Watch registers one remote exact-key or prefix watch.
func (client *CompactPeerWatchClient) Watch(ctx context.Context, request CompactPeerWatchRequest) (*CompactPeerRemoteWatcher, error) {
	if client == nil {
		return nil, ErrCompactPeerWatchClosed
	}
	normalized, err := normalizeCompactPeerWatchRequest(request)
	if err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil, ErrCompactPeerWatchClosed
	}
	if len(client.watches) >= client.max {
		client.mu.Unlock()
		return nil, ErrCompactPeerWatchLimit
	}
	client.nextID++
	if client.nextID == 0 {
		client.nextID++
	}
	watcher := newCompactPeerRemoteWatcher(client, client.nextID, normalized)
	client.watches[watcher.id] = watcher
	session := client.session
	client.mu.Unlock()
	if session == nil {
		client.removeWatcher(watcher)
		return nil, ErrCompactPeerWatchClosed
	}
	if err := client.register(ctx, session, watcher); err != nil {
		client.removeWatcher(watcher)
		return nil, err
	}
	return watcher, nil
}

// Reconnect replaces the underlying session and re-registers active watches.
// The new connection must already have completed the caller's authentication
// and protocol handshake.
func (client *CompactPeerWatchClient) Reconnect(conn net.Conn) error {
	if client == nil {
		return ErrCompactPeerWatchClosed
	}
	client.reconnectMu.Lock()
	defer client.reconnectMu.Unlock()
	client.mu.RLock()
	if client.closed {
		client.mu.RUnlock()
		return ErrCompactPeerWatchClosed
	}
	oldGeneration := client.generation
	newGeneration := oldGeneration + 1
	if newGeneration == 0 {
		newGeneration = 1
	}
	options := client.options
	options.Handler = client.handlerForGeneration(newGeneration)
	watchers := make([]*CompactPeerRemoteWatcher, 0, len(client.watches))
	for _, watcher := range client.watches {
		watchers = append(watchers, watcher)
	}
	client.mu.RUnlock()
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return ErrCompactPeerWatchClosed
	}
	client.generation = newGeneration
	client.mu.Unlock()
	session, err := NewCompactPeerSession(conn, options)
	if err != nil {
		client.restoreGeneration(oldGeneration, newGeneration)
		return err
	}
	for _, watcher := range watchers {
		if watcher.isClosed() {
			continue
		}
		if err := client.register(context.Background(), session, watcher); err != nil {
			_ = session.Close()
			client.restoreGeneration(oldGeneration, newGeneration)
			return err
		}
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		_ = session.Close()
		client.restoreGeneration(oldGeneration, newGeneration)
		return ErrCompactPeerWatchClosed
	}
	old := client.session
	client.session = session
	client.options = options
	client.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	return nil
}

// Close unregisters local watchers and terminates the underlying session.
func (client *CompactPeerWatchClient) Close() error {
	if client == nil {
		return ErrCompactPeerWatchClosed
	}
	client.mu.Lock()
	if client.closed {
		client.mu.Unlock()
		return nil
	}
	client.closed = true
	session := client.session
	watchers := make([]*CompactPeerRemoteWatcher, 0, len(client.watches))
	for _, watcher := range client.watches {
		watchers = append(watchers, watcher)
	}
	client.watches = make(map[uint64]*CompactPeerRemoteWatcher)
	client.mu.Unlock()
	for _, watcher := range watchers {
		watcher.closeLocal()
	}
	if session != nil {
		return session.Close()
	}
	return nil
}

func (client *CompactPeerWatchClient) register(ctx context.Context, session *CompactPeerSession, watcher *CompactPeerRemoteWatcher) error {
	request := watcher.requestSnapshot()
	payload, err := encodeCompactPeerWatchRegistration(watcher.id, request)
	if err != nil {
		return err
	}
	_, err = session.Call(ctx, compactPeerWatchRegisterCommand, payload)
	return err
}

func (client *CompactPeerWatchClient) handleRequest(ctx context.Context, frame CompactFrame) (CompactFrame, error) {
	if bytes.Equal(frame.Command, compactPeerWatchEventCommand) {
		id, event, err := decodeCompactPeerWatchEvent(frame.Payload)
		if err != nil {
			return CompactFrame{}, err
		}
		client.mu.RLock()
		watcher := client.watches[id]
		client.mu.RUnlock()
		if watcher == nil {
			return CompactFrame{}, ErrCompactPeerWatchNotFound
		}
		watcher.publish(event)
		return CompactFrame{}, nil
	}
	if client.ordinary == nil {
		return CompactFrame{}, ErrCompactPeerHandlerRequired
	}
	return client.ordinary(ctx, frame)
}

func (client *CompactPeerWatchClient) handlerForGeneration(generation uint64) CompactPeerHandler {
	return func(ctx context.Context, frame CompactFrame) (CompactFrame, error) {
		client.mu.RLock()
		active := !client.closed && client.generation == generation
		client.mu.RUnlock()
		if !active {
			return CompactFrame{}, ErrCompactPeerWatchClosed
		}
		return client.handleRequest(ctx, frame)
	}
}

func (client *CompactPeerWatchClient) restoreGeneration(previous, replaced uint64) {
	client.mu.Lock()
	if client.generation == replaced && !client.closed {
		client.generation = previous
	}
	client.mu.Unlock()
}

func (client *CompactPeerWatchClient) removeWatcher(watcher *CompactPeerRemoteWatcher) {
	client.mu.Lock()
	if client.watches[watcher.id] == watcher {
		delete(client.watches, watcher.id)
	}
	client.mu.Unlock()
	watcher.closeLocal()
}

func (client *CompactPeerWatchClient) closeWatcher(watcher *CompactPeerRemoteWatcher) error {
	client.mu.Lock()
	if client.watches[watcher.id] == watcher {
		delete(client.watches, watcher.id)
	}
	session := client.session
	client.mu.Unlock()
	watcher.closeLocal()
	if session == nil || client.isClosed() {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	payload, err := encodeCompactPeerWatchID(watcher.id)
	if err != nil {
		return err
	}
	_, err = session.Call(ctx, compactPeerWatchUnregisterCommand, payload)
	return err
}

func (client *CompactPeerWatchClient) isClosed() bool {
	client.mu.RLock()
	closed := client.closed
	client.mu.RUnlock()
	return closed
}

// CompactPeerRemoteWatcher is a bounded event stream backed by one remote
// registration. Close is idempotent and closes Events after pending delivery
// has stopped.
type CompactPeerRemoteWatcher struct {
	client  *CompactPeerWatchClient
	id      uint64
	request CompactPeerWatchRequest
	events  chan CompactPeerWatchEvent
	done    chan struct{}

	mu        sync.Mutex
	closed    bool
	lastEpoch uint64
	closeOnce sync.Once
	publishWG sync.WaitGroup
}

func newCompactPeerRemoteWatcher(client *CompactPeerWatchClient, id uint64, request CompactPeerWatchRequest) *CompactPeerRemoteWatcher {
	return &CompactPeerRemoteWatcher{
		client:    client,
		id:        id,
		request:   request,
		events:    make(chan CompactPeerWatchEvent, request.Buffer),
		done:      make(chan struct{}),
		lastEpoch: request.LastEpoch,
	}
}

// Events returns the bounded remote event stream.
func (watcher *CompactPeerRemoteWatcher) Events() <-chan CompactPeerWatchEvent {
	if watcher == nil {
		return nil
	}
	return watcher.events
}

// Close removes the remote registration and closes the local event stream.
func (watcher *CompactPeerRemoteWatcher) Close() error {
	if watcher == nil || watcher.client == nil {
		return ErrCompactPeerWatchClosed
	}
	return watcher.client.closeWatcher(watcher)
}

func (watcher *CompactPeerRemoteWatcher) requestSnapshot() CompactPeerWatchRequest {
	watcher.mu.Lock()
	request := watcher.request
	request.LastEpoch = watcher.lastEpoch
	watcher.mu.Unlock()
	return request
}

func (watcher *CompactPeerRemoteWatcher) isClosed() bool {
	watcher.mu.Lock()
	closed := watcher.closed
	watcher.mu.Unlock()
	return closed
}

func (watcher *CompactPeerRemoteWatcher) publish(event CompactPeerWatchEvent) {
	watcher.mu.Lock()
	if watcher.closed {
		watcher.mu.Unlock()
		return
	}
	if event.Epoch > watcher.lastEpoch {
		watcher.lastEpoch = event.Epoch
	}
	watcher.publishWG.Add(1)
	done := watcher.done
	watcher.mu.Unlock()
	defer watcher.publishWG.Done()
	select {
	case watcher.events <- event:
	case <-done:
	}
}

func (watcher *CompactPeerRemoteWatcher) closeLocal() {
	watcher.closeOnce.Do(func() {
		watcher.mu.Lock()
		watcher.closed = true
		close(watcher.done)
		watcher.mu.Unlock()
		watcher.publishWG.Wait()
		close(watcher.events)
	})
}

func normalizeCompactPeerWatchRequest(request CompactPeerWatchRequest) (CompactPeerWatchRequest, error) {
	if (request.Key == "") == (request.Prefix == "") {
		return CompactPeerWatchRequest{}, fmt.Errorf("%w: exactly one non-empty key or prefix is required", ErrCompactPeerWatchOptionsInvalid)
	}
	if len(request.Key) > maxCompactPeerWatchFilterBytes || len(request.Prefix) > maxCompactPeerWatchFilterBytes {
		return CompactPeerWatchRequest{}, fmt.Errorf("%w: key or prefix is too long", ErrCompactPeerWatchOptionsInvalid)
	}
	if request.Buffer == 0 {
		request.Buffer = DefaultCompactPeerWatchBuffer
	}
	if request.Buffer < 1 || request.Buffer > MaxCompactPeerWatchBuffer {
		return CompactPeerWatchRequest{}, ErrCompactPeerWatchOptionsInvalid
	}
	if !request.Coalesce && request.CoalesceWindow != 0 {
		return CompactPeerWatchRequest{}, fmt.Errorf("%w: coalesce window requires coalescing", ErrCompactPeerWatchOptionsInvalid)
	}
	if request.Coalesce {
		if request.CoalesceWindow == 0 {
			request.CoalesceWindow = time.Millisecond
		}
		if request.CoalesceWindow < 0 {
			return CompactPeerWatchRequest{}, ErrCompactPeerWatchOptionsInvalid
		}
	}
	return request, nil
}

func encodeCompactPeerWatchRegistration(id uint64, request CompactPeerWatchRequest) ([]byte, error) {
	normalized, err := normalizeCompactPeerWatchRequest(request)
	if err != nil || id == 0 {
		if err != nil {
			return nil, err
		}
		return nil, ErrCompactPeerWatchProtocol
	}
	flags := byte(0)
	if normalized.Key != "" {
		flags |= compactPeerWatchFlagKey
	} else {
		flags |= compactPeerWatchFlagPrefix
	}
	if normalized.Coalesce {
		flags |= compactPeerWatchFlagCoalesce
	}
	payload := make([]byte, 0, 32+len(normalized.Key)+len(normalized.Prefix))
	payload = append(payload, compactPeerWatchWireVersion, flags)
	payload = appendCompactPeerWatchUvarint(payload, id)
	payload = appendCompactPeerWatchUvarint(payload, uint64(normalized.Buffer))
	payload = appendCompactPeerWatchUvarint(payload, uint64(normalized.CoalesceWindow))
	payload = appendCompactPeerWatchUvarint(payload, normalized.LastEpoch)
	payload = appendCompactPeerWatchString(payload, normalized.Key)
	payload = appendCompactPeerWatchString(payload, normalized.Prefix)
	return payload, nil
}

func decodeCompactPeerWatchRegistration(payload []byte) (uint64, CompactPeerWatchRequest, error) {
	if len(payload) < 2 || payload[0] != compactPeerWatchWireVersion {
		return 0, CompactPeerWatchRequest{}, ErrCompactPeerWatchProtocol
	}
	flags := payload[1]
	if flags&^(compactPeerWatchFlagKey|compactPeerWatchFlagPrefix|compactPeerWatchFlagCoalesce) != 0 {
		return 0, CompactPeerWatchRequest{}, ErrCompactPeerWatchProtocol
	}
	position := 2
	id, err := readCompactPeerWatchUvarint(payload, &position)
	if err != nil {
		return 0, CompactPeerWatchRequest{}, err
	}
	buffer, err := readCompactPeerWatchUvarint(payload, &position)
	if err != nil || buffer > uint64(MaxCompactPeerWatchBuffer) {
		return 0, CompactPeerWatchRequest{}, ErrCompactPeerWatchProtocol
	}
	window, err := readCompactPeerWatchUvarint(payload, &position)
	if err != nil || window > uint64(time.Duration(1<<63-1)) {
		return 0, CompactPeerWatchRequest{}, ErrCompactPeerWatchProtocol
	}
	lastEpoch, err := readCompactPeerWatchUvarint(payload, &position)
	if err != nil {
		return 0, CompactPeerWatchRequest{}, err
	}
	key, err := readCompactPeerWatchString(payload, &position)
	if err != nil {
		return 0, CompactPeerWatchRequest{}, err
	}
	prefix, err := readCompactPeerWatchString(payload, &position)
	if err != nil || position != len(payload) {
		return 0, CompactPeerWatchRequest{}, ErrCompactPeerWatchProtocol
	}
	request := CompactPeerWatchRequest{
		Key:            key,
		Prefix:         prefix,
		Buffer:         int(buffer),
		Coalesce:       flags&compactPeerWatchFlagCoalesce != 0,
		CoalesceWindow: time.Duration(window),
		LastEpoch:      lastEpoch,
	}
	if (flags&compactPeerWatchFlagKey != 0) != (key != "") || (flags&compactPeerWatchFlagPrefix != 0) != (prefix != "") {
		return 0, CompactPeerWatchRequest{}, ErrCompactPeerWatchProtocol
	}
	normalized, err := normalizeCompactPeerWatchRequest(request)
	if err != nil {
		return 0, CompactPeerWatchRequest{}, err
	}
	return id, normalized, nil
}

func encodeCompactPeerWatchEvent(id uint64, event CompactPeerWatchEvent) ([]byte, error) {
	if id == 0 || len(event.Key) > maxCompactPeerWatchFilterBytes || len(event.Operation) > maxCompactPeerWatchOperationBytes {
		return nil, ErrCompactPeerWatchProtocol
	}
	flags := byte(0)
	if event.Gap {
		flags = 1
	}
	payload := make([]byte, 0, 24+len(event.Key)+len(event.Operation))
	payload = append(payload, compactPeerWatchWireVersion, flags)
	payload = appendCompactPeerWatchUvarint(payload, id)
	payload = appendCompactPeerWatchUvarint(payload, event.Epoch)
	payload = appendCompactPeerWatchString(payload, event.Operation)
	payload = appendCompactPeerWatchString(payload, event.Key)
	return payload, nil
}

func decodeCompactPeerWatchEvent(payload []byte) (uint64, CompactPeerWatchEvent, error) {
	if len(payload) < 2 || payload[0] != compactPeerWatchWireVersion || payload[1]&^byte(1) != 0 {
		return 0, CompactPeerWatchEvent{}, ErrCompactPeerWatchProtocol
	}
	position := 2
	id, err := readCompactPeerWatchUvarint(payload, &position)
	if err != nil {
		return 0, CompactPeerWatchEvent{}, err
	}
	epoch, err := readCompactPeerWatchUvarint(payload, &position)
	if err != nil {
		return 0, CompactPeerWatchEvent{}, err
	}
	operation, err := readCompactPeerWatchStringWithLimit(payload, &position, maxCompactPeerWatchOperationBytes)
	if err != nil {
		return 0, CompactPeerWatchEvent{}, err
	}
	key, err := readCompactPeerWatchString(payload, &position)
	if err != nil || position != len(payload) || id == 0 {
		return 0, CompactPeerWatchEvent{}, ErrCompactPeerWatchProtocol
	}
	return id, CompactPeerWatchEvent{Key: key, Operation: operation, Epoch: epoch, Gap: payload[1]&1 != 0}, nil
}

func encodeCompactPeerWatchID(id uint64) ([]byte, error) {
	if id == 0 {
		return nil, ErrCompactPeerWatchProtocol
	}
	payload := []byte{compactPeerWatchWireVersion}
	return appendCompactPeerWatchUvarint(payload, id), nil
}

func decodeCompactPeerWatchID(payload []byte) (uint64, error) {
	if len(payload) < 2 || payload[0] != compactPeerWatchWireVersion {
		return 0, ErrCompactPeerWatchProtocol
	}
	position := 1
	id, err := readCompactPeerWatchUvarint(payload, &position)
	if err != nil || position != len(payload) || id == 0 {
		return 0, ErrCompactPeerWatchProtocol
	}
	return id, nil
}

func appendCompactPeerWatchUvarint(payload []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	count := binary.PutUvarint(encoded[:], value)
	return append(payload, encoded[:count]...)
}

func appendCompactPeerWatchString(payload []byte, value string) []byte {
	payload = appendCompactPeerWatchUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func readCompactPeerWatchUvarint(payload []byte, position *int) (uint64, error) {
	if position == nil || *position >= len(payload) {
		return 0, ErrCompactPeerWatchProtocol
	}
	value, count := binary.Uvarint(payload[*position:])
	if count <= 0 {
		return 0, ErrCompactPeerWatchProtocol
	}
	*position += count
	return value, nil
}

func readCompactPeerWatchString(payload []byte, position *int) (string, error) {
	return readCompactPeerWatchStringWithLimit(payload, position, maxCompactPeerWatchFilterBytes)
}

func readCompactPeerWatchStringWithLimit(payload []byte, position *int, limit int) (string, error) {
	length, err := readCompactPeerWatchUvarint(payload, position)
	if err != nil || length > uint64(limit) || position == nil || length > uint64(len(payload)-*position) {
		return "", ErrCompactPeerWatchProtocol
	}
	start := *position
	*position += int(length)
	return string(payload[start:*position]), nil
}
