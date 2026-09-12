package hatCache

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// KeyChangeOperation identifies the successful mutation that produced an
// event. A set includes updates to an existing value and mutations of a
// collection stored under the key.
type KeyChangeOperation string

const (
	KeyChangeSet    KeyChangeOperation = "set"
	KeyChangeDelete KeyChangeOperation = "delete"
)

const (
	// DefaultKeyWatcherBuffer bounds the amount of mutation history retained by
	// a watcher created with WatchKey.
	DefaultKeyWatcherBuffer = 64
	// MaxKeyWatcherBuffer prevents an accidental or untrusted registration from
	// reserving an unbounded amount of memory.
	MaxKeyWatcherBuffer = 1 << 20
	// DefaultKeyWatcherCoalesceWindow is used when a coalesced watcher does not
	// specify a window.
	DefaultKeyWatcherCoalesceWindow = time.Millisecond
)

// KeyWatcherOptions configures an exact-key or prefix watcher. Exactly one of
// Key and Prefix must be set. Coalescing is opt-in and retains at most Buffer
// distinct keys per delivery window.
type KeyWatcherOptions struct {
	Key            string
	Prefix         string
	Buffer         int
	Coalesce       bool
	CoalesceWindow time.Duration
}

// KeyChangeEvent reports a successful mutation for one watched key. Epoch is
// the trie mutation epoch after the mutation was recorded and is monotonic for
// events delivered by one watcher.
type KeyChangeEvent struct {
	Key       string
	Operation KeyChangeOperation
	Epoch     uint64
}

// KeyWatcher receives ordered events for one exact key or a key prefix.
// Non-coalesced writers apply bounded backpressure when the channel is full;
// coalesced watchers retain the latest event for each matching key in a bounded
// window. Call Close when the watcher is no longer needed.
type KeyWatcher struct {
	owner           *HatTrie
	key             string
	prefix          string
	prefixFilter    bool
	id              uint64
	buffer          int
	events          chan KeyChangeEvent
	closed          bool
	coalesce        bool
	coalesceWindow  time.Duration
	coalesceMu      sync.Mutex
	coalesceSpace   *sync.Cond
	coalescePending map[string]KeyChangeEvent
	coalesceOrder   []string
	coalesceWake    chan struct{}
	coalesceStop    chan struct{}
	coalesceDone    chan struct{}
}

// Events returns the read-only event stream. The channel is closed by Close or
// when the owning trie is destroyed.
func (watcher *KeyWatcher) Events() <-chan KeyChangeEvent {
	if watcher == nil {
		return nil
	}
	return watcher.events
}

// Close unregisters the watcher and closes its event stream. Close is safe to
// call more than once.
func (watcher *KeyWatcher) Close() {
	if watcher == nil {
		return
	}
	owner := watcher.owner
	owner.mu.Lock()
	if watcher.closed {
		owner.mu.Unlock()
		return
	}
	watcher.closed = true
	if watcher.prefixFilter {
		if watchers := owner.keyPrefixWatchers[watcher.prefix]; watchers != nil {
			delete(watchers, watcher.id)
			if len(watchers) == 0 {
				delete(owner.keyPrefixWatchers, watcher.prefix)
			}
		}
	} else if watchers := owner.keyWatchers[watcher.key]; watchers != nil {
		delete(watchers, watcher.id)
		if len(watchers) == 0 {
			delete(owner.keyWatchers, watcher.key)
		}
	}
	coalesced := watcher.stopCoalescerLocked()
	owner.mu.Unlock()
	if coalesced {
		<-watcher.coalesceDone
	}
	close(watcher.events)
}

// WatchKey registers a bounded exact-key watcher using
// DefaultKeyWatcherBuffer. The default cache write path does not allocate or
// send watcher events when no watcher is registered.
func (ht *HatTrie) WatchKey(key string) (*KeyWatcher, error) {
	return ht.WatchKeyWithBuffer(key, DefaultKeyWatcherBuffer)
}

// WatchKeyWithBuffer registers an exact-key watcher with an explicit channel
// capacity. A full channel pauses successful writes until the consumer drains
// it, preserving every event without an unbounded queue.
func (ht *HatTrie) WatchKeyWithBuffer(key string, buffer int) (*KeyWatcher, error) {
	if buffer <= 0 || buffer > MaxKeyWatcherBuffer {
		return nil, fmt.Errorf("key watcher buffer must be between 1 and %d", MaxKeyWatcherBuffer)
	}
	return ht.WatchKeyWithOptions(KeyWatcherOptions{Key: key, Buffer: buffer})
}

// WatchKeyWithOptions registers an exact-key or prefix watcher. Prefix
// watchers are supported only by an unpartitioned trie because writes routed
// to independent local partitions cannot be observed by a root-owned prefix
// registration. Coalesced delivery emits the latest operation and epoch for
// each key after CoalesceWindow; it is useful for invalidation consumers that
// can refresh a key once instead of processing every intermediate mutation.
func (ht *HatTrie) WatchKeyWithOptions(options KeyWatcherOptions) (*KeyWatcher, error) {
	if ht == nil {
		return nil, ErrNilHatTrie
	}
	if (options.Key == "") == (options.Prefix == "") {
		return nil, fmt.Errorf("key watcher requires exactly one non-empty key or prefix")
	}
	if options.Prefix != "" && ht.localPartitions.Load() != nil {
		return nil, fmt.Errorf("prefix key watchers are not supported on partitioned tries")
	}
	filterValue := options.Key
	if options.Prefix != "" {
		filterValue = options.Prefix
	}
	if err := validateKey(filterValue); err != nil {
		return nil, err
	}
	buffer := options.Buffer
	if buffer == 0 {
		buffer = DefaultKeyWatcherBuffer
	}
	if buffer <= 0 || buffer > MaxKeyWatcherBuffer {
		return nil, fmt.Errorf("key watcher buffer must be between 1 and %d", MaxKeyWatcherBuffer)
	}
	if !options.Coalesce && options.CoalesceWindow != 0 {
		return nil, fmt.Errorf("key watcher coalesce window requires coalesced delivery")
	}
	coalesceWindow := options.CoalesceWindow
	if options.Coalesce {
		if coalesceWindow == 0 {
			coalesceWindow = DefaultKeyWatcherCoalesceWindow
		}
		if coalesceWindow < 0 {
			return nil, fmt.Errorf("key watcher coalesce window must not be negative")
		}
	}
	if options.Prefix == "" {
		if partition := ht.localPartitionForKey(options.Key); partition != nil {
			return partition.WatchKeyWithOptions(options)
		}
	}

	ht.mu.Lock()
	if ht.root == nil {
		ht.mu.Unlock()
		return nil, ErrNilHatTrie
	}
	ht.nextKeyWatcherID++
	if ht.nextKeyWatcherID == 0 {
		ht.nextKeyWatcherID++
	}
	watcher := &KeyWatcher{
		owner:          ht,
		key:            options.Key,
		prefix:         options.Prefix,
		prefixFilter:   options.Prefix != "",
		id:             ht.nextKeyWatcherID,
		buffer:         buffer,
		events:         make(chan KeyChangeEvent, buffer),
		coalesce:       options.Coalesce,
		coalesceWindow: coalesceWindow,
	}
	if watcher.coalesce {
		watcher.coalesceSpace = sync.NewCond(&watcher.coalesceMu)
		watcher.coalescePending = make(map[string]KeyChangeEvent)
		watcher.coalesceWake = make(chan struct{}, 1)
		watcher.coalesceStop = make(chan struct{})
		watcher.coalesceDone = make(chan struct{})
		go watcher.runCoalescer()
	}
	if watcher.prefixFilter {
		if ht.keyPrefixWatchers == nil {
			ht.keyPrefixWatchers = make(map[string]map[uint64]*KeyWatcher)
		}
		watchers := ht.keyPrefixWatchers[watcher.prefix]
		if watchers == nil {
			watchers = make(map[uint64]*KeyWatcher)
			ht.keyPrefixWatchers[watcher.prefix] = watchers
		}
		watchers[watcher.id] = watcher
	} else {
		if ht.keyWatchers == nil {
			ht.keyWatchers = make(map[string]map[uint64]*KeyWatcher)
		}
		watchers := ht.keyWatchers[watcher.key]
		if watchers == nil {
			watchers = make(map[uint64]*KeyWatcher)
			ht.keyWatchers[watcher.key] = watchers
		}
		watchers[watcher.id] = watcher
	}
	ht.mu.Unlock()
	return watcher, nil
}

func (ht *HatTrie) notifyKeyWatchersLocked(key string, operation KeyChangeOperation) {
	event := KeyChangeEvent{Key: key, Operation: operation, Epoch: ht.mutationEpoch}
	for _, watcher := range ht.keyWatchers[key] {
		if !watcher.closed {
			watcher.publish(event)
		}
	}
	for prefix, watchers := range ht.keyPrefixWatchers {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, watcher := range watchers {
			if !watcher.closed {
				watcher.publish(event)
			}
		}
	}
}

func (ht *HatTrie) closeKeyWatchersLocked() {
	for key, watchers := range ht.keyWatchers {
		for id, watcher := range watchers {
			watcher.closed = true
			coalesced := watcher.stopCoalescerLocked()
			if coalesced {
				<-watcher.coalesceDone
			}
			close(watcher.events)
			delete(watchers, id)
		}
		delete(ht.keyWatchers, key)
	}
	for prefix, watchers := range ht.keyPrefixWatchers {
		for id, watcher := range watchers {
			watcher.closed = true
			coalesced := watcher.stopCoalescerLocked()
			if coalesced {
				<-watcher.coalesceDone
			}
			close(watcher.events)
			delete(watchers, id)
		}
		delete(ht.keyPrefixWatchers, prefix)
	}
	ht.keyWatchers = nil
	ht.keyPrefixWatchers = nil
}

func (watcher *KeyWatcher) publish(event KeyChangeEvent) {
	if !watcher.coalesce {
		watcher.events <- event
		return
	}
	watcher.coalesceMu.Lock()
	first := false
	for {
		if watcher.closed {
			watcher.coalesceMu.Unlock()
			return
		}
		if _, exists := watcher.coalescePending[event.Key]; exists {
			watcher.coalescePending[event.Key] = event
			break
		}
		if len(watcher.coalescePending) < watcher.buffer {
			if len(watcher.coalescePending) == 0 {
				first = true
			}
			watcher.coalescePending[event.Key] = event
			watcher.coalesceOrder = append(watcher.coalesceOrder, event.Key)
			break
		}
		watcher.coalesceSpace.Wait()
	}
	watcher.coalesceMu.Unlock()
	if first {
		select {
		case watcher.coalesceWake <- struct{}{}:
		default:
		}
	}
}

func (watcher *KeyWatcher) runCoalescer() {
	defer close(watcher.coalesceDone)
	for {
		select {
		case <-watcher.coalesceWake:
			timer := time.NewTimer(watcher.coalesceWindow)
			select {
			case <-timer.C:
				watcher.flushCoalesced()
			case <-watcher.coalesceStop:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return
			}
		case <-watcher.coalesceStop:
			return
		}
	}
}

func (watcher *KeyWatcher) flushCoalesced() {
	watcher.coalesceMu.Lock()
	if len(watcher.coalesceOrder) == 0 {
		watcher.coalesceMu.Unlock()
		return
	}
	batch := make([]KeyChangeEvent, len(watcher.coalesceOrder))
	for index, key := range watcher.coalesceOrder {
		batch[index] = watcher.coalescePending[key]
		delete(watcher.coalescePending, key)
	}
	watcher.coalesceOrder = watcher.coalesceOrder[:0]
	watcher.coalesceSpace.Broadcast()
	watcher.coalesceMu.Unlock()

	for _, event := range batch {
		select {
		case watcher.events <- event:
		case <-watcher.coalesceStop:
			return
		}
	}
}

func (watcher *KeyWatcher) stopCoalescerLocked() bool {
	if !watcher.coalesce {
		return false
	}
	watcher.coalesceMu.Lock()
	close(watcher.coalesceStop)
	watcher.coalesceSpace.Broadcast()
	watcher.coalesceMu.Unlock()
	return true
}
