package hatCache

import (
	"context"
	"sync"
	"sync/atomic"

	"hatrie_cache/hat/hatPeer"
)

// OpenCompactPeerWatch adapts HatTrie key watchers to the bounded compact-peer
// watch protocol. The trie has no replay log, so a reconnect after an epoch
// change produces one explicit gap event instead of silently losing updates.
func (ht *HatTrie) OpenCompactPeerWatch(ctx context.Context, request hatPeer.CompactPeerWatchRequest) (hatPeer.CompactPeerWatchSource, error) {
	if ht == nil {
		return nil, ErrNilHatTrie
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	watcher, err := ht.WatchKeyWithOptions(KeyWatcherOptions{
		Key:            request.Key,
		Prefix:         request.Prefix,
		Buffer:         request.Buffer,
		Coalesce:       request.Coalesce,
		CoalesceWindow: request.CoalesceWindow,
	})
	if err != nil {
		return nil, err
	}
	currentEpoch := atomic.LoadUint64(&ht.mutationEpoch)
	source := &hatTrieCompactPeerWatchSource{
		watcher: watcher,
		events:  make(chan hatPeer.CompactPeerWatchEvent, request.Buffer),
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
		gap:     request.LastEpoch != 0 && request.LastEpoch != currentEpoch,
		epoch:   currentEpoch,
	}
	go source.run()
	return source, nil
}

type hatTrieCompactPeerWatchSource struct {
	watcher   *KeyWatcher
	events    chan hatPeer.CompactPeerWatchEvent
	stop      chan struct{}
	done      chan struct{}
	gap       bool
	epoch     uint64
	closeOnce sync.Once
}

func (source *hatTrieCompactPeerWatchSource) Events() <-chan hatPeer.CompactPeerWatchEvent {
	if source == nil {
		return nil
	}
	return source.events
}

func (source *hatTrieCompactPeerWatchSource) Close() error {
	if source == nil {
		return nil
	}
	source.closeOnce.Do(func() {
		close(source.stop)
		source.watcher.Close()
		<-source.done
	})
	return nil
}

func (source *hatTrieCompactPeerWatchSource) run() {
	defer close(source.events)
	defer close(source.done)
	if source.gap {
		if !source.send(hatPeer.CompactPeerWatchEvent{Operation: "gap", Epoch: source.epoch, Gap: true}) {
			source.drain()
			return
		}
	}
	for {
		event, ok := <-source.watcher.Events()
		if !ok {
			return
		}
		if !source.send(hatPeer.CompactPeerWatchEvent{
			Key:       event.Key,
			Operation: string(event.Operation),
			Epoch:     event.Epoch,
		}) {
			source.drain()
			return
		}
	}
}

func (source *hatTrieCompactPeerWatchSource) send(event hatPeer.CompactPeerWatchEvent) bool {
	select {
	case source.events <- event:
		return true
	case <-source.stop:
		return false
	}
}

func (source *hatTrieCompactPeerWatchSource) drain() {
	for range source.watcher.Events() {
	}
}
