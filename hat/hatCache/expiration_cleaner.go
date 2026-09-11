package hatCache

import (
	"context"
	"sync"
	"time"
)

type expirationCleanerState struct {
	wake     chan struct{}
	done     chan struct{}
	stopped  chan struct{}
	stopOnce sync.Once
}

type expirationCleanerSignalSet struct {
	states []*expirationCleanerState
}

func newExpirationCleanerState() *expirationCleanerState {
	return &expirationCleanerState{
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
}

func (state *expirationCleanerState) signal() {
	select {
	case state.wake <- struct{}{}:
	default:
	}
}

func expirationCleanerTries(ht *HatTrie) []*HatTrie {
	tries := []*HatTrie{ht}
	if partitions := ht.localPartitionSet(); partitions != nil {
		tries = append(tries, partitions.tries...)
	}
	return tries
}

func registerExpirationCleaner(tries []*HatTrie, state *expirationCleanerState) {
	for _, trie := range tries {
		if trie == nil {
			continue
		}
		trie.mu.Lock()
		if trie.expirationCleanerSignals == nil {
			trie.expirationCleanerSignals = &expirationCleanerSignalSet{}
		}
		trie.expirationCleanerSignals.states = append(trie.expirationCleanerSignals.states, state)
		trie.mu.Unlock()
	}
}

func unregisterExpirationCleaner(tries []*HatTrie, state *expirationCleanerState) {
	for _, trie := range tries {
		if trie == nil {
			continue
		}
		trie.mu.Lock()
		signals := trie.expirationCleanerSignals
		if signals != nil {
			for index, candidate := range signals.states {
				if candidate != state {
					continue
				}
				last := len(signals.states) - 1
				signals.states[index] = signals.states[last]
				signals.states[last] = nil
				signals.states = signals.states[:last]
				break
			}
			if len(signals.states) == 0 {
				trie.expirationCleanerSignals = nil
			}
		}
		trie.mu.Unlock()
	}
}

func (ht *HatTrie) signalExpirationCleanerLocked() {
	if ht == nil || ht.expirationCleanerSignals == nil {
		return
	}
	for _, state := range ht.expirationCleanerSignals.states {
		state.signal()
	}
}

func (ht *HatTrie) runExpirationCleaner(ctx context.Context, interval time.Duration, state *expirationCleanerState) {
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		wait, open := ht.nextExpirationWait(interval)
		if !open {
			return
		}
		resetExpirationCleanerTimer(timer, wait)

		select {
		case <-timer.C:
			if !ht.vacuumExpiredIfOpen() {
				return
			}
		case <-state.wake:
		case <-ctx.Done():
			return
		case <-state.done:
			return
		}
	}
}

func resetExpirationCleanerTimer(timer *time.Timer, wait time.Duration) {
	if wait < 0 {
		wait = 0
	}
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(wait)
}

func (ht *HatTrie) nextExpirationWait(maxWait time.Duration) (time.Duration, bool) {
	if ht == nil {
		return 0, false
	}
	if partitions := ht.localPartitionSet(); partitions != nil {
		wait := maxWait
		for _, child := range partitions.tries {
			childWait, open := child.nextExpirationWait(maxWait)
			if !open {
				return 0, false
			}
			if childWait < wait {
				wait = childWait
			}
		}
		return wait, true
	}

	ht.mu.RLock()
	if ht.root == nil {
		ht.mu.RUnlock()
		return 0, false
	}
	entry, ok := ht.expirations.Peek()
	now := ht.currentTime()
	ht.mu.RUnlock()
	if !ok {
		return maxWait, true
	}
	wait := entry.at.Sub(now)
	if wait <= 0 {
		return 0, true
	}
	if wait > maxWait {
		return maxWait, true
	}
	return wait, true
}

func (ht *HatTrie) signalExpirationCleanerForDeadlineLocked(at time.Time, before expirationEntry, hadBefore bool) {
	if ht.expirationCleanerSignals == nil {
		return
	}
	if !hadBefore || at.Before(before.at) {
		ht.signalExpirationCleanerLocked()
	}
}

func (ht *HatTrie) expirationCleanerHeadLocked() (expirationEntry, bool) {
	if ht.expirationCleanerSignals == nil {
		return expirationEntry{}, false
	}
	return ht.expirations.Peek()
}
