package hatCache

import (
	"errors"
	"strconv"
	"time"
)

var (
	// ErrPriorityQueueVisibilityInvalid reports a non-positive visibility timeout.
	ErrPriorityQueueVisibilityInvalid = errors.New("hatriecache: priority queue visibility timeout must be positive")
	// ErrPriorityQueueLeaseTokenRequired reports an empty lease token.
	ErrPriorityQueueLeaseTokenRequired = errors.New("hatriecache: priority queue lease token is required")
	// ErrPriorityQueueLeaseExhausted reports that no new lease token can be issued.
	ErrPriorityQueueLeaseExhausted = errors.New("hatriecache: priority queue lease token sequence is exhausted")
)

// PriorityQueueLease is one priority-queue item temporarily owned by a
// worker. The item becomes visible again when ExpiresAt passes unless it is
// acknowledged first.
type PriorityQueueLease struct {
	Token     string       `json:"token"`
	Item      PriorityItem `json:"item"`
	ExpiresAt time.Time    `json:"expires_at"`
}

type priorityQueueLease struct {
	item      priorityQueueItem
	expiresAt time.Time
}

func commandPriorityQueueVisibility(request CacheCommandRequest) (time.Duration, bool) {
	if request.TTLSeconds == nil || *request.TTLSeconds <= 0 || *request.TTLSeconds > maxCommandTTLSeconds {
		return 0, false
	}
	return time.Duration(*request.TTLSeconds) * time.Second, true
}

func (pq *priorityQueueData) requeueExpiredPriorityQueueLeases(now time.Time) int {
	if pq == nil || len(pq.leases) == 0 {
		return 0
	}
	requeued := 0
	for token, lease := range pq.leases {
		if now.Before(lease.expiresAt) {
			continue
		}
		delete(pq.leases, token)
		pq.pushItem(lease.item)
		requeued++
	}
	return requeued
}

func (pq *priorityQueueData) claimPriorityQueueItem(visibility time.Duration, now time.Time) (PriorityQueueLease, bool, int, error) {
	if visibility <= 0 {
		return PriorityQueueLease{}, false, 0, ErrPriorityQueueVisibilityInvalid
	}
	requeued := pq.requeueExpiredPriorityQueueLeases(now)
	item, ok := pq.popItemRetain()
	if !ok {
		return PriorityQueueLease{}, false, requeued, nil
	}
	if pq.nextSequence == ^uint64(0) {
		pq.pushItem(item)
		return PriorityQueueLease{}, false, requeued, ErrPriorityQueueLeaseExhausted
	}
	if pq.leases == nil {
		pq.leases = make(map[string]priorityQueueLease)
	}
	pq.nextSequence++
	token := strconv.FormatUint(pq.nextSequence, 36)
	expiresAt := now.Add(visibility)
	pq.leases[token] = priorityQueueLease{item: item, expiresAt: expiresAt}
	return PriorityQueueLease{
		Token:     token,
		Item:      item.PriorityItem(),
		ExpiresAt: expiresAt,
	}, true, requeued, nil
}

func (pq *priorityQueueData) acknowledgePriorityQueueLease(token string, now time.Time) (bool, int, error) {
	if token == "" {
		return false, 0, ErrPriorityQueueLeaseTokenRequired
	}
	requeued := pq.requeueExpiredPriorityQueueLeases(now)
	lease, ok := pq.leases[token]
	if !ok {
		return false, requeued, nil
	}
	delete(pq.leases, token)
	lease.item.clearValue()
	return true, requeued, nil
}

// ClaimPriorityQueue claims the next item for visibility. The item is hidden
// from other queue readers until acknowledged or until visibility expires.
func (ht *HatTrie) ClaimPriorityQueue(key string, visibility time.Duration) (PriorityQueueLease, bool) {
	lease, ok, _ := ht.ClaimPriorityQueueChecked(key, visibility)
	return lease, ok
}

// ClaimPriorityQueueChecked claims the next priority-queue item for a bounded
// visibility interval. The operation is opt-in; ordinary POPPQ remains
// destructive and does not create leases.
func (ht *HatTrie) ClaimPriorityQueueChecked(key string, visibility time.Duration) (PriorityQueueLease, bool, error) {
	if ht == nil {
		return PriorityQueueLease{}, false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.ClaimPriorityQueueChecked(key, visibility)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return PriorityQueueLease{}, false, err
	}
	if !hval.IsPriorityQueue() {
		ht.recordReadLocked(false, key)
		return PriorityQueueLease{}, false, nil
	}
	lease, ok, requeued, err := ht.priorityQueues.array[hval.Index].claimPriorityQueueItem(visibility, ht.currentTime())
	if err != nil {
		ht.recordReadLocked(false, key)
		return PriorityQueueLease{}, false, err
	}
	if ok || requeued > 0 {
		ht.recordWriteLocked(key)
	} else {
		ht.recordReadLocked(false, key)
	}
	return lease, ok, nil
}

// AckPriorityQueue acknowledges a previously claimed item.
func (ht *HatTrie) AckPriorityQueue(key, token string) bool {
	acked, _ := ht.AckPriorityQueueChecked(key, token)
	return acked
}

// AckPriorityQueueChecked removes a live visibility lease. A false result is
// returned for an unknown or already expired token.
func (ht *HatTrie) AckPriorityQueueChecked(key, token string) (bool, error) {
	if ht == nil {
		return false, ErrNilHatTrie
	}
	if partition := ht.localPartitionForKey(key); partition != nil {
		return partition.AckPriorityQueueChecked(key, token)
	}
	ht.mu.Lock()
	defer ht.mu.Unlock()

	hval, err := ht.getLockedChecked(key)
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, err
	}
	if !hval.IsPriorityQueue() {
		ht.recordReadLocked(false, key)
		return false, nil
	}
	acked, requeued, err := ht.priorityQueues.array[hval.Index].acknowledgePriorityQueueLease(token, ht.currentTime())
	if err != nil {
		ht.recordReadLocked(false, key)
		return false, err
	}
	if acked || requeued > 0 {
		ht.recordWriteLocked(key)
	} else {
		ht.recordReadLocked(false, key)
	}
	return acked, nil
}
