package hatDataStructure

// indexStatsBinding is deliberately caller-configured: generic index keys do
// not have one allocation-free hash implementation that is correct for every
// type. The binding is copied while the owning index lock is held, then the
// collector is updated after the lock is released by read paths.
type indexStatsBinding[K any] struct {
	collector *IndexStats
	hash      func(K) uint64
}

func validateIndexStatsBinding[K any](collector *IndexStats, hash func(K) uint64) error {
	if collector == nil {
		return ErrIndexStatsCollectorRequired
	}
	if hash == nil {
		return ErrIndexStatsHasherRequired
	}
	return nil
}

func (binding indexStatsBinding[K]) observeKey(key K) {
	if binding.collector == nil {
		return
	}
	binding.collector.ObserveKeyHash(binding.hash(key))
}

func (binding indexStatsBinding[K]) observeLookup(key K, postingLength int) {
	if binding.collector == nil {
		return
	}
	if postingLength < 0 {
		postingLength = 0
	}
	binding.collector.ObserveLookup(binding.hash(key), uint64(postingLength))
}

func (binding indexStatsBinding[K]) snapshot() IndexStatsSnapshot {
	if binding.collector == nil {
		return IndexStatsSnapshot{}
	}
	return binding.collector.Snapshot()
}

// AttachStats enables bounded cardinality, posting-length, and hot-key
// diagnostics for an index. The caller owns the key hash function; it should
// be stable and well distributed, and must not retain the key. Existing keys
// are observed once during attachment. The collector is disabled by default.
func (index *HashIndex[T, K]) AttachStats(collector *IndexStats, hash func(K) uint64) error {
	if index == nil {
		return ErrHashIndexNil
	}
	if err := validateIndexStatsBinding(collector, hash); err != nil {
		return err
	}
	index.mu.Lock()
	index.stats = indexStatsBinding[K]{collector: collector, hash: hash}
	if index.unique {
		for key := range index.uniqueByKey {
			index.stats.observeKey(key)
		}
	} else {
		for key := range index.postings {
			index.stats.observeKey(key)
		}
	}
	index.mu.Unlock()
	return nil
}

// DetachStats disables diagnostics for the index. Existing collector data is
// retained by the caller and is not reset.
func (index *HashIndex[T, K]) DetachStats() {
	if index == nil {
		return
	}
	index.mu.Lock()
	index.stats = indexStatsBinding[K]{}
	index.mu.Unlock()
}

// Stats returns an owned point-in-time diagnostic report, or an empty report
// when diagnostics are detached.
func (index *HashIndex[T, K]) Stats() IndexStatsSnapshot {
	if index == nil {
		return IndexStatsSnapshot{}
	}
	index.mu.RLock()
	binding := index.stats
	index.mu.RUnlock()
	return binding.snapshot()
}

// AttachStats enables bounded diagnostics for a functional index. Existing
// keys are observed once during attachment.
func (index *FunctionalIndex[T, K]) AttachStats(collector *IndexStats, hash func(K) uint64) error {
	if index == nil {
		return ErrFunctionalIndexNil
	}
	if err := validateIndexStatsBinding(collector, hash); err != nil {
		return err
	}
	index.mu.Lock()
	index.stats = indexStatsBinding[K]{collector: collector, hash: hash}
	for key := range index.postings {
		index.stats.observeKey(key)
	}
	index.mu.Unlock()
	return nil
}

// DetachStats disables diagnostics for a functional index.
func (index *FunctionalIndex[T, K]) DetachStats() {
	if index == nil {
		return
	}
	index.mu.Lock()
	index.stats = indexStatsBinding[K]{}
	index.mu.Unlock()
}

// Stats returns an owned point-in-time diagnostic report for a functional
// index, or an empty report when diagnostics are detached.
func (index *FunctionalIndex[T, K]) Stats() IndexStatsSnapshot {
	if index == nil {
		return IndexStatsSnapshot{}
	}
	index.mu.RLock()
	binding := index.stats
	index.mu.RUnlock()
	return binding.snapshot()
}
