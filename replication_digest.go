package hatriecache

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
)

const replicationDigestCapabilityTTL = 5 * time.Minute
const maxReplicationDigestCapabilities = 1024

type replicationDigestCapability struct {
	address     string
	fingerprint string
	expiresAt   time.Time
}

const (
	defaultReplicationDigestPageEntries = 16384
	maxReplicationDigestPageEntries     = 16384
	replicationDigestInitialPageEntries = 1024
	maxReplicationDigestResponseBytes   = 512 << 10
	replicationDigestPageMessage        = "replication digest page"
	replicationDigestFinalMessage       = "replication digest final"
	replicationDigestEqualMessage       = "replication digest equal"
	replicationMerkleEqualMessage       = "replication merkle equal"
	replicationMerkleMismatchMessage    = "replication merkle mismatch"
	replicationDigestRootMetadata       = "digest_root"
	replicationDigestAfterKeyMetadata   = "after_key"
	replicationDigestModeMetadata       = "digest_mode"
	replicationDigestMerkleMode         = "merkle"
	replicationDigestBucketsMetadata    = "digest_buckets"
)

type replicationDigest struct {
	hash uint64
	size uint64
}

type replicationDigestTargetInventory struct {
	target     TopologyNode
	prefix     string
	pageSize   int
	root       *xxhash.Digest
	rootSum    uint64
	entryCount uint64
	buckets    replicationMerkleBucketMask
	hasBuckets bool
}

type replicationDigestChange struct {
	key    string
	delete bool
}

type replicationDigestSourceEntry struct {
	key    string
	digest replicationDigest
}

type replicationDigestSourceMode uint8

const (
	replicationDigestSourceKeysOnly replicationDigestSourceMode = 1 << iota
	replicationDigestSourceInvariantScope
)

type replicationDigestSourceIterator struct {
	ctx         context.Context
	trie        *HatTrie
	routing     replicationRoutingSnapshot
	source      string
	inventory   replicationDigestTargetInventory
	cursor      replicationSyncCursor
	afterKey    string
	hasAfterKey bool
	done        bool
	entries     []replicationDigestSourceEntry
	index       int
	scratch     []byte
	mode        replicationDigestSourceMode
}

type replicationDigestChangeWriter struct {
	replicator  *HTTPReplicator
	ctx         context.Context
	trie        *HatTrie
	routing     replicationRoutingSnapshot
	target      TopologyNode
	grpcSession *replicationGRPCSyncSession
	fallback    bool
	limit       int
	changes     []replicationDigestChange
	targets     []ReplicationTargetResult
	changed     int
	deleted     int
}

type replicationDigestPage struct {
	entries      []CacheCommandResponse
	nextAfterKey string
	hasMore      bool
	equal        bool
}

type replicationDigestTargetOutcome struct {
	targets  []ReplicationTargetResult
	changed  int
	deleted  int
	fallback bool
}

func (replicator *HTTPReplicator) syncAllPaged(ctx context.Context, trie *HatTrie, prefix string, pageSize int) ReplicationResult {
	ctx = replicationContext(ctx)
	result := ReplicationResult{Command: "SYNC", Key: prefix}
	if replicator == nil {
		result.Skipped = true
		result.Reason = "replication is not configured"
		return result
	}
	if trie == nil {
		result.Skipped = true
		result.Reason = "trie is not configured"
		return result
	}
	if err := ctx.Err(); err != nil {
		result.Skipped = true
		result.Reason = err.Error()
		return result
	}
	if pageSize <= 0 {
		pageSize = defaultReplicationSyncKeyPageSize
	}

	routing, ok := replicator.snapshotReplicationRouting()
	if !ok {
		result.Skipped = true
		result.Reason = "no sync targets"
		return result
	}
	if target, single := singleReplicationDigestTarget(routing, replicator.self); single && replicator.replicationDigestUnsupported(target, routing.fingerprint) {
		var grpcSession *replicationGRPCSyncSession
		if replicator.transport == ReplicationTransportGRPCStream {
			grpcSession = newReplicationGRPCSyncSession(ctx, replicator)
			defer grpcSession.close()
		}
		inventory := newReplicationDigestTargetInventory(target, prefix, pageSize)
		targets, changed, deleted, fallback := replicator.syncDigestTargetFallback(ctx, trie, routing, *inventory, grpcSession)
		result.Entries = changed
		result.Targets = targets
		fallbacks := 0
		if fallback {
			fallbacks = 1
		}
		if err := ctx.Err(); err != nil {
			if len(result.Targets) == 0 {
				result.Skipped = true
			}
			result.Reason = err.Error()
			return result
		}
		result.Reason = fmt.Sprintf("digest sync compared %d entries, transferred %d, deleted %d, fallbacks %d", result.Entries, changed, deleted, fallbacks)
		return result
	}
	if merkleResult, handled := replicator.syncAllMerkle(ctx, trie, prefix, pageSize, routing); handled {
		return merkleResult
	}
	inventories, entries, err := replicator.replicationDigestInventories(ctx, trie, prefix, pageSize, routing)
	result.Entries = entries
	if err != nil {
		result.Skipped = true
		result.Reason = err.Error()
		return result
	}
	if len(inventories) == 0 {
		result.Skipped = true
		result.Reason = "no sync targets"
		return result
	}

	var grpcSession *replicationGRPCSyncSession
	if replicator.transport == ReplicationTransportGRPCStream {
		grpcSession = newReplicationGRPCSyncSession(ctx, replicator)
		defer grpcSession.close()
	}
	outcomes := make([]replicationDigestTargetOutcome, len(inventories))
	executeTarget := func(index int) {
		outcome := &outcomes[index]
		outcome.targets, outcome.changed, outcome.deleted, outcome.fallback = replicator.syncDigestTarget(ctx, trie, routing, inventories[index], grpcSession)
	}
	maxInFlight := replicator.maxInFlight
	if maxInFlight <= 1 || len(inventories) <= 1 {
		for index := range inventories {
			executeTarget(index)
		}
	} else {
		if maxInFlight > len(inventories) {
			maxInFlight = len(inventories)
		}
		jobs := make(chan int)
		var workers sync.WaitGroup
		workers.Add(maxInFlight)
		for worker := 0; worker < maxInFlight; worker++ {
			go func() {
				defer workers.Done()
				for index := range jobs {
					executeTarget(index)
				}
			}()
		}
		for index := range inventories {
			jobs <- index
		}
		close(jobs)
		workers.Wait()
	}
	changed := 0
	deleted := 0
	fallbacks := 0
	for _, outcome := range outcomes {
		result.Targets = append(result.Targets, outcome.targets...)
		changed += outcome.changed
		deleted += outcome.deleted
		if outcome.fallback {
			fallbacks++
		}
	}
	if err := ctx.Err(); err != nil {
		if len(result.Targets) == 0 {
			result.Skipped = true
		}
		result.Reason = err.Error()
		return result
	}
	result.Reason = fmt.Sprintf("digest sync compared %d entries, transferred %d, deleted %d, fallbacks %d", entries, changed, deleted, fallbacks)
	return result
}

type replicationMerkleTargetPreflight struct {
	target   TopologyNode
	result   ReplicationTargetResult
	snapshot replicationMerkleSnapshot
	equal    bool
}

func (replicator *HTTPReplicator) syncAllMerkle(ctx context.Context, trie *HatTrie, prefix string, pageSize int, routing replicationRoutingSnapshot) (ReplicationResult, bool) {
	result := ReplicationResult{Command: "SYNC", Key: prefix}
	if prefix != "" || len(routing.shards) != 1 {
		return result, false
	}
	shard := routing.shards[0]
	if routing.leaders[shard.ID].Leader != replicator.self {
		return result, false
	}
	targets := routing.targets[shard.ID]
	if len(targets) == 0 {
		return result, false
	}
	sourceSnapshot, err := trie.replicationMerkleSnapshot()
	if err != nil {
		result.Skipped = true
		result.Reason = err.Error()
		return result, true
	}
	result.Entries = int(sourceSnapshot.count)
	preflights := make([]replicationMerkleTargetPreflight, 0, len(targets))
	for _, target := range targets {
		pageResult, response := replicator.executeReplicationDigestTargetPage(ctx, target, replicationMerkleRequest(routing, replicator.self, sourceSnapshot))
		if !pageResult.OK {
			if pageResult.unsupportedTypedReplication {
				return ReplicationResult{}, false
			}
			result.Targets = append(result.Targets, pageResult)
			result.Reason = pageResult.Error
			return result, true
		}
		targetSnapshot, equal, supported, decodeErr := decodeReplicationMerkleResponse(response)
		if !supported {
			return ReplicationResult{}, false
		}
		if decodeErr != nil {
			pageResult.OK = false
			pageResult.Error = decodeErr.Error()
			result.Targets = append(result.Targets, pageResult)
			result.Reason = decodeErr.Error()
			return result, true
		}
		preflights = append(preflights, replicationMerkleTargetPreflight{target: target, result: pageResult, snapshot: targetSnapshot, equal: equal})
	}

	var grpcSession *replicationGRPCSyncSession
	if replicator.transport == ReplicationTransportGRPCStream {
		grpcSession = newReplicationGRPCSyncSession(ctx, replicator)
		defer grpcSession.close()
	}
	changed := 0
	deleted := 0
	for _, preflight := range preflights {
		if preflight.equal {
			result.Targets = append(result.Targets, preflight.result)
			continue
		}
		mask := sourceSnapshot.changedBuckets(preflight.snapshot)
		inventory, inventoryErr := replicator.replicationDigestInventoryForTarget(ctx, trie, pageSize, routing, preflight.target, mask)
		if inventoryErr != nil {
			preflight.result.OK = false
			preflight.result.Error = inventoryErr.Error()
			result.Targets = append(result.Targets, preflight.result)
			result.Reason = inventoryErr.Error()
			return result, true
		}
		targetResults, targetChanged, targetDeleted, _ := replicator.syncDigestTarget(ctx, trie, routing, inventory, grpcSession)
		result.Targets = append(result.Targets, targetResults...)
		changed += targetChanged
		deleted += targetDeleted
	}
	if err := ctx.Err(); err != nil {
		result.Reason = err.Error()
		return result, true
	}
	if changed == 0 && deleted == 0 {
		result.Reason = fmt.Sprintf("merkle equal across %d entries", result.Entries)
	} else {
		result.Reason = fmt.Sprintf("merkle digest compared %d entries, transferred %d, deleted %d", result.Entries, changed, deleted)
	}
	return result, true
}

func replicationMerkleRequest(routing replicationRoutingSnapshot, source string, snapshot replicationMerkleSnapshot) CacheCommandRequest {
	limit := int64(defaultReplicationDigestPageEntries)
	return CacheCommandRequest{
		Command:  replicationDigestCommand,
		Priority: &limit,
		Pairs: Map{
			replicationMetaSourceNode:          source,
			replicationMetaTopologyFingerprint: routing.fingerprint,
			replicationDigestModeMetadata:      replicationDigestMerkleMode,
			replicationDigestRootMetadata: encodeReplicationValueDigest(replicationDigest{
				hash: snapshot.root,
				size: snapshot.count,
			}),
		},
	}
}

func decodeReplicationMerkleResponse(response CacheCommandResponse) (replicationMerkleSnapshot, bool, bool, error) {
	switch response.Message {
	case replicationMerkleEqualMessage:
		return replicationMerkleSnapshot{}, true, true, nil
	case replicationMerkleMismatchMessage:
		snapshot, err := decodeReplicationMerkleLeaves(response.Value)
		return snapshot, false, true, err
	default:
		return replicationMerkleSnapshot{}, false, false, nil
	}
}

func (replicator *HTTPReplicator) replicationDigestInventories(ctx context.Context, trie *HatTrie, prefix string, pageSize int, routing replicationRoutingSnapshot) ([]replicationDigestTargetInventory, int, error) {
	inventories := make(map[TopologyNode]*replicationDigestTargetInventory)
	for _, shard := range routing.shards {
		if routing.leaders[shard.ID].Leader != replicator.self {
			continue
		}
		for _, target := range routing.targets[shard.ID] {
			if _, ok := inventories[target]; !ok {
				inventories[target] = newReplicationDigestTargetInventory(target, prefix, pageSize)
			}
		}
	}

	afterKey := ""
	hasAfterKey := false
	cursor := &replicationSyncCursor{packedKeys: true}
	defer cursor.close(trie)
	entries := 0
	var scratch []byte
	for {
		page, err := replicationSyncEntriesPageWithCursor(trie, prefix, afterKey, hasAfterKey, pageSize, cursor, func(entry Entry) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			route, ok := routing.replicationScanRouteForKey(entry.Key)
			if !ok || route.Leader.Leader != replicator.self {
				return nil
			}
			targets := routing.replicationTargets(route, replicator.self)
			if len(targets) == 0 {
				return nil
			}
			var dumpErr error
			scratch, ok, dumpErr = trie.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(scratch[:0], entry)
			if dumpErr != nil || !ok {
				return dumpErr
			}
			digest := replicationValueDigest(scratch)
			for _, target := range targets {
				inventory := inventories[target]
				if inventory == nil {
					inventory = newReplicationDigestTargetInventory(target, prefix, pageSize)
					inventories[target] = inventory
				}
				appendReplicationDigestRoot(inventory.root, entry.Key, digest)
				inventory.entryCount++
			}
			entries++
			return nil
		})
		if err != nil {
			return nil, entries, err
		}
		if !page.hasMore {
			break
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
	}

	out := make([]replicationDigestTargetInventory, 0, len(inventories))
	for _, inventory := range inventories {
		inventory.rootSum = inventory.root.Sum64()
		inventory.root = nil
		out = append(out, *inventory)
	}
	sort.Slice(out, func(i, j int) bool {
		return replicationTaskTargetKey(out[i].target) < replicationTaskTargetKey(out[j].target)
	})
	return out, entries, nil
}

func (replicator *HTTPReplicator) replicationDigestInventoryForTarget(ctx context.Context, trie *HatTrie, pageSize int, routing replicationRoutingSnapshot, target TopologyNode, buckets replicationMerkleBucketMask) (replicationDigestTargetInventory, error) {
	inventory := newReplicationDigestTargetInventory(target, "", pageSize)
	inventory.buckets = buckets
	inventory.hasBuckets = true
	afterKey := ""
	hasAfterKey := false
	cursor := &replicationSyncCursor{}
	defer cursor.close(trie)
	var scratch []byte
	for {
		page, err := replicationSyncEntriesPageWithCursor(trie, "", afterKey, hasAfterKey, pageSize, cursor, func(entry Entry) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !buckets.containsKey(entry.Key) {
				return nil
			}
			route, ok := routing.replicationScanRouteForKey(entry.Key)
			if !ok || route.Leader.Leader != replicator.self || !replicationRouteTargetsNode(routing, route, replicator.self, target.ID) {
				return nil
			}
			var dumpErr error
			scratch, ok, dumpErr = trie.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(scratch[:0], entry)
			if dumpErr != nil || !ok {
				return dumpErr
			}
			appendReplicationDigestRoot(inventory.root, entry.Key, replicationValueDigest(scratch))
			inventory.entryCount++
			return nil
		})
		if err != nil {
			return replicationDigestTargetInventory{}, err
		}
		if !page.hasMore {
			break
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
	}
	inventory.rootSum = inventory.root.Sum64()
	inventory.root = nil
	return *inventory, nil
}

func (replicator *HTTPReplicator) syncDigestTarget(ctx context.Context, trie *HatTrie, routing replicationRoutingSnapshot, inventory replicationDigestTargetInventory, grpcSession *replicationGRPCSyncSession) ([]ReplicationTargetResult, int, int, bool) {
	afterKey := ""
	hasAfterKey := false
	lastRemoteKey := ""
	hasLastRemoteKey := false
	lastDigestResult := ReplicationTargetResult{Node: inventory.target.ID, Address: inventory.target.Address}
	var source *replicationDigestSourceIterator
	var writer *replicationDigestChangeWriter
	var local replicationDigestSourceEntry
	localOK := false
	defer func() {
		if source != nil {
			source.close()
		}
	}()
	for {
		pageResult, response := replicator.executeReplicationDigestTargetPage(ctx, inventory.target, prefixDigestRequest(routing, replicator.self, inventory.prefix, afterKey, hasAfterKey, inventory))
		lastDigestResult = pageResult
		if !pageResult.OK {
			if pageResult.unsupportedTypedReplication {
				replicator.markReplicationDigestUnsupported(inventory.target, routing.fingerprint)
				return replicator.syncDigestTargetFallback(ctx, trie, routing, inventory, grpcSession)
			}
			if writer != nil {
				return append(writer.targets, pageResult), writer.changed, writer.deleted, false
			}
			return []ReplicationTargetResult{pageResult}, 0, 0, false
		}
		page, supported, err := decodeReplicationDigestResponse(response)
		if !supported {
			replicator.markReplicationDigestUnsupported(inventory.target, routing.fingerprint)
			return replicator.syncDigestTargetFallback(ctx, trie, routing, inventory, grpcSession)
		}
		replicator.markReplicationDigestSupported(inventory.target, routing.fingerprint)
		if err != nil {
			pageResult.OK = false
			pageResult.Error = err.Error()
			return []ReplicationTargetResult{pageResult}, 0, 0, false
		}
		if page.equal {
			if source != nil || hasAfterKey {
				pageResult.OK = false
				pageResult.Error = "hatriecache: unexpected equality response after replication digest paging"
				return append(writer.targets, pageResult), writer.changed, writer.deleted, false
			}
			return []ReplicationTargetResult{lastDigestResult}, 0, 0, false
		}
		if source == nil {
			source = newReplicationDigestSourceIterator(ctx, trie, routing, replicator.self, inventory)
			source.prevalidateScope()
			writer = replicator.newReplicationDigestChangeWriter(ctx, trie, routing, inventory.target, inventory.pageSize, grpcSession, false)
			local, localOK, err = source.next()
			if err != nil {
				pageResult.OK = false
				pageResult.Error = err.Error()
				return []ReplicationTargetResult{pageResult}, 0, 0, false
			}
		}
		for _, remote := range page.entries {
			key := remote.Message
			route, routed := routing.replicationScanRouteForKey(key)
			if !strings.HasPrefix(key, inventory.prefix) || (inventory.hasBuckets && !inventory.buckets.containsKey(key)) || !routed || route.Leader.Leader != replicator.self || !replicationRouteTargetsNode(routing, route, replicator.self, inventory.target.ID) {
				pageResult.OK = false
				pageResult.Error = "hatriecache: key outside replication digest scope"
				return append(writer.targets, pageResult), writer.changed, writer.deleted, false
			}
			if hasLastRemoteKey && key <= lastRemoteKey {
				pageResult.OK = false
				pageResult.Error = "hatriecache: replication digest entries are not strictly ordered"
				return append(writer.targets, pageResult), writer.changed, writer.deleted, false
			}
			lastRemoteKey = key
			hasLastRemoteKey = true
			remoteDigest, err := decodeReplicationValueDigest(remote.Value)
			if err != nil {
				pageResult.OK = false
				pageResult.Error = err.Error()
				return append(writer.targets, pageResult), writer.changed, writer.deleted, false
			}
			for localOK && local.key < key {
				if !writer.add(local.key) {
					return writer.targets, writer.changed, writer.deleted, false
				}
				local, localOK, err = source.next()
				if err != nil {
					pageResult.OK = false
					pageResult.Error = err.Error()
					return append(writer.targets, pageResult), writer.changed, writer.deleted, false
				}
			}
			if !localOK || local.key > key {
				if !writer.addDelete(key) {
					return writer.targets, writer.changed, writer.deleted, false
				}
				continue
			}
			if local.digest != remoteDigest && !writer.add(key) {
				return writer.targets, writer.changed, writer.deleted, false
			}
			local, localOK, err = source.next()
			if err != nil {
				pageResult.OK = false
				pageResult.Error = err.Error()
				return append(writer.targets, pageResult), writer.changed, writer.deleted, false
			}
		}
		if !page.hasMore {
			break
		}
		if hasAfterKey && page.nextAfterKey <= afterKey {
			pageResult.OK = false
			pageResult.Error = "hatriecache: replication digest cursor did not advance"
			return append(writer.targets, pageResult), writer.changed, writer.deleted, false
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
	}
	for localOK {
		if !writer.add(local.key) {
			return writer.targets, writer.changed, writer.deleted, false
		}
		var err error
		local, localOK, err = source.next()
		if err != nil {
			lastDigestResult.OK = false
			lastDigestResult.Error = err.Error()
			return append(writer.targets, lastDigestResult), writer.changed, writer.deleted, false
		}
	}
	if !writer.flush() {
		return writer.targets, writer.changed, writer.deleted, false
	}
	if writer.changed == 0 && writer.deleted == 0 {
		return []ReplicationTargetResult{lastDigestResult}, 0, 0, false
	}
	return writer.targets, writer.changed, writer.deleted, false
}

func singleReplicationDigestTarget(routing replicationRoutingSnapshot, source string) (TopologyNode, bool) {
	var target TopologyNode
	found := false
	for _, shard := range routing.shards {
		if routing.leaders[shard.ID].Leader != source {
			continue
		}
		for _, candidate := range routing.targets[shard.ID] {
			if !found {
				target = candidate
				found = true
				continue
			}
			if candidate != target {
				return TopologyNode{}, false
			}
		}
	}
	return target, found
}

func (replicator *HTTPReplicator) replicationDigestCapabilityNow() time.Time {
	if replicator != nil && replicator.capabilityNow != nil {
		return replicator.capabilityNow()
	}
	return time.Now()
}

func (replicator *HTTPReplicator) replicationDigestUnsupported(target TopologyNode, fingerprint string) bool {
	if replicator == nil {
		return false
	}
	replicator.mu.RLock()
	capability, ok := replicator.digestUnsupported[target.ID]
	replicator.mu.RUnlock()
	if !ok || capability.address != target.Address || capability.fingerprint != fingerprint {
		return false
	}
	return replicator.replicationDigestCapabilityNow().Before(capability.expiresAt)
}

func (replicator *HTTPReplicator) markReplicationDigestUnsupported(target TopologyNode, fingerprint string) {
	if replicator == nil || strings.TrimSpace(target.ID) == "" {
		return
	}
	capability := replicationDigestCapability{
		address:     target.Address,
		fingerprint: fingerprint,
		expiresAt:   replicator.replicationDigestCapabilityNow().Add(replicationDigestCapabilityTTL),
	}
	replicator.mu.Lock()
	_, exists := replicator.digestUnsupported[target.ID]
	if replicator.digestUnsupported == nil || (len(replicator.digestUnsupported) >= maxReplicationDigestCapabilities && !exists) {
		replicator.digestUnsupported = make(map[string]replicationDigestCapability)
	}
	replicator.digestUnsupported[target.ID] = capability
	replicator.mu.Unlock()
}

func (replicator *HTTPReplicator) markReplicationDigestSupported(target TopologyNode, fingerprint string) {
	if replicator == nil {
		return
	}
	replicator.mu.Lock()
	capability, ok := replicator.digestUnsupported[target.ID]
	if ok && capability.address == target.Address && capability.fingerprint == fingerprint {
		delete(replicator.digestUnsupported, target.ID)
	}
	replicator.mu.Unlock()
}

func newReplicationDigestTargetInventory(target TopologyNode, prefix string, pageSize int) *replicationDigestTargetInventory {
	return &replicationDigestTargetInventory{
		target:   target,
		prefix:   prefix,
		pageSize: pageSize,
		root:     xxhash.New(),
	}
}

func newReplicationDigestSourceIterator(ctx context.Context, trie *HatTrie, routing replicationRoutingSnapshot, source string, inventory replicationDigestTargetInventory) *replicationDigestSourceIterator {
	return newReplicationDigestSourceIteratorMode(ctx, trie, routing, source, inventory, false)
}

func newReplicationDigestKeySourceIterator(ctx context.Context, trie *HatTrie, routing replicationRoutingSnapshot, source string, inventory replicationDigestTargetInventory) *replicationDigestSourceIterator {
	return newReplicationDigestSourceIteratorMode(ctx, trie, routing, source, inventory, true)
}

func newReplicationDigestSourceIteratorMode(ctx context.Context, trie *HatTrie, routing replicationRoutingSnapshot, source string, inventory replicationDigestTargetInventory, keysOnly bool) *replicationDigestSourceIterator {
	pageSize := inventory.pageSize
	if pageSize <= 0 {
		pageSize = defaultReplicationSyncKeyPageSize
	}
	inventory.pageSize = pageSize
	capacity := pageSize
	if capacity > replicationDigestInitialPageEntries {
		capacity = replicationDigestInitialPageEntries
	}
	var entries []replicationDigestSourceEntry
	if !keysOnly {
		entries = make([]replicationDigestSourceEntry, 0, capacity)
	}
	mode := replicationDigestSourceMode(0)
	if keysOnly {
		mode = replicationDigestSourceKeysOnly
	}
	return &replicationDigestSourceIterator{
		ctx:       ctx,
		trie:      trie,
		routing:   routing,
		source:    source,
		inventory: inventory,
		entries:   entries,
		mode:      mode,
	}
}

func (iterator *replicationDigestSourceIterator) prevalidateScope() {
	if iterator != nil && replicationDigestSourceScopeIsInvariant(iterator.routing, iterator.source, iterator.inventory) {
		iterator.mode |= replicationDigestSourceInvariantScope
	}
}

func replicationDigestSourceScopeIsInvariant(routing replicationRoutingSnapshot, source string, inventory replicationDigestTargetInventory) bool {
	if inventory.hasBuckets || len(routing.shards) != 1 || source != routing.self {
		return false
	}
	shard := routing.shards[0]
	if routing.leaders[shard.ID].Leader != source {
		return false
	}
	for _, target := range routing.targets[shard.ID] {
		if target.ID == inventory.target.ID {
			return true
		}
	}
	return false
}

func (iterator *replicationDigestSourceIterator) close() {
	if iterator == nil {
		return
	}
	iterator.cursor.close(iterator.trie)
}

func (iterator *replicationDigestSourceIterator) next() (replicationDigestSourceEntry, bool, error) {
	if iterator == nil {
		return replicationDigestSourceEntry{}, false, nil
	}
	for iterator.index >= len(iterator.entries) {
		if iterator.done {
			return replicationDigestSourceEntry{}, false, nil
		}
		iterator.entries = iterator.entries[:0]
		iterator.index = 0
		page, err := replicationSyncEntriesPageWithCursor(iterator.trie, iterator.inventory.prefix, iterator.afterKey, iterator.hasAfterKey, iterator.inventory.pageSize, &iterator.cursor, func(entry Entry) error {
			included, err := iterator.includes(entry)
			if err != nil || !included {
				return err
			}
			var ok bool
			digest := replicationDigest{}
			if iterator.mode&replicationDigestSourceKeysOnly == 0 {
				var dumpErr error
				iterator.scratch, ok, dumpErr = iterator.trie.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(iterator.scratch[:0], entry)
				if dumpErr != nil || !ok {
					return dumpErr
				}
				digest = replicationValueDigest(iterator.scratch)
			}
			iterator.entries = append(iterator.entries, replicationDigestSourceEntry{key: entry.Key, digest: digest})
			return nil
		})
		if err != nil {
			return replicationDigestSourceEntry{}, false, err
		}
		iterator.finishPage(page)
	}
	entry := iterator.entries[iterator.index]
	iterator.index++
	return entry, true, nil
}

func (iterator *replicationDigestSourceIterator) appendFallbackChanges(changes []replicationDigestChange, limit int) ([]replicationDigestChange, bool, error) {
	if iterator == nil || iterator.done {
		return changes, true, nil
	}
	if limit <= 0 {
		limit = 1
	}
	page, err := replicationSyncEntriesPageWithCursor(iterator.trie, iterator.inventory.prefix, iterator.afterKey, iterator.hasAfterKey, limit, &iterator.cursor, func(entry Entry) error {
		included, err := iterator.includes(entry)
		if err != nil || !included {
			return err
		}
		changes = append(changes, replicationDigestChange{key: entry.Key})
		return nil
	})
	if err != nil {
		return changes, false, err
	}
	iterator.finishPage(page)
	return changes, iterator.done, nil
}

func (iterator *replicationDigestSourceIterator) appendFallbackKeys(arena *replicationSyncPayloadArena, limit int) (bool, error) {
	if iterator == nil || iterator.done {
		return true, nil
	}
	if arena == nil {
		return false, errors.New("hatriecache: replication fallback arena is nil")
	}
	if limit <= 0 {
		limit = 1
	}
	iterator.cursor.packedKeys = true
	iterator.cursor.sharedReadOnly = true
	startedScannedPage := false
	page, err := replicationSyncEntriesPageWithCursor(iterator.trie, iterator.inventory.prefix, iterator.afterKey, iterator.hasAfterKey, limit, &iterator.cursor, func(entry Entry) error {
		included, err := iterator.includes(entry)
		if err != nil || !included {
			return err
		}
		if iterator.cursor.carryValues && iterator.cursor.scan != nil {
			if !startedScannedPage {
				arena.beginScannedValues(iterator.cursor.scan.generation)
				startedScannedPage = true
			}
			return arena.appendScannedKey(entry.Key, entry.Value)
		}
		return arena.appendKey(entry.Key)
	})
	if err != nil {
		return false, err
	}
	iterator.finishPage(page)
	return iterator.done, nil
}

func (iterator *replicationDigestSourceIterator) includes(entry Entry) (bool, error) {
	if err := iterator.ctx.Err(); err != nil {
		return false, err
	}
	if iterator.mode&replicationDigestSourceInvariantScope != 0 {
		return true, nil
	}
	if iterator.inventory.hasBuckets && !iterator.inventory.buckets.containsKey(entry.Key) {
		return false, nil
	}
	route, ok := iterator.routing.replicationScanRouteForKey(entry.Key)
	if !ok || route.Leader.Leader != iterator.source || !replicationRouteTargetsNode(iterator.routing, route, iterator.source, iterator.inventory.target.ID) {
		return false, nil
	}
	return true, nil
}

func (iterator *replicationDigestSourceIterator) finishPage(page replicationSyncPage) {
	if page.hasMore {
		iterator.afterKey = page.nextAfterKey
		iterator.hasAfterKey = true
		return
	}
	iterator.done = true
}

func (replicator *HTTPReplicator) newReplicationDigestChangeWriter(ctx context.Context, trie *HatTrie, routing replicationRoutingSnapshot, target TopologyNode, limit int, grpcSession *replicationGRPCSyncSession, fallback bool) *replicationDigestChangeWriter {
	if limit < defaultReplicationSyncKeyPageSize {
		limit = defaultReplicationSyncKeyPageSize
	}
	return &replicationDigestChangeWriter{
		replicator:  replicator,
		ctx:         ctx,
		trie:        trie,
		routing:     routing,
		target:      target,
		grpcSession: grpcSession,
		fallback:    fallback,
		limit:       limit,
		changes:     make([]replicationDigestChange, 0, limit),
	}
}

func (writer *replicationDigestChangeWriter) add(key string) bool {
	return writer.addChange(replicationDigestChange{key: key})
}

func (writer *replicationDigestChangeWriter) addDelete(key string) bool {
	return writer.addChange(replicationDigestChange{key: key, delete: true})
}

func (writer *replicationDigestChangeWriter) addChange(change replicationDigestChange) bool {
	writer.changes = append(writer.changes, change)
	return len(writer.changes) < writer.limit || writer.flush()
}

func (writer *replicationDigestChangeWriter) flush() bool {
	if writer == nil || len(writer.changes) == 0 {
		return true
	}
	targets, changed, deleted, _ := writer.replicator.executeReplicationDigestChanges(writer.ctx, writer.trie, writer.routing, writer.target, writer.changes, writer.grpcSession, writer.fallback)
	writer.targets = append(writer.targets, targets...)
	writer.changed += changed
	writer.deleted += deleted
	writer.changes = writer.changes[:0]
	for _, target := range targets {
		if !target.OK {
			return false
		}
	}
	return true
}

func prefixDigestRequest(routing replicationRoutingSnapshot, source string, prefix string, afterKey string, hasAfterKey bool, inventory replicationDigestTargetInventory) CacheCommandRequest {
	limit := int64(defaultReplicationDigestPageEntries)
	request := CacheCommandRequest{
		Command:  replicationDigestCommand,
		Key:      prefix,
		Subkey:   afterKey,
		Priority: &limit,
		Pairs: Map{
			replicationMetaSourceNode:          source,
			replicationMetaTopologyFingerprint: routing.fingerprint,
		},
	}
	if hasAfterKey {
		request.Pairs[replicationDigestAfterKeyMetadata] = "1"
	} else {
		request.Pairs[replicationDigestRootMetadata] = encodeReplicationValueDigest(replicationDigest{
			hash: inventory.rootSum,
			size: inventory.entryCount,
		})
	}
	if inventory.hasBuckets {
		request.Pairs[replicationDigestBucketsMetadata] = encodeReplicationMerkleBucketMask(inventory.buckets)
	}
	return request
}

func (replicator *HTTPReplicator) syncDigestTargetFallback(ctx context.Context, trie *HatTrie, routing replicationRoutingSnapshot, inventory replicationDigestTargetInventory, grpcSession *replicationGRPCSyncSession) ([]ReplicationTargetResult, int, int, bool) {
	inventory.hasBuckets = false
	inventory.buckets = replicationMerkleBucketMask{}
	if trie.localPartitionSet() == nil && (grpcSession != nil || replicator.wireFormat != CommandWireFormatJSON) {
		return replicator.syncDigestTargetPackedFallback(ctx, trie, routing, inventory, grpcSession)
	}
	source := newReplicationDigestKeySourceIterator(ctx, trie, routing, replicator.self, inventory)
	source.prevalidateScope()
	defer source.close()
	writer := replicator.newReplicationDigestChangeWriter(ctx, trie, routing, inventory.target, inventory.pageSize, grpcSession, true)
	done := false
	for !done {
		remaining := writer.limit - len(writer.changes)
		if remaining <= 0 {
			if !writer.flush() {
				return writer.targets, writer.changed, writer.deleted, true
			}
			remaining = writer.limit
		}
		scanLimit := inventory.pageSize
		if scanLimit <= 0 {
			scanLimit = defaultReplicationSyncKeyPageSize
		}
		if scanLimit > remaining {
			scanLimit = remaining
		}
		var err error
		writer.changes, done, err = source.appendFallbackChanges(writer.changes, scanLimit)
		if err != nil {
			failure := ReplicationTargetResult{Node: inventory.target.ID, Address: inventory.target.Address, Error: err.Error()}
			return append(writer.targets, failure), writer.changed, writer.deleted, true
		}
		if (len(writer.changes) >= writer.limit || done) && !writer.flush() {
			return writer.targets, writer.changed, writer.deleted, true
		}
	}
	if len(writer.targets) == 0 {
		writer.targets = append(writer.targets, ReplicationTargetResult{Node: inventory.target.ID, Address: inventory.target.Address, OK: true})
	}
	return writer.targets, writer.changed, writer.deleted, true
}

func (replicator *HTTPReplicator) syncDigestTargetPackedFallback(ctx context.Context, trie *HatTrie, routing replicationRoutingSnapshot, inventory replicationDigestTargetInventory, grpcSession *replicationGRPCSyncSession) ([]ReplicationTargetResult, int, int, bool) {
	source := newReplicationDigestKeySourceIterator(ctx, trie, routing, replicator.self, inventory)
	source.prevalidateScope()
	defer source.close()
	limit := inventory.pageSize
	if limit < defaultReplicationSyncKeyPageSize {
		limit = defaultReplicationSyncKeyPageSize
	}
	targets := make([]ReplicationTargetResult, 0, 1)
	changed := 0
	deleted := 0
	done := false
	arenaCount := 2
	if grpcSession != nil {
		arenaCount = 1
	}
	arenas := make([]*replicationSyncPayloadArena, arenaCount)
	page := 0
	for !done {
		arenaIndex := page % len(arenas)
		arena := arenas[arenaIndex]
		if arena == nil {
			arena = newReplicationSyncPayloadArena(limit)
			arenas[arenaIndex] = arena
		}
		arena.reset()
		for len(arena.records) < limit && !done {
			remaining := limit - len(arena.records)
			scanLimit := inventory.pageSize
			if scanLimit <= 0 {
				scanLimit = defaultReplicationSyncKeyPageSize
			}
			if scanLimit > remaining {
				scanLimit = remaining
			}
			var err error
			done, err = source.appendFallbackKeys(arena, scanLimit)
			if err != nil {
				failure := ReplicationTargetResult{Node: inventory.target.ID, Address: inventory.target.Address, Error: err.Error()}
				return append(targets, failure), changed, deleted, true
			}
		}
		if len(arena.records) == 0 {
			continue
		}
		group, pageChanged, pageDeleted, _, err := replicator.prepareReplicationDigestPackedTaskGroup(trie, inventory.target, arena, routing.fingerprint)
		changed += pageChanged
		deleted += pageDeleted
		if err != nil {
			failure := ReplicationTargetResult{Node: inventory.target.ID, Address: inventory.target.Address, Error: err.Error()}
			return append(targets, failure), changed, deleted, true
		}
		groups := []replicationTaskGroup{group}
		if replicator.batchMaxBytes > 0 {
			groups = splitReplicationTaskGroupsByMaxBytes(groups, replicator.batchMaxBytes)
		}
		pageResult := ReplicationResult{}
		if grpcSession != nil {
			pageResult = grpcSession.executeReplicationTaskGroups(ctx, pageResult, groups)
		} else {
			pageResult = replicator.executeReplicationTaskGroups(ctx, pageResult, groups)
		}
		targets = append(targets, pageResult.Targets...)
		for _, target := range pageResult.Targets {
			if !target.OK {
				return targets, changed, deleted, true
			}
		}
		page++
	}
	if len(targets) == 0 {
		targets = append(targets, ReplicationTargetResult{Node: inventory.target.ID, Address: inventory.target.Address, OK: true})
	}
	return targets, changed, deleted, true
}

func (replicator *HTTPReplicator) executeReplicationDigestChanges(ctx context.Context, trie *HatTrie, routing replicationRoutingSnapshot, target TopologyNode, changes []replicationDigestChange, grpcSession *replicationGRPCSyncSession, fallback bool) ([]ReplicationTargetResult, int, int, bool) {
	sort.Slice(changes, func(i, j int) bool { return changes[i].key < changes[j].key })
	group, changed, deleted, err := replicator.prepareReplicationDigestTaskGroup(trie, target, changes, grpcSession != nil, routing.fingerprint)
	if err != nil {
		return []ReplicationTargetResult{{Node: target.ID, Address: target.Address, Error: err.Error()}}, changed, deleted, fallback
	}
	if group.replicationSyncPayloadBatch().len() == 0 && len(group.payloads) == 0 {
		return []ReplicationTargetResult{{Node: target.ID, Address: target.Address, OK: true}}, changed, deleted, fallback
	}
	groups := []replicationTaskGroup{group}
	if replicator.batchMaxBytes > 0 {
		groups = splitReplicationTaskGroupsByMaxBytes(groups, replicator.batchMaxBytes)
	}
	pageResult := ReplicationResult{}
	if grpcSession != nil {
		pageResult = grpcSession.executeReplicationTaskGroups(ctx, pageResult, groups)
	} else {
		pageResult = replicator.executeReplicationTaskGroups(ctx, pageResult, groups)
	}
	return pageResult.Targets, changed, deleted, fallback
}

func (replicator *HTTPReplicator) prepareReplicationDigestTaskGroup(trie *HatTrie, target TopologyNode, changes []replicationDigestChange, grpcAvailable bool, topologyFingerprint string) (replicationTaskGroup, int, int, error) {
	compact := grpcAvailable || replicator.wireFormat != CommandWireFormatJSON
	if compact {
		for _, change := range changes {
			if change.delete {
				compact = false
				break
			}
		}
	}
	var payloads []CacheCommandRequest
	var syncPayloadArena *replicationSyncPayloadArena
	if compact {
		syncPayloadArena = newReplicationSyncPayloadDirectArena(len(changes))
	} else {
		payloads = make([]CacheCommandRequest, 0, len(changes))
	}
	changed := 0
	deleted := 0
	for _, change := range changes {
		valueOffset := 0
		var data []byte
		var exists bool
		var err error
		if compact {
			valueOffset = len(syncPayloadArena.values)
			syncPayloadArena.values, exists, err = trie.appendCommandDumpEntryBinaryWithoutStats(syncPayloadArena.values, change.key)
		} else {
			data, exists, err = trie.commandDumpEntryBinaryWithoutStats(change.key)
		}
		if err != nil {
			return replicationTaskGroup{}, changed, deleted, err
		}
		if exists {
			if compact {
				appendErr := syncPayloadArena.appendDirectRecord(change.key, valueOffset, len(syncPayloadArena.values)-valueOffset)
				if appendErr != nil {
					return replicationTaskGroup{}, changed, deleted, appendErr
				}
			} else {
				payloads = append(payloads, CacheCommandRequest{Command: replicationSetCompactCommand, Key: change.key, BinaryValue: data})
			}
			changed++
			continue
		}
		if compact {
			payloads = make([]CacheCommandRequest, 0, len(changes))
			batch := replicationSyncPayloadBatch{arena: syncPayloadArena, count: uint32(len(syncPayloadArena.directRecords))}
			for index := 0; index < batch.len(); index++ {
				payload := batch.payload(index)
				payloads = append(payloads, CacheCommandRequest{Command: replicationSetCompactCommand, Key: payload.key, BinaryValue: payload.binaryValue})
			}
			syncPayloadArena = nil
			compact = false
		}
		payloads = append(payloads, CacheCommandRequest{Command: "INTERNALDEL", Key: change.key})
		deleted++
	}

	group := replicationTaskGroup{
		target:           target,
		deferredMetadata: true,
		metadataSource:   replicator.self,
		metadataTopology: topologyFingerprint,
	}
	if compact {
		group.syncPayloadArena = syncPayloadArena
		group.syncPayloadRecordCount = uint32(len(syncPayloadArena.directRecords))
		return group, changed, deleted, nil
	}
	group.payloads = payloads
	group.keys = make([]string, len(payloads))
	for index := range payloads {
		group.keys[index] = payloads[index].Key
	}
	return group, changed, deleted, nil
}

func (replicator *HTTPReplicator) prepareReplicationDigestPackedTaskGroup(trie *HatTrie, target TopologyNode, arena *replicationSyncPayloadArena, topologyFingerprint string) (replicationTaskGroup, int, int, int, error) {
	if arena == nil || len(arena.keyRecords) != len(arena.records) {
		return replicationTaskGroup{}, 0, 0, 0, errors.New("hatriecache: invalid packed replication fallback arena")
	}
	arena.values = arena.values[:0]
	var payloads []CacheCommandRequest
	compact := true
	changed := 0
	deleted := 0
	visit := func(index int, key string, hval HatValue) error {
		if !hval.Empty() {
			valueOffset := len(arena.values)
			var exists bool
			var appendErr error
			arena.values, exists, appendErr = trie.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(arena.values, Entry{Key: key, Value: hval})
			if appendErr != nil {
				return appendErr
			}
			if exists {
				valueLength := len(arena.values) - valueOffset
				if uint64(valueOffset)+uint64(valueLength) > uint64(math.MaxUint32) {
					return errors.New("hatriecache: replication sync payload arena exceeds 4 GiB")
				}
				record := &arena.records[index]
				record.valueOffset = uint32(valueOffset)
				record.valueLength = uint32(valueLength)
				if !compact {
					payloads = append(payloads, CacheCommandRequest{Command: replicationSetCompactCommand, Key: key, BinaryValue: arena.values[valueOffset:]})
				}
				changed++
				return nil
			}
		}
		if compact {
			payloads = make([]CacheCommandRequest, 0, len(arena.records))
			for previous := 0; previous < index; previous++ {
				payload := arena.payload(uint32(previous))
				payloads = append(payloads, CacheCommandRequest{Command: replicationSetCompactCommand, Key: payload.key, BinaryValue: payload.binaryValue})
			}
			compact = false
		}
		payloads = append(payloads, CacheCommandRequest{Command: "INTERNALDEL", Key: key})
		deleted++
		return nil
	}
	nativeBatches, err := trie.visitReplicationScannedValuesWithoutStats(arena, visit, nil)
	if err != nil {
		return replicationTaskGroup{}, changed, deleted, nativeBatches, err
	}
	group := replicationTaskGroup{
		target:           target,
		deferredMetadata: true,
		metadataSource:   replicator.self,
		metadataTopology: topologyFingerprint,
	}
	if compact {
		group.syncPayloadArena = arena
		group.syncPayloadRecordCount = uint32(len(arena.records))
		return group, changed, deleted, nativeBatches, nil
	}
	group.payloads = payloads
	group.keys = make([]string, len(payloads))
	for index := range payloads {
		group.keys[index] = payloads[index].Key
	}
	return group, changed, deleted, nativeBatches, nil
}

func (trie *HatTrie) visitReplicationScannedValuesWithoutStats(arena *replicationSyncPayloadArena, visit func(index int, key string, hval HatValue) error, afterChunk func(int)) (int, error) {
	if arena == nil {
		return 0, errors.New("hatriecache: replication fallback arena is nil")
	}
	if !arena.scannedValuesValid {
		return trie.visitPackedValuesWithoutStats(arena.keys, arena.keyRecords, visit)
	}
	for start := 0; start < len(arena.records); start += defaultHatTrieScanBatchEntries {
		end := start + defaultHatTrieScanBatchEntries
		if end > len(arena.records) {
			end = len(arena.records)
		}
		trie.mu.RLock()
		canReuse := len(trie.counterWriteStripes) == 0 && len(trie.expires) == 0 && trie.localPartitionSet() == nil && atomic.LoadUint64(&trie.mutationEpoch) == arena.scannedValueEpoch
		if !canReuse {
			trie.mu.RUnlock()
			return trie.visitPackedValuesWithoutStatsFrom(arena.keys, arena.keyRecords, start, visit)
		}
		for index := start; index < end; index++ {
			if err := visit(index, arena.key(uint32(index)), arena.records[index].scannedValue()); err != nil {
				trie.mu.RUnlock()
				return 0, err
			}
		}
		trie.mu.RUnlock()
		if afterChunk != nil {
			afterChunk(end)
		}
	}
	return 0, nil
}

func (replicator *HTTPReplicator) executeReplicationDigestTargetPage(ctx context.Context, target TopologyNode, request CacheCommandRequest) (ReplicationTargetResult, CacheCommandResponse) {
	if result, allowed, state := replicator.beforeReplicationTarget(target); !allowed {
		return result, CacheCommandResponse{}
	} else {
		startedAt := time.Now()
		result, response := replicator.postReplicationCommandResponse(ctx, target, request, func(message string) bool {
			return message == "unsupported command" || message == "key is required"
		})
		if !result.unsupportedTypedReplication {
			replicator.recordReplicationTargetLatency(target, time.Since(startedAt))
		}
		return replicator.afterReplicationTarget(target, state, result), response
	}
}

func executeInternalReplicationDigest(ctx context.Context, trie *HatTrie, request CacheCommandRequest, options commandExecutionOptions) (CacheCommandResponse, bool) {
	if err := ctx.Err(); err != nil {
		return commandError(err.Error()), false
	}
	source := commandPairString(request.Pairs, replicationMetaSourceNode)
	if source == "" {
		return commandError("replication digest source is required"), false
	}
	routing, ok := newReplicationRoutingSnapshot(source, options.Topology, options.Election)
	if !ok {
		return commandError("replication digest topology is required"), false
	}
	if fingerprint := commandPairString(request.Pairs, replicationMetaTopologyFingerprint); fingerprint != "" && fingerprint != routing.fingerprint {
		return commandError("replication topology fingerprint mismatch"), false
	}
	targetNode := strings.TrimSpace(options.NodeName)
	if targetNode == "" {
		targetNode = strings.TrimSpace(routing.topology.Self)
	}
	if targetNode == "" || targetNode == source {
		return commandError("replication digest target node is invalid"), false
	}
	if commandPairString(request.Pairs, replicationDigestModeMetadata) == replicationDigestMerkleMode {
		if request.Key != "" || len(routing.shards) != 1 {
			return commandError("replication Merkle digest requires a whole-dataset single-shard sync"), false
		}
		shard := routing.shards[0]
		if routing.leaders[shard.ID].Leader != source || !replicationRouteTargetsNode(routing, ElectionKeyRoute{Route: TopologyRoute{Shard: shard, Owners: routing.owners[shard.ID]}}, source, targetNode) {
			return commandError("replication Merkle digest source is outside target scope"), false
		}
		expected, err := decodeReplicationValueDigest(commandPairString(request.Pairs, replicationDigestRootMetadata))
		if err != nil {
			return commandError(err.Error()), false
		}
		actual, err := trie.replicationMerkleSnapshot()
		if err != nil {
			return commandError(err.Error()), false
		}
		if actual.root == expected.hash && actual.count == expected.size {
			return CacheCommandResponse{OK: true, Message: replicationMerkleEqualMessage}, false
		}
		return CacheCommandResponse{OK: true, Message: replicationMerkleMismatchMessage, Value: encodeReplicationMerkleLeaves(actual)}, false
	}
	limit, err := replicationDigestRequestLimit(request.Priority)
	if err != nil {
		return commandError(err.Error()), false
	}
	hasAfterKey := commandPairString(request.Pairs, replicationDigestAfterKeyMetadata) == "1"
	buckets := replicationMerkleBucketMask{}
	hasBuckets := false
	if encodedBuckets := commandPairString(request.Pairs, replicationDigestBucketsMetadata); encodedBuckets != "" {
		buckets, err = decodeReplicationMerkleBucketMask(encodedBuckets)
		if err != nil {
			return commandError(err.Error()), false
		}
		hasBuckets = true
	}
	if !hasAfterKey {
		if encodedRoot := commandPairString(request.Pairs, replicationDigestRootMetadata); encodedRoot != "" {
			expected, err := decodeReplicationValueDigest(encodedRoot)
			if err != nil {
				return commandError(err.Error()), false
			}
			actual, err := trie.replicationDigestRoot(request.Key, routing, source, targetNode, buckets, hasBuckets)
			if err != nil {
				return commandError(err.Error()), false
			}
			if actual == expected {
				return CacheCommandResponse{OK: true, Message: replicationDigestEqualMessage}, false
			}
		}
	}
	page, err := trie.replicationDigestPage(request.Key, request.Subkey, hasAfterKey, limit, routing, source, targetNode, buckets, hasBuckets)
	if err != nil {
		return commandError(err.Error()), false
	}
	message := replicationDigestFinalMessage
	if page.hasMore {
		message = replicationDigestPageMessage
	}
	return CacheCommandResponse{OK: true, Message: message, Value: page.nextAfterKey, Responses: page.entries}, false
}

func replicationDigestRequestLimit(priority *int64) (int, error) {
	if priority == nil {
		return defaultReplicationDigestPageEntries, nil
	}
	if *priority <= 0 || *priority > maxReplicationDigestPageEntries {
		return 0, fmt.Errorf("replication digest page size must be between 1 and %d", maxReplicationDigestPageEntries)
	}
	return int(*priority), nil
}

func (trie *HatTrie) replicationDigestPage(prefix string, afterKey string, hasAfterKey bool, limit int, routing replicationRoutingSnapshot, source string, targetNode string, buckets replicationMerkleBucketMask, hasBuckets bool) (replicationDigestPage, error) {
	if trie == nil {
		return replicationDigestPage{}, ErrNilHatTrie
	}
	trie.mu.Lock()
	defer trie.mu.Unlock()
	trie.ensureOpen()
	scan, err := trie.newScanCursorLocked(prefix, true)
	if err != nil {
		return replicationDigestPage{}, err
	}
	defer scan.closeLocked(trie)
	now := time.Time{}
	if len(trie.expires) > 0 {
		now = trie.currentTime()
	}
	capacity := limit
	if capacity > replicationDigestInitialPageEntries {
		capacity = replicationDigestInitialPageEntries
	}
	page := replicationDigestPage{entries: make([]CacheCommandResponse, 0, capacity)}
	estimatedBytes := 64
	var scratch []byte
	for {
		entry, ok := scan.currentLiveEntryLocked(trie, now)
		if !ok {
			return page, nil
		}
		if hasAfterKey && entry.Key <= afterKey {
			scan.consume()
			continue
		}
		if hasBuckets && !buckets.containsKey(entry.Key) {
			page.nextAfterKey = entry.Key
			scan.consume()
			continue
		}
		route, routed := routing.replicationScanRouteForKey(entry.Key)
		if !routed || route.Leader.Leader != source || !replicationRouteTargetsNode(routing, route, source, targetNode) {
			page.nextAfterKey = entry.Key
			scan.consume()
			continue
		}
		if len(page.entries) >= limit {
			page.hasMore = true
			return page, nil
		}
		var dumpErr error
		scratch, ok, dumpErr = trie.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(scratch[:0], entry)
		if dumpErr != nil {
			return replicationDigestPage{}, dumpErr
		}
		if !ok {
			page.nextAfterKey = entry.Key
			scan.consume()
			continue
		}
		encoded := encodeReplicationValueDigest(replicationValueDigest(scratch))
		entryBytes := len(entry.Key) + len(encoded) + 32
		if len(page.entries) > 0 && estimatedBytes+entryBytes > maxReplicationDigestResponseBytes {
			page.hasMore = true
			return page, nil
		}
		page.entries = append(page.entries, CacheCommandResponse{OK: true, Message: entry.Key, Value: encoded})
		estimatedBytes += entryBytes
		page.nextAfterKey = entry.Key
		scan.consume()
	}
}

func replicationRouteTargetsNode(routing replicationRoutingSnapshot, route ElectionKeyRoute, source string, targetNode string) bool {
	for _, target := range routing.replicationTargets(route, source) {
		if target.ID == targetNode {
			return true
		}
	}
	return false
}

func (trie *HatTrie) replicationDigestRoot(prefix string, routing replicationRoutingSnapshot, source string, targetNode string, buckets replicationMerkleBucketMask, hasBuckets bool) (replicationDigest, error) {
	if trie == nil {
		return replicationDigest{}, ErrNilHatTrie
	}
	trie.mu.Lock()
	defer trie.mu.Unlock()
	trie.ensureOpen()
	scan, err := trie.newPackedScanCursorLocked(prefix, true)
	if err != nil {
		return replicationDigest{}, err
	}
	defer scan.closeLocked(trie)
	now := time.Time{}
	if len(trie.expires) > 0 {
		now = trie.currentTime()
	}
	hasher := xxhash.New()
	count := uint64(0)
	var scratch []byte
	for {
		entry, ok := scan.currentLiveEntryLocked(trie, now)
		if !ok {
			return replicationDigest{hash: hasher.Sum64(), size: count}, nil
		}
		if hasBuckets && !buckets.containsKey(entry.Key) {
			scan.consume()
			continue
		}
		route, routed := routing.replicationScanRouteForKey(entry.Key)
		if routed && route.Leader.Leader == source && replicationRouteTargetsNode(routing, route, source, targetNode) {
			var dumpErr error
			scratch, ok, dumpErr = trie.appendCommandDumpScannedEntryBinaryWithoutStatsLocked(scratch[:0], entry)
			if dumpErr != nil {
				return replicationDigest{}, dumpErr
			}
			if ok {
				appendReplicationDigestRoot(hasher, entry.Key, replicationValueDigest(scratch))
				count++
			}
		}
		scan.consume()
	}
}

func appendReplicationDigestRoot(hasher *xxhash.Digest, key string, digest replicationDigest) {
	if hasher == nil {
		return
	}
	var metadata [24]byte
	binary.LittleEndian.PutUint64(metadata[:8], uint64(len(key)))
	binary.LittleEndian.PutUint64(metadata[8:16], digest.hash)
	binary.LittleEndian.PutUint64(metadata[16:], digest.size)
	_, _ = hasher.Write(metadata[:])
	_, _ = hasher.WriteString(key)
}

func replicationValueDigest(data []byte) replicationDigest {
	return replicationDigest{hash: xxhash.Sum64(data), size: uint64(len(data))}
}

func encodeReplicationValueDigest(digest replicationDigest) string {
	var data [16]byte
	binary.LittleEndian.PutUint64(data[:8], digest.hash)
	binary.LittleEndian.PutUint64(data[8:], digest.size)
	return base64.RawStdEncoding.EncodeToString(data[:])
}

func decodeReplicationValueDigest(value string) (replicationDigest, error) {
	var data [16]byte
	if len(value) != base64.RawStdEncoding.EncodedLen(len(data)) {
		return replicationDigest{}, errors.New("hatriecache: invalid replication digest")
	}
	n, err := base64.RawStdEncoding.Decode(data[:], []byte(value))
	if err != nil || n != len(data) {
		return replicationDigest{}, errors.New("hatriecache: invalid replication digest")
	}
	return replicationDigest{hash: binary.LittleEndian.Uint64(data[:8]), size: binary.LittleEndian.Uint64(data[8:])}, nil
}

func decodeReplicationDigestResponse(response CacheCommandResponse) (replicationDigestPage, bool, error) {
	page := replicationDigestPage{nextAfterKey: response.Value}
	switch response.Message {
	case replicationDigestEqualMessage:
		page.equal = true
	case replicationDigestPageMessage:
		page.hasMore = true
	case replicationDigestFinalMessage:
	default:
		return replicationDigestPage{}, false, nil
	}
	page.entries = response.Responses
	for _, entry := range page.entries {
		if !entry.OK || !validKey(entry.Message) {
			return replicationDigestPage{}, true, errors.New("hatriecache: invalid replication digest entry")
		}
	}
	return page, true, nil
}
