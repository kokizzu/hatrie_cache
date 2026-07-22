package hatriecache

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"
)

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
	keysOnly    bool
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
			route, ok := routing.routeForKey(entry.Key)
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
			route, ok := routing.routeForKey(entry.Key)
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
				return replicator.syncDigestTargetFallback(ctx, trie, routing, inventory, grpcSession)
			}
			if writer != nil {
				return append(writer.targets, pageResult), writer.changed, writer.deleted, false
			}
			return []ReplicationTargetResult{pageResult}, 0, 0, false
		}
		page, supported, err := decodeReplicationDigestResponse(response)
		if !supported {
			return replicator.syncDigestTargetFallback(ctx, trie, routing, inventory, grpcSession)
		}
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
			route, routed := routing.routeForKey(key)
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
	return &replicationDigestSourceIterator{
		ctx:       ctx,
		trie:      trie,
		routing:   routing,
		source:    source,
		inventory: inventory,
		entries:   entries,
		keysOnly:  keysOnly,
	}
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
			if !iterator.keysOnly {
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

func (iterator *replicationDigestSourceIterator) includes(entry Entry) (bool, error) {
	if err := iterator.ctx.Err(); err != nil {
		return false, err
	}
	if iterator.inventory.hasBuckets && !iterator.inventory.buckets.containsKey(entry.Key) {
		return false, nil
	}
	route, ok := iterator.routing.routeForKey(entry.Key)
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
	source := newReplicationDigestKeySourceIterator(ctx, trie, routing, replicator.self, inventory)
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
		route, routed := routing.routeForKey(entry.Key)
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
		route, routed := routing.routeForKey(entry.Key)
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
