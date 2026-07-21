package hatriecache

import (
	"errors"
	"os"
	"time"

	"github.com/cespare/xxhash/v2"
)

func (ht *HatTrie) loadSnapshotStaged(path string) (SnapshotMetadata, error) {
	if ht == nil {
		return SnapshotMetadata{}, ErrNilHatTrie
	}
	ht.snapshotCaptureMu.Lock()
	defer ht.snapshotCaptureMu.Unlock()

	file, err := os.Open(path)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	defer file.Close()

	stage, err := newSnapshotRestoreStage(ht)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	adopted := false
	defer func() {
		if !adopted {
			stage.Destroy()
		}
	}()
	if hook := ht.snapshotRestoreStageHook; hook != nil {
		if err := hook(stage); err != nil {
			return SnapshotMetadata{}, err
		}
	}

	now := ht.currentTime()
	metadata, err := scanSnapshotIntoRestoreStage(file, stage, now)
	if err != nil {
		return SnapshotMetadata{}, err
	}
	if metadata.Version != snapshotVersion {
		return SnapshotMetadata{}, errors.New("hatriecache: unsupported snapshot version")
	}
	if err := ht.adoptSnapshotRestoreStage(stage); err != nil {
		return SnapshotMetadata{}, err
	}
	adopted = true
	stage.Destroy()
	return SnapshotMetadata{JournalSequence: metadata.JournalSequence}, nil
}

func newSnapshotRestoreStage(target *HatTrie) (*HatTrie, error) {
	stage, err := newSnapshotRestoreTrie(target)
	if err != nil {
		return nil, err
	}
	liveSet := target.localPartitionSet()
	if liveSet == nil {
		return stage, nil
	}

	set := &localPartitionSet{tries: make([]*HatTrie, 0, len(liveSet.tries)), mask: liveSet.mask}
	for _, child := range liveSet.tries {
		stagedChild, err := newSnapshotRestoreTrie(child)
		if err != nil {
			for _, created := range set.tries {
				created.Destroy()
			}
			stage.Destroy()
			return nil, err
		}
		set.tries = append(set.tries, stagedChild)
	}
	stage.localPartitions.Store(set)
	return stage, nil
}

func newSnapshotRestoreTrie(target *HatTrie) (*HatTrie, error) {
	target.mu.RLock()
	target.ensureOpen()
	root := target.disks.configuredRoot()
	now := target.now
	keyStatsMode := target.keyStatsMode
	keyStatsCapacity := target.keyStatsCapacity
	stripes := len(target.counterWriteStripes)
	target.mu.RUnlock()

	generationDir, err := os.MkdirTemp(root, ".snapshot-restore-*")
	if err != nil {
		return nil, err
	}
	stage, err := CreateHatTrieWithDiskDir(generationDir, false)
	if err != nil {
		_ = os.RemoveAll(generationDir)
		return nil, err
	}
	stage.disks.rootDir = root
	stage.disks.generationDir = true
	stage.now = now
	if err := stage.ConfigureKeyStats(keyStatsMode, keyStatsCapacity); err != nil {
		stage.Destroy()
		return nil, err
	}
	if stripes != 0 {
		if err := stage.ConfigureCounterWriteStripes(stripes); err != nil {
			stage.Destroy()
			return nil, err
		}
	}
	return stage, nil
}

func scanSnapshotIntoRestoreStage(file *os.File, stage *HatTrie, now time.Time) (snapshotFileMetadata, error) {
	set := stage.localPartitionSet()
	if set == nil {
		stage.mu.Lock()
		defer stage.mu.Unlock()
		return scanSnapshotFileReader(file, func(entry snapshotEntry) error {
			operation, active, err := validateSnapshotLoadEntry(entry, now, true)
			if err != nil || !active {
				return err
			}
			if stage.tryLocation(operation.entry.Key) != nil {
				return errSnapshotDuplicateActiveKey
			}
			_, err = stage.applySnapshotOperationAtLocked(operation, now)
			return err
		})
	}

	pool := newLocalPartitionRestorePool(set, func(partition int, operation snapshotOperation) error {
		child := set.tries[partition]
		if child.tryLocation(operation.entry.Key) != nil {
			return errSnapshotDuplicateActiveKey
		}
		_, err := child.applySnapshotOperationAtLocked(operation, now)
		return err
	})
	metadata, scanErr := scanSnapshotFileReader(file, func(entry snapshotEntry) error {
		operation, active, err := validateSnapshotLoadEntry(entry, now, true)
		if err != nil || !active {
			return err
		}
		partition := int(xxhash.Sum64String(operation.entry.Key) & set.mask)
		return pool.dispatch(partition, detachLocalPartitionRestoreOperation(operation))
	})
	if err := pool.finish(scanErr); err != nil {
		return snapshotFileMetadata{}, err
	}
	return metadata, nil
}

func (ht *HatTrie) adoptSnapshotRestoreStage(stage *HatTrie) error {
	if hook := ht.snapshotRestoreCutoverHook; hook != nil {
		started := time.Now()
		defer func() { hook(time.Since(started)) }()
	}
	liveSet := ht.localPartitionSet()
	stagedSet := stage.localPartitionSet()
	if (liveSet == nil) != (stagedSet == nil) {
		return errors.New("hatriecache: local partition configuration changed during snapshot restore")
	}
	if liveSet == nil {
		ht.mu.Lock()
		defer ht.mu.Unlock()
		if ht.localPartitionSet() != nil {
			return errors.New("hatriecache: local partition configuration changed during snapshot restore")
		}
		if !snapshotRestoreConfigurationMatches(ht, stage) {
			return errors.New("hatriecache: key stats configuration changed during snapshot restore")
		}
		adoptSnapshotDiskOwnership(ht.disks, stage.disks)
		swapSnapshotOwnedDataLocked(ht, stage)
		ht.mutationEpoch++
		ht.memoryCompactionEpoch = 0
		return nil
	}
	if len(liveSet.tries) != len(stagedSet.tries) {
		return errors.New("hatriecache: local partition configuration changed during snapshot restore")
	}

	ht.mu.Lock()
	defer ht.mu.Unlock()
	for _, child := range liveSet.tries {
		child.snapshotCaptureMu.Lock()
	}
	defer func() {
		for idx := len(liveSet.tries) - 1; idx >= 0; idx-- {
			liveSet.tries[idx].snapshotCaptureMu.Unlock()
		}
	}()
	for _, child := range liveSet.tries {
		child.mu.Lock()
	}
	defer func() {
		for idx := len(liveSet.tries) - 1; idx >= 0; idx-- {
			liveSet.tries[idx].mu.Unlock()
		}
	}()
	if ht.localPartitionSet() != liveSet {
		return errors.New("hatriecache: local partition configuration changed during snapshot restore")
	}
	if !snapshotRestoreConfigurationMatches(ht, stage) {
		return errors.New("hatriecache: key stats configuration changed during snapshot restore")
	}
	for idx, child := range liveSet.tries {
		if !snapshotRestoreConfigurationMatches(child, stagedSet.tries[idx]) {
			return errors.New("hatriecache: key stats configuration changed during snapshot restore")
		}
	}
	for idx, child := range liveSet.tries {
		stagedChild := stagedSet.tries[idx]
		adoptSnapshotDiskOwnership(child.disks, stagedChild.disks)
		swapSnapshotOwnedDataLocked(child, stagedChild)
		child.mutationEpoch++
		child.memoryCompactionEpoch = 0
	}
	return nil
}

func snapshotRestoreConfigurationMatches(live *HatTrie, staged *HatTrie) bool {
	return live.keyStatsMode == staged.keyStatsMode && live.keyStatsCapacity == staged.keyStatsCapacity
}

func adoptSnapshotDiskOwnership(live *DiskStorage, staged *DiskStorage) {
	if live == nil || staged == nil {
		return
	}
	staged.ownedDir = live.ownedDir
	live.ownedDir = false
}

func swapSnapshotOwnedDataLocked(live *HatTrie, staged *HatTrie) {
	live.root, staged.root = staged.root, live.root
	live.strings, staged.strings = staged.strings, live.strings
	live.raws, staged.raws = staged.raws, live.raws
	live.disks, staged.disks = staged.disks, live.disks
	live.maps, staged.maps = staged.maps, live.maps
	live.slices, staged.slices = staged.slices, live.slices
	live.sets, staged.sets = staged.sets, live.sets
	live.priorityQueues, staged.priorityQueues = staged.priorityQueues, live.priorityQueues
	live.bloomFilters, staged.bloomFilters = staged.bloomFilters, live.bloomFilters
	live.countMinSketches, staged.countMinSketches = staged.countMinSketches, live.countMinSketches
	live.hyperLogLogs, staged.hyperLogLogs = staged.hyperLogLogs, live.hyperLogLogs
	live.topKs, staged.topKs = staged.topKs, live.topKs
	live.cuckooFilters, staged.cuckooFilters = staged.cuckooFilters, live.cuckooFilters
	live.roaringBitmaps, staged.roaringBitmaps = staged.roaringBitmaps, live.roaringBitmaps
	live.quantileSketches, staged.quantileSketches = staged.quantileSketches, live.quantileSketches
	live.fenwickTrees, staged.fenwickTrees = staged.fenwickTrees, live.fenwickTrees
	live.sparseBitsets, staged.sparseBitsets = staged.sparseBitsets, live.sparseBitsets
	live.reservoirSamples, staged.reservoirSamples = staged.reservoirSamples, live.reservoirSamples
	live.xorFilters, staged.xorFilters = staged.xorFilters, live.xorFilters
	live.radixTrees, staged.radixTrees = staged.radixTrees, live.radixTrees
	live.dbrefs, staged.dbrefs = staged.dbrefs, live.dbrefs
	live.expires, staged.expires = staged.expires, live.expires
	live.expirations, staged.expirations = staged.expirations, live.expirations
	live.hotKey, staged.hotKey = staged.hotKey, live.hotKey
	live.hotValue, staged.hotValue = staged.hotValue, live.hotValue
	live.hotValid, staged.hotValid = staged.hotValid, live.hotValid
	live.keyStats, staged.keyStats = staged.keyStats, live.keyStats
	live.keyStatsSlots, staged.keyStatsSlots = staged.keyStatsSlots, live.keyStatsSlots
	live.keyStatsFree, staged.keyStatsFree = staged.keyStatsFree, live.keyStatsFree
	live.keyStatsHand, staged.keyStatsHand = staged.keyStatsHand, live.keyStatsHand
	live.levelDBSpillKeys, staged.levelDBSpillKeys = staged.levelDBSpillKeys, live.levelDBSpillKeys
	live.levelDBHotBytes, staged.levelDBHotBytes = staged.levelDBHotBytes, live.levelDBHotBytes
	live.levelDBHotValues, staged.levelDBHotValues = staged.levelDBHotValues, live.levelDBHotValues
	live.replicationMerkle, staged.replicationMerkle = staged.replicationMerkle, live.replicationMerkle
}
