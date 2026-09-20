package hatCache

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"hatrie_cache/hat/hatJournal"
)

const DefaultPartitionedCommandJournalGroupCommitMaxBatch = 1

var (
	ErrNilPartitionedCommandJournal    = errors.New("hatriecache: nil partitioned command journal")
	ErrPartitionedCommandJournalClosed = errors.New("hatriecache: partitioned command journal is closed")
)

// PartitionedCommandJournalOptions configures one durable command journal per
// local partition. A zero GroupCommitMaxBatch selects synchronous per-command
// durability; callers can explicitly set a larger batch to opt into group
// commit for higher throughput and weaker per-command durability latency.
type PartitionedCommandJournalOptions struct {
	Partitions int
	Journal    CommandJournalOptions
}

// ValidatePartitionedCommandJournalOptions validates and normalizes options.
// Partitions are deliberately required: this journal is an opt-in replacement
// for a single ordered CommandJournal, not a change to the default trie mode.
func ValidatePartitionedCommandJournalOptions(options PartitionedCommandJournalOptions) (PartitionedCommandJournalOptions, error) {
	if options.Partitions == DefaultLocalPartitions {
		return PartitionedCommandJournalOptions{}, errors.New("hatriecache: partitioned command journal requires local partitions")
	}
	if err := ValidateLocalPartitions(options.Partitions); err != nil {
		return PartitionedCommandJournalOptions{}, err
	}
	journalOptions := options.Journal
	if journalOptions.Format == "" {
		journalOptions.Format = DefaultCommandJournalFormat
	}
	if journalOptions.GroupCommitMaxBatch == 0 {
		journalOptions.GroupCommitMaxBatch = DefaultPartitionedCommandJournalGroupCommitMaxBatch
	}
	normalized, err := hatJournal.ValidateOptions(journalOptions)
	if err != nil {
		return PartitionedCommandJournalOptions{}, err
	}
	return PartitionedCommandJournalOptions{
		Partitions: options.Partitions,
		Journal:    normalized,
	}, nil
}

// PartitionedCommandJournal stores commands in independent per-partition
// files. It is intended for explicitly partitioned deployments that prefer
// independent synchronous durability and recovery checkpoints.
//
// The default global CommandJournal remains the compatibility path. This type
// intentionally rejects batches spanning multiple local partitions because a
// set of independent files cannot provide one atomic cross-partition order.
type PartitionedCommandJournal struct {
	mu       sync.RWMutex
	path     string
	journals []*CommandJournal
	closed   bool
	closeErr error
}

// OpenPartitionedCommandJournal opens one command journal below path for each
// configured local partition. The path is a directory owned by this journal.
func OpenPartitionedCommandJournal(path string, options PartitionedCommandJournalOptions) (*PartitionedCommandJournal, error) {
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("hatriecache: partitioned command journal path is required")
	}
	normalized, err := ValidatePartitionedCommandJournalOptions(options)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(path, 0o700); err != nil {
		return nil, err
	}

	journals := make([]*CommandJournal, 0, normalized.Partitions)
	for partition := 0; partition < normalized.Partitions; partition++ {
		journalPath := filepath.Join(path, fmt.Sprintf("partition-%03d", partition), "commands.journal")
		journal, err := OpenCommandJournalWithOptions(journalPath, normalized.Journal)
		if err != nil {
			for _, opened := range journals {
				_ = opened.Close()
			}
			return nil, err
		}
		journals = append(journals, journal)
	}
	return &PartitionedCommandJournal{path: path, journals: journals}, nil
}

// Partitions returns the fixed number of independently durable partitions.
func (journal *PartitionedCommandJournal) Partitions() int {
	if journal == nil {
		return 0
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	return len(journal.journals)
}

// PartitionPath returns the append-journal path for one partition. Backups can
// use this stable path together with the corresponding snapshot watermark.
func (journal *PartitionedCommandJournal) PartitionPath(partition int) (string, error) {
	if journal == nil {
		return "", ErrNilPartitionedCommandJournal
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	if journal.closed {
		return "", ErrPartitionedCommandJournalClosed
	}
	if partition < 0 || partition >= len(journal.journals) {
		return "", fmt.Errorf("hatriecache: partition %d is outside 0 through %d", partition, len(journal.journals)-1)
	}
	return filepath.Join(journal.path, fmt.Sprintf("partition-%03d", partition), "commands.journal"), nil
}

// Sequence returns the local journal sequence for one partition.
func (journal *PartitionedCommandJournal) Sequence(partition int) (uint64, error) {
	child, err := journal.partitionJournal(partition)
	if err != nil {
		return 0, err
	}
	return child.Sequence(), nil
}

// Sequences returns a copy of all local journal sequences in partition order.
func (journal *PartitionedCommandJournal) Sequences() ([]uint64, error) {
	if journal == nil {
		return nil, ErrNilPartitionedCommandJournal
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	if journal.closed {
		return nil, ErrPartitionedCommandJournalClosed
	}
	sequences := make([]uint64, len(journal.journals))
	for partition, child := range journal.journals {
		sequences[partition] = child.Sequence()
	}
	return sequences, nil
}

// ExecuteCommand routes a journaled key command to the journal for its local
// partition. Read-only commands keep the normal trie path. A BATCH is allowed
// only when every child request targets one local partition.
func (journal *PartitionedCommandJournal) ExecuteCommand(trie *HatTrie, request CacheCommandRequest) CacheCommandResponse {
	if journal == nil {
		return commandError(ErrNilPartitionedCommandJournal.Error())
	}
	if trie == nil {
		return commandError(ErrNilHatTrie.Error())
	}
	if !commandShouldJournal(request) {
		return trie.ExecuteCommand(request)
	}
	partition, err := journal.requestPartition(trie, request)
	if err != nil {
		return commandError(err.Error())
	}
	child, err := journal.partitionJournal(partition)
	if err != nil {
		return commandError(err.Error())
	}
	return child.ExecuteCommand(trie, request)
}

// Replay replays each local journal in deterministic partition order. The
// after slice contains one recovery watermark per partition; nil means zero
// for every partition. There is intentionally no global sequence number.
func (journal *PartitionedCommandJournal) Replay(trie *HatTrie, after []uint64) ([]uint64, error) {
	if journal == nil {
		return nil, ErrNilPartitionedCommandJournal
	}
	if trie == nil {
		return nil, ErrNilHatTrie
	}
	if err := journal.validateTriePartitions(trie); err != nil {
		return nil, err
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	if journal.closed {
		return nil, ErrPartitionedCommandJournalClosed
	}
	if len(after) != 0 && len(after) != len(journal.journals) {
		return nil, fmt.Errorf("hatriecache: replay watermark count %d does not match partition count %d", len(after), len(journal.journals))
	}
	sequences := make([]uint64, len(journal.journals))
	for partition, child := range journal.journals {
		var afterSequence uint64
		if len(after) != 0 {
			afterSequence = after[partition]
		}
		sequence, err := child.Replay(trie, afterSequence)
		if err != nil {
			return nil, fmt.Errorf("hatriecache: replay partition %d: %w", partition, err)
		}
		sequences[partition] = sequence
	}
	return sequences, nil
}

// Close closes every partition journal. It is safe to call more than once.
func (journal *PartitionedCommandJournal) Close() error {
	if journal == nil {
		return nil
	}
	journal.mu.Lock()
	if journal.closed {
		err := journal.closeErr
		journal.mu.Unlock()
		return err
	}
	journal.closed = true
	children := append([]*CommandJournal(nil), journal.journals...)
	journal.mu.Unlock()

	var closeErr error
	for _, child := range children {
		closeErr = errors.Join(closeErr, child.Close())
	}

	journal.mu.Lock()
	journal.closeErr = closeErr
	journal.mu.Unlock()
	return closeErr
}

func (journal *PartitionedCommandJournal) partitionJournal(partition int) (*CommandJournal, error) {
	if journal == nil {
		return nil, ErrNilPartitionedCommandJournal
	}
	journal.mu.RLock()
	defer journal.mu.RUnlock()
	if journal.closed {
		return nil, ErrPartitionedCommandJournalClosed
	}
	if partition < 0 || partition >= len(journal.journals) {
		return nil, fmt.Errorf("hatriecache: partition %d is outside 0 through %d", partition, len(journal.journals)-1)
	}
	return journal.journals[partition], nil
}

func (journal *PartitionedCommandJournal) validateTriePartitions(trie *HatTrie) error {
	set := trie.localPartitionSet()
	if set == nil || len(set.tries) != journal.Partitions() {
		return fmt.Errorf("hatriecache: trie partition count does not match partition journal count %d", journal.Partitions())
	}
	return nil
}

func (journal *PartitionedCommandJournal) requestPartition(trie *HatTrie, request CacheCommandRequest) (int, error) {
	if err := journal.validateTriePartitions(trie); err != nil {
		return 0, err
	}
	if len(request.Batch) != 0 || strings.EqualFold(strings.TrimSpace(request.Command), "BATCH") {
		if len(request.Batch) == 0 {
			return 0, errors.New("hatriecache: partitioned command journal requires a non-empty BATCH")
		}
		partition := -1
		for _, child := range request.Batch {
			candidate, err := localPartitionForJournalKey(trie, child.Key)
			if err != nil {
				return 0, err
			}
			if partition < 0 {
				partition = candidate
				continue
			}
			if partition != candidate {
				return 0, errors.New("hatriecache: partitioned command journal requires one local partition per BATCH")
			}
		}
		return partition, nil
	}
	return localPartitionForJournalKey(trie, request.Key)
}

func localPartitionForJournalKey(trie *HatTrie, key string) (int, error) {
	if strings.TrimSpace(key) == "" {
		return 0, errors.New("hatriecache: partitioned command journal requires a key-addressed command")
	}
	partition, enabled, err := trie.LocalPartitionForKey(key)
	if err != nil {
		return 0, err
	}
	if !enabled {
		return 0, errors.New("hatriecache: partitioned command journal requires local partitions")
	}
	return partition, nil
}
