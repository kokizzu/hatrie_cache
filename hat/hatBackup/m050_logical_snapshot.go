package hatBackup

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	// LogicalSnapshotManifestVersion is the current logical snapshot contract.
	LogicalSnapshotManifestVersion uint64 = 1
	// MaxLogicalSnapshotRecords bounds all checkpoint records in one manifest.
	MaxLogicalSnapshotRecords = 4096
	// MaxLogicalSnapshotManifestBytes bounds the encoded manifest.
	MaxLogicalSnapshotManifestBytes = 64 << 20

	logicalSnapshotManifestHeaderSize = 5
	maxLogicalSnapshotStringBytes     = 1 << 20
	logicalSnapshotCodecVersion       = 1
)

var (
	logicalSnapshotManifestMagic = [4]byte{'H', 'L', 'M', '1'}

	// ErrLogicalSnapshotInvalid indicates malformed or inconsistent snapshot
	// metadata.
	ErrLogicalSnapshotInvalid = errors.New("hatriecache: logical snapshot manifest is invalid")
	// ErrLogicalSnapshotHistoryGap indicates that the journal cannot replay the
	// sequence immediately after the captured snapshot boundary.
	ErrLogicalSnapshotHistoryGap = errors.New("hatriecache: logical snapshot journal history has a gap")
	// ErrLogicalSnapshotCoverageInvalid indicates an invalid replay coverage
	// declaration.
	ErrLogicalSnapshotCoverageInvalid = errors.New("hatriecache: logical snapshot journal coverage is invalid")
)

// SourceOffsetCheckpoint records one source position captured at the exact
// logical snapshot boundary. Partition is optional for non-partitioned sources.
type SourceOffsetCheckpoint struct {
	SourceID  string
	Partition string
	Offset    uint64
	Epoch     uint64
}

// FrontierCheckpoint records one maintained arrangement/dataflow frontier.
type FrontierCheckpoint struct {
	ID         string
	Lower      uint64
	Upper      uint64
	Generation uint64
}

// SubscriptionCheckpoint records one subscriber's replay position and as-of
// frontier. FrontierID must refer to a FrontierCheckpoint in the manifest.
type SubscriptionCheckpoint struct {
	ID            string
	FrontierID    string
	AsOf          uint64
	AckedSequence uint64
}

// LogicalSnapshotManifest joins immutable storage, source positions, dataflow
// frontiers, and subscription checkpoints into one recoverable boundary.
type LogicalSnapshotManifest struct {
	Version           uint64
	SnapshotID        string
	CreatedAt         time.Time
	BundleBackupID    string
	StorageGeneration uint64
	JournalSequence   uint64
	SourceOffsets     []SourceOffsetCheckpoint
	Frontiers         []FrontierCheckpoint
	Subscriptions     []SubscriptionCheckpoint
}

// LogicalSnapshotJournalCoverage describes the journal range available for
// replay after restoring a manifest.
type LogicalSnapshotJournalCoverage struct {
	HasEntries    bool
	FirstSequence uint64
	LastSequence  uint64
}

// LogicalSnapshotRestoreStep is one ordered, caller-executed restore phase.
type LogicalSnapshotRestoreStep string

const (
	RestoreImmutableStorage LogicalSnapshotRestoreStep = "restore-immutable-storage"
	RestoreSourceOffsets    LogicalSnapshotRestoreStep = "restore-source-offsets"
	RestoreFrontiers        LogicalSnapshotRestoreStep = "restore-frontiers"
	RestoreSubscriptions    LogicalSnapshotRestoreStep = "restore-subscriptions"
	ReplayJournal           LogicalSnapshotRestoreStep = "replay-journal"
)

// LogicalSnapshotRestorePlan is a validated restore order. The package does
// not perform filesystem, source, or journal side effects.
type LogicalSnapshotRestorePlan struct {
	Manifest      LogicalSnapshotManifest
	Steps         []LogicalSnapshotRestoreStep
	ReplayFrom    uint64
	ReplayThrough uint64
}

// Normalize validates and returns a deterministic, independent manifest copy.
func (manifest LogicalSnapshotManifest) Normalize() (LogicalSnapshotManifest, error) {
	if manifest.Version != LogicalSnapshotManifestVersion {
		return LogicalSnapshotManifest{}, invalidLogicalSnapshot("unsupported version %d", manifest.Version)
	}
	snapshotID, err := normalizeLogicalSnapshotID("snapshot ID", manifest.SnapshotID, false)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	bundleID, err := normalizeLogicalSnapshotID("bundle backup ID", manifest.BundleBackupID, false)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	if manifest.CreatedAt.IsZero() {
		return LogicalSnapshotManifest{}, invalidLogicalSnapshot("created time is required")
	}
	if manifest.StorageGeneration == 0 {
		return LogicalSnapshotManifest{}, invalidLogicalSnapshot("storage generation is required")
	}
	if len(manifest.SourceOffsets)+len(manifest.Frontiers)+len(manifest.Subscriptions) > MaxLogicalSnapshotRecords {
		return LogicalSnapshotManifest{}, invalidLogicalSnapshot("record limit exceeded")
	}

	normalized := LogicalSnapshotManifest{
		Version:           manifest.Version,
		SnapshotID:        snapshotID,
		CreatedAt:         manifest.CreatedAt.UTC(),
		BundleBackupID:    bundleID,
		StorageGeneration: manifest.StorageGeneration,
		JournalSequence:   manifest.JournalSequence,
		SourceOffsets:     make([]SourceOffsetCheckpoint, 0, len(manifest.SourceOffsets)),
		Frontiers:         make([]FrontierCheckpoint, 0, len(manifest.Frontiers)),
		Subscriptions:     make([]SubscriptionCheckpoint, 0, len(manifest.Subscriptions)),
	}

	sourceKeys := make(map[[2]string]struct{}, len(manifest.SourceOffsets))
	for _, input := range manifest.SourceOffsets {
		sourceID, err := normalizeLogicalSnapshotID("source ID", input.SourceID, false)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		partition, err := normalizeLogicalSnapshotID("source partition", input.Partition, true)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		key := [2]string{sourceID, partition}
		if _, exists := sourceKeys[key]; exists {
			return LogicalSnapshotManifest{}, invalidLogicalSnapshot("duplicate source offset %q/%q", sourceID, partition)
		}
		sourceKeys[key] = struct{}{}
		normalized.SourceOffsets = append(normalized.SourceOffsets, SourceOffsetCheckpoint{
			SourceID: sourceID, Partition: partition, Offset: input.Offset, Epoch: input.Epoch,
		})
	}
	sort.Slice(normalized.SourceOffsets, func(i, j int) bool {
		left, right := normalized.SourceOffsets[i], normalized.SourceOffsets[j]
		if left.SourceID != right.SourceID {
			return left.SourceID < right.SourceID
		}
		return left.Partition < right.Partition
	})

	frontierIDs := make(map[string]struct{}, len(manifest.Frontiers))
	for _, input := range manifest.Frontiers {
		id, err := normalizeLogicalSnapshotID("frontier ID", input.ID, false)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		if input.Lower > input.Upper {
			return LogicalSnapshotManifest{}, invalidLogicalSnapshot("frontier %q lower exceeds upper", id)
		}
		if _, exists := frontierIDs[id]; exists {
			return LogicalSnapshotManifest{}, invalidLogicalSnapshot("duplicate frontier %q", id)
		}
		frontierIDs[id] = struct{}{}
		normalized.Frontiers = append(normalized.Frontiers, FrontierCheckpoint{
			ID: id, Lower: input.Lower, Upper: input.Upper, Generation: input.Generation,
		})
	}
	sort.Slice(normalized.Frontiers, func(i, j int) bool {
		return normalized.Frontiers[i].ID < normalized.Frontiers[j].ID
	})

	subscriptionIDs := make(map[string]struct{}, len(manifest.Subscriptions))
	for _, input := range manifest.Subscriptions {
		id, err := normalizeLogicalSnapshotID("subscription ID", input.ID, false)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		frontierID, err := normalizeLogicalSnapshotID("subscription frontier ID", input.FrontierID, false)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		if _, exists := frontierIDs[frontierID]; !exists {
			return LogicalSnapshotManifest{}, invalidLogicalSnapshot("subscription %q refers to missing frontier %q", id, frontierID)
		}
		if input.AsOf > manifest.JournalSequence {
			return LogicalSnapshotManifest{}, invalidLogicalSnapshot("subscription %q as-of sequence exceeds snapshot boundary", id)
		}
		if input.AckedSequence > manifest.JournalSequence {
			return LogicalSnapshotManifest{}, invalidLogicalSnapshot("subscription %q acknowledged sequence exceeds snapshot boundary", id)
		}
		if _, exists := subscriptionIDs[id]; exists {
			return LogicalSnapshotManifest{}, invalidLogicalSnapshot("duplicate subscription %q", id)
		}
		subscriptionIDs[id] = struct{}{}
		normalized.Subscriptions = append(normalized.Subscriptions, SubscriptionCheckpoint{
			ID: id, FrontierID: frontierID, AsOf: input.AsOf, AckedSequence: input.AckedSequence,
		})
	}
	sort.Slice(normalized.Subscriptions, func(i, j int) bool {
		return normalized.Subscriptions[i].ID < normalized.Subscriptions[j].ID
	})
	return normalized, nil
}

// MarshalBinary encodes a deterministic compact manifest. The format is
// versioned independently from the logical manifest contract.
func (manifest LogicalSnapshotManifest) MarshalBinary() ([]byte, error) {
	normalized, err := manifest.Normalize()
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, logicalSnapshotManifestHeaderSize+256)
	encoded = append(encoded, logicalSnapshotManifestMagic[:]...)
	encoded = append(encoded, logicalSnapshotCodecVersion)
	encoded = appendLogicalSnapshotUvarint(encoded, normalized.Version)
	encoded = appendLogicalSnapshotVarint(encoded, normalized.CreatedAt.UnixNano())
	encoded = appendLogicalSnapshotString(encoded, normalized.SnapshotID)
	encoded = appendLogicalSnapshotString(encoded, normalized.BundleBackupID)
	encoded = appendLogicalSnapshotUvarint(encoded, normalized.StorageGeneration)
	encoded = appendLogicalSnapshotUvarint(encoded, normalized.JournalSequence)
	encoded = appendLogicalSnapshotUvarint(encoded, uint64(len(normalized.SourceOffsets)))
	for _, checkpoint := range normalized.SourceOffsets {
		encoded = appendLogicalSnapshotString(encoded, checkpoint.SourceID)
		encoded = appendLogicalSnapshotString(encoded, checkpoint.Partition)
		encoded = appendLogicalSnapshotUvarint(encoded, checkpoint.Offset)
		encoded = appendLogicalSnapshotUvarint(encoded, checkpoint.Epoch)
	}
	encoded = appendLogicalSnapshotUvarint(encoded, uint64(len(normalized.Frontiers)))
	for _, checkpoint := range normalized.Frontiers {
		encoded = appendLogicalSnapshotString(encoded, checkpoint.ID)
		encoded = appendLogicalSnapshotUvarint(encoded, checkpoint.Lower)
		encoded = appendLogicalSnapshotUvarint(encoded, checkpoint.Upper)
		encoded = appendLogicalSnapshotUvarint(encoded, checkpoint.Generation)
	}
	encoded = appendLogicalSnapshotUvarint(encoded, uint64(len(normalized.Subscriptions)))
	for _, checkpoint := range normalized.Subscriptions {
		encoded = appendLogicalSnapshotString(encoded, checkpoint.ID)
		encoded = appendLogicalSnapshotString(encoded, checkpoint.FrontierID)
		encoded = appendLogicalSnapshotUvarint(encoded, checkpoint.AsOf)
		encoded = appendLogicalSnapshotUvarint(encoded, checkpoint.AckedSequence)
	}
	if len(encoded) > MaxLogicalSnapshotManifestBytes {
		return nil, invalidLogicalSnapshot("encoded size exceeds %d bytes", MaxLogicalSnapshotManifestBytes)
	}
	return encoded, nil
}

// DecodeLogicalSnapshotManifest decodes and validates a binary manifest.
func DecodeLogicalSnapshotManifest(payload []byte) (LogicalSnapshotManifest, error) {
	if len(payload) < logicalSnapshotManifestHeaderSize || len(payload) > MaxLogicalSnapshotManifestBytes || !bytes.Equal(payload[:4], logicalSnapshotManifestMagic[:]) || payload[4] != logicalSnapshotCodecVersion {
		return LogicalSnapshotManifest{}, ErrLogicalSnapshotInvalid
	}
	offset := logicalSnapshotManifestHeaderSize
	version, err := readLogicalSnapshotUvarint(payload, &offset)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	createdUnixNano, err := readLogicalSnapshotVarint(payload, &offset)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	snapshotID, err := readLogicalSnapshotString(payload, &offset)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	bundleID, err := readLogicalSnapshotString(payload, &offset)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	storageGeneration, err := readLogicalSnapshotUvarint(payload, &offset)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	journalSequence, err := readLogicalSnapshotUvarint(payload, &offset)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	sourceCount, err := readLogicalSnapshotUvarint(payload, &offset)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	if sourceCount > MaxLogicalSnapshotRecords {
		return LogicalSnapshotManifest{}, ErrLogicalSnapshotInvalid
	}
	sources := make([]SourceOffsetCheckpoint, 0, int(sourceCount))
	for index := uint64(0); index < sourceCount; index++ {
		sourceID, err := readLogicalSnapshotString(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		partition, err := readLogicalSnapshotString(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		offsetValue, err := readLogicalSnapshotUvarint(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		epoch, err := readLogicalSnapshotUvarint(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		sources = append(sources, SourceOffsetCheckpoint{SourceID: sourceID, Partition: partition, Offset: offsetValue, Epoch: epoch})
	}
	frontierCount, err := readLogicalSnapshotUvarint(payload, &offset)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	if frontierCount > MaxLogicalSnapshotRecords {
		return LogicalSnapshotManifest{}, ErrLogicalSnapshotInvalid
	}
	frontiers := make([]FrontierCheckpoint, 0, int(frontierCount))
	for index := uint64(0); index < frontierCount; index++ {
		id, err := readLogicalSnapshotString(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		lower, err := readLogicalSnapshotUvarint(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		upper, err := readLogicalSnapshotUvarint(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		generation, err := readLogicalSnapshotUvarint(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		frontiers = append(frontiers, FrontierCheckpoint{ID: id, Lower: lower, Upper: upper, Generation: generation})
	}
	subscriptionCount, err := readLogicalSnapshotUvarint(payload, &offset)
	if err != nil {
		return LogicalSnapshotManifest{}, err
	}
	if subscriptionCount > MaxLogicalSnapshotRecords {
		return LogicalSnapshotManifest{}, ErrLogicalSnapshotInvalid
	}
	subscriptions := make([]SubscriptionCheckpoint, 0, int(subscriptionCount))
	for index := uint64(0); index < subscriptionCount; index++ {
		id, err := readLogicalSnapshotString(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		frontierID, err := readLogicalSnapshotString(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		asOf, err := readLogicalSnapshotUvarint(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		acked, err := readLogicalSnapshotUvarint(payload, &offset)
		if err != nil {
			return LogicalSnapshotManifest{}, err
		}
		subscriptions = append(subscriptions, SubscriptionCheckpoint{ID: id, FrontierID: frontierID, AsOf: asOf, AckedSequence: acked})
	}
	if offset != len(payload) {
		return LogicalSnapshotManifest{}, ErrLogicalSnapshotInvalid
	}
	return (LogicalSnapshotManifest{
		Version:           version,
		SnapshotID:        snapshotID,
		CreatedAt:         time.Unix(0, createdUnixNano).UTC(),
		BundleBackupID:    bundleID,
		StorageGeneration: storageGeneration,
		JournalSequence:   journalSequence,
		SourceOffsets:     sources,
		Frontiers:         frontiers,
		Subscriptions:     subscriptions,
	}).Normalize()
}

// UnmarshalBinary decodes a manifest without mutating the receiver on error.
func (manifest *LogicalSnapshotManifest) UnmarshalBinary(payload []byte) error {
	if manifest == nil {
		return ErrLogicalSnapshotInvalid
	}
	decoded, err := DecodeLogicalSnapshotManifest(payload)
	if err != nil {
		return err
	}
	*manifest = decoded
	return nil
}

// PlanLogicalSnapshotRestore validates journal coverage and returns the only
// safe phase order. It performs no restore side effects.
func PlanLogicalSnapshotRestore(manifest LogicalSnapshotManifest, coverage LogicalSnapshotJournalCoverage) (LogicalSnapshotRestorePlan, error) {
	normalized, err := manifest.Normalize()
	if err != nil {
		return LogicalSnapshotRestorePlan{}, err
	}
	plan := LogicalSnapshotRestorePlan{
		Manifest: normalized,
		Steps: []LogicalSnapshotRestoreStep{
			RestoreImmutableStorage,
			RestoreSourceOffsets,
			RestoreFrontiers,
			RestoreSubscriptions,
		},
	}
	if !coverage.HasEntries {
		if coverage.FirstSequence != 0 || coverage.LastSequence != 0 {
			return LogicalSnapshotRestorePlan{}, ErrLogicalSnapshotCoverageInvalid
		}
		return plan, nil
	}
	if coverage.FirstSequence > coverage.LastSequence || coverage.LastSequence <= normalized.JournalSequence {
		return LogicalSnapshotRestorePlan{}, ErrLogicalSnapshotCoverageInvalid
	}
	if normalized.JournalSequence == math.MaxUint64 {
		return LogicalSnapshotRestorePlan{}, ErrLogicalSnapshotCoverageInvalid
	}
	plan.ReplayFrom = normalized.JournalSequence + 1
	if coverage.FirstSequence > plan.ReplayFrom {
		return LogicalSnapshotRestorePlan{}, fmt.Errorf("%w: first available sequence %d, required %d", ErrLogicalSnapshotHistoryGap, coverage.FirstSequence, plan.ReplayFrom)
	}
	plan.ReplayThrough = coverage.LastSequence
	plan.Steps = append(plan.Steps, ReplayJournal)
	return plan, nil
}

func normalizeLogicalSnapshotID(label, value string, allowEmpty bool) (string, error) {
	if allowEmpty && value == "" {
		return "", nil
	}
	if value == "" || strings.TrimSpace(value) != value || len(value) > maxLogicalSnapshotStringBytes {
		return "", invalidLogicalSnapshot("%s is empty, oversized, or has surrounding whitespace", label)
	}
	return value, nil
}

func invalidLogicalSnapshot(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrLogicalSnapshotInvalid, fmt.Sprintf(format, args...))
}

func appendLogicalSnapshotString(payload []byte, value string) []byte {
	payload = appendLogicalSnapshotUvarint(payload, uint64(len(value)))
	return append(payload, value...)
}

func appendLogicalSnapshotUvarint(payload []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutUvarint(encoded[:], value)
	return append(payload, encoded[:size]...)
}

func appendLogicalSnapshotVarint(payload []byte, value int64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	size := binary.PutVarint(encoded[:], value)
	return append(payload, encoded[:size]...)
}

func readLogicalSnapshotUvarint(payload []byte, offset *int) (uint64, error) {
	if offset == nil || *offset < 0 || *offset >= len(payload) {
		return 0, ErrLogicalSnapshotInvalid
	}
	value, size := binary.Uvarint(payload[*offset:])
	if size <= 0 {
		return 0, ErrLogicalSnapshotInvalid
	}
	*offset += size
	return value, nil
}

func readLogicalSnapshotVarint(payload []byte, offset *int) (int64, error) {
	if offset == nil || *offset < 0 || *offset >= len(payload) {
		return 0, ErrLogicalSnapshotInvalid
	}
	value, size := binary.Varint(payload[*offset:])
	if size <= 0 {
		return 0, ErrLogicalSnapshotInvalid
	}
	*offset += size
	return value, nil
}

func readLogicalSnapshotString(payload []byte, offset *int) (string, error) {
	if offset == nil || *offset < 0 || *offset > len(payload) {
		return "", ErrLogicalSnapshotInvalid
	}
	length, size := binary.Uvarint(payload[*offset:])
	if size <= 0 || size > len(payload)-*offset {
		return "", ErrLogicalSnapshotInvalid
	}
	if length > maxLogicalSnapshotStringBytes || length > uint64(len(payload)-*offset-size) {
		return "", ErrLogicalSnapshotInvalid
	}
	*offset += size
	end := *offset + int(length)
	value := string(payload[*offset:end])
	*offset = end
	return value, nil
}
