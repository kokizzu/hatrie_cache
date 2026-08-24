// Package hatBackup provides the portable backup manifest model used by
// operators and recovery tooling.
package hatBackup

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// BundleVersion is the current backup bundle manifest version.
const BundleVersion = 1

type Mode string

const (
	ModeAuto              Mode = "auto"
	ModeSnapshot          Mode = "snapshot"
	ModePebbleCheckpoint  Mode = "pebble-checkpoint"
	ModePebbleIncremental Mode = "pebble-incremental"
)

// PartitionMetadata identifies the partition coverage of a backup.
type PartitionMetadata struct {
	Mode                string   `json:"mode,omitempty"`
	Partitions          []string `json:"partitions,omitempty"`
	NodeID              string   `json:"node_id,omitempty"`
	TopologyEpoch       uint64   `json:"topology_epoch,omitempty"`
	TopologyFingerprint string   `json:"topology_fingerprint,omitempty"`
	KeyPrefixes         []string `json:"key_prefixes,omitempty"`
}

// BundleFile records an archived payload's name, size, and checksum.
type BundleFile struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"`
}

// BundleManifest describes a recoverable cache backup bundle.
type BundleManifest struct {
	Version           int                `json:"version"`
	CreatedAt         time.Time          `json:"created_at"`
	Mode              Mode               `json:"mode,omitempty"`
	Snapshot          string             `json:"snapshot,omitempty"`
	SnapshotFormat    string             `json:"snapshot_format,omitempty"`
	Store             string             `json:"store,omitempty"`
	StorageBackend    string             `json:"storage_backend,omitempty"`
	StorageFormat     string             `json:"storage_format,omitempty"`
	StorageGeneration uint64             `json:"storage_generation,omitempty"`
	StorageIdentity   string             `json:"storage_identity,omitempty"`
	BackupID          string             `json:"backup_id,omitempty"`
	ParentBackupID    string             `json:"parent_backup_id,omitempty"`
	Incremental       bool               `json:"incremental,omitempty"`
	NewObjects        int                `json:"new_objects,omitempty"`
	ReusedObjects     int                `json:"reused_objects,omitempty"`
	NewObjectBytes    int64              `json:"new_object_bytes,omitempty"`
	ReusedObjectBytes int64              `json:"reused_object_bytes,omitempty"`
	Journal           string             `json:"journal,omitempty"`
	JournalFormat     string             `json:"journal_format,omitempty"`
	JournalSequence   uint64             `json:"journal_sequence"`
	Partition         *PartitionMetadata `json:"partition,omitempty"`
	Files             []BundleFile       `json:"files"`
	RestoreHint       string             `json:"restore_hint"`
}

// ParseMode parses one supported backup mode.
func ParseMode(value string) (Mode, error) {
	switch Mode(strings.ToLower(strings.TrimSpace(value))) {
	case "", ModeAuto:
		return ModeAuto, nil
	case ModeSnapshot:
		return ModeSnapshot, nil
	case ModePebbleCheckpoint:
		return ModePebbleCheckpoint, nil
	case ModePebbleIncremental:
		return ModePebbleIncremental, nil
	default:
		return "", errors.New("hatriecache: backup mode must be auto, snapshot, pebble-checkpoint, or pebble-incremental")
	}
}

// NormalizePartitionMetadata validates and copies portable partition metadata.
func NormalizePartitionMetadata(input PartitionMetadata) (*PartitionMetadata, error) {
	out := PartitionMetadata{
		Mode:                strings.TrimSpace(input.Mode),
		NodeID:              strings.TrimSpace(input.NodeID),
		TopologyEpoch:       input.TopologyEpoch,
		TopologyFingerprint: strings.TrimSpace(input.TopologyFingerprint),
	}
	var err error
	out.Partitions, err = normalizeStringList("partition", input.Partitions)
	if err != nil {
		return nil, err
	}
	out.KeyPrefixes, err = normalizeStringList("key prefix", input.KeyPrefixes)
	if err != nil {
		return nil, err
	}
	if out.Mode == "" && out.NodeID == "" && out.TopologyEpoch == 0 && out.TopologyFingerprint == "" && len(out.Partitions) == 0 && len(out.KeyPrefixes) == 0 {
		return nil, nil
	}
	if out.Mode == "" {
		out.Mode = "partitioned"
	}
	if len(out.Partitions) == 0 {
		return nil, errors.New("hatriecache: backup partition metadata requires at least one partition id")
	}
	return &out, nil
}

// ClonePartitionMetadata returns an independent copy of metadata.
func ClonePartitionMetadata(input *PartitionMetadata) *PartitionMetadata {
	if input == nil {
		return nil
	}
	out := *input
	out.Partitions = append([]string(nil), input.Partitions...)
	out.KeyPrefixes = append([]string(nil), input.KeyPrefixes...)
	return &out
}

func normalizeStringList(label string, values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]string, 0, len(values))
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("hatriecache: backup %s id is required", label)
		}
		if _, ok := seen[value]; ok {
			return nil, fmt.Errorf("hatriecache: duplicate backup %s %q", label, value)
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out, nil
}
