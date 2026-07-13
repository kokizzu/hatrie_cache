package hatriecache

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"
)

const snapshotVersion = 1

type snapshotFile struct {
	Version         int             `json:"version"`
	JournalSequence uint64          `json:"journal_sequence,omitempty"`
	Entries         []snapshotEntry `json:"entries"`
}

type SnapshotMetadata struct {
	JournalSequence uint64
}

type snapshotEntry struct {
	Key       string     `json:"key"`
	Type      string     `json:"type"`
	Counter   int32      `json:"counter,omitempty"`
	String    string     `json:"string,omitempty"`
	Bytes     string     `json:"bytes,omitempty"`
	Map       Map        `json:"map"`
	Slice     Slice      `json:"slice"`
	Set       Set        `json:"set"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Stats     *KeyStats  `json:"stats,omitempty"`
}

type snapshotOperation struct {
	entry snapshotEntry
	bytes []byte
}

func (ht *HatTrie) SaveSnapshot(path string) error {
	return ht.SaveSnapshotWithJournalSequence(path, 0)
}

func (ht *HatTrie) SaveSnapshotWithJournalSequence(path string, journalSequence uint64) error {
	snapshot, err := ht.snapshot()
	if err != nil {
		return err
	}
	snapshot.JournalSequence = journalSequence
	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return writeFileAtomic(path, data)
}

func (ht *HatTrie) LoadSnapshot(path string) error {
	_, err := ht.LoadSnapshotWithMetadata(path)
	return err
}

func (ht *HatTrie) LoadSnapshotWithMetadata(path string) (SnapshotMetadata, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return SnapshotMetadata{}, err
	}

	var snapshot snapshotFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&snapshot); err != nil {
		return SnapshotMetadata{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return SnapshotMetadata{}, errors.New("hatriecache: invalid snapshot JSON")
		}
		return SnapshotMetadata{}, err
	}
	if snapshot.Version != snapshotVersion {
		return SnapshotMetadata{}, errors.New("hatriecache: unsupported snapshot version")
	}

	now := ht.currentTime()
	operations := make([]snapshotOperation, 0, len(snapshot.Entries))
	for _, entry := range snapshot.Entries {
		if entry.ExpiresAt != nil && !now.Before(*entry.ExpiresAt) {
			continue
		}
		operation, err := validateSnapshotEntry(entry)
		if err != nil {
			return SnapshotMetadata{}, err
		}
		operations = append(operations, operation)
	}

	for _, operation := range operations {
		if err := ht.applySnapshotOperation(operation); err != nil {
			return SnapshotMetadata{}, err
		}
	}
	return SnapshotMetadata{JournalSequence: snapshot.JournalSequence}, nil
}

func (ht *HatTrie) snapshot() (snapshotFile, error) {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	ht.ensureOpen()
	entries := ht.entriesWithPrefixLocked("", true)
	snapshot := snapshotFile{
		Version: snapshotVersion,
		Entries: make([]snapshotEntry, 0, len(entries)),
	}
	for _, entry := range entries {
		snapshotEntry, err := ht.snapshotEntryLocked(entry)
		if err != nil {
			return snapshotFile{}, err
		}
		snapshot.Entries = append(snapshot.Entries, snapshotEntry)
	}
	return snapshot, nil
}

func (ht *HatTrie) snapshotEntryLocked(entry Entry) (snapshotEntry, error) {
	out := snapshotEntry{
		Key:       entry.Key,
		Type:      monitoringType(entry.Value),
		ExpiresAt: snapshotExpiresAt(ht.expires[entry.Key]),
	}
	if stats, ok := ht.keyStats[entry.Key]; ok {
		stats.updateRates()
		out.Stats = &stats
	}
	switch entry.Value.Type() {
	case DATAVALUE_TYPE_COUNTER:
		out.Counter = entry.Value.Index
	case DATAVALUE_TYPE_RAW_STRING:
		out.String = string(ht.raws.array[entry.Value.Index])
	case DATAVALUE_TYPE_RAW_BYTES:
		value, err := ht.bytesValueLocked(entry.Value)
		if err != nil {
			return snapshotEntry{}, err
		}
		out.Bytes = base64.StdEncoding.EncodeToString(value)
	case DATAVALUE_TYPE_MAP:
		out.Map = cloneMap(ht.maps.array[entry.Value.Index])
	case DATAVALUE_TYPE_SLICE:
		out.Slice = ht.slices.array[entry.Value.Index].Slice()
	case DATAVALUE_TYPE_LEVELDB_REF:
		return ht.levelDBReferenceSnapshotEntryLocked(entry.Key, entry.Value)
	case DATAVALUE_TYPE_SET:
		out.Set = ht.sets.array[entry.Value.Index].Values()
	default:
		return snapshotEntry{}, errors.New("hatriecache: unsupported snapshot value type")
	}
	return out, nil
}

func snapshotExpiresAt(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func (ht *HatTrie) bytesValueLocked(hval HatValue) ([]byte, error) {
	if hval.OnDisk() {
		return ht.disks.Get(hval.Index)
	}
	return cloneBytes(ht.raws.array[hval.Index]), nil
}

func validateSnapshotEntry(entry snapshotEntry) (snapshotOperation, error) {
	operation := snapshotOperation{entry: entry}
	switch entry.Type {
	case "counter", "string", "map", "slice", "set":
		return operation, nil
	case "bytes":
		value, err := base64.StdEncoding.DecodeString(entry.Bytes)
		if err != nil {
			return snapshotOperation{}, err
		}
		operation.bytes = value
		return operation, nil
	default:
		return snapshotOperation{}, errors.New("hatriecache: unsupported snapshot value type")
	}
}

func (ht *HatTrie) applySnapshotOperation(operation snapshotOperation) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	_, err := ht.applySnapshotOperationLocked(operation)
	return err
}

func (ht *HatTrie) applySnapshotOperationLocked(operation snapshotOperation) (HatValue, error) {
	entry := operation.entry
	rawPtr := ht.upsertLocation(entry.Key)
	old := HatValue{}
	old.fromValue(*rawPtr)
	if !old.Empty() {
		ht.returnStorage(old)
	}
	delete(ht.expires, entry.Key)

	hval := HatValue{}
	switch entry.Type {
	case "counter":
		hval = HatValue{Index: entry.Counter, Flags: DATAVALUE_TYPE_COUNTER}
	case "string":
		idx := ht.raws.Add([]byte(entry.String))
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_RAW_STRING}
	case "bytes":
		ht.storeBytesLocked(rawPtr, HatValue{}, operation.bytes)
		hval.fromValue(*rawPtr)
	case "map":
		idx := ht.maps.Add(entry.Map)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_MAP}
	case "slice":
		idx := ht.slices.Add(entry.Slice)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SLICE}
	case "set":
		idx := ht.sets.Add(entry.Set)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_SET}
	default:
		return HatValue{}, errors.New("hatriecache: unsupported snapshot value type")
	}
	if entry.Type != "bytes" {
		*rawPtr = hval.toValue()
	}
	if entry.ExpiresAt != nil {
		ht.expires[entry.Key] = *entry.ExpiresAt
		hval.Flags |= 1 << DATAVALUE_TTL_BIT_SHIFT
		*rawPtr = hval.toValue()
	}
	ht.restoreKeyStatsLocked(entry.Key, entry.Stats)
	return hval, nil
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return nil
}
