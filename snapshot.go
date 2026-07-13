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
	Key           string              `json:"key"`
	Type          string              `json:"type"`
	Counter       int32               `json:"counter,omitempty"`
	String        string              `json:"string,omitempty"`
	Bytes         string              `json:"bytes,omitempty"`
	Map           Map                 `json:"map"`
	Slice         Slice               `json:"slice"`
	Set           Set                 `json:"set"`
	PriorityQueue []priorityQueueItem `json:"priority_queue"`
	ExpiresAt     *time.Time          `json:"expires_at,omitempty"`
	Stats         *KeyStats           `json:"stats,omitempty"`
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

	snapshot, err := decodeSnapshotFileJSON(data)
	if err != nil {
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
	case DATAVALUE_TYPE_PRIORITY_QUEUE:
		out.PriorityQueue = ht.priorityQueues.array[entry.Value.Index].SnapshotItems()
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
	case "counter", "string", "map", "slice", "set", "priority_queue":
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

func decodeSnapshotEntryJSON(data []byte) (snapshotEntry, error) {
	return decodeSnapshotEntryJSONRequiredKey(data, false)
}

func decodeSnapshotFileJSON(data []byte) (snapshotFile, error) {
	var raw struct {
		Version         int               `json:"version"`
		JournalSequence uint64            `json:"journal_sequence,omitempty"`
		Entries         []json.RawMessage `json:"entries"`
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&raw); err != nil {
		return snapshotFile{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return snapshotFile{}, errors.New("hatriecache: invalid snapshot JSON")
		}
		return snapshotFile{}, err
	}

	snapshot := snapshotFile{
		Version:         raw.Version,
		JournalSequence: raw.JournalSequence,
		Entries:         make([]snapshotEntry, 0, len(raw.Entries)),
	}
	for _, data := range raw.Entries {
		entry, err := decodeSnapshotEntryJSONRequiredKey(data, true)
		if err != nil {
			return snapshotFile{}, err
		}
		snapshot.Entries = append(snapshot.Entries, entry)
	}
	return snapshot, nil
}

func decodeSnapshotEntryJSONRequiredKey(data []byte, requiredKey bool) (snapshotEntry, error) {
	if requiredKey {
		hasKey, err := snapshotEntryHasKey(data)
		if err != nil {
			return snapshotEntry{}, err
		}
		if !hasKey {
			return snapshotEntry{}, errors.New("hatriecache: snapshot entry key is required")
		}
	}

	var entry snapshotEntry
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&entry); err != nil {
		return snapshotEntry{}, err
	}
	var extra struct{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return snapshotEntry{}, errors.New("hatriecache: invalid snapshot entry JSON")
		}
		return snapshotEntry{}, err
	}
	return entry, nil
}

func snapshotEntryHasKey(data []byte) (bool, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return false, err
	}
	value, ok := fields["key"]
	if ok && string(value) == "null" {
		return false, errors.New("hatriecache: snapshot entry key must be a string")
	}
	return ok, nil
}

func (ht *HatTrie) applySnapshotOperation(operation snapshotOperation) error {
	ht.mu.Lock()
	defer ht.mu.Unlock()

	_, err := ht.applySnapshotOperationLocked(operation)
	return err
}

func (ht *HatTrie) applySnapshotOperationLocked(operation snapshotOperation) (HatValue, error) {
	entry := operation.entry
	if entry.ExpiresAt != nil && !ht.currentTime().Before(*entry.ExpiresAt) {
		if ht.deleteLocked(entry.Key) {
			ht.recordExpirationLocked(entry.Key)
		}
		return HatValue{}, nil
	}

	rawPtr := ht.upsertLocation(entry.Key)
	old := HatValue{}
	old.fromValue(*rawPtr)
	if !old.Empty() {
		ht.returnStorage(old)
	}
	ht.clearExpirationLocked(entry.Key)

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
	case "priority_queue":
		idx := ht.priorityQueues.AddItems(entry.PriorityQueue)
		hval = HatValue{Index: idx, Flags: DATAVALUE_TYPE_PRIORITY_QUEUE}
	default:
		return HatValue{}, errors.New("hatriecache: unsupported snapshot value type")
	}
	if entry.Type != "bytes" {
		*rawPtr = hval.toValue()
	}
	if entry.ExpiresAt != nil {
		hval = ht.setExpirationLocked(entry.Key, *entry.ExpiresAt, rawPtr, hval)
	}
	ht.restoreKeyStatsLocked(entry.Key, entry.Stats)
	return hval, nil
}

func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
	}
	if _, err := tmp.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := tmp.Sync(); err != nil {
		cleanup()
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
	return syncDirectory(dir)
}

func syncDirectory(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}
