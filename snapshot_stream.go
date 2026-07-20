package hatriecache

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"runtime"
	"sort"

	"hatrie_cache/internal/jsonwire"

	"github.com/syndtr/goleveldb/leveldb"
)

const snapshotStreamCapturePageBytes = 1 << 20

type snapshotStreamCapture struct {
	pages        []snapshotStreamCapturePage
	count        int
	replacements map[string]snapshotCaptureReplacement
}

type snapshotStreamCapturePage struct {
	records [][]byte
	bytes   int
}

func (capture *snapshotStreamCapture) append(entry snapshotEntry) error {
	data := entry.levelDBRecord
	if data == nil {
		var err error
		data, err = marshalLevelDBEntry(entry, StorageFormatBinary)
		if err != nil {
			return err
		}
	}
	if len(capture.pages) == 0 ||
		len(capture.pages[len(capture.pages)-1].records) == snapshotCapturePageEntries ||
		capture.pages[len(capture.pages)-1].bytes+len(data) > snapshotStreamCapturePageBytes {
		capture.pages = append(capture.pages, snapshotStreamCapturePage{
			records: make([][]byte, 0, snapshotCapturePageEntries),
		})
	}
	last := len(capture.pages) - 1
	capture.pages[last].records = append(capture.pages[last].records, data)
	capture.pages[last].bytes += len(data)
	capture.count++
	return nil
}

func (capture snapshotStreamCapture) visitRecords(visit func([]byte, bool) error) error {
	for _, page := range capture.pages {
		for _, record := range page.records {
			if len(record) == 0 {
				return errors.New("hatriecache: empty streamed snapshot record")
			}
			if err := visit(record, levelDBEntryDataIsBinary(record)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (capture snapshotStreamCapture) visitMergedRecords(visit func([]byte, bool) error) error {
	if len(capture.replacements) == 0 {
		return capture.visitRecords(visit)
	}
	keys := make([]string, 0, len(capture.replacements))
	for key := range capture.replacements {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	replacementIndex := 0
	visitReplacement := func(key string) error {
		replacement := capture.replacements[key]
		if !replacement.present {
			return nil
		}
		data, err := marshalLevelDBEntry(replacement.entry, StorageFormatBinary)
		if err != nil {
			return err
		}
		return visit(data, true)
	}
	err := capture.visitRecords(func(data []byte, binaryRecord bool) error {
		key, err := snapshotStreamRecordKey(data, binaryRecord)
		if err != nil {
			return err
		}
		for replacementIndex < len(keys) && keys[replacementIndex] < key {
			if err := visitReplacement(keys[replacementIndex]); err != nil {
				return err
			}
			replacementIndex++
		}
		if replacementIndex < len(keys) && keys[replacementIndex] == key {
			if err := visitReplacement(keys[replacementIndex]); err != nil {
				return err
			}
			replacementIndex++
			return nil
		}
		return visit(data, binaryRecord)
	})
	if err != nil {
		return err
	}
	for replacementIndex < len(keys) {
		if err := visitReplacement(keys[replacementIndex]); err != nil {
			return err
		}
		replacementIndex++
	}
	return nil
}

func snapshotStreamRecordKey(data []byte, binaryRecord bool) (string, error) {
	if !binaryRecord {
		entry, err := decodeSnapshotEntryJSONRequiredKey(data, true)
		return entry.Key, err
	}
	if !levelDBEntryDataIsBinary(data) {
		return "", errors.New("hatriecache: invalid binary streamed snapshot record")
	}
	reader := newBinaryFieldReader(data[len(levelDBBinaryMagic):])
	return reader.readString()
}

func (ht *HatTrie) captureSnapshotStreamForStoreAtBarrier(currentStore *LevelDBStore, currentDB *leveldb.DB, barrier snapshotCaptureBarrier) (snapshotStreamCapture, uint64, error) {
	if ht == nil {
		return snapshotStreamCapture{}, 0, ErrNilHatTrie
	}
	if ht.localPartitionSet() != nil {
		capture := snapshotStreamCapture{}
		replacements, sequence, err := ht.visitCapturedLocalPartitionEntries(currentStore, currentDB, barrier, capture.append)
		if err != nil {
			return snapshotStreamCapture{}, 0, err
		}
		capture.replacements = replacements
		return capture, sequence, nil
	}
	ht.snapshotCaptureMu.Lock()
	defer ht.snapshotCaptureMu.Unlock()

	tracker := &snapshotMutationTracker{dirty: make(map[string]struct{})}
	ht.mu.Lock()
	func() {
		defer ht.mu.Unlock()
		ht.ensureOpen()
		ht.snapshotMutations = tracker
	}()
	active := true
	defer func() {
		if !active {
			return
		}
		ht.mu.Lock()
		if ht.snapshotMutations == tracker {
			ht.snapshotMutations = nil
		}
		ht.mu.Unlock()
	}()

	capture := snapshotStreamCapture{}
	cursor := &replicationSyncCursor{packedKeys: true}
	afterKey := ""
	hasAfterKey := false
	pageNumber := 0
	for {
		page, err := replicationSyncEntriesPageWithCursor(ht, "", afterKey, hasAfterKey, snapshotCaptureScanPageEntries, cursor, func(entry Entry) error {
			captured, err := ht.captureSnapshotEntryForStoreLocked(entry, currentStore, currentDB)
			if err != nil {
				return err
			}
			return capture.append(captured)
		})
		if err != nil {
			cursor.close(ht)
			return snapshotStreamCapture{}, 0, err
		}
		pageNumber++
		if hook := ht.snapshotCapturePageHook; hook != nil {
			hook(pageNumber)
		}
		if !page.hasMore {
			break
		}
		afterKey = page.nextAfterKey
		hasAfterKey = true
		runtime.Gosched()
	}
	cursor.close(ht)

	replacements, sequence, err := ht.captureSnapshotMutationReplacements(tracker, currentStore, currentDB, barrier)
	if err != nil {
		return snapshotStreamCapture{}, 0, err
	}
	capture.replacements = replacements
	active = false
	return capture, sequence, nil
}

func writeStreamSnapshot(writer io.Writer, journalSequence uint64, format SnapshotFormat, capture snapshotStreamCapture) error {
	switch format {
	case SnapshotFormatBinary:
		return writeStreamSnapshotBinary(writer, journalSequence, capture)
	case SnapshotFormatGzipBestBinary:
		return writeStreamSnapshotGzipBinary(writer, journalSequence, capture, acquireSnapshotBestGzipWriter, releaseSnapshotBestGzipWriter)
	case SnapshotFormatGzipBinary:
		return writeStreamSnapshotGzipBinary(writer, journalSequence, capture, jsonwire.AcquireGzipWriter, jsonwire.ReleaseGzipWriter)
	case SnapshotFormatJSON:
		return writeStreamSnapshotJSON(writer, journalSequence, capture)
	case SnapshotFormatGzipBestJSON:
		return writeStreamSnapshotGzipJSON(writer, journalSequence, capture, acquireSnapshotBestGzipWriter, releaseSnapshotBestGzipWriter)
	case SnapshotFormatGzipJSON:
		return writeStreamSnapshotGzipJSON(writer, journalSequence, capture, jsonwire.AcquireGzipWriter, jsonwire.ReleaseGzipWriter)
	default:
		return fmt.Errorf("hatriecache: unsupported snapshot format %q", format)
	}
}

func writeStreamSnapshotBinary(writer io.Writer, journalSequence uint64, capture snapshotStreamCapture) error {
	header := newBinaryFieldWriter(snapshotBinaryMagic, len(snapshotBinaryMagic)+(2*binaryFieldMaxVarintLen64))
	header.writeUvarint(uint64(snapshotVersion))
	header.writeUvarint(journalSequence)
	if err := writeSnapshotBinaryBytes(writer, header.bytes()); err != nil {
		return err
	}
	return capture.visitMergedRecords(func(data []byte, binaryRecord bool) error {
		if !binaryRecord {
			entry, err := decodeLevelDBEntry(data)
			if err != nil {
				return err
			}
			data, err = marshalLevelDBEntry(entry, StorageFormatBinary)
			if err != nil {
				return err
			}
		}
		return writeSnapshotBinaryRecord(writer, data)
	})
}

func writeStreamSnapshotGzipBinary(writer io.Writer, journalSequence uint64, capture snapshotStreamCapture, acquire func(io.Writer) *gzip.Writer, release func(*gzip.Writer)) error {
	gzipWriter := acquire(writer)
	err := writeStreamSnapshotBinary(gzipWriter, journalSequence, capture)
	closeErr := gzipWriter.Close()
	release(gzipWriter)
	if err != nil {
		return err
	}
	return closeErr
}

func writeStreamSnapshotJSON(writer io.Writer, journalSequence uint64, capture snapshotStreamCapture) error {
	if _, err := io.WriteString(writer, "{\n"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(writer, "  \"version\": %d,\n", snapshotVersion); err != nil {
		return err
	}
	if journalSequence != 0 {
		if _, err := fmt.Fprintf(writer, "  \"journal_sequence\": %d,\n", journalSequence); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(writer, "  \"entries\": ["); err != nil {
		return err
	}
	first := true
	err := capture.visitMergedRecords(func(data []byte, binaryRecord bool) error {
		if !binaryRecord {
			if first {
				if _, err := io.WriteString(writer, "\n"); err != nil {
					return err
				}
				first = false
			} else if _, err := io.WriteString(writer, ",\n"); err != nil {
				return err
			}
			return writeSnapshotRawEntryJSON(writer, data, "    ")
		}
		entry, err := decodeLevelDBEntry(data)
		if err != nil {
			return err
		}
		if first {
			if _, err := io.WriteString(writer, "\n"); err != nil {
				return err
			}
			first = false
		} else if _, err := io.WriteString(writer, ",\n"); err != nil {
			return err
		}
		return writeSnapshotEntryFieldsJSON(writer, entry, "    ")
	})
	if err != nil {
		return err
	}
	if first {
		if _, err := io.WriteString(writer, "]\n"); err != nil {
			return err
		}
	} else if _, err := io.WriteString(writer, "\n  ]\n"); err != nil {
		return err
	}
	_, err = io.WriteString(writer, "}\n")
	return err
}

func writeStreamSnapshotGzipJSON(writer io.Writer, journalSequence uint64, capture snapshotStreamCapture, acquire func(io.Writer) *gzip.Writer, release func(*gzip.Writer)) error {
	gzipWriter := acquire(writer)
	err := writeStreamSnapshotJSON(gzipWriter, journalSequence, capture)
	closeErr := gzipWriter.Close()
	release(gzipWriter)
	if err != nil {
		return err
	}
	return closeErr
}
