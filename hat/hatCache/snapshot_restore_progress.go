package hatCache

import "io"

// SnapshotRestoreProgress describes the portion of a snapshot that has been
// decoded during an opt-in staged restore.
type SnapshotRestoreProgress struct {
	EntriesRead uint64
	BytesRead   int64
	TotalBytes  int64
}

// SnapshotRestoreProgressFunc receives one update for each decoded snapshot
// entry. Returning an error cancels the restore before its staged generation
// is adopted.
type SnapshotRestoreProgressFunc func(SnapshotRestoreProgress) error

// LoadSnapshotWithProgress restores a snapshot while reporting decoded entry
// and input-byte progress. The existing LoadSnapshot and
// LoadSnapshotWithMetadata methods remain the zero-overhead default path.
func (ht *HatTrie) LoadSnapshotWithProgress(path string, progress SnapshotRestoreProgressFunc) (SnapshotMetadata, error) {
	if progress == nil {
		return ht.LoadSnapshotWithMetadata(path)
	}
	return ht.loadSnapshotStagedWithProgress(path, progress)
}

type snapshotRestoreProgressReader struct {
	reader      io.Reader
	callback    SnapshotRestoreProgressFunc
	totalBytes  int64
	bytesRead   int64
	entriesRead uint64
}

func (reader *snapshotRestoreProgressReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	reader.bytesRead += int64(count)
	return count, err
}

func (reader *snapshotRestoreProgressReader) reportEntry() error {
	reader.entriesRead++
	return reader.callback(SnapshotRestoreProgress{
		EntriesRead: reader.entriesRead,
		BytesRead:   reader.bytesRead,
		TotalBytes:  reader.totalBytes,
	})
}
