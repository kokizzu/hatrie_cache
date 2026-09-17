package hatSql

import (
	"encoding/binary"
	"sort"
)

const (
	sqlSinkExactlyOnceFileMagic0   = byte('H')
	sqlSinkExactlyOnceFileMagic1   = byte('S')
	sqlSinkExactlyOnceFileMagic2   = byte('E')
	sqlSinkExactlyOnceFileMagic3   = byte('1')
	sqlSinkExactlyOnceFileVersion  = byte(1)
	maxSQLSinkExactlyOnceFileNames = MaxSQLSinkExactlyOnceCapacity
	maxSQLSinkExactlyOnceProgress  = MaxSQLSinkExactlyOncePartitions
	maxSQLSinkExactlyOnceFileBytes = 64 << 20
)

func marshalSQLSinkExactlyOnceFile(file sqlSinkExactlyOnceFile) ([]byte, error) {
	if len(file.Checkpoints) > maxSQLSinkExactlyOnceFileNames {
		return nil, ErrSQLSinkExactlyOnceInvalid
	}
	names := make([]string, 0, len(file.Checkpoints))
	for name, snapshot := range file.Checkpoints {
		if _, err := normalizeSQLSinkExactlyOnceName(name); err != nil {
			return nil, ErrSQLSinkExactlyOnceInvalid
		}
		if err := validateSQLSinkExactlyOnceCheckpoint(snapshot); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	sort.Strings(names)
	data := make([]byte, 0, 64)
	data = append(data, sqlSinkExactlyOnceFileMagic0, sqlSinkExactlyOnceFileMagic1, sqlSinkExactlyOnceFileMagic2, sqlSinkExactlyOnceFileMagic3, sqlSinkExactlyOnceFileVersion)
	data = appendSQLSinkExactlyOnceUvarint(data, uint64(len(names)))
	for _, name := range names {
		data = appendSQLSinkExactlyOnceString(data, name)
		snapshot := file.Checkpoints[name]
		capacity := snapshot.Capacity
		if capacity <= 0 {
			capacity = DefaultSQLSinkExactlyOnceCapacity
		}
		data = appendSQLSinkExactlyOnceUvarint(data, uint64(capacity))
		data = appendSQLSinkExactlyOnceUvarint(data, uint64(len(snapshot.Commits)))
		for _, commit := range snapshot.Commits {
			data = appendSQLSinkExactlyOnceString(data, commit.Sink)
			data = appendSQLSinkExactlyOnceString(data, commit.TransactionID)
			data = appendSQLSinkExactlyOnceString(data, commit.IdempotencyKey)
			data = appendSQLSinkExactlyOnceUvarint(data, uint64(len(commit.Progress)))
			for _, progress := range commit.Progress {
				data = appendSQLSinkExactlyOnceString(data, progress.Sink)
				data = appendSQLSinkExactlyOnceString(data, progress.Partition)
				data = appendSQLSinkExactlyOnceUvarint(data, progress.Frontier)
			}
		}
		data = appendSQLSinkExactlyOnceUvarint(data, uint64(len(snapshot.Progress)))
		for _, progress := range snapshot.Progress {
			data = appendSQLSinkExactlyOnceString(data, progress.Sink)
			data = appendSQLSinkExactlyOnceString(data, progress.Partition)
			data = appendSQLSinkExactlyOnceUvarint(data, progress.Frontier)
		}
	}
	if len(data) > maxSQLSinkExactlyOnceFileBytes {
		return nil, ErrSQLSinkExactlyOnceInvalid
	}
	return data, nil
}

func unmarshalSQLSinkExactlyOnceFile(data []byte) (sqlSinkExactlyOnceFile, error) {
	if len(data) < 5 || len(data) > maxSQLSinkExactlyOnceFileBytes || data[0] != sqlSinkExactlyOnceFileMagic0 || data[1] != sqlSinkExactlyOnceFileMagic1 || data[2] != sqlSinkExactlyOnceFileMagic2 || data[3] != sqlSinkExactlyOnceFileMagic3 || data[4] != sqlSinkExactlyOnceFileVersion {
		return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
	}
	reader := sqlSinkExactlyOnceReader{data: data, offset: 5}
	nameCount, ok := reader.uvarint()
	if !ok || nameCount > maxSQLSinkExactlyOnceFileNames {
		return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
	}
	file := sqlSinkExactlyOnceFile{Checkpoints: make(map[string]SQLSinkExactlyOnceCheckpoint, int(nameCount))}
	for index := uint64(0); index < nameCount; index++ {
		name, ok := reader.string(maxSQLSinkExactlyOnceStringBytes)
		if !ok {
			return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
		}
		if _, err := normalizeSQLSinkExactlyOnceName(name); err != nil {
			return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
		}
		capacity, ok := reader.uvarint()
		if !ok || capacity == 0 || capacity > MaxSQLSinkExactlyOnceCapacity {
			return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
		}
		commitCount, ok := reader.uvarint()
		if !ok || commitCount > capacity {
			return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
		}
		snapshot := SQLSinkExactlyOnceCheckpoint{Capacity: int(capacity), Commits: make([]SQLSinkCommit, 0, int(commitCount))}
		for commitIndex := uint64(0); commitIndex < commitCount; commitIndex++ {
			sink, sinkOK := reader.string(maxSQLSinkExactlyOnceStringBytes)
			transactionID, transactionOK := reader.string(maxSQLSinkExactlyOnceStringBytes)
			idempotencyKey, keyOK := reader.string(maxSQLSinkExactlyOnceStringBytes)
			progressCount, progressOK := reader.uvarint()
			if !sinkOK || !transactionOK || !keyOK || !progressOK || progressCount == 0 || progressCount > maxSQLSinkExactlyOnceProgress {
				return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
			}
			commit := SQLSinkCommit{Sink: sink, TransactionID: transactionID, IdempotencyKey: idempotencyKey, Progress: make([]SQLSinkProgress, 0, int(progressCount))}
			for progressIndex := uint64(0); progressIndex < progressCount; progressIndex++ {
				progressSink, progressSinkOK := reader.string(maxSQLSinkExactlyOnceStringBytes)
				partition, partitionOK := reader.string(maxSQLSinkExactlyOnceStringBytes)
				frontier, frontierOK := reader.uvarint()
				if !progressSinkOK || !partitionOK || !frontierOK {
					return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
				}
				commit.Progress = append(commit.Progress, SQLSinkProgress{Sink: progressSink, Partition: partition, Frontier: frontier})
			}
			snapshot.Commits = append(snapshot.Commits, commit)
		}
		progressCount, ok := reader.uvarint()
		if !ok || progressCount > maxSQLSinkExactlyOnceProgress {
			return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
		}
		snapshot.Progress = make([]SQLSinkProgress, 0, int(progressCount))
		for progressIndex := uint64(0); progressIndex < progressCount; progressIndex++ {
			progressSink, progressSinkOK := reader.string(maxSQLSinkExactlyOnceStringBytes)
			partition, partitionOK := reader.string(maxSQLSinkExactlyOnceStringBytes)
			frontier, frontierOK := reader.uvarint()
			if !progressSinkOK || !partitionOK || !frontierOK {
				return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
			}
			snapshot.Progress = append(snapshot.Progress, SQLSinkProgress{Sink: progressSink, Partition: partition, Frontier: frontier})
		}
		if _, found := file.Checkpoints[name]; found {
			return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
		}
		if err := validateSQLSinkExactlyOnceCheckpoint(snapshot); err != nil {
			return sqlSinkExactlyOnceFile{}, err
		}
		file.Checkpoints[name] = snapshot
	}
	if reader.offset != len(reader.data) {
		return sqlSinkExactlyOnceFile{}, ErrSQLSinkExactlyOnceInvalid
	}
	return file, nil
}

func appendSQLSinkExactlyOnceUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	length := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:length]...)
}

func appendSQLSinkExactlyOnceString(destination []byte, value string) []byte {
	destination = appendSQLSinkExactlyOnceUvarint(destination, uint64(len(value)))
	return append(destination, value...)
}

type sqlSinkExactlyOnceReader struct {
	data   []byte
	offset int
}

func (reader *sqlSinkExactlyOnceReader) uvarint() (uint64, bool) {
	if reader == nil || reader.offset >= len(reader.data) {
		return 0, false
	}
	value, length := binary.Uvarint(reader.data[reader.offset:])
	if length <= 0 {
		return 0, false
	}
	reader.offset += length
	return value, true
}

func (reader *sqlSinkExactlyOnceReader) string(maxBytes int) (string, bool) {
	length, ok := reader.uvarint()
	if !ok || length > uint64(maxBytes) || length > uint64(len(reader.data)-reader.offset) {
		return "", false
	}
	start := reader.offset
	reader.offset += int(length)
	return string(reader.data[start:reader.offset]), true
}
