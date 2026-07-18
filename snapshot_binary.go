package hatriecache

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
)

var snapshotBinaryMagic = []byte{'h', 'c', 's', 'n', 1}

const (
	maxSnapshotBinaryRecordBytes               = 1 << 30
	maxSnapshotBinaryReusableRecordBufferBytes = 1 << 20
)

var errSnapshotBinaryRecordTooLarge = errors.New("hatriecache: binary snapshot entry is too large")

func writeCapturedSnapshotBinary(writer io.Writer, journalSequence uint64, capture snapshotCapture) error {
	header := newBinaryFieldWriter(snapshotBinaryMagic, len(snapshotBinaryMagic)+(2*binaryFieldMaxVarintLen64))
	header.writeUvarint(uint64(snapshotVersion))
	header.writeUvarint(journalSequence)
	if err := writeSnapshotBinaryBytes(writer, header.bytes()); err != nil {
		return err
	}

	for _, page := range capture.pages {
		for _, entry := range page {
			data := entry.levelDBRecord
			if !levelDBEntryDataIsBinary(data) {
				var err error
				data, err = marshalLevelDBEntry(entry, StorageFormatBinary)
				if err != nil {
					return err
				}
			}
			if err := writeSnapshotBinaryRecord(writer, data); err != nil {
				return err
			}
		}
	}
	return nil
}

func writeCapturedSnapshotGzipBinary(writer io.Writer, journalSequence uint64, capture snapshotCapture, acquire func(io.Writer) *gzip.Writer, release func(*gzip.Writer)) error {
	gzipWriter := acquire(writer)
	err := writeCapturedSnapshotBinary(gzipWriter, journalSequence, capture)
	closeErr := gzipWriter.Close()
	release(gzipWriter)
	if err != nil {
		return err
	}
	return closeErr
}

func (ht *HatTrie) snapshotEntryBinaryRecordLocked(entry Entry) ([]byte, error) {
	if entry.Value.Type() == DATAVALUE_TYPE_RAW_BYTES && entry.Value.OnDisk() {
		return ht.levelDBDiskBytesEntryDataBinaryLocked(entry)
	}
	snapshotEntry, err := ht.snapshotEntryLocked(entry)
	if err != nil {
		return nil, err
	}
	return marshalLevelDBEntry(snapshotEntry, StorageFormatBinary)
}

func writeSnapshotBinaryRecord(writer io.Writer, data []byte) error {
	if err := validateSnapshotBinaryRecordSize(uint64(len(data))); err != nil {
		return err
	}
	var scratch [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], uint64(len(data)))
	if err := writeSnapshotBinaryBytes(writer, scratch[:n]); err != nil {
		return err
	}
	return writeSnapshotBinaryBytes(writer, data)
}

func writeSnapshotBinaryBytes(writer io.Writer, data []byte) error {
	n, err := writer.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

func snapshotReaderIsBinary(reader *bufio.Reader) (bool, error) {
	header, err := reader.Peek(len(snapshotBinaryMagic))
	if err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, bufio.ErrBufferFull) {
			return false, nil
		}
		return false, err
	}
	return bytes.Equal(header, snapshotBinaryMagic), nil
}

func scanSnapshotFileBinaryReader(reader *bufio.Reader, visit func(snapshotEntry) error) (snapshotFileMetadata, error) {
	header := make([]byte, len(snapshotBinaryMagic))
	if _, err := io.ReadFull(reader, header); err != nil {
		return snapshotFileMetadata{}, err
	}
	if !bytes.Equal(header, snapshotBinaryMagic) {
		return snapshotFileMetadata{}, errors.New("hatriecache: invalid binary snapshot")
	}

	version, err := binary.ReadUvarint(reader)
	if err != nil {
		return snapshotFileMetadata{}, err
	}
	if version > uint64(int(^uint(0)>>1)) {
		return snapshotFileMetadata{}, errors.New("hatriecache: invalid binary snapshot version")
	}
	journalSequence, err := binary.ReadUvarint(reader)
	if err != nil {
		return snapshotFileMetadata{}, err
	}
	metadata := snapshotFileMetadata{
		Version:         int(version),
		JournalSequence: journalSequence,
	}

	var data []byte
	for {
		size, err := binary.ReadUvarint(reader)
		if errors.Is(err, io.EOF) {
			return metadata, nil
		}
		if err != nil {
			return snapshotFileMetadata{}, err
		}
		if err := validateSnapshotBinaryRecordSize(size); err != nil {
			return snapshotFileMetadata{}, err
		}
		var record []byte
		record, data = snapshotBinaryRecordBuffer(data, int(size))
		if _, err := io.ReadFull(reader, record); err != nil {
			return snapshotFileMetadata{}, err
		}
		entry, err := decodeLevelDBEntry(record)
		if err != nil {
			return snapshotFileMetadata{}, err
		}
		if visit != nil {
			if err := visit(entry); err != nil {
				return snapshotFileMetadata{}, err
			}
		}
	}
}

func snapshotBinaryRecordBuffer(buffer []byte, size int) ([]byte, []byte) {
	if size > maxSnapshotBinaryReusableRecordBufferBytes {
		return make([]byte, size), buffer
	}
	if size > cap(buffer) || cap(buffer) > maxSnapshotBinaryReusableRecordBufferBytes {
		buffer = make([]byte, size)
	} else {
		buffer = buffer[:size]
	}
	return buffer, buffer
}

func validateSnapshotBinaryRecordSize(size uint64) error {
	if size > maxSnapshotBinaryRecordBytes || size > uint64(int(^uint(0)>>1)) {
		return errSnapshotBinaryRecordTooLarge
	}
	return nil
}
