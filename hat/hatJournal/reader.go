package hatJournal

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

var zstdFrameMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

const maxZstdDecoderMemory = 256 << 20

// OpenReader opens a journal file and transparently decodes an archived zstd
// segment. Detection uses the frame magic rather than the filename so a crash
// during archive replacement can safely leave a raw segment under a .zst name.
// The returned closer owns the underlying file and decoder.
func OpenReader(path string) (io.ReadCloser, *bufio.Reader, SegmentCompression, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, SegmentCompressionNone, err
	}
	var magic [4]byte
	readBytes, readErr := io.ReadFull(file, magic[:])
	if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
		_ = file.Close()
		return nil, nil, SegmentCompressionNone, readErr
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		_ = file.Close()
		return nil, nil, SegmentCompressionNone, err
	}
	if readBytes == len(magic) && bytes.Equal(magic[:], zstdFrameMagic) {
		decoder, err := zstd.NewReader(file, zstd.WithDecoderMaxMemory(maxZstdDecoderMemory))
		if err != nil {
			_ = file.Close()
			return nil, nil, SegmentCompressionNone, err
		}
		reader := &journalReader{file: file, decoder: decoder, source: decoder}
		return reader, bufio.NewReader(decoder), SegmentCompressionZstd, nil
	}
	reader := &journalReader{file: file, source: file}
	return reader, bufio.NewReader(file), SegmentCompressionNone, nil
}

type journalReader struct {
	file    *os.File
	decoder *zstd.Decoder
	source  io.Reader
}

func (reader *journalReader) Read(data []byte) (int, error) {
	return reader.source.Read(data)
}

func (reader *journalReader) Close() error {
	if reader.decoder != nil {
		reader.decoder.Close()
	}
	return reader.file.Close()
}
