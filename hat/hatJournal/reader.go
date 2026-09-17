package hatJournal

import (
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

var zstdFrameMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}

const maxZstdDecoderMemory = 256 << 20

type journalReader struct {
	file    *os.File
	decoder *zstd.Decoder
	source  io.Reader
}

func (reader *journalReader) Read(data []byte) (int, error) {
	return reader.source.Read(data)
}

func (reader *journalReader) PhysicalBytes() int64 {
	if physical, ok := reader.source.(interface{ PhysicalBytes() int64 }); ok {
		return physical.PhysicalBytes()
	}
	return -1
}

func (reader *journalReader) Close() error {
	if reader.decoder != nil {
		reader.decoder.Close()
	}
	return reader.file.Close()
}
