package hatCodec

import (
	"bytes"
	"compress/flate"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	compressedBlockHeader                   = "HCB1"
	defaultCompressedBlockSize              = 64 << 10
	maxCompressedBlockSize                  = 64 << 20
	maxCompressedBlockDecodedBytes          = 1 << 30
	compressedBlockStoredPayloadFlag uint64 = 1 << 63
	compressedBlockPayloadLengthMask        = ^compressedBlockStoredPayloadFlag
)

// CompressedBlockOptions controls EncodeCompressedBlocks. Zero values use a
// 64 KiB block size and flate.BestSpeed.
type CompressedBlockOptions struct {
	BlockSize int
	Level     int
}

// EncodeCompressedBlocks encodes data as independently compressed blocks. A
// block can be stored raw when compression would make it larger, so each
// payload remains bounded and independently decodable.
func EncodeCompressedBlocks(data []byte, options CompressedBlockOptions) ([]byte, error) {
	blockSize, level, err := normalizeCompressedBlockOptions(options)
	if err != nil {
		return nil, err
	}
	if len(data) > int(^uint(0)>>1)-len(compressedBlockHeader) {
		return nil, fmt.Errorf("compressed block input is too large")
	}
	encoded := make([]byte, 0, len(data)+len(compressedBlockHeader))
	encoded = append(encoded, compressedBlockHeader...)
	var compressed bytes.Buffer
	writer, err := flate.NewWriter(&compressed, level)
	if err != nil {
		return nil, fmt.Errorf("create compressed block writer: %w", err)
	}
	for start := 0; start < len(data); {
		end := start + blockSize
		if end > len(data) {
			end = len(data)
		}
		block := data[start:end]
		compressed.Reset()
		writer.Reset(&compressed)
		if _, err := writer.Write(block); err != nil {
			return nil, fmt.Errorf("compress block: %w", err)
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("close compressed block: %w", err)
		}
		payload := compressed.Bytes()
		stored := len(payload) >= len(block)
		if stored {
			payload = block
		}
		encoded = appendCompressedBlockUvarint(encoded, uint64(len(block)))
		payloadLength := uint64(len(payload))
		if stored {
			payloadLength |= compressedBlockStoredPayloadFlag
		}
		encoded = appendCompressedBlockUvarint(encoded, payloadLength)
		var checksum [4]byte
		binary.LittleEndian.PutUint32(checksum[:], crc32.ChecksumIEEE(block))
		encoded = append(encoded, checksum[:]...)
		encoded = append(encoded, payload...)
		start = end
	}
	return encoded, nil
}

// DecodeCompressedBlocks decodes a complete independently compressed-block
// stream and verifies every block checksum. It rejects invalid framing,
// truncated payloads, decompression expansion beyond the safety limits, and
// checksum mismatches.
func DecodeCompressedBlocks(encoded []byte) ([]byte, error) {
	if len(encoded) < len(compressedBlockHeader) || string(encoded[:len(compressedBlockHeader)]) != compressedBlockHeader {
		return nil, fmt.Errorf("invalid compressed block header")
	}
	decoded := make([]byte, 0)
	offset := len(compressedBlockHeader)
	for offset < len(encoded) {
		rawLength, next, err := readCompressedBlockUvarint(encoded, offset, "uncompressed length")
		if err != nil {
			return nil, err
		}
		offset = next
		if rawLength == 0 || rawLength > maxCompressedBlockSize {
			return nil, fmt.Errorf("invalid compressed block uncompressed length %d", rawLength)
		}
		payloadLengthWithFlag, next, err := readCompressedBlockUvarint(encoded, offset, "payload length")
		if err != nil {
			return nil, err
		}
		offset = next
		stored := payloadLengthWithFlag&compressedBlockStoredPayloadFlag != 0
		payloadLength := payloadLengthWithFlag & compressedBlockPayloadLengthMask
		if payloadLength > maxCompressedBlockSize {
			return nil, fmt.Errorf("invalid compressed block payload length %d", payloadLength)
		}
		if stored && payloadLength != rawLength {
			return nil, fmt.Errorf("stored compressed block length %d does not match uncompressed length %d", payloadLength, rawLength)
		}
		if len(encoded)-offset < 4 {
			return nil, fmt.Errorf("compressed block checksum is truncated")
		}
		expectedChecksum := binary.LittleEndian.Uint32(encoded[offset : offset+4])
		offset += 4
		if payloadLength > uint64(len(encoded)-offset) {
			return nil, fmt.Errorf("compressed block payload length %d exceeds remaining input", payloadLength)
		}
		payloadEnd := offset + int(payloadLength)
		payload := encoded[offset:payloadEnd]
		offset = payloadEnd
		if uint64(len(decoded)) > uint64(maxCompressedBlockDecodedBytes)-rawLength {
			return nil, fmt.Errorf("decoded compressed blocks exceed %d bytes", maxCompressedBlockDecodedBytes)
		}
		if stored {
			decoded = append(decoded, payload...)
		} else {
			block, err := decodeCompressedBlock(payload, int(rawLength))
			if err != nil {
				return nil, err
			}
			decoded = append(decoded, block...)
		}
		blockStart := len(decoded) - int(rawLength)
		if crc32.ChecksumIEEE(decoded[blockStart:]) != expectedChecksum {
			return nil, fmt.Errorf("compressed block checksum mismatch")
		}
	}
	return decoded, nil
}

func normalizeCompressedBlockOptions(options CompressedBlockOptions) (int, int, error) {
	blockSize := options.BlockSize
	if blockSize == 0 {
		blockSize = defaultCompressedBlockSize
	}
	if blockSize < 1 || blockSize > maxCompressedBlockSize {
		return 0, 0, fmt.Errorf("compressed block size %d is outside 1..%d", blockSize, maxCompressedBlockSize)
	}
	level := options.Level
	if level == 0 {
		level = flate.BestSpeed
	}
	if level < flate.HuffmanOnly || level > flate.BestCompression {
		return 0, 0, fmt.Errorf("compressed block level %d is outside %d..%d", level, flate.HuffmanOnly, flate.BestCompression)
	}
	return blockSize, level, nil
}

func appendCompressedBlockUvarint(destination []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(destination, encoded[:n]...)
}

func readCompressedBlockUvarint(encoded []byte, offset int, label string) (uint64, int, error) {
	value, size := binary.Uvarint(encoded[offset:])
	if size <= 0 {
		return 0, offset, fmt.Errorf("compressed block %s is invalid", label)
	}
	return value, offset + size, nil
}

func decodeCompressedBlock(payload []byte, expectedLength int) ([]byte, error) {
	reader := flate.NewReader(bytes.NewReader(payload))
	decompressed := make([]byte, expectedLength)
	if _, err := io.ReadFull(reader, decompressed); err != nil {
		reader.Close()
		return nil, fmt.Errorf("decompress block: %w", err)
	}
	var extra [1]byte
	if count, err := reader.Read(extra[:]); count != 0 || err != io.EOF {
		reader.Close()
		if err != nil {
			return nil, fmt.Errorf("decompress block trailer: %w", err)
		}
		return nil, fmt.Errorf("decompressed block exceeds declared length")
	}
	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("close decompressed block: %w", err)
	}
	return decompressed, nil
}
