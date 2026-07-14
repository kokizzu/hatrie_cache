package hatriecache

import (
	"encoding/binary"
	"errors"
	"io"
	"math"
)

const binaryFieldMaxVarintLen64 = binary.MaxVarintLen64

type binaryFieldWriter struct {
	buf []byte
}

func newBinaryFieldWriter(prefix []byte, capacity int) binaryFieldWriter {
	if capacity < len(prefix) {
		capacity = len(prefix)
	}
	buf := make([]byte, 0, capacity)
	buf = append(buf, prefix...)
	return binaryFieldWriter{buf: buf}
}

func (writer *binaryFieldWriter) bytes() []byte {
	return writer.buf
}

func (writer *binaryFieldWriter) Write(data []byte) (int, error) {
	writer.buf = append(writer.buf, data...)
	return len(data), nil
}

func (writer *binaryFieldWriter) writeBool(value bool) {
	if value {
		writer.buf = append(writer.buf, 1)
		return
	}
	writer.buf = append(writer.buf, 0)
}

func (writer *binaryFieldWriter) writeUvarint(value uint64) {
	var scratch [binaryFieldMaxVarintLen64]byte
	n := binary.PutUvarint(scratch[:], value)
	writer.buf = append(writer.buf, scratch[:n]...)
}

func (writer *binaryFieldWriter) writeVarint(value int64) {
	var scratch [binaryFieldMaxVarintLen64]byte
	n := binary.PutVarint(scratch[:], value)
	writer.buf = append(writer.buf, scratch[:n]...)
}

func (writer *binaryFieldWriter) writeBytes(value []byte) {
	writer.writeUvarint(uint64(len(value)))
	writer.buf = append(writer.buf, value...)
}

func (writer *binaryFieldWriter) writeString(value string) {
	writer.writeUvarint(uint64(len(value)))
	writer.buf = append(writer.buf, value...)
}

func (writer *binaryFieldWriter) writeFloat64(value float64) {
	var scratch [8]byte
	binary.LittleEndian.PutUint64(scratch[:], math.Float64bits(value))
	writer.buf = append(writer.buf, scratch[:]...)
}

func binaryUvarintSize(value uint64) int {
	size := 1
	for value >= 0x80 {
		value >>= 7
		size++
	}
	return size
}

func binaryVarintSize(value int64) int {
	var scratch [binaryFieldMaxVarintLen64]byte
	return binary.PutVarint(scratch[:], value)
}

type binaryFieldReader struct {
	data []byte
	off  int
}

func newBinaryFieldReader(data []byte) binaryFieldReader {
	return binaryFieldReader{data: data}
}

func (reader *binaryFieldReader) done() bool {
	return reader.off == len(reader.data)
}

func (reader *binaryFieldReader) readBool() (bool, error) {
	if reader.off >= len(reader.data) {
		return false, io.ErrUnexpectedEOF
	}
	value := reader.data[reader.off]
	reader.off++
	switch value {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, errors.New("hatriecache: invalid binary boolean")
	}
}

func (reader *binaryFieldReader) readUvarint() (uint64, error) {
	value, n := binary.Uvarint(reader.data[reader.off:])
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, errors.New("hatriecache: invalid binary unsigned integer")
	}
	reader.off += n
	return value, nil
}

func (reader *binaryFieldReader) readVarint() (int64, error) {
	value, n := binary.Varint(reader.data[reader.off:])
	if n == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	if n < 0 {
		return 0, errors.New("hatriecache: invalid binary signed integer")
	}
	reader.off += n
	return value, nil
}

func (reader *binaryFieldReader) readBytes() ([]byte, error) {
	size, err := reader.readUvarint()
	if err != nil {
		return nil, err
	}
	if size > uint64(len(reader.data)-reader.off) {
		return nil, io.ErrUnexpectedEOF
	}
	start := reader.off
	reader.off += int(size)
	return reader.data[start:reader.off], nil
}

func (reader *binaryFieldReader) readString() (string, error) {
	value, err := reader.readBytes()
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (reader *binaryFieldReader) readFloat64() (float64, error) {
	if len(reader.data)-reader.off < 8 {
		return 0, io.ErrUnexpectedEOF
	}
	value := binary.LittleEndian.Uint64(reader.data[reader.off : reader.off+8])
	reader.off += 8
	return math.Float64frombits(value), nil
}
