package hatriecache

import (
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
)

func TestBinaryFieldCodecRoundTrip(t *testing.T) {
	writer := newBinaryFieldWriter([]byte{'h', 'c'}, 2)
	writer.writeBool(true)
	writer.writeBool(false)
	writer.writeUvarint(1<<63 + 17)
	writer.writeVarint(-987654321)
	writer.writeString("session:1")
	writer.writeBytes([]byte{1, 2, 3})
	writer.writeFloat64(math.Pi)

	data := writer.bytes()
	if !bytes.HasPrefix(data, []byte{'h', 'c'}) {
		t.Fatalf("encoded prefix = %q, want hc", data[:2])
	}
	reader := newBinaryFieldReader(data[2:])
	if got, err := reader.readBool(); err != nil || !got {
		t.Fatalf("readBool(true) = %v/%v, want true/nil", got, err)
	}
	if got, err := reader.readBool(); err != nil || got {
		t.Fatalf("readBool(false) = %v/%v, want false/nil", got, err)
	}
	if got, err := reader.readUvarint(); err != nil || got != 1<<63+17 {
		t.Fatalf("readUvarint() = %d/%v, want large value/nil", got, err)
	}
	if got, err := reader.readVarint(); err != nil || got != -987654321 {
		t.Fatalf("readVarint() = %d/%v, want -987654321/nil", got, err)
	}
	if got, err := reader.readString(); err != nil || got != "session:1" {
		t.Fatalf("readString() = %q/%v, want session:1/nil", got, err)
	}
	if got, err := reader.readBytes(); err != nil || !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("readBytes() = %v/%v, want [1 2 3]/nil", got, err)
	}
	if got, err := reader.readFloat64(); err != nil || math.Float64bits(got) != math.Float64bits(math.Pi) {
		t.Fatalf("readFloat64() = %v/%v, want pi/nil", got, err)
	}
	if !reader.done() {
		t.Fatal("reader.done() = false, want true")
	}
}

func TestBinaryFieldReaderRejectsMalformedValues(t *testing.T) {
	t.Run("bool", func(t *testing.T) {
		reader := newBinaryFieldReader([]byte{2})
		if _, err := reader.readBool(); err == nil || !strings.Contains(err.Error(), "invalid binary boolean") {
			t.Fatalf("readBool(invalid) error = %v, want invalid binary boolean", err)
		}
	})

	t.Run("uvarint eof", func(t *testing.T) {
		reader := newBinaryFieldReader([]byte{0x80})
		if _, err := reader.readUvarint(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("readUvarint(short) error = %v, want unexpected EOF", err)
		}
	})

	t.Run("bytes eof", func(t *testing.T) {
		reader := newBinaryFieldReader([]byte{4, 'a'})
		if _, err := reader.readBytes(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("readBytes(short) error = %v, want unexpected EOF", err)
		}
	})

	t.Run("float eof", func(t *testing.T) {
		reader := newBinaryFieldReader([]byte{1, 2, 3})
		if _, err := reader.readFloat64(); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("readFloat64(short) error = %v, want unexpected EOF", err)
		}
	})
}
