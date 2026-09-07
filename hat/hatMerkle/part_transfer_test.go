package hatMerkle_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"testing"

	"hatrie_cache/hat/hatMerkle"
)

func TestCopyImmutablePartStreamsExactPayloadAndVerifiesChecksum(t *testing.T) {
	payload := bytes.Repeat([]byte("immutable-part-"), 4096)
	checksum := hatMerkle.ChecksumPart(payload)
	var destination bytes.Buffer

	written, err := hatMerkle.CopyImmutablePart(&destination, bytes.NewReader(payload), checksum)
	if err != nil {
		t.Fatalf("CopyImmutablePart() error = %v", err)
	}
	if written != int64(len(payload)) || !bytes.Equal(destination.Bytes(), payload) {
		t.Fatalf("copied payload = %d bytes/%d destination bytes, want %d exact bytes", written, destination.Len(), len(payload))
	}
}

func TestCopyImmutablePartRejectsTruncatedOversizedAndCorruptPayloads(t *testing.T) {
	payload := []byte("immutable-part")
	checksum := hatMerkle.ChecksumPart(payload)
	tests := []struct {
		name string
		data []byte
		want error
	}{
		{name: "truncated", data: payload[:len(payload)-1], want: hatMerkle.ErrPartTransferSize},
		{name: "oversized", data: append(append([]byte(nil), payload...), '!'), want: hatMerkle.ErrPartTransferSize},
		{name: "corrupt", data: []byte("immutable-parx"), want: hatMerkle.ErrInvalidPartChecksum},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var destination bytes.Buffer
			_, err := hatMerkle.CopyImmutablePart(&destination, bytes.NewReader(test.data), checksum)
			if !errors.Is(err, test.want) {
				t.Fatalf("CopyImmutablePart() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestCopyImmutablePartRejectsTrailingReaderErrorsAndNilEndpoints(t *testing.T) {
	checksum := hatMerkle.ChecksumPart([]byte("part"))
	readerError := errors.New("reader failed")
	_, err := hatMerkle.CopyImmutablePart(io.Discard, errorReader{err: readerError}, checksum)
	if !errors.Is(err, readerError) {
		t.Fatalf("reader error = %v, want %v", err, readerError)
	}
	if _, err := hatMerkle.CopyImmutablePart(nil, bytes.NewReader([]byte("part")), checksum); !errors.Is(err, hatMerkle.ErrPartTransferEndpoint) {
		t.Fatalf("nil destination error = %v, want ErrPartTransferEndpoint", err)
	}
	if _, err := hatMerkle.CopyImmutablePart(io.Discard, nil, checksum); !errors.Is(err, hatMerkle.ErrPartTransferEndpoint) {
		t.Fatalf("nil source error = %v, want ErrPartTransferEndpoint", err)
	}
}

func TestCopyImmutablePartFileStreamsExactImmutableFile(t *testing.T) {
	payload := bytes.Repeat([]byte("file-backed-part-"), 4096)
	checksum := hatMerkle.ChecksumPart(payload)
	file, err := os.CreateTemp(t.TempDir(), "part-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(payload); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}

	var destination bytes.Buffer
	written, err := hatMerkle.CopyImmutablePartFile(&destination, file, checksum)
	if err != nil {
		t.Fatalf("CopyImmutablePartFile() error = %v", err)
	}
	if written != int64(len(payload)) || !bytes.Equal(destination.Bytes(), payload) {
		t.Fatalf("copied file payload = %d bytes/%d destination bytes, want %d exact bytes", written, destination.Len(), len(payload))
	}
}

func TestCopyImmutablePartFileRejectsWrongRemainingSize(t *testing.T) {
	file, err := os.CreateTemp(t.TempDir(), "part-*")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write([]byte("part")); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	_, err = hatMerkle.CopyImmutablePartFile(io.Discard, file, hatMerkle.ChecksumPart([]byte("different")))
	if !errors.Is(err, hatMerkle.ErrPartTransferSize) {
		t.Fatalf("wrong size error = %v, want ErrPartTransferSize", err)
	}
}

func BenchmarkCopyImmutablePart(b *testing.B) {
	payload := bytes.Repeat([]byte("immutable-part-"), 1<<16)
	checksum := hatMerkle.ChecksumPart(payload)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.Run("verified-reader", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			written, err := hatMerkle.CopyImmutablePart(io.Discard, bytes.NewReader(payload), checksum)
			if err != nil || written != int64(len(payload)) {
				b.Fatalf("CopyImmutablePart() = %d, %v", written, err)
			}
		}
	})

	file, err := os.CreateTemp(b.TempDir(), "part-*")
	if err != nil {
		b.Fatal(err)
	}
	if _, err := file.Write(payload); err != nil {
		file.Close()
		b.Fatal(err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		b.Fatal(err)
	}
	b.Run("file-fastpath", func(b *testing.B) {
		for index := 0; index < b.N; index++ {
			if _, err := file.Seek(0, io.SeekStart); err != nil {
				b.Fatal(err)
			}
			written, err := hatMerkle.CopyImmutablePartFile(io.Discard, file, checksum)
			if err != nil || written != int64(len(payload)) {
				b.Fatalf("CopyImmutablePartFile() = %d, %v", written, err)
			}
		}
	})
	if err := file.Close(); err != nil {
		b.Fatal(err)
	}
}

type errorReader struct {
	err error
}

func (reader errorReader) Read([]byte) (int, error) {
	return 0, reader.err
}
