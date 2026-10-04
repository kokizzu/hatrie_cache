package hatMappedPart

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"testing"
)

func TestOpenMappedReadOnlyPartValidatesAndReads(t *testing.T) {
	want := []byte("immutable-part")
	path := t.TempDir() + "/part.bin"
	if err := os.WriteFile(path, want, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(want)
	part, err := OpenMappedReadOnlyPart(path, MappedReadOnlyPartOptions{
		MaxBytes:       uint64(len(want)),
		ExpectedSize:   uint64(len(want)),
		VerifySize:     true,
		ExpectedSHA256: hex.EncodeToString(digest[:]),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(part.Bytes()); got != string(want) {
		t.Fatalf("Bytes() = %q, want %q", got, want)
	}
	if got := part.Size(); got != uint64(len(want)) {
		t.Fatalf("Size() = %d, want %d", got, len(want))
	}
	if got := part.ChecksumSHA256(); got != hex.EncodeToString(digest[:]) {
		t.Fatalf("ChecksumSHA256() = %q, want %q", got, hex.EncodeToString(digest[:]))
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}
	if got := part.Bytes(); got != nil {
		t.Fatalf("Bytes() after Close() = %v, want nil", got)
	}
	if err := part.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
}

func TestOpenMappedReadOnlyPartRejectsUnsafeInputs(t *testing.T) {
	path := t.TempDir() + "/part.bin"
	if err := os.WriteFile(path, []byte("part"), 0o600); err != nil {
		t.Fatal(err)
	}
	wrongDigest := sha256.Sum256([]byte("wrong"))
	tests := []struct {
		name    string
		options MappedReadOnlyPartOptions
		want    error
	}{
		{name: "missing byte budget", options: MappedReadOnlyPartOptions{}, want: ErrMappedReadOnlyPartOptionsInvalid},
		{name: "too small byte budget", options: MappedReadOnlyPartOptions{MaxBytes: 3}, want: ErrMappedReadOnlyPartTooLarge},
		{name: "size mismatch", options: MappedReadOnlyPartOptions{MaxBytes: 4, VerifySize: true, ExpectedSize: 3}, want: ErrMappedReadOnlyPartSizeMismatch},
		{name: "checksum mismatch", options: MappedReadOnlyPartOptions{MaxBytes: 4, ExpectedSHA256: hex.EncodeToString(wrongDigest[:])}, want: ErrMappedReadOnlyPartChecksumMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			part, err := OpenMappedReadOnlyPart(path, test.options)
			if part != nil {
				_ = part.Close()
			}
			if !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
	link := t.TempDir() + "/part-link"
	if err := os.Symlink(path, link); err == nil {
		part, err := OpenMappedReadOnlyPart(link, MappedReadOnlyPartOptions{MaxBytes: 4})
		if part != nil {
			_ = part.Close()
		}
		if !errors.Is(err, ErrMappedReadOnlyPartNotRegular) {
			t.Fatalf("symlink error = %v, want %v", err, ErrMappedReadOnlyPartNotRegular)
		}
	}
}
