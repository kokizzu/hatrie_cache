package hatJournal

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestTR008EncryptedReaderSupportsKeyRotationAndLegacyRecords(t *testing.T) {
	oldKey := []byte("01234567890123456789012345678901")
	newKey := []byte("abcdefghijklmnopqrstuvwxyzABCDEF")
	oldEncryptor, err := NewRecordEncryptor(EncryptionOptions{KeyID: "old-key", Key: oldKey})
	if err != nil {
		t.Fatalf("NewRecordEncryptor(old) error = %v", err)
	}
	newEncryptor, err := NewRecordEncryptor(EncryptionOptions{KeyID: "new-key", Key: newKey})
	if err != nil {
		t.Fatalf("NewRecordEncryptor(new) error = %v", err)
	}
	var encoded []byte
	encoded = append(encoded, []byte("legacy HJE1 value\n")...)
	encoded, err = oldEncryptor.AppendRecord(encoded, []byte("old-record"))
	if err != nil {
		t.Fatalf("AppendRecord(old) error = %v", err)
	}
	encoded, err = newEncryptor.AppendRecord(encoded, []byte("new-record"))
	if err != nil {
		t.Fatalf("AppendRecord(new) error = %v", err)
	}

	path := filepath.Join(t.TempDir(), "journal.log")
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	handle, reader, _, err := OpenReaderWithEncryption(path, EncryptionOptions{
		KeyID: "new-key",
		Key:   newKey,
		Keyring: map[string][]byte{
			"old-key": oldKey,
		},
	})
	if err != nil {
		t.Fatalf("OpenReaderWithEncryption() error = %v", err)
	}
	decoded, err := io.ReadAll(reader)
	if closeErr := handle.Close(); err != nil || closeErr != nil {
		t.Fatalf("ReadAll/Close() = %v/%v", err, closeErr)
	}
	if string(decoded) != "legacy HJE1 value\nold-recordnew-record" {
		t.Fatalf("decoded records = %q, want mixed legacy and rotated records", decoded)
	}
}

func TestTR008EncryptedReaderRejectsTamperingAndAllowsTruncatedTail(t *testing.T) {
	key := []byte("12345678901234567890123456789012")
	encryptor, err := NewRecordEncryptor(EncryptionOptions{KeyID: "current", Key: key})
	if err != nil {
		t.Fatalf("NewRecordEncryptor() error = %v", err)
	}
	encoded, err := encryptor.AppendRecord(nil, []byte("authenticated"))
	if err != nil {
		t.Fatalf("AppendRecord() error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "journal.log")

	tampered := append([]byte(nil), encoded...)
	tampered[len(tampered)-1] ^= 1
	if err := os.WriteFile(path, tampered, 0o600); err != nil {
		t.Fatalf("WriteFile(tampered) error = %v", err)
	}
	handle, reader, _, err := OpenReaderWithEncryption(path, EncryptionOptions{KeyID: "current", Key: key})
	if err != nil {
		t.Fatalf("OpenReaderWithEncryption(tampered) error = %v", err)
	}
	_, readErr := io.ReadAll(reader)
	closeErr := handle.Close()
	if !errors.Is(readErr, ErrEncryptionAuthentication) || closeErr != nil {
		t.Fatalf("tampered ReadAll/Close() = %v/%v, want authentication error/nil", readErr, closeErr)
	}
	if _, inspectErr := Inspect(path, InspectOptions{Encryption: EncryptionOptions{KeyID: "current", Key: key}}); !errors.Is(inspectErr, ErrEncryptionAuthentication) {
		t.Fatalf("Inspect(tampered) error = %v, want authentication error", inspectErr)
	}

	if err := os.WriteFile(path, encoded[:len(encoded)-1], 0o600); err != nil {
		t.Fatalf("WriteFile(truncated) error = %v", err)
	}
	handle, reader, _, err = OpenReaderWithEncryption(path, EncryptionOptions{KeyID: "current", Key: key})
	if err != nil {
		t.Fatalf("OpenReaderWithEncryption(truncated) error = %v", err)
	}
	decoded, readErr := io.ReadAll(reader)
	closeErr = handle.Close()
	if readErr != nil || closeErr != nil || len(decoded) != 0 {
		t.Fatalf("truncated ReadAll/Close() = %q/%v/%v, want empty clean tail", decoded, readErr, closeErr)
	}
}

func TestTR008EncryptionOptionsValidationClonesKeyMaterial(t *testing.T) {
	key := []byte("12345678901234567890123456789012")
	keyringKey := []byte("abcdefghijklmnopqrstuvwxyzABCDEF")
	options, err := ValidateEncryptionOptions(EncryptionOptions{
		KeyID: "current",
		Key:   key,
		Keyring: map[string][]byte{
			"old": keyringKey,
		},
	})
	if err != nil {
		t.Fatalf("ValidateEncryptionOptions() error = %v", err)
	}
	key[0]++
	keyringKey[0]++
	if bytes.Equal(options.Key, key) || bytes.Equal(options.Keyring["old"], keyringKey) {
		t.Fatal("ValidateEncryptionOptions() retained caller-owned key slices")
	}
	if _, err := ValidateEncryptionOptions(EncryptionOptions{KeyID: "missing"}); err == nil {
		t.Fatal("ValidateEncryptionOptions() accepted a key ID without key material")
	}
}
