package hatCodec

import (
	"bytes"
	"io"
	"testing"
)

func TestStreamCipherRoundTripAndAuthenticatesFrames(t *testing.T) {
	cipher, err := NewStreamCipher("test-key", bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	plain := bytes.Repeat([]byte("hatrie encrypted spill frame\n"), 4000)
	var encrypted bytes.Buffer
	writer, err := cipher.NewWriter(&encrypted, []byte("sql-spill/sort"))
	if err != nil {
		t.Fatal(err)
	}
	if written, err := writer.Write(plain); err != nil || written != len(plain) {
		t.Fatalf("Write() = (%d, %v), want (%d, nil)", written, err, len(plain))
	}
	reader, err := cipher.NewReader(bytes.NewReader(encrypted.Bytes()), []byte("sql-spill/sort"))
	if err != nil {
		t.Fatal(err)
	}
	decrypted, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decrypted, plain) {
		t.Fatal("decrypted stream differs from plaintext")
	}

	tampered := append([]byte(nil), encrypted.Bytes()...)
	tampered[len(tampered)-1] ^= 1
	reader, err = cipher.NewReader(bytes.NewReader(tampered), []byte("sql-spill/sort"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("tampered ciphertext was accepted")
	}
}

func TestStreamCipherRejectsInvalidKeyAndAssociatedData(t *testing.T) {
	if _, err := NewStreamCipher("test", []byte("short")); err == nil {
		t.Fatal("short key was accepted")
	}
	cipher, err := NewStreamCipher("test", bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	var encrypted bytes.Buffer
	writer, err := cipher.NewWriter(&encrypted, []byte("namespace/a"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Write([]byte("secret")); err != nil {
		t.Fatal(err)
	}
	reader, err := cipher.NewReader(bytes.NewReader(encrypted.Bytes()), []byte("namespace/b"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(reader); err == nil {
		t.Fatal("wrong associated data was accepted")
	}
}
