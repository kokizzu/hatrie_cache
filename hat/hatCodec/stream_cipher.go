package hatCodec

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
)

var streamCipherMagic = [6]byte{'H', 'T', 'S', 'C', 1, 12}

const (
	streamCipherFrameBytes    = 32 << 10
	streamCipherMaxFrameBytes = streamCipherFrameBytes + 64
)

// StreamCipher encrypts independently authenticated stream frames with
// AES-GCM. KeyID is metadata only; callers must supply key material through a
// secret manager and never persist it beside the encrypted data.
type StreamCipher struct {
	keyID string
	aead  cipher.AEAD
}

// NewStreamCipher validates key material and constructs an AES-GCM stream
// cipher. AES accepts 16, 24, or 32 byte keys; production deployments should
// prefer a 32 byte key.
func NewStreamCipher(keyID string, key []byte) (*StreamCipher, error) {
	if keyID == "" {
		return nil, fmt.Errorf("stream cipher key ID is required")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create AES stream cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create AES-GCM stream cipher: %w", err)
	}
	if aead.NonceSize() != int(streamCipherMagic[5]) {
		return nil, fmt.Errorf("unsupported AES-GCM nonce size %d", aead.NonceSize())
	}
	return &StreamCipher{keyID: keyID, aead: aead}, nil
}

// KeyID returns the non-secret identifier associated with this key.
func (cipher *StreamCipher) KeyID() string {
	if cipher == nil {
		return ""
	}
	return cipher.keyID
}

// NewWriter writes a framed AES-GCM stream. Associated data should identify
// the namespace and content type, preventing a valid ciphertext from being
// replayed into a different persisted namespace or spill class.
func (cipher *StreamCipher) NewWriter(writer io.Writer, associatedData []byte) (io.Writer, error) {
	if cipher == nil || cipher.aead == nil {
		return nil, fmt.Errorf("stream cipher is required")
	}
	if writer == nil {
		return nil, fmt.Errorf("stream cipher writer is required")
	}
	nonce := make([]byte, cipher.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate stream cipher nonce: %w", err)
	}
	header := append(streamCipherMagic[:], nonce...)
	if err := writeAll(writer, header); err != nil {
		return nil, fmt.Errorf("write stream cipher header: %w", err)
	}
	return &streamCipherWriter{writer: writer, cipher: cipher, nonce: nonce, associatedData: append([]byte(nil), associatedData...)}, nil
}

// NewReader verifies a framed AES-GCM stream produced by NewWriter.
func (cipher *StreamCipher) NewReader(reader io.Reader, associatedData []byte) (io.Reader, error) {
	if cipher == nil || cipher.aead == nil {
		return nil, fmt.Errorf("stream cipher is required")
	}
	if reader == nil {
		return nil, fmt.Errorf("stream cipher reader is required")
	}
	header := make([]byte, len(streamCipherMagic)+cipher.aead.NonceSize())
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, fmt.Errorf("read stream cipher header: %w", err)
	}
	for index, value := range streamCipherMagic {
		if header[index] != value {
			return nil, fmt.Errorf("invalid stream cipher header")
		}
	}
	return &streamCipherReader{
		reader:         reader,
		cipher:         cipher,
		nonce:          append([]byte(nil), header[len(streamCipherMagic):]...),
		associatedData: append([]byte(nil), associatedData...),
	}, nil
}

type streamCipherWriter struct {
	writer         io.Writer
	cipher         *StreamCipher
	nonce          []byte
	associatedData []byte
	sequence       uint64
}

func (writer *streamCipherWriter) Write(plain []byte) (int, error) {
	written := 0
	for len(plain) > 0 {
		length := len(plain)
		if length > streamCipherFrameBytes {
			length = streamCipherFrameBytes
		}
		part := plain[:length]
		sealed := writer.cipher.aead.Seal(nil, streamCipherNonce(writer.nonce, writer.sequence), part, streamCipherAssociatedData(writer.associatedData, writer.sequence))
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(sealed)))
		if err := writeAll(writer.writer, size[:]); err != nil {
			return written, err
		}
		if err := writeAll(writer.writer, sealed); err != nil {
			return written, err
		}
		writer.sequence++
		written += length
		plain = plain[length:]
	}
	return written, nil
}

type streamCipherReader struct {
	reader         io.Reader
	cipher         *StreamCipher
	nonce          []byte
	associatedData []byte
	sequence       uint64
	plain          []byte
}

func (reader *streamCipherReader) Read(destination []byte) (int, error) {
	for len(reader.plain) == 0 {
		if err := reader.nextFrame(); err != nil {
			return 0, err
		}
	}
	read := copy(destination, reader.plain)
	reader.plain = reader.plain[read:]
	return read, nil
}

func (reader *streamCipherReader) nextFrame() error {
	var size [4]byte
	read, err := io.ReadFull(reader.reader, size[:])
	if err != nil {
		if err == io.EOF && read == 0 {
			return io.EOF
		}
		return fmt.Errorf("read encrypted stream frame: %w", err)
	}
	length := binary.BigEndian.Uint32(size[:])
	if length < uint32(reader.cipher.aead.Overhead()) || length > streamCipherMaxFrameBytes {
		return fmt.Errorf("invalid encrypted stream frame length %d", length)
	}
	sealed := make([]byte, int(length))
	if _, err := io.ReadFull(reader.reader, sealed); err != nil {
		return fmt.Errorf("read encrypted stream frame: %w", err)
	}
	plain, err := reader.cipher.aead.Open(nil, streamCipherNonce(reader.nonce, reader.sequence), sealed, streamCipherAssociatedData(reader.associatedData, reader.sequence))
	if err != nil {
		return fmt.Errorf("authenticate encrypted stream frame: %w", err)
	}
	reader.sequence++
	reader.plain = plain
	return nil
}

func streamCipherNonce(base []byte, sequence uint64) []byte {
	nonce := append([]byte(nil), base...)
	binary.BigEndian.PutUint64(nonce[len(nonce)-8:], binary.BigEndian.Uint64(nonce[len(nonce)-8:])+sequence)
	return nonce
}

func streamCipherAssociatedData(associatedData []byte, sequence uint64) []byte {
	data := make([]byte, len(associatedData)+8)
	copy(data, associatedData)
	binary.BigEndian.PutUint64(data[len(associatedData):], sequence)
	return data
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}
