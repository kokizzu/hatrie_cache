package hatJournal

import (
	"bufio"
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

var (
	// ErrEncryptionKey reports an encrypted record without a configured key.
	ErrEncryptionKey = errors.New("hatJournal: encryption key is unavailable")
	// ErrEncryptionAuthentication reports a modified or incorrectly keyed record.
	ErrEncryptionAuthentication = errors.New("hatJournal: encrypted record authentication failed")
)

var encryptedRecordMagic = []byte{0x48, 0x4a, 0x45, 0x31}

const (
	encryptedRecordVersion     byte = 1
	encryptedRecordNonceBytes       = 12
	maxEncryptionKeyIDBytes         = 256
	maxEncryptedPlaintextBytes      = 1 << 30
)

// EncryptionOptions configures optional authenticated journal encryption.
// Key is the current append key. Keyring contains keys for existing key IDs so
// records written before a rotation remain readable. An empty value disables
// encryption and preserves the legacy journal format.
type EncryptionOptions struct {
	KeyID   string
	Key     []byte
	Keyring map[string][]byte
}

// Enabled reports whether encryption or encrypted-record decryption is
// configured.
func (options EncryptionOptions) Enabled() bool {
	return len(options.Key) > 0 || len(options.Keyring) > 0
}

// ValidateEncryptionOptions validates and clones key material so callers can
// safely reuse or mutate their input after opening a journal.
func ValidateEncryptionOptions(options EncryptionOptions) (EncryptionOptions, error) {
	if len(options.KeyID) > maxEncryptionKeyIDBytes {
		return EncryptionOptions{}, fmt.Errorf("hatJournal: encryption key ID must be at most %d bytes", maxEncryptionKeyIDBytes)
	}
	if len(options.Key) == 0 && options.KeyID != "" && len(options.Keyring) == 0 {
		return EncryptionOptions{}, errors.New("hatJournal: encryption key ID requires a current key or keyring")
	}
	if len(options.Key) > 0 && options.KeyID == "" {
		return EncryptionOptions{}, errors.New("hatJournal: current encryption key ID is required")
	}

	normalized := EncryptionOptions{KeyID: options.KeyID}
	if len(options.Key) > 0 {
		if err := validateEncryptionKey(options.KeyID, options.Key); err != nil {
			return EncryptionOptions{}, err
		}
		normalized.Key = append([]byte(nil), options.Key...)
	}
	if len(options.Keyring) > 0 {
		normalized.Keyring = make(map[string][]byte, len(options.Keyring))
		for keyID, key := range options.Keyring {
			if len(keyID) == 0 {
				return EncryptionOptions{}, errors.New("hatJournal: encryption keyring ID must not be empty")
			}
			if len(keyID) > maxEncryptionKeyIDBytes {
				return EncryptionOptions{}, fmt.Errorf("hatJournal: encryption keyring ID must be at most %d bytes", maxEncryptionKeyIDBytes)
			}
			if err := validateEncryptionKey(keyID, key); err != nil {
				return EncryptionOptions{}, err
			}
			normalized.Keyring[keyID] = append([]byte(nil), key...)
		}
	}
	if normalized.KeyID != "" && len(normalized.Key) > 0 {
		if key, ok := normalized.Keyring[normalized.KeyID]; ok && !bytes.Equal(key, normalized.Key) {
			return EncryptionOptions{}, fmt.Errorf("hatJournal: encryption key ID %q has conflicting current and keyring keys", normalized.KeyID)
		}
	}
	return normalized, nil
}

func validateEncryptionKey(keyID string, key []byte) error {
	if _, err := aes.NewCipher(key); err != nil {
		return fmt.Errorf("hatJournal: encryption key %q must be 16, 24, or 32 bytes", keyID)
	}
	return nil
}

// RecordEncryptor encrypts complete journal records using AES-GCM. It is
// reusable so append-heavy paths do not rebuild the cipher for every record.
type RecordEncryptor struct {
	keyID string
	aead  cipher.AEAD
}

// NewRecordEncryptor creates an append encryptor from the current key.
func NewRecordEncryptor(options EncryptionOptions) (*RecordEncryptor, error) {
	normalized, err := ValidateEncryptionOptions(options)
	if err != nil {
		return nil, err
	}
	if len(normalized.Key) == 0 {
		return nil, ErrEncryptionKey
	}
	aead, err := newEncryptionAEAD(normalized.Key)
	if err != nil {
		return nil, err
	}
	return &RecordEncryptor{keyID: normalized.KeyID, aead: aead}, nil
}

// WriteRecord writes one legacy-format record inside one authenticated frame.
// A nil encryptor writes the record unchanged.
func (encryptor *RecordEncryptor) WriteRecord(writer io.Writer, record []byte) (int, error) {
	if encryptor == nil {
		return writer.Write(record)
	}
	frame, err := encryptor.AppendRecord(nil, record)
	if err != nil {
		return 0, err
	}
	n, err := writer.Write(frame)
	if err == nil && n != len(frame) {
		err = io.ErrShortWrite
	}
	if err != nil {
		return n, err
	}
	return len(record), nil
}

// AppendRecord appends one authenticated encrypted frame to dst. A nil
// encryptor appends the legacy-format record unchanged.
func (encryptor *RecordEncryptor) AppendRecord(dst []byte, record []byte) ([]byte, error) {
	if encryptor == nil {
		return append(dst, record...), nil
	}
	if len(record) > maxEncryptedPlaintextBytes {
		return nil, fmt.Errorf("hatJournal: encrypted record exceeds %d bytes", maxEncryptedPlaintextBytes)
	}
	var nonce [encryptedRecordNonceBytes]byte
	if _, err := io.ReadFull(cryptorand.Reader, nonce[:]); err != nil {
		return nil, err
	}
	header := make([]byte, 0, len(encryptedRecordMagic)+1+binary.MaxVarintLen64+len(encryptor.keyID)+encryptedRecordNonceBytes+binary.MaxVarintLen64)
	header = append(header, encryptedRecordMagic...)
	header = append(header, encryptedRecordVersion)
	header = appendJournalUvarint(header, uint64(len(encryptor.keyID)))
	header = append(header, encryptor.keyID...)
	header = append(header, nonce[:]...)
	ciphertextLength := len(record) + encryptor.aead.Overhead()
	header = appendJournalUvarint(header, uint64(ciphertextLength))
	ciphertext := encryptor.aead.Seal(nil, nonce[:], record, header)
	dst = append(dst, header...)
	return append(dst, ciphertext...), nil
}

func newEncryptionAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func appendJournalUvarint(dst []byte, value uint64) []byte {
	var encoded [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(encoded[:], value)
	return append(dst, encoded[:n]...)
}

// OpenReaderWithEncryption opens a journal and decodes archived compression
// plus optional encrypted record frames. Legacy plaintext records are passed
// through, which permits a key rotation without rewriting the active file.
func OpenReaderWithEncryption(path string, options EncryptionOptions) (io.ReadCloser, *bufio.Reader, SegmentCompression, error) {
	normalized, err := ValidateEncryptionOptions(options)
	if err != nil {
		return nil, nil, SegmentCompressionNone, err
	}
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
	var source io.Reader = file
	var decoder *zstd.Decoder
	compression := SegmentCompressionNone
	if readBytes == len(magic) && bytes.Equal(magic[:], zstdFrameMagic) {
		decoder, err = zstd.NewReader(file, zstd.WithDecoderMaxMemory(maxZstdDecoderMemory))
		if err != nil {
			_ = file.Close()
			return nil, nil, SegmentCompressionNone, err
		}
		source = decoder
		compression = SegmentCompressionZstd
	}
	if normalized.Enabled() {
		framed, frameErr := newEncryptedFrameReader(source, normalized)
		if frameErr != nil {
			if decoder != nil {
				decoder.Close()
			}
			_ = file.Close()
			return nil, nil, SegmentCompressionNone, frameErr
		}
		source = framed
	}
	reader := &journalReader{file: file, decoder: decoder, source: source}
	return reader, bufio.NewReader(source), compression, nil
}

// OpenReader opens a journal without encryption, retaining the legacy API.
func OpenReader(path string) (io.ReadCloser, *bufio.Reader, SegmentCompression, error) {
	return OpenReaderWithEncryption(path, EncryptionOptions{})
}

type encryptedFrameReader struct {
	input               *bufio.Reader
	keys                map[string]cipher.AEAD
	pending             []byte
	pendingEncodedBytes int64
	rawPending          []byte
	rawRemaining        int64
	rawJSON             bool
	rawTail             bool
	physicalBytes       int64
}

func newEncryptedFrameReader(source io.Reader, options EncryptionOptions) (*encryptedFrameReader, error) {
	keys := make(map[string]cipher.AEAD, len(options.Keyring)+1)
	if len(options.Key) > 0 {
		aead, err := newEncryptionAEAD(options.Key)
		if err != nil {
			return nil, err
		}
		keys[options.KeyID] = aead
	}
	for keyID, key := range options.Keyring {
		if _, exists := keys[keyID]; exists {
			continue
		}
		aead, err := newEncryptionAEAD(key)
		if err != nil {
			return nil, err
		}
		keys[keyID] = aead
	}
	return &encryptedFrameReader{input: bufio.NewReader(source), keys: keys}, nil
}

func (reader *encryptedFrameReader) Read(data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	if len(reader.pending) > 0 {
		n := copy(data, reader.pending)
		reader.pending = reader.pending[n:]
		if len(reader.pending) == 0 {
			reader.physicalBytes += reader.pendingEncodedBytes
			reader.pendingEncodedBytes = 0
		}
		return n, nil
	}
	if len(reader.rawPending) > 0 {
		n := copy(data, reader.rawPending)
		reader.rawPending = reader.rawPending[n:]
		reader.physicalBytes += int64(n)
		return n, nil
	}
	if reader.rawRemaining > 0 {
		readLength := len(data)
		if int64(readLength) > reader.rawRemaining {
			readLength = int(reader.rawRemaining)
		}
		n, err := reader.input.Read(data[:readLength])
		reader.rawRemaining -= int64(n)
		reader.physicalBytes += int64(n)
		return n, err
	}
	if reader.rawTail {
		n, err := reader.input.Read(data)
		reader.physicalBytes += int64(n)
		return n, err
	}
	if reader.rawJSON {
		return reader.readLegacyJSON(data)
	}

	header, err := reader.input.Peek(len(encryptedRecordMagic))
	if len(header) == len(encryptedRecordMagic) && bytes.Equal(header, encryptedRecordMagic) {
		record, encodedBytes, frameErr := reader.readFrame()
		if frameErr != nil {
			return 0, frameErr
		}
		if len(record) == 0 {
			reader.physicalBytes += encodedBytes
			return 0, nil
		}
		reader.pending = record
		reader.pendingEncodedBytes = encodedBytes
		return reader.Read(data)
	}
	if len(header) == 0 && err != nil {
		return 0, err
	}
	binaryHeader, binaryErr := reader.input.Peek(len(binaryMagic))
	if len(binaryHeader) == len(binaryMagic) && bytes.Equal(binaryHeader, binaryMagic) {
		if startErr := reader.startLegacyBinary(); startErr != nil {
			return 0, startErr
		}
		return reader.Read(data)
	}
	if len(binaryHeader) == 0 && binaryErr != nil {
		return 0, binaryErr
	}
	reader.rawJSON = true
	return reader.Read(data)
}

func (reader *encryptedFrameReader) PhysicalBytes() int64 {
	return reader.physicalBytes
}

func (reader *encryptedFrameReader) startLegacyBinary() error {
	header, err := reader.input.Peek(len(binaryMagic))
	if err != nil {
		return err
	}
	reader.rawPending = append(reader.rawPending, header...)
	if _, err := reader.input.Discard(len(binaryMagic)); err != nil {
		return err
	}
	size, sizeBytes, complete, err := readUvarint(reader.input)
	reader.rawPending = append(reader.rawPending, sizeBytes...)
	if err != nil || !complete || size > uint64(^uint64(0)>>1) {
		reader.rawTail = true
		return nil
	}
	reader.rawRemaining = int64(size)
	return nil
}

func (reader *encryptedFrameReader) readLegacyJSON(data []byte) (int, error) {
	part, err := reader.input.ReadSlice('\n')
	if len(part) == 0 {
		return 0, err
	}
	n := copy(data, part)
	if n < len(part) {
		reader.rawPending = append(reader.rawPending, part[n:]...)
	}
	if part[len(part)-1] == '\n' {
		reader.rawJSON = false
	}
	reader.physicalBytes += int64(n)
	return n, nil
}

func (reader *encryptedFrameReader) readFrame() ([]byte, int64, error) {
	header := make([]byte, len(encryptedRecordMagic)+1)
	if err := readEncryptedBytes(reader.input, header); err != nil {
		return nil, 0, err
	}
	if !bytes.Equal(header[:len(encryptedRecordMagic)], encryptedRecordMagic) {
		return nil, 0, errors.New("hatJournal: invalid encrypted record magic")
	}
	if header[len(encryptedRecordMagic)] != encryptedRecordVersion {
		return nil, 0, fmt.Errorf("hatJournal: unsupported encrypted record version %d", header[len(encryptedRecordMagic)])
	}
	keyIDLength, keyIDBytes, err := readEncryptedUvarint(reader.input)
	if err != nil {
		return nil, 0, err
	}
	header = append(header, keyIDBytes...)
	if keyIDLength == 0 || keyIDLength > maxEncryptionKeyIDBytes {
		return nil, 0, fmt.Errorf("hatJournal: encrypted record key ID length %d is invalid", keyIDLength)
	}
	keyID := make([]byte, int(keyIDLength))
	if err := readEncryptedBytes(reader.input, keyID); err != nil {
		return nil, 0, err
	}
	header = append(header, keyID...)
	nonce := make([]byte, encryptedRecordNonceBytes)
	if err := readEncryptedBytes(reader.input, nonce); err != nil {
		return nil, 0, err
	}
	header = append(header, nonce...)
	ciphertextLength, ciphertextLengthBytes, err := readEncryptedUvarint(reader.input)
	if err != nil {
		return nil, 0, err
	}
	header = append(header, ciphertextLengthBytes...)
	aead, ok := reader.keys[string(keyID)]
	if !ok {
		return nil, 0, fmt.Errorf("%w: %q", ErrEncryptionKey, string(keyID))
	}
	maxCiphertextLength := uint64(maxEncryptedPlaintextBytes + aead.Overhead())
	if ciphertextLength < uint64(aead.Overhead()) || ciphertextLength > maxCiphertextLength {
		return nil, 0, fmt.Errorf("hatJournal: encrypted record payload length %d is invalid", ciphertextLength)
	}
	ciphertext := make([]byte, int(ciphertextLength))
	if err := readEncryptedBytes(reader.input, ciphertext); err != nil {
		return nil, 0, err
	}
	record, err := aead.Open(nil, nonce, ciphertext, header)
	if err != nil {
		return nil, 0, fmt.Errorf("%w: %q", ErrEncryptionAuthentication, string(keyID))
	}
	return record, int64(len(header)) + int64(len(ciphertext)), nil
}

func readEncryptedBytes(reader *bufio.Reader, data []byte) error {
	if _, err := io.ReadFull(reader, data); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return io.EOF
		}
		return err
	}
	return nil
}

func readEncryptedUvarint(reader *bufio.Reader) (uint64, []byte, error) {
	raw := make([]byte, 0, binary.MaxVarintLen64)
	for index := 0; index < binary.MaxVarintLen64; index++ {
		value, err := reader.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, nil, io.EOF
			}
			return 0, nil, err
		}
		raw = append(raw, value)
		if value < 0x80 {
			decoded, n := binary.Uvarint(raw)
			if n <= 0 {
				return 0, nil, errors.New("hatJournal: invalid encrypted record varint")
			}
			return decoded, raw, nil
		}
	}
	return 0, nil, errors.New("hatJournal: encrypted record varint is too long")
}
