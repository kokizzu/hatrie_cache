package hatBackup

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
)

const (
	// ObjectStoreEncryptionAlgorithm identifies the authenticated object
	// envelope used by encrypted object-store backups.
	ObjectStoreEncryptionAlgorithm = "AES-256-GCM-CHUNKED-V1"
	// DefaultObjectStoreEncryptionChunkSize bounds the transient plaintext and
	// ciphertext buffers used while an object is uploaded or restored.
	DefaultObjectStoreEncryptionChunkSize = 64 << 10

	objectStoreEncryptionVersion         byte = 1
	objectStorePayloadHeaderSize              = 20
	objectStoreManifestHeaderSize             = 20
	objectStoreEncryptionTagSize              = 16
	maxObjectStoreEncryptionKeyIDBytes        = 128
	maxObjectStoreEncryptionFrames            = uint64(^uint32(0))
	maxObjectStoreEncryptedManifestBytes      = DefaultObjectStoreManifestMaxBytes + objectStoreManifestHeaderSize + maxObjectStoreEncryptionKeyIDBytes + objectStoreEncryptionTagSize
)

var (
	objectStorePayloadMagic  = [4]byte{'h', 'b', 'p', '1'}
	objectStoreManifestMagic = [4]byte{'h', 'b', 'm', '1'}
	// ErrObjectStoreEncryptionInvalid is returned for malformed encrypted
	// envelopes, unavailable keys, and authentication failures.
	ErrObjectStoreEncryptionInvalid = errors.New("hatriecache: object store encryption is invalid")
)

// ObjectStoreEncryptionKey is one AES-256 key in an object-store target's
// keyring. Keep retired keys in the keyring while backups written with them
// remain within the recovery window.
type ObjectStoreEncryptionKey struct {
	ID  string
	Key []byte
}

// ObjectStoreTargetOptions configures optional object-store encryption and
// payload addressing. An empty keyring preserves the historical unencrypted
// object format. When a keyring is provided, ActiveEncryptionKeyID selects the
// key for new backups; all keys remain available for restoring older rotated
// backups. ObjectStoreLayoutAuto keeps path addressing for snapshots and selects
// content-addressed objects for incremental Pebble backups.
type ObjectStoreTargetOptions struct {
	EncryptionKeys        []ObjectStoreEncryptionKey
	ActiveEncryptionKeyID string
	Layout                ObjectStoreLayout
}

type objectStoreEncryptionConfig struct {
	keys      map[string][]byte
	activeKey string
}

func newObjectStoreEncryptionConfig(options ObjectStoreTargetOptions) (*objectStoreEncryptionConfig, error) {
	active := strings.TrimSpace(options.ActiveEncryptionKeyID)
	if len(options.EncryptionKeys) == 0 {
		if active != "" {
			return nil, fmt.Errorf("%w: active key requires at least one encryption key", ErrObjectStoreEncryptionInvalid)
		}
		return nil, nil
	}
	if active == "" {
		return nil, fmt.Errorf("%w: active encryption key id is required", ErrObjectStoreEncryptionInvalid)
	}
	config := &objectStoreEncryptionConfig{keys: make(map[string][]byte, len(options.EncryptionKeys)), activeKey: active}
	for _, input := range options.EncryptionKeys {
		id, err := normalizeObjectStoreEncryptionKeyID(input.ID)
		if err != nil {
			return nil, err
		}
		if _, exists := config.keys[id]; exists {
			return nil, fmt.Errorf("%w: duplicate encryption key id %q", ErrObjectStoreEncryptionInvalid, id)
		}
		if len(input.Key) != 32 {
			return nil, fmt.Errorf("%w: encryption key %q must be 32 bytes", ErrObjectStoreEncryptionInvalid, id)
		}
		config.keys[id] = append([]byte(nil), input.Key...)
	}
	if _, exists := config.keys[active]; !exists {
		return nil, fmt.Errorf("%w: active encryption key %q is not in the keyring", ErrObjectStoreEncryptionInvalid, active)
	}
	return config, nil
}

func normalizeObjectStoreEncryptionKeyID(value string) (string, error) {
	id := strings.TrimSpace(value)
	if id == "" {
		return "", fmt.Errorf("%w: encryption key id is required", ErrObjectStoreEncryptionInvalid)
	}
	if len([]byte(id)) > maxObjectStoreEncryptionKeyIDBytes {
		return "", fmt.Errorf("%w: encryption key id must be <= %d bytes", ErrObjectStoreEncryptionInvalid, maxObjectStoreEncryptionKeyIDBytes)
	}
	return id, nil
}

func (config *objectStoreEncryptionConfig) active() (string, []byte, error) {
	if config == nil {
		return "", nil, fmt.Errorf("%w: encryption is disabled", ErrObjectStoreEncryptionInvalid)
	}
	key, ok := config.keys[config.activeKey]
	if !ok {
		return "", nil, fmt.Errorf("%w: active encryption key %q is unavailable", ErrObjectStoreEncryptionInvalid, config.activeKey)
	}
	return config.activeKey, key, nil
}

func (config *objectStoreEncryptionConfig) lookup(id string) ([]byte, bool) {
	if config == nil {
		return nil, false
	}
	key, ok := config.keys[id]
	return key, ok
}

func encryptionMetadataForKey(keyID string) *EncryptionMetadata {
	return &EncryptionMetadata{
		Algorithm: ObjectStoreEncryptionAlgorithm,
		KeyID:     keyID,
		ChunkSize: DefaultObjectStoreEncryptionChunkSize,
	}
}

func validateObjectStoreEncryptionMetadata(metadata *EncryptionMetadata) error {
	if metadata == nil {
		return nil
	}
	if metadata.Algorithm != ObjectStoreEncryptionAlgorithm {
		return fmt.Errorf("%w: unsupported algorithm %q", ErrObjectStoreEncryptionInvalid, metadata.Algorithm)
	}
	id, err := normalizeObjectStoreEncryptionKeyID(metadata.KeyID)
	if err != nil {
		return err
	}
	if id != metadata.KeyID {
		return fmt.Errorf("%w: encryption key id has surrounding whitespace", ErrObjectStoreEncryptionInvalid)
	}
	if metadata.ChunkSize != DefaultObjectStoreEncryptionChunkSize {
		return fmt.Errorf("%w: unsupported encryption chunk size %d", ErrObjectStoreEncryptionInvalid, metadata.ChunkSize)
	}
	return nil
}

func newObjectStoreAESGCM(key []byte) (cipher.AEAD, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("%w: encryption key must be 32 bytes", ErrObjectStoreEncryptionInvalid)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("%w: create AES cipher: %v", ErrObjectStoreEncryptionInvalid, err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("%w: create GCM: %v", ErrObjectStoreEncryptionInvalid, err)
	}
	return aead, nil
}

func newObjectStoreNonce(size int) ([]byte, error) {
	nonce := make([]byte, size)
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("%w: generate nonce: %v", ErrObjectStoreEncryptionInvalid, err)
	}
	return nonce, nil
}

func objectStoreFrameNonce(base []byte, frame uint32) []byte {
	nonce := append([]byte(nil), base...)
	binary.BigEndian.PutUint32(nonce[len(nonce)-4:], frame)
	return nonce
}

func objectStorePayloadAAD(relative string) []byte {
	return []byte("hatrie-cache/object-store/payload/v1/" + relative)
}

func encryptedObjectSize(plainSize int64, chunkSize int) (int64, error) {
	if plainSize < 0 {
		return 0, fmt.Errorf("%w: negative plaintext size", ErrObjectStoreEncryptionInvalid)
	}
	if chunkSize < 1 {
		return 0, fmt.Errorf("%w: encryption chunk size must be positive", ErrObjectStoreEncryptionInvalid)
	}
	chunks := uint64(plainSize) / uint64(chunkSize)
	if plainSize%int64(chunkSize) != 0 {
		chunks++
	}
	if chunks > maxObjectStoreEncryptionFrames {
		return 0, fmt.Errorf("%w: plaintext requires too many encryption frames", ErrObjectStoreEncryptionInvalid)
	}
	overhead := uint64(objectStorePayloadHeaderSize) + chunks*uint64(4+objectStoreEncryptionTagSize)
	total := uint64(plainSize) + overhead
	if total > uint64(math.MaxInt64) {
		return 0, fmt.Errorf("%w: encrypted object size overflows int64", ErrObjectStoreEncryptionInvalid)
	}
	return int64(total), nil
}

type objectStorePayloadEncryptReader struct {
	ctx        context.Context
	reader     io.Reader
	aead       cipher.AEAD
	baseNonce  []byte
	aad        []byte
	chunkSize  int
	remaining  int64
	frame      uint32
	headerSent bool
	output     []byte
	outputPos  int
	done       bool
}

func newObjectStorePayloadEncryptReader(ctx context.Context, reader io.Reader, relative string, size int64, key []byte, chunkSize int) (*objectStorePayloadEncryptReader, error) {
	aead, err := newObjectStoreAESGCM(key)
	if err != nil {
		return nil, err
	}
	if _, err := encryptedObjectSize(size, chunkSize); err != nil {
		return nil, err
	}
	nonce, err := newObjectStoreNonce(aead.NonceSize())
	if err != nil {
		return nil, err
	}
	header := make([]byte, objectStorePayloadHeaderSize)
	copy(header[:4], objectStorePayloadMagic[:])
	header[4] = objectStoreEncryptionVersion
	copy(header[8:], nonce)
	return &objectStorePayloadEncryptReader{
		ctx:       ctx,
		reader:    reader,
		aead:      aead,
		baseNonce: nonce,
		aad:       objectStorePayloadAAD(relative),
		chunkSize: chunkSize,
		remaining: size,
		output:    header,
	}, nil
}

func (reader *objectStorePayloadEncryptReader) Read(destination []byte) (int, error) {
	if len(destination) == 0 {
		return 0, nil
	}
	for {
		if reader.outputPos < len(reader.output) {
			n := copy(destination, reader.output[reader.outputPos:])
			reader.outputPos += n
			return n, nil
		}
		reader.output = nil
		reader.outputPos = 0
		if reader.done {
			return 0, io.EOF
		}
		if err := checkObjectStoreContext(reader.ctx); err != nil {
			return 0, err
		}
		if !reader.headerSent {
			reader.headerSent = true
			continue
		}
		if reader.remaining == 0 {
			reader.done = true
			return 0, io.EOF
		}
		chunkLength := int64(reader.chunkSize)
		if chunkLength > reader.remaining {
			chunkLength = reader.remaining
		}
		plaintext := make([]byte, chunkLength)
		if _, err := io.ReadFull(reader.reader, plaintext); err != nil {
			return 0, fmt.Errorf("%w: read plaintext frame: %v", ErrObjectStoreEncryptionInvalid, err)
		}
		if reader.frame == ^uint32(0) && reader.remaining > chunkLength {
			return 0, fmt.Errorf("%w: too many encryption frames", ErrObjectStoreEncryptionInvalid)
		}
		nonce := objectStoreFrameNonce(reader.baseNonce, reader.frame)
		sealed := reader.aead.Seal(nil, nonce, plaintext, reader.aad)
		reader.output = make([]byte, 4+len(sealed))
		binary.BigEndian.PutUint32(reader.output[:4], uint32(len(plaintext)))
		copy(reader.output[4:], sealed)
		reader.remaining -= chunkLength
		reader.frame++
	}
}

type objectStorePayloadDecryptReader struct {
	ctx        context.Context
	reader     io.Reader
	aead       cipher.AEAD
	baseNonce  []byte
	aad        []byte
	chunkSize  int
	expected   int64
	consumed   int64
	frame      uint32
	headerRead bool
	output     []byte
	outputPos  int
	done       bool
}

func newObjectStorePayloadDecryptReader(ctx context.Context, reader io.Reader, relative string, metadata EncryptionMetadata, size int64, key []byte) (*objectStorePayloadDecryptReader, error) {
	if err := validateObjectStoreEncryptionMetadata(&metadata); err != nil {
		return nil, err
	}
	if size < 0 {
		return nil, fmt.Errorf("%w: negative plaintext size", ErrObjectStoreEncryptionInvalid)
	}
	aead, err := newObjectStoreAESGCM(key)
	if err != nil {
		return nil, err
	}
	return &objectStorePayloadDecryptReader{
		ctx:       ctx,
		reader:    reader,
		aead:      aead,
		aad:       objectStorePayloadAAD(relative),
		chunkSize: metadata.ChunkSize,
		expected:  size,
	}, nil
}

func (reader *objectStorePayloadDecryptReader) Read(destination []byte) (int, error) {
	if len(destination) == 0 {
		return 0, nil
	}
	for {
		if reader.outputPos < len(reader.output) {
			n := copy(destination, reader.output[reader.outputPos:])
			reader.outputPos += n
			return n, nil
		}
		reader.output = nil
		reader.outputPos = 0
		if reader.done {
			return 0, io.EOF
		}
		if err := checkObjectStoreContext(reader.ctx); err != nil {
			return 0, err
		}
		if !reader.headerRead {
			if err := reader.readHeader(); err != nil {
				return 0, err
			}
		}
		if reader.consumed == reader.expected {
			var extra [1]byte
			n, err := io.ReadFull(reader.reader, extra[:])
			if n > 0 {
				return 0, fmt.Errorf("%w: encrypted payload has trailing bytes", ErrObjectStoreEncryptionInvalid)
			}
			if err == io.EOF {
				reader.done = true
				return 0, io.EOF
			}
			if err != nil {
				return 0, fmt.Errorf("%w: inspect encrypted payload trailer: %v", ErrObjectStoreEncryptionInvalid, err)
			}
			continue
		}
		var lengthBytes [4]byte
		if _, err := io.ReadFull(reader.reader, lengthBytes[:]); err != nil {
			return 0, fmt.Errorf("%w: read encrypted frame length: %v", ErrObjectStoreEncryptionInvalid, err)
		}
		length := int64(binary.BigEndian.Uint32(lengthBytes[:]))
		if length < 1 || length > int64(reader.chunkSize) || length > reader.expected-reader.consumed {
			return 0, fmt.Errorf("%w: invalid encrypted frame length %d", ErrObjectStoreEncryptionInvalid, length)
		}
		ciphertext := make([]byte, int(length)+objectStoreEncryptionTagSize)
		if _, err := io.ReadFull(reader.reader, ciphertext); err != nil {
			return 0, fmt.Errorf("%w: read encrypted frame: %v", ErrObjectStoreEncryptionInvalid, err)
		}
		if reader.frame == ^uint32(0) && reader.expected-reader.consumed > length {
			return 0, fmt.Errorf("%w: too many encrypted frames", ErrObjectStoreEncryptionInvalid)
		}
		nonce := objectStoreFrameNonce(reader.baseNonce, reader.frame)
		plaintext, err := reader.aead.Open(nil, nonce, ciphertext, reader.aad)
		if err != nil {
			return 0, fmt.Errorf("%w: authenticate encrypted frame: %v", ErrObjectStoreEncryptionInvalid, err)
		}
		if int64(len(plaintext)) != length {
			return 0, fmt.Errorf("%w: decrypted frame length mismatch", ErrObjectStoreEncryptionInvalid)
		}
		reader.consumed += length
		reader.frame++
		reader.output = plaintext
	}
}

func (reader *objectStorePayloadDecryptReader) readHeader() error {
	header := make([]byte, objectStorePayloadHeaderSize)
	if _, err := io.ReadFull(reader.reader, header); err != nil {
		return fmt.Errorf("%w: read encrypted payload header: %v", ErrObjectStoreEncryptionInvalid, err)
	}
	if !bytes.Equal(header[:4], objectStorePayloadMagic[:]) || header[4] != objectStoreEncryptionVersion {
		return fmt.Errorf("%w: unsupported encrypted payload header", ErrObjectStoreEncryptionInvalid)
	}
	reader.baseNonce = append([]byte(nil), header[8:]...)
	if len(reader.baseNonce) != reader.aead.NonceSize() {
		return fmt.Errorf("%w: invalid encrypted payload nonce", ErrObjectStoreEncryptionInvalid)
	}
	reader.headerRead = true
	return nil
}

func encryptObjectStoreManifest(data []byte, keyID string, key []byte) ([]byte, error) {
	aead, err := newObjectStoreAESGCM(key)
	if err != nil {
		return nil, err
	}
	id, err := normalizeObjectStoreEncryptionKeyID(keyID)
	if err != nil {
		return nil, err
	}
	nonce, err := newObjectStoreNonce(aead.NonceSize())
	if err != nil {
		return nil, err
	}
	header := make([]byte, objectStoreManifestHeaderSize+len(id))
	copy(header[:4], objectStoreManifestMagic[:])
	header[4] = objectStoreEncryptionVersion
	binary.BigEndian.PutUint16(header[6:8], uint16(len(id)))
	copy(header[8:20], nonce)
	copy(header[20:], id)
	sealed := aead.Seal(nil, nonce, data, header)
	return append(header, sealed...), nil
}

func decryptObjectStoreManifest(data []byte, config *objectStoreEncryptionConfig) ([]byte, string, bool, error) {
	if len(data) < len(objectStoreManifestMagic) || !bytes.Equal(data[:4], objectStoreManifestMagic[:]) {
		return data, "", false, nil
	}
	if len(data) < objectStoreManifestHeaderSize+objectStoreEncryptionTagSize {
		return nil, "", true, fmt.Errorf("%w: encrypted manifest is truncated", ErrObjectStoreEncryptionInvalid)
	}
	if data[4] != objectStoreEncryptionVersion {
		return nil, "", true, fmt.Errorf("%w: unsupported encrypted manifest version", ErrObjectStoreEncryptionInvalid)
	}
	keyIDLength := int(binary.BigEndian.Uint16(data[6:8]))
	if keyIDLength < 1 || keyIDLength > maxObjectStoreEncryptionKeyIDBytes {
		return nil, "", true, fmt.Errorf("%w: invalid encrypted manifest key id length", ErrObjectStoreEncryptionInvalid)
	}
	headerLength := objectStoreManifestHeaderSize + keyIDLength
	if len(data) < headerLength+objectStoreEncryptionTagSize {
		return nil, "", true, fmt.Errorf("%w: encrypted manifest is truncated", ErrObjectStoreEncryptionInvalid)
	}
	keyID := string(data[objectStoreManifestHeaderSize:headerLength])
	if normalized, err := normalizeObjectStoreEncryptionKeyID(keyID); err != nil || normalized != keyID {
		return nil, "", true, fmt.Errorf("%w: invalid encrypted manifest key id", ErrObjectStoreEncryptionInvalid)
	}
	key, ok := config.lookup(keyID)
	if !ok {
		return nil, keyID, true, fmt.Errorf("%w: encryption key %q is not available", ErrObjectStoreEncryptionInvalid, keyID)
	}
	aead, err := newObjectStoreAESGCM(key)
	if err != nil {
		return nil, keyID, true, err
	}
	nonce := data[8:20]
	plaintext, err := aead.Open(nil, nonce, data[headerLength:], data[:headerLength])
	if err != nil {
		return nil, keyID, true, fmt.Errorf("%w: authenticate encrypted manifest: %v", ErrObjectStoreEncryptionInvalid, err)
	}
	return plaintext, keyID, true, nil
}
