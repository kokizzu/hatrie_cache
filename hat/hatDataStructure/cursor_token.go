package hatDataStructure

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
)

var (
	ErrCursorTokenCodecNil        = errors.New("hatDataStructure: cursor token codec is nil")
	ErrCursorTokenSecretInvalid   = errors.New("hatDataStructure: cursor token secret is invalid")
	ErrCursorTokenInvalid         = errors.New("hatDataStructure: cursor token is invalid")
	ErrCursorTokenAuthentication  = errors.New("hatDataStructure: cursor token authentication failed")
	ErrCursorTokenBindingMismatch = errors.New("hatDataStructure: cursor token binding mismatch")
)

const (
	MinCursorTokenSecretBytes = 16
	MaxCursorTokenIndexBytes  = 256
	MaxCursorTokenKeyBytes    = 16 << 10
	MaxCursorTokenBytes       = 24 << 10

	cursorTokenHeaderBytes = 4 + 1 + 2 + 4 + 8 + 8
	cursorTokenMACBytes    = sha256.Size
)

const cursorTokenMagic = "hct1"

// CursorToken is a verified continuation position for an ordered index. Key
// is copied on decode and may be passed to an index-specific key decoder.
type CursorToken struct {
	Index         string
	SchemaVersion uint64
	Key           []byte
	ID            uint64
}

// CursorTokenCodec signs bounded binary continuation tokens with a secret.
// The secret is copied by the constructor and is never included in a token.
type CursorTokenCodec struct {
	secret []byte
}

// NewCursorTokenCodec creates a codec. Secrets shorter than 16 bytes are
// rejected to avoid accidentally deploying a trivially guessable signer.
func NewCursorTokenCodec(secret []byte) (*CursorTokenCodec, error) {
	if len(secret) < MinCursorTokenSecretBytes {
		return nil, ErrCursorTokenSecretInvalid
	}
	return &CursorTokenCodec{secret: append([]byte(nil), secret...)}, nil
}

// Encode creates a URL-safe continuation token bound to index and schemaVersion.
func (codec *CursorTokenCodec) Encode(index string, schemaVersion uint64, key []byte, id uint64) (string, error) {
	if codec == nil {
		return "", ErrCursorTokenCodecNil
	}
	if len(codec.secret) < MinCursorTokenSecretBytes {
		return "", ErrCursorTokenSecretInvalid
	}
	index = strings.TrimSpace(index)
	if index == "" || len(index) > MaxCursorTokenIndexBytes {
		return "", fmt.Errorf("%w: index length must be between 1 and %d", ErrCursorTokenInvalid, MaxCursorTokenIndexBytes)
	}
	if len(key) > MaxCursorTokenKeyBytes {
		return "", fmt.Errorf("%w: key length must be <= %d", ErrCursorTokenInvalid, MaxCursorTokenKeyBytes)
	}
	payloadBytes := cursorTokenHeaderBytes + len(index) + len(key)
	raw := make([]byte, payloadBytes+cursorTokenMACBytes)
	copy(raw[:4], cursorTokenMagic)
	raw[4] = 1
	binary.BigEndian.PutUint16(raw[5:7], uint16(len(index)))
	binary.BigEndian.PutUint32(raw[7:11], uint32(len(key)))
	binary.BigEndian.PutUint64(raw[11:19], schemaVersion)
	binary.BigEndian.PutUint64(raw[19:27], id)
	offset := cursorTokenHeaderBytes
	copy(raw[offset:], index)
	offset += len(index)
	copy(raw[offset:], key)
	mac := hmac.New(sha256.New, codec.secret)
	_, _ = mac.Write(raw[:payloadBytes])
	copy(raw[payloadBytes:], mac.Sum(nil))
	token := base64.RawURLEncoding.EncodeToString(raw)
	if len(token) > MaxCursorTokenBytes {
		return "", fmt.Errorf("%w: encoded token exceeds %d bytes", ErrCursorTokenInvalid, MaxCursorTokenBytes)
	}
	return token, nil
}

// Decode verifies and decodes a continuation token without trusting its
// index, schema version, key, or row ID until the MAC has been checked.
func (codec *CursorTokenCodec) Decode(token string) (CursorToken, error) {
	if codec == nil {
		return CursorToken{}, ErrCursorTokenCodecNil
	}
	if len(codec.secret) < MinCursorTokenSecretBytes {
		return CursorToken{}, ErrCursorTokenSecretInvalid
	}
	if token == "" || len(token) > MaxCursorTokenBytes {
		return CursorToken{}, ErrCursorTokenInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) < cursorTokenHeaderBytes+cursorTokenMACBytes {
		return CursorToken{}, ErrCursorTokenInvalid
	}
	if string(raw[:4]) != cursorTokenMagic || raw[4] != 1 {
		return CursorToken{}, ErrCursorTokenInvalid
	}
	indexLength := int(binary.BigEndian.Uint16(raw[5:7]))
	keyLength := int(binary.BigEndian.Uint32(raw[7:11]))
	if indexLength == 0 || indexLength > MaxCursorTokenIndexBytes || keyLength > MaxCursorTokenKeyBytes {
		return CursorToken{}, ErrCursorTokenInvalid
	}
	payloadBytes := cursorTokenHeaderBytes + indexLength + keyLength
	if len(raw) != payloadBytes+cursorTokenMACBytes {
		return CursorToken{}, ErrCursorTokenInvalid
	}
	mac := hmac.New(sha256.New, codec.secret)
	_, _ = mac.Write(raw[:payloadBytes])
	if !hmac.Equal(raw[payloadBytes:], mac.Sum(nil)) {
		return CursorToken{}, ErrCursorTokenAuthentication
	}
	offset := cursorTokenHeaderBytes
	index := string(raw[offset : offset+indexLength])
	offset += indexLength
	key := append([]byte(nil), raw[offset:offset+keyLength]...)
	return CursorToken{
		Index:         index,
		SchemaVersion: binary.BigEndian.Uint64(raw[11:19]),
		Key:           key,
		ID:            binary.BigEndian.Uint64(raw[19:27]),
	}, nil
}

// DecodeFor verifies a token and its expected index/schema binding together.
func (codec *CursorTokenCodec) DecodeFor(token, index string, schemaVersion uint64) (CursorToken, error) {
	decoded, err := codec.Decode(token)
	if err != nil {
		return CursorToken{}, err
	}
	if decoded.Index != strings.TrimSpace(index) || decoded.SchemaVersion != schemaVersion {
		return CursorToken{}, ErrCursorTokenBindingMismatch
	}
	return decoded, nil
}
