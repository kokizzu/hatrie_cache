// Package hatPagination provides authenticated continuation tokens for ordered
// cross-request pagination.
package hatPagination

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash"
	"sync"
	"time"
)

const (
	defaultTokenTTL   = 15 * time.Minute
	defaultMaxKeySize = 4 << 10
	maxNamespaceSize  = 1<<16 - 1
	maxEncodedKeySize = 1<<16 - 1
	tokenHeaderSize   = 4 + 1 + 8 + 8 + 2
	tokenMACSize      = 16
	tokenFormat       = byte(1)
)

var (
	ErrAuthentication   = errors.New("pagination token authentication failed")
	ErrExpired          = errors.New("pagination token expired")
	ErrInvalidConfig    = errors.New("invalid pagination token configuration")
	ErrInvalidNamespace = errors.New("invalid pagination token namespace")
	ErrInvalidTime      = errors.New("pagination token time must be Unix-compatible")
	ErrKeyTooLarge      = errors.New("pagination token key is too large")
	ErrMalformed        = errors.New("malformed pagination token")
	ErrVersionMismatch  = errors.New("pagination token schema version mismatch")
	ErrWeakSecret       = errors.New("pagination token secret is too short")
)

// Config controls token expiry and the maximum serialized cursor key.
// A negative TTL disables expiry; zero selects DefaultConfig's TTL.
type Config struct {
	TTL         time.Duration
	MaxKeyBytes int
}

// DefaultConfig returns bounded defaults suitable for HTTP pagination.
func DefaultConfig() Config {
	return Config{TTL: defaultTokenTTL, MaxKeyBytes: defaultMaxKeySize}
}

// Codec signs and validates continuation tokens. A Codec is safe for concurrent
// use after construction.
type Codec struct {
	secret      []byte
	ttl         time.Duration
	maxKeyBytes int
	macPool     *sync.Pool
}

// NewCodec creates a token codec. The secret must contain at least 16 bytes;
// it is copied so later caller mutation cannot change authentication behavior.
func NewCodec(secret []byte, config Config) (*Codec, error) {
	if len(secret) < 16 {
		return nil, ErrWeakSecret
	}
	if config.TTL == 0 {
		config.TTL = defaultTokenTTL
	}
	if config.MaxKeyBytes == 0 {
		config.MaxKeyBytes = defaultMaxKeySize
	}
	if config.MaxKeyBytes < 1 || config.MaxKeyBytes > maxEncodedKeySize {
		return nil, ErrInvalidConfig
	}
	codec := &Codec{
		secret:      append([]byte(nil), secret...),
		ttl:         config.TTL,
		maxKeyBytes: config.MaxKeyBytes,
		macPool:     &sync.Pool{},
	}
	codec.macPool.New = func() any {
		return hmac.New(sha256.New, codec.secret)
	}
	return codec, nil
}

// Encode returns a compact binary token bound to namespace and schemaVersion.
// The namespace is authenticated but not repeated in the token payload.
func (c *Codec) Encode(namespace string, schemaVersion uint64, key []byte, now time.Time) ([]byte, error) {
	if err := c.validate(namespace, key); err != nil {
		return nil, err
	}
	expires, err := c.expiry(now)
	if err != nil {
		return nil, err
	}
	token := make([]byte, tokenHeaderSize+len(key)+tokenMACSize)
	payload := token[:len(token)-tokenMACSize]
	copy(payload[:4], "HPT1")
	payload[4] = tokenFormat
	binary.BigEndian.PutUint64(payload[5:13], expires)
	binary.BigEndian.PutUint64(payload[13:21], schemaVersion)
	binary.BigEndian.PutUint16(payload[21:23], uint16(len(key)))
	copy(payload[tokenHeaderSize:], key)
	c.macInto(namespace, payload, token[len(payload):])
	return token, nil
}

// Decode authenticates and validates a binary token, returning an owned copy
// of the cursor key.
func (c *Codec) Decode(token []byte, namespace string, schemaVersion uint64, now time.Time) ([]byte, error) {
	if !c.valid() {
		return nil, ErrInvalidConfig
	}
	if err := c.validateNamespace(namespace); err != nil {
		return nil, err
	}
	if len(token) < tokenHeaderSize+tokenMACSize {
		return nil, ErrMalformed
	}
	payloadSize := len(token) - tokenMACSize
	if payloadSize > tokenHeaderSize+c.maxKeyBytes {
		return nil, ErrKeyTooLarge
	}
	payload := token[:payloadSize]
	var expectedMAC [tokenMACSize]byte
	c.macInto(namespace, payload, expectedMAC[:])
	if !hmac.Equal(expectedMAC[:], token[payloadSize:]) {
		return nil, ErrAuthentication
	}
	if string(payload[:4]) != "HPT1" || payload[4] != tokenFormat {
		return nil, ErrMalformed
	}
	if binary.BigEndian.Uint64(payload[13:21]) != schemaVersion {
		return nil, ErrVersionMismatch
	}
	if err := validateExpiry(binary.BigEndian.Uint64(payload[5:13]), now); err != nil {
		return nil, err
	}
	keySize := int(binary.BigEndian.Uint16(payload[21:23]))
	if keySize > c.maxKeyBytes || keySize != payloadSize-tokenHeaderSize {
		return nil, ErrMalformed
	}
	return append([]byte(nil), payload[tokenHeaderSize:]...), nil
}

// EncodeText returns an unpadded URL-safe representation for query strings
// and path segments.
func (c *Codec) EncodeText(namespace string, schemaVersion uint64, key []byte, now time.Time) (string, error) {
	token, err := c.Encode(namespace, schemaVersion, key, now)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(token), nil
}

// DecodeText decodes an unpadded URL-safe token and applies the same checks as
// Decode.
func (c *Codec) DecodeText(token string, namespace string, schemaVersion uint64, now time.Time) ([]byte, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, ErrMalformed
	}
	return c.Decode(raw, namespace, schemaVersion, now)
}

func (c *Codec) validate(namespace string, key []byte) error {
	if !c.valid() {
		return ErrInvalidConfig
	}
	if err := c.validateNamespace(namespace); err != nil {
		return err
	}
	if len(key) > c.maxKeyBytes {
		return ErrKeyTooLarge
	}
	return nil
}

func (c *Codec) valid() bool {
	return c != nil && c.macPool != nil && len(c.secret) >= 16 && c.maxKeyBytes >= 1 && c.maxKeyBytes <= maxEncodedKeySize
}

func (c *Codec) validateNamespace(namespace string) error {
	if len(namespace) == 0 || len(namespace) > maxNamespaceSize {
		return ErrInvalidNamespace
	}
	return nil
}

func (c *Codec) expiry(now time.Time) (uint64, error) {
	if c.ttl < 0 {
		return 0, nil
	}
	unix := now.Unix()
	if unix < 0 {
		return 0, ErrInvalidTime
	}
	seconds := uint64(c.ttl / time.Second)
	if c.ttl%time.Second != 0 {
		seconds++
	}
	if seconds > ^uint64(0)-uint64(unix) {
		return 0, ErrInvalidTime
	}
	return uint64(unix) + seconds, nil
}

func validateExpiry(expires uint64, now time.Time) error {
	if expires == 0 {
		return nil
	}
	unix := now.Unix()
	if unix < 0 {
		return ErrInvalidTime
	}
	if uint64(unix) >= expires {
		return ErrExpired
	}
	return nil
}

func (c *Codec) macInto(namespace string, payload []byte, destination []byte) {
	h := c.macPool.Get().(hash.Hash)
	h.Reset()
	h.Write([]byte("hatrie-pagination-token-v1"))
	var length [2]byte
	binary.BigEndian.PutUint16(length[:], uint16(len(namespace)))
	h.Write(length[:])
	h.Write([]byte(namespace))
	h.Write(payload)
	var digest [sha256.Size]byte
	sum := h.Sum(digest[:0])
	copy(destination, sum[:tokenMACSize])
	c.macPool.Put(h)
}
