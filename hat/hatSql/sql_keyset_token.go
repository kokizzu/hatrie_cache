package hatSql

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"time"
)

const (
	// DefaultSQLKeysetTokenMaxAge bounds how long a stateless keyset cursor is
	// accepted when the caller does not provide an explicit age.
	DefaultSQLKeysetTokenMaxAge  = 15 * time.Minute
	MinSQLKeysetTokenSecretBytes = 16
	MaxSQLKeysetCursorBytes      = 96 << 10
	MaxSQLKeysetTokenBytes       = 128 << 10

	sqlKeysetTokenHeaderBytes = 4 + 1 + 8 + 4
	sqlKeysetTokenMACBytes    = sha256.Size
)

const sqlKeysetTokenMagic = "hks1"

var (
	ErrSQLKeysetTokenCodecNil       = errors.New("hatSql: SQL keyset token codec is nil")
	ErrSQLKeysetTokenSecretInvalid  = errors.New("hatSql: SQL keyset token secret is invalid")
	ErrSQLKeysetTokenMaxAgeInvalid  = errors.New("hatSql: SQL keyset token max age is invalid")
	ErrSQLKeysetTokenInvalid        = errors.New("hatSql: SQL keyset token is invalid")
	ErrSQLKeysetTokenAuthentication = errors.New("hatSql: SQL keyset token authentication failed")
	ErrSQLKeysetTokenExpired        = errors.New("hatSql: SQL keyset token expired")
)

// SQLKeysetTokenCodecOptions configures signed keyset cursor handling.
// Secret must remain private to the service. Tokens authenticate a cursor but
// do not replace query authorization.
type SQLKeysetTokenCodecOptions struct {
	Secret []byte
	MaxAge time.Duration
	Now    func() time.Time
}

// SQLKeysetToken is the verified cursor carried by a signed keyset token.
type SQLKeysetToken struct {
	Cursor   string
	IssuedAt time.Time
}

// SQLKeysetTokenCodec signs and verifies bounded, stateless keyset cursors.
// The codec retains only a copy of the signing secret and a clock.
type SQLKeysetTokenCodec struct {
	secret []byte
	maxAge time.Duration
	now    func() time.Time
}

// NewSQLKeysetTokenCodec creates a keyset token codec. A zero MaxAge uses
// DefaultSQLKeysetTokenMaxAge; a negative MaxAge is rejected.
func NewSQLKeysetTokenCodec(options SQLKeysetTokenCodecOptions) (*SQLKeysetTokenCodec, error) {
	if len(options.Secret) < MinSQLKeysetTokenSecretBytes {
		return nil, ErrSQLKeysetTokenSecretInvalid
	}
	maxAge := options.MaxAge
	if maxAge == 0 {
		maxAge = DefaultSQLKeysetTokenMaxAge
	}
	if maxAge < 0 {
		return nil, ErrSQLKeysetTokenMaxAgeInvalid
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &SQLKeysetTokenCodec{
		secret: append([]byte(nil), options.Secret...),
		maxAge: maxAge,
		now:    now,
	}, nil
}

func (codec *SQLKeysetTokenCodec) validate() error {
	if codec == nil {
		return ErrSQLKeysetTokenCodecNil
	}
	if len(codec.secret) < MinSQLKeysetTokenSecretBytes {
		return ErrSQLKeysetTokenSecretInvalid
	}
	if codec.maxAge <= 0 || codec.now == nil {
		return ErrSQLKeysetTokenMaxAgeInvalid
	}
	return nil
}

// Encode creates a signed, URL-safe token for a raw SQL keyset cursor.
func (codec *SQLKeysetTokenCodec) Encode(cursor string) (string, error) {
	if err := codec.validate(); err != nil {
		return "", err
	}
	if cursor == "" || len(cursor) > MaxSQLKeysetCursorBytes {
		return "", ErrSQLKeysetTokenInvalid
	}
	issuedAt := codec.now().UTC()
	issuedAtNanos := issuedAt.UnixNano()
	if issuedAtNanos < 0 {
		return "", ErrSQLKeysetTokenInvalid
	}
	payloadBytes := sqlKeysetTokenHeaderBytes + len(cursor)
	raw := make([]byte, payloadBytes+sqlKeysetTokenMACBytes)
	copy(raw[:4], sqlKeysetTokenMagic)
	raw[4] = 1
	binary.BigEndian.PutUint64(raw[5:13], uint64(issuedAtNanos))
	binary.BigEndian.PutUint32(raw[13:17], uint32(len(cursor)))
	copy(raw[17:payloadBytes], cursor)
	mac := hmac.New(sha256.New, codec.secret)
	_, _ = mac.Write(raw[:payloadBytes])
	copy(raw[payloadBytes:], mac.Sum(nil))
	token := base64.RawURLEncoding.EncodeToString(raw)
	if len(token) > MaxSQLKeysetTokenBytes {
		return "", ErrSQLKeysetTokenInvalid
	}
	return token, nil
}

// Decode verifies a token before returning its cursor or timestamp.
func (codec *SQLKeysetTokenCodec) Decode(token string) (SQLKeysetToken, error) {
	if err := codec.validate(); err != nil {
		return SQLKeysetToken{}, err
	}
	if token == "" || len(token) > MaxSQLKeysetTokenBytes {
		return SQLKeysetToken{}, ErrSQLKeysetTokenInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) < sqlKeysetTokenHeaderBytes+sqlKeysetTokenMACBytes {
		return SQLKeysetToken{}, ErrSQLKeysetTokenInvalid
	}
	// Raw base64 permits alternate spellings when unused trailing bits are
	// non-zero. Reject those aliases before HMAC verification so a tampered
	// token can never decode to the same authenticated bytes.
	if base64.RawURLEncoding.EncodeToString(raw) != token {
		return SQLKeysetToken{}, ErrSQLKeysetTokenAuthentication
	}
	if string(raw[:4]) != sqlKeysetTokenMagic || raw[4] != 1 {
		return SQLKeysetToken{}, ErrSQLKeysetTokenInvalid
	}
	cursorBytes := int(binary.BigEndian.Uint32(raw[13:17]))
	if cursorBytes == 0 || cursorBytes > MaxSQLKeysetCursorBytes {
		return SQLKeysetToken{}, ErrSQLKeysetTokenInvalid
	}
	payloadBytes := sqlKeysetTokenHeaderBytes + cursorBytes
	if len(raw) != payloadBytes+sqlKeysetTokenMACBytes {
		return SQLKeysetToken{}, ErrSQLKeysetTokenInvalid
	}
	mac := hmac.New(sha256.New, codec.secret)
	_, _ = mac.Write(raw[:payloadBytes])
	if !hmac.Equal(raw[payloadBytes:], mac.Sum(nil)) {
		return SQLKeysetToken{}, ErrSQLKeysetTokenAuthentication
	}
	issuedAtNanos := binary.BigEndian.Uint64(raw[5:13])
	if issuedAtNanos > uint64(1<<63-1) {
		return SQLKeysetToken{}, ErrSQLKeysetTokenInvalid
	}
	issuedAt := time.Unix(0, int64(issuedAtNanos)).UTC()
	now := codec.now().UTC()
	if now.Before(issuedAt) {
		return SQLKeysetToken{}, ErrSQLKeysetTokenInvalid
	}
	if now.Sub(issuedAt) > codec.maxAge {
		return SQLKeysetToken{}, ErrSQLKeysetTokenExpired
	}
	return SQLKeysetToken{
		Cursor:   string(raw[17:payloadBytes]),
		IssuedAt: issuedAt,
	}, nil
}
