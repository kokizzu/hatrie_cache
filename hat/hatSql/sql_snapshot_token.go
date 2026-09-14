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
	// DefaultSQLSnapshotTokenMaxAge bounds how long a token can select a
	// historical frontier when the caller does not provide an explicit age.
	DefaultSQLSnapshotTokenMaxAge  = 15 * time.Minute
	MinSQLSnapshotTokenSecretBytes = 16
	MaxSQLSnapshotTokenBytes       = 256

	sqlSnapshotTokenPayloadBytes = 4 + 1 + 8 + 8
	sqlSnapshotTokenMACBytes     = sha256.Size
)

const sqlSnapshotTokenMagic = "hst1"

var (
	ErrSQLSnapshotTokenCodecNil       = errors.New("hatSql: SQL snapshot token codec is nil")
	ErrSQLSnapshotTokenSecretInvalid  = errors.New("hatSql: SQL snapshot token secret is invalid")
	ErrSQLSnapshotTokenMaxAgeInvalid  = errors.New("hatSql: SQL snapshot token max age is invalid")
	ErrSQLSnapshotTokenInvalid        = errors.New("hatSql: SQL snapshot token is invalid")
	ErrSQLSnapshotTokenAuthentication = errors.New("hatSql: SQL snapshot token authentication failed")
	ErrSQLSnapshotTokenExpired        = errors.New("hatSql: SQL snapshot token expired")
	ErrSQLSnapshotTokenConflict       = errors.New("hatSql: SQL snapshot token conflicts with AsOfFrontier")
)

// SQLSnapshotTokenCodecOptions configures signed snapshot token handling.
// Secret must be kept private by the service; tokens only carry an
// authenticated frontier and do not replace query authorization.
type SQLSnapshotTokenCodecOptions struct {
	Secret []byte
	MaxAge time.Duration
	Now    func() time.Time
}

// SQLSnapshotToken is the verified frontier carried by a snapshot token.
type SQLSnapshotToken struct {
	Frontier uint64
	IssuedAt time.Time
}

// SQLSnapshotTokenCodec signs and verifies bounded, stateless snapshot
// tokens. The codec retains only a copy of the signing secret and a clock.
type SQLSnapshotTokenCodec struct {
	secret []byte
	maxAge time.Duration
	now    func() time.Time
}

// NewSQLSnapshotTokenCodec creates a snapshot token codec. A zero MaxAge uses
// DefaultSQLSnapshotTokenMaxAge; a negative MaxAge is rejected.
func NewSQLSnapshotTokenCodec(options SQLSnapshotTokenCodecOptions) (*SQLSnapshotTokenCodec, error) {
	if len(options.Secret) < MinSQLSnapshotTokenSecretBytes {
		return nil, ErrSQLSnapshotTokenSecretInvalid
	}
	maxAge := options.MaxAge
	if maxAge == 0 {
		maxAge = DefaultSQLSnapshotTokenMaxAge
	}
	if maxAge < 0 {
		return nil, ErrSQLSnapshotTokenMaxAgeInvalid
	}
	now := options.Now
	if now == nil {
		now = time.Now
	}
	return &SQLSnapshotTokenCodec{
		secret: append([]byte(nil), options.Secret...),
		maxAge: maxAge,
		now:    now,
	}, nil
}

func (codec *SQLSnapshotTokenCodec) validate() error {
	if codec == nil {
		return ErrSQLSnapshotTokenCodecNil
	}
	if len(codec.secret) < MinSQLSnapshotTokenSecretBytes {
		return ErrSQLSnapshotTokenSecretInvalid
	}
	if codec.maxAge <= 0 || codec.now == nil {
		return ErrSQLSnapshotTokenMaxAgeInvalid
	}
	return nil
}

// Encode creates a signed token for frontier.
func (codec *SQLSnapshotTokenCodec) Encode(frontier uint64) (string, error) {
	if err := codec.validate(); err != nil {
		return "", err
	}
	issuedAt := codec.now().UTC()
	if issuedAt.UnixNano() < 0 {
		return "", ErrSQLSnapshotTokenInvalid
	}
	payloadBytes := sqlSnapshotTokenPayloadBytes
	raw := make([]byte, payloadBytes+sqlSnapshotTokenMACBytes)
	copy(raw[:4], sqlSnapshotTokenMagic)
	raw[4] = 1
	binary.BigEndian.PutUint64(raw[5:13], uint64(issuedAt.UnixNano()))
	binary.BigEndian.PutUint64(raw[13:21], frontier)
	mac := hmac.New(sha256.New, codec.secret)
	_, _ = mac.Write(raw[:payloadBytes])
	copy(raw[payloadBytes:], mac.Sum(nil))
	token := base64.RawURLEncoding.EncodeToString(raw)
	if len(token) > MaxSQLSnapshotTokenBytes {
		return "", ErrSQLSnapshotTokenInvalid
	}
	return token, nil
}

// Decode verifies and decodes a token. Authentication is checked before any
// token field is trusted for frontier or time decisions.
func (codec *SQLSnapshotTokenCodec) Decode(token string) (SQLSnapshotToken, error) {
	if err := codec.validate(); err != nil {
		return SQLSnapshotToken{}, err
	}
	if token == "" || len(token) > MaxSQLSnapshotTokenBytes {
		return SQLSnapshotToken{}, ErrSQLSnapshotTokenInvalid
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != sqlSnapshotTokenPayloadBytes+sqlSnapshotTokenMACBytes {
		return SQLSnapshotToken{}, ErrSQLSnapshotTokenInvalid
	}
	if string(raw[:4]) != sqlSnapshotTokenMagic || raw[4] != 1 {
		return SQLSnapshotToken{}, ErrSQLSnapshotTokenInvalid
	}
	mac := hmac.New(sha256.New, codec.secret)
	_, _ = mac.Write(raw[:sqlSnapshotTokenPayloadBytes])
	if !hmac.Equal(raw[sqlSnapshotTokenPayloadBytes:], mac.Sum(nil)) {
		return SQLSnapshotToken{}, ErrSQLSnapshotTokenAuthentication
	}
	issuedAt := time.Unix(0, int64(binary.BigEndian.Uint64(raw[5:13]))).UTC()
	now := codec.now().UTC()
	if now.Before(issuedAt) {
		return SQLSnapshotToken{}, ErrSQLSnapshotTokenInvalid
	}
	if now.Sub(issuedAt) > codec.maxAge {
		return SQLSnapshotToken{}, ErrSQLSnapshotTokenExpired
	}
	return SQLSnapshotToken{
		Frontier: binary.BigEndian.Uint64(raw[13:21]),
		IssuedAt: issuedAt,
	}, nil
}

// Apply verifies token and converts it to the existing AsOfFrontier query
// option. Explicit frontiers and tokens cannot be combined accidentally.
func (codec *SQLSnapshotTokenCodec) Apply(token string, options *SQLQueryOptions) error {
	if codec == nil {
		return ErrSQLSnapshotTokenCodecNil
	}
	if options == nil {
		return ErrSQLSnapshotTokenInvalid
	}
	if options.AsOfFrontier != nil {
		return ErrSQLSnapshotTokenConflict
	}
	decoded, err := codec.Decode(token)
	if err != nil {
		return err
	}
	frontier := decoded.Frontier
	options.AsOfFrontier = &frontier
	return nil
}

func (options *SQLQueryOptions) normalizeSQLSnapshotToken() error {
	if options == nil || options.SnapshotToken == "" {
		return nil
	}
	if options.SnapshotTokenCodec == nil {
		return ErrSQLSnapshotTokenCodecNil
	}
	return options.SnapshotTokenCodec.Apply(options.SnapshotToken, options)
}
