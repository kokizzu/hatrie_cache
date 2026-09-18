package hatSql

import (
	"errors"
	"testing"
	"time"
)

func TestSQLKeysetTokenRoundTripAndExpiration(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	codec, err := NewSQLKeysetTokenCodec(SQLKeysetTokenCodecOptions{
		Secret: []byte("keyset-token-secret-2026"),
		MaxAge: time.Minute,
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewSQLKeysetTokenCodec() error = %v", err)
	}
	legacyCursor := "p:eyJmIjoiZmluZ2VycHJpbnQiLCJyIjoyLCJwIjpbXX0"
	token, err := codec.Encode(legacyCursor)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if token == legacyCursor || len(token) > MaxSQLKeysetTokenBytes {
		t.Fatalf("encoded token = %q, length=%d", token, len(token))
	}
	decoded, err := codec.Decode(token)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded.Cursor != legacyCursor || !decoded.IssuedAt.Equal(now) {
		t.Fatalf("decoded token = %#v, want cursor %q at %s", decoded, legacyCursor, now)
	}

	now = now.Add(time.Minute + time.Nanosecond)
	if _, err := codec.Decode(token); !errors.Is(err, ErrSQLKeysetTokenExpired) {
		t.Fatalf("expired Decode() error = %v, want ErrSQLKeysetTokenExpired", err)
	}
}

func TestSQLKeysetTokenRejectsTamperingAndInvalidConfiguration(t *testing.T) {
	if _, err := NewSQLKeysetTokenCodec(SQLKeysetTokenCodecOptions{Secret: []byte("short")}); !errors.Is(err, ErrSQLKeysetTokenSecretInvalid) {
		t.Fatalf("short secret error = %v, want ErrSQLKeysetTokenSecretInvalid", err)
	}
	if _, err := NewSQLKeysetTokenCodec(SQLKeysetTokenCodecOptions{
		Secret: []byte("keyset-token-secret-2026"),
		MaxAge: -time.Second,
	}); !errors.Is(err, ErrSQLKeysetTokenMaxAgeInvalid) {
		t.Fatalf("negative max age error = %v, want ErrSQLKeysetTokenMaxAgeInvalid", err)
	}

	codec, err := NewSQLKeysetTokenCodec(SQLKeysetTokenCodecOptions{Secret: []byte("keyset-token-secret-2026")})
	if err != nil {
		t.Fatal(err)
	}
	token, err := codec.Encode("cursor")
	if err != nil {
		t.Fatal(err)
	}
	tampered := []byte(token)
	if tampered[len(tampered)-1] == 'A' {
		tampered[len(tampered)-1] = 'B'
	} else {
		tampered[len(tampered)-1] = 'A'
	}
	if _, err := codec.Decode(string(tampered)); !errors.Is(err, ErrSQLKeysetTokenAuthentication) {
		t.Fatalf("tampered Decode() error = %v, want ErrSQLKeysetTokenAuthentication", err)
	}

	other, err := NewSQLKeysetTokenCodec(SQLKeysetTokenCodecOptions{Secret: []byte("other-keyset-secret-2026")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := other.Decode(token); !errors.Is(err, ErrSQLKeysetTokenAuthentication) {
		t.Fatalf("wrong secret Decode() error = %v, want ErrSQLKeysetTokenAuthentication", err)
	}
	if _, err := codec.Encode(""); !errors.Is(err, ErrSQLKeysetTokenInvalid) {
		t.Fatalf("empty cursor Encode() error = %v, want ErrSQLKeysetTokenInvalid", err)
	}
	if _, err := (*SQLKeysetTokenCodec)(nil).Decode(token); !errors.Is(err, ErrSQLKeysetTokenCodecNil) {
		t.Fatalf("nil codec Decode() error = %v, want ErrSQLKeysetTokenCodecNil", err)
	}
}
