package hatPagination

import (
	"bytes"
	"errors"
	"sync"
	"testing"
	"time"
)

var testCursorTime = time.Unix(1_700_000_000, 0).UTC()

func TestCursorTokenRoundTrip(t *testing.T) {
	codec := newTestCodec(t, Config{TTL: time.Minute, MaxKeyBytes: 64})
	key := []byte{0, 1, 2, 127, 128, 255}
	token, err := codec.Encode("orders", 7, key, testCursorTime)
	if err != nil {
		t.Fatal(err)
	}
	got, err := codec.Decode(token, "orders", 7, testCursorTime.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, key) {
		t.Fatalf("decoded key = %v, want %v", got, key)
	}
	got[0] = 99
	if bytes.Equal(got, key) {
		t.Fatal("Decode returned the caller's key buffer")
	}
}

func TestCursorTokenBindsNamespaceAndVersion(t *testing.T) {
	codec := newTestCodec(t, Config{TTL: time.Minute})
	token, err := codec.Encode("orders", 7, []byte("cursor"), testCursorTime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(token, "users", 7, testCursorTime); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("wrong namespace error = %v, want %v", err, ErrAuthentication)
	}
	if _, err := codec.Decode(token, "orders", 8, testCursorTime); !errors.Is(err, ErrVersionMismatch) {
		t.Fatalf("wrong version error = %v, want %v", err, ErrVersionMismatch)
	}
}

func TestCursorTokenRejectsTampering(t *testing.T) {
	codec := newTestCodec(t, Config{TTL: time.Minute})
	token, err := codec.Encode("orders", 7, []byte("cursor"), testCursorTime)
	if err != nil {
		t.Fatal(err)
	}
	token[len(token)/2] ^= 1
	if _, err := codec.Decode(token, "orders", 7, testCursorTime); !errors.Is(err, ErrAuthentication) {
		t.Fatalf("tampered token error = %v, want %v", err, ErrAuthentication)
	}
}

func TestCursorTokenExpires(t *testing.T) {
	codec := newTestCodec(t, Config{TTL: time.Minute})
	token, err := codec.Encode("orders", 7, []byte("cursor"), testCursorTime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(token, "orders", 7, testCursorTime.Add(time.Minute)); !errors.Is(err, ErrExpired) {
		t.Fatalf("expired token error = %v, want %v", err, ErrExpired)
	}
}

func TestCursorTokenLimitsKeyAndNamespace(t *testing.T) {
	codec := newTestCodec(t, Config{TTL: time.Minute, MaxKeyBytes: 4})
	if _, err := codec.Encode("orders", 7, []byte("12345"), testCursorTime); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("large key error = %v, want %v", err, ErrKeyTooLarge)
	}
	if _, err := codec.Encode("", 7, []byte("key"), testCursorTime); !errors.Is(err, ErrInvalidNamespace) {
		t.Fatalf("empty namespace error = %v, want %v", err, ErrInvalidNamespace)
	}
}

func TestCursorTokenTextRoundTrip(t *testing.T) {
	codec := newTestCodec(t, Config{TTL: time.Minute})
	token, err := codec.EncodeText("orders", 7, []byte("cursor"), testCursorTime)
	if err != nil {
		t.Fatal(err)
	}
	for _, char := range token {
		if !(char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' || char >= '0' && char <= '9' || char == '-' || char == '_') {
			t.Fatalf("text token contains non-URL-safe character %q", char)
		}
	}
	key, err := codec.DecodeText(token, "orders", 7, testCursorTime)
	if err != nil {
		t.Fatal(err)
	}
	if string(key) != "cursor" {
		t.Fatalf("decoded text key = %q, want cursor", key)
	}
}

func TestCursorTokenRequiresStrongSecret(t *testing.T) {
	if _, err := NewCodec([]byte("short"), DefaultConfig()); !errors.Is(err, ErrWeakSecret) {
		t.Fatalf("weak secret error = %v, want %v", err, ErrWeakSecret)
	}
}

func TestCursorTokenCopiesSecret(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	codec, err := NewCodec(secret, Config{TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	secret[0] ^= 1
	token, err := codec.Encode("orders", 7, []byte("cursor"), testCursorTime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(token, "orders", 7, testCursorTime); err != nil {
		t.Fatalf("Decode after caller secret mutation = %v", err)
	}
}

func TestCursorTokenSupportsNoExpiry(t *testing.T) {
	codec := newTestCodec(t, Config{TTL: -1})
	token, err := codec.Encode("orders", 7, []byte("cursor"), testCursorTime)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Decode(token, "orders", 7, testCursorTime.Add(100*365*24*time.Hour)); err != nil {
		t.Fatalf("Decode no-expiry token = %v", err)
	}
}

func TestCursorTokenRejectsMalformedTextAndConfig(t *testing.T) {
	codec := newTestCodec(t, Config{TTL: time.Minute})
	if _, err := codec.DecodeText("not valid!", "orders", 7, testCursorTime); !errors.Is(err, ErrMalformed) {
		t.Fatalf("malformed text error = %v, want %v", err, ErrMalformed)
	}
	if _, err := NewCodec([]byte("0123456789abcdef"), Config{MaxKeyBytes: 1 << 16}); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("large max-key config error = %v, want %v", err, ErrInvalidConfig)
	}
}

func TestCursorCodecIsSafeForConcurrentUse(t *testing.T) {
	codec := newTestCodec(t, Config{TTL: time.Minute})
	errorsCh := make(chan error, 16)
	var group sync.WaitGroup
	for i := 0; i < 16; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			for j := 0; j < 100; j++ {
				token, err := codec.Encode("orders", 7, []byte("cursor"), testCursorTime)
				if err != nil {
					errorsCh <- err
					return
				}
				if _, err := codec.Decode(token, "orders", 7, testCursorTime); err != nil {
					errorsCh <- err
					return
				}
			}
		}()
	}
	group.Wait()
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
}

func newTestCodec(t *testing.T, config Config) *Codec {
	t.Helper()
	codec, err := NewCodec([]byte("0123456789abcdef0123456789abcdef"), config)
	if err != nil {
		t.Fatal(err)
	}
	return codec
}
