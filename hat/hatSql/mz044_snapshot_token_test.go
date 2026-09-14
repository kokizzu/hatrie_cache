package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLSnapshotTokenRoundTripAndQueryApplication(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	codec, err := NewSQLSnapshotTokenCodec(SQLSnapshotTokenCodecOptions{
		Secret: []byte("snapshot-token-secret-2026"),
		MaxAge: time.Minute,
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewSQLSnapshotTokenCodec() error = %v", err)
	}
	token, err := codec.Encode(42)
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	decoded, err := codec.Decode(token)
	if err != nil {
		t.Fatalf("Decode() error = %v", err)
	}
	if decoded.Frontier != 42 || !decoded.IssuedAt.Equal(now) {
		t.Fatalf("decoded token = %#v, want frontier 42 at %s", decoded, now)
	}

	result, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('items') SELECT value", mz044SnapshotTokenResolver{}, nil, SQLQueryOptions{
		SnapshotToken:      token,
		SnapshotTokenCodec: codec,
	})
	if err != nil {
		t.Fatalf("ExecuteSQLQueryParameters() error = %v", err)
	}
	if len(result.Rows) != 1 || result.Rows[0]["value"] != int64(42) {
		t.Fatalf("token query rows = %#v, want frontier-bound row", result.Rows)
	}
}

func TestSQLSnapshotTokenRejectsTamperingAndExpiration(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	codec, err := NewSQLSnapshotTokenCodec(SQLSnapshotTokenCodecOptions{
		Secret: []byte("snapshot-token-secret-2026"),
		MaxAge: time.Minute,
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := codec.Encode(7)
	if err != nil {
		t.Fatal(err)
	}
	tampered := []byte(token)
	if tampered[len(tampered)-1] == 'A' {
		tampered[len(tampered)-1] = 'B'
	} else {
		tampered[len(tampered)-1] = 'A'
	}
	if _, err := codec.Decode(string(tampered)); !errors.Is(err, ErrSQLSnapshotTokenAuthentication) {
		t.Fatalf("tampered Decode() error = %v, want authentication error", err)
	}
	now = now.Add(time.Minute + time.Nanosecond)
	if _, err := codec.Decode(token); !errors.Is(err, ErrSQLSnapshotTokenExpired) {
		t.Fatalf("expired Decode() error = %v, want expiration error", err)
	}
}

func TestSQLSnapshotTokenRejectsInvalidConfigurationAndConflicts(t *testing.T) {
	if _, err := NewSQLSnapshotTokenCodec(SQLSnapshotTokenCodecOptions{Secret: []byte("short")}); !errors.Is(err, ErrSQLSnapshotTokenSecretInvalid) {
		t.Fatalf("short secret error = %v, want ErrSQLSnapshotTokenSecretInvalid", err)
	}
	if _, err := NewSQLSnapshotTokenCodec(SQLSnapshotTokenCodecOptions{
		Secret: []byte("snapshot-token-secret-2026"),
		MaxAge: -time.Second,
	}); !errors.Is(err, ErrSQLSnapshotTokenMaxAgeInvalid) {
		t.Fatalf("negative max age error = %v, want ErrSQLSnapshotTokenMaxAgeInvalid", err)
	}

	codec, err := NewSQLSnapshotTokenCodec(SQLSnapshotTokenCodecOptions{Secret: []byte("snapshot-token-secret-2026")})
	if err != nil {
		t.Fatal(err)
	}
	token, err := codec.Encode(1)
	if err != nil {
		t.Fatal(err)
	}
	options := SQLQueryOptions{AsOfFrontier: ptrUint64(2)}
	if err := codec.Apply(token, &options); !errors.Is(err, ErrSQLSnapshotTokenConflict) {
		t.Fatalf("conflicting Apply() error = %v, want ErrSQLSnapshotTokenConflict", err)
	}
	if err := (*SQLSnapshotTokenCodec)(nil).Apply(token, &SQLQueryOptions{}); !errors.Is(err, ErrSQLSnapshotTokenCodecNil) {
		t.Fatalf("nil codec Apply() error = %v, want ErrSQLSnapshotTokenCodecNil", err)
	}
}

func TestSQLSnapshotTokenDefaultAndStreamExecution(t *testing.T) {
	now := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
	codec, err := NewSQLSnapshotTokenCodec(SQLSnapshotTokenCodecOptions{
		Secret: []byte("snapshot-token-secret-2026"),
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	token, err := codec.Encode(9)
	if err != nil {
		t.Fatal(err)
	}
	if len(token) > MaxSQLSnapshotTokenBytes {
		t.Fatalf("token length = %d, want <= %d", len(token), MaxSQLSnapshotTokenBytes)
	}

	result, err := ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('items') SELECT value", mz044SnapshotTokenResolver{}, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("default query error = %v", err)
	}
	if result.Rows[0]["value"] != "live" {
		t.Fatalf("default query value = %#v, want live", result.Rows[0]["value"])
	}

	var streamed []SQLRow
	err = ExecuteSQLQueryRows(context.Background(), "FROM CACHE('items') SELECT value", mz044SnapshotTokenResolver{}, nil, SQLQueryOptions{
		SnapshotToken:      token,
		SnapshotTokenCodec: codec,
	}, func(_ []string, row SQLRow) error {
		streamed = append(streamed, row)
		return nil
	})
	if err != nil {
		t.Fatalf("stream query error = %v", err)
	}
	if len(streamed) != 1 || streamed[0]["value"] != int64(9) {
		t.Fatalf("streamed rows = %#v, want frontier-bound row", streamed)
	}

	_, err = ExecuteSQLQueryParameters(context.Background(), "FROM CACHE('items') SELECT value", mz044SnapshotTokenResolver{}, nil, SQLQueryOptions{SnapshotToken: token})
	if !errors.Is(err, ErrSQLSnapshotTokenCodecNil) {
		t.Fatalf("missing codec query error = %v, want ErrSQLSnapshotTokenCodecNil", err)
	}
}

type mz044SnapshotTokenResolver struct{}

func (mz044SnapshotTokenResolver) ResolveSQLSource(string, string) ([]Row, error) {
	return []Row{{"value": "live"}}, nil
}

func (mz044SnapshotTokenResolver) BeginSQLSnapshotAt(_ context.Context, frontier uint64) (SQLSourceResolver, func(), error) {
	return SourceResolverFunc(func(string, string) ([]Row, error) {
		return []Row{{"value": int64(frontier)}}, nil
	}), func() {}, nil
}

func ptrUint64(value uint64) *uint64 {
	return &value
}
