package hatSql

import (
	"context"
	"testing"
	"time"
)

var (
	mz044SnapshotTokenBenchmarkString string
	mz044SnapshotTokenBenchmarkValue  SQLSnapshotToken
)

func newMZ044SnapshotTokenBenchmarkCodec(b *testing.B) (*SQLSnapshotTokenCodec, string) {
	b.Helper()
	codec, err := NewSQLSnapshotTokenCodec(SQLSnapshotTokenCodecOptions{
		Secret: []byte("snapshot-token-benchmark-secret"),
		Now:    func() time.Time { return time.Unix(1_700_000_000, 0).UTC() },
	})
	if err != nil {
		b.Fatal(err)
	}
	token, err := codec.Encode(42)
	if err != nil {
		b.Fatal(err)
	}
	return codec, token
}

func BenchmarkMZ044SQLSnapshotTokenEncode(b *testing.B) {
	codec, _ := newMZ044SnapshotTokenBenchmarkCodec(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		token, err := codec.Encode(uint64(i))
		if err != nil {
			b.Fatal(err)
		}
		mz044SnapshotTokenBenchmarkString = token
	}
}

func BenchmarkMZ044SQLSnapshotTokenDecode(b *testing.B) {
	codec, token := newMZ044SnapshotTokenBenchmarkCodec(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoded, err := codec.Decode(token)
		if err != nil {
			b.Fatal(err)
		}
		mz044SnapshotTokenBenchmarkValue = decoded
	}
}

func BenchmarkMZ044SQLSnapshotTokenApply(b *testing.B) {
	codec, token := newMZ044SnapshotTokenBenchmarkCodec(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		options := SQLQueryOptions{}
		if err := codec.Apply(token, &options); err != nil {
			b.Fatal(err)
		}
		mz044SnapshotTokenBenchmarkValue.Frontier = *options.AsOfFrontier
	}
}

func BenchmarkMZ044SQLSnapshotTokenQuery(b *testing.B) {
	codec, token := newMZ044SnapshotTokenBenchmarkCodec(b)
	resolver := mz044SnapshotTokenResolver{}
	options := SQLQueryOptions{SnapshotToken: token, SnapshotTokenCodec: codec}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ExecuteSQLQueryParameters(ctx, "FROM CACHE('items') SELECT value", resolver, nil, options)
		if err != nil {
			b.Fatal(err)
		}
		mz044SnapshotTokenBenchmarkValue.Frontier = uint64(result.Rows[0]["value"].(int64))
	}
}

func BenchmarkMZ044SQLAsOfFrontierQuery(b *testing.B) {
	frontier := uint64(42)
	resolver := mz044SnapshotTokenResolver{}
	options := SQLQueryOptions{AsOfFrontier: &frontier}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ExecuteSQLQueryParameters(ctx, "FROM CACHE('items') SELECT value", resolver, nil, options)
		if err != nil {
			b.Fatal(err)
		}
		mz044SnapshotTokenBenchmarkValue.Frontier = uint64(result.Rows[0]["value"].(int64))
	}
}

func BenchmarkMZ044SQLDefaultQuery(b *testing.B) {
	resolver := mz044SnapshotTokenResolver{}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := ExecuteSQLQueryParameters(ctx, "FROM CACHE('items') SELECT value", resolver, nil, SQLQueryOptions{})
		if err != nil {
			b.Fatal(err)
		}
		mz044SnapshotTokenBenchmarkString = result.Rows[0]["value"].(string)
	}
}
