package hatSql_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestSQLKeysetTokenCodecIntegratesWithDirectAndPartitionedPages(t *testing.T) {
	now := time.Date(2026, 9, 18, 12, 0, 0, 0, time.UTC)
	codec, err := hatSql.NewSQLKeysetTokenCodec(hatSql.SQLKeysetTokenCodecOptions{
		Secret: []byte("keyset-token-secret-2026"),
		MaxAge: time.Minute,
		Now:    func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	options := hatSql.SQLQueryOptions{KeysetCursorTokenCodec: codec}

	direct := &keysetPaginationResolver{rows: keysetPaginationRows(8)}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score"
	first, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, direct, nil, options, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, direct, nil, options, 2, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if want := rowsToQueryRows(direct.rows[2:4]); !reflect.DeepEqual(second.Rows, want) {
		t.Fatalf("secure direct page = %#v, want %#v", second.Rows, want)
	}
	if decoded, err := codec.Decode(first.NextCursor); err != nil || decoded.Cursor == first.NextCursor {
		t.Fatalf("secure direct cursor decode = %#v, %v", decoded, err)
	}

	partitioned := &partitionedKeysetResolver{partitions: []hatSql.SQLSourcePartition{
		{Name: "apac", Rows: []hatSql.Row{
			{"id": "a-1", "score": int64(1)},
			{"id": "a-3", "score": int64(3)},
		}},
		{Name: "eu", Rows: []hatSql.Row{
			{"id": "e-2", "score": int64(2)},
			{"id": "e-4", "score": int64(4)},
		}},
	}}
	partitionedQuery := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score"
	partitionedFirst, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), partitionedQuery, partitioned, nil, options, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	partitionedSecond, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), partitionedQuery, partitioned, nil, options, 2, partitionedFirst.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	if want := []hatSql.Row{{"id": "a-3", "score": int64(3)}, {"id": "e-4", "score": int64(4)}}; !reflect.DeepEqual(partitionedSecond.Rows, want) {
		t.Fatalf("secure partitioned page = %#v, want %#v", partitionedSecond.Rows, want)
	}

	tampered := []byte(first.NextCursor)
	tampered[len(tampered)-1] ^= 1
	if _, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, direct, nil, options, 2, string(tampered)); !errors.Is(err, hatSql.ErrSQLKeysetTokenAuthentication) {
		t.Fatalf("tampered page error = %v, want ErrSQLKeysetTokenAuthentication", err)
	}
}
