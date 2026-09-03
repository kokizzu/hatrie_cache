package hatSql_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestSQLKeysetPaginationPreservesRowsAndAdvancesAfterPosition(t *testing.T) {
	rows := keysetPaginationRows(32)
	resolver := &keysetPaginationResolver{rows: rows}
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score"
	first, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 5, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 5, first.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	wantFirst := rowsToQueryRows(rows[:5])
	wantSecond := rowsToQueryRows(rows[5:10])
	if !reflect.DeepEqual(first.Rows, wantFirst) || !reflect.DeepEqual(second.Rows, wantSecond) {
		t.Fatalf("keyset pages = %#v / %#v, want %#v / %#v", first.Rows, second.Rows, wantFirst, wantSecond)
	}
	if !first.HasMore || !second.HasMore || first.NextCursor == second.NextCursor {
		t.Fatalf("keyset cursors = first(%t,%q) second(%t,%q)", first.HasMore, first.NextCursor, second.HasMore, second.NextCursor)
	}
	if resolver.afters[0].Valid || !resolver.afters[1].Valid || resolver.afters[1].Tie != 4 {
		t.Fatalf("keyset after positions = %#v, want invalid then tie 4", resolver.afters)
	}
	if resolver.visited != 12 {
		t.Fatalf("keyset source visits = %d, want 12 including lookahead rows", resolver.visited)
	}
}

func TestSQLKeysetPaginationMatchesOffsetPagination(t *testing.T) {
	rows := keysetPaginationRows(32)
	keysetResolver := &keysetPaginationResolver{rows: rows}
	offsetResolver := hatSql.SQLSourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return rows, nil })
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score"
	var keysetRows []hatSql.Row
	cursor := ""
	for page := 0; page < 7; page++ {
		result, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, keysetResolver, nil, hatSql.SQLQueryOptions{}, 5, cursor)
		if err != nil {
			t.Fatal(err)
		}
		keysetRows = append(keysetRows, result.Rows...)
		cursor = result.NextCursor
		if !result.HasMore {
			break
		}
	}
	offsetRows := make([]hatSql.Row, 0, len(keysetRows))
	cursor = ""
	for page := 0; page < 7; page++ {
		result, err := hatSql.ExecuteSQLQueryPage(context.Background(), query, offsetResolver, nil, hatSql.SQLQueryOptions{}, 5, cursor)
		if err != nil {
			t.Fatal(err)
		}
		offsetRows = append(offsetRows, result.Rows...)
		cursor = result.NextCursor
		if !result.HasMore {
			break
		}
	}
	if !reflect.DeepEqual(keysetRows, offsetRows) {
		t.Fatalf("keyset rows = %#v, offset rows = %#v", keysetRows, offsetRows)
	}
}

func TestSQLKeysetPaginationRejectsUnsupportedResolver(t *testing.T) {
	resolver := hatSql.SQLSourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return []hatSql.Row{{"id": 1, "score": 1}}, nil
	})
	_, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), "SELECT e.id FROM CACHE('events') AS e ORDER BY e.score", resolver, nil, hatSql.SQLQueryOptions{}, 5, "")
	if err == nil {
		t.Fatal("keyset pagination accepted a resolver without ordered-after support")
	}
}

func BenchmarkSQLKeysetPaginationDeepPage(b *testing.B) {
	rows := keysetPaginationRows(100000)
	query := "SELECT e.id, e.score FROM CACHE('events') AS e ORDER BY e.score"
	b.Run("Offset", func(b *testing.B) {
		resolver := hatSql.SQLSourceResolverFunc(func(string, string) ([]hatSql.Row, error) { return rows, nil })
		cursor := ""
		for page := 0; page < 900; page++ {
			result, err := hatSql.ExecuteSQLQueryPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 100, cursor)
			if err != nil {
				b.Fatal(err)
			}
			cursor = result.NextCursor
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := hatSql.ExecuteSQLQueryPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 100, cursor); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("Keyset", func(b *testing.B) {
		resolver := &keysetPaginationResolver{rows: rows}
		cursor := ""
		for page := 0; page < 900; page++ {
			result, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 100, cursor)
			if err != nil {
				b.Fatal(err)
			}
			cursor = result.NextCursor
		}
		b.ResetTimer()
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			if _, err := hatSql.ExecuteSQLQueryKeysetPage(context.Background(), query, resolver, nil, hatSql.SQLQueryOptions{}, 100, cursor); err != nil {
				b.Fatal(err)
			}
		}
	})
}

type keysetPaginationResolver struct {
	rows    []hatSql.Row
	afters  []hatSql.SQLKeysetPosition
	visited int
}

func (resolver *keysetPaginationResolver) ResolveSQLSource(string, string) ([]hatSql.Row, error) {
	return resolver.rows, nil
}

func (resolver *keysetPaginationResolver) StreamSQLOrderedSourceAfter(ctx context.Context, name, key, field string, desc, nullsFirst, nullsLast bool, after hatSql.SQLKeysetPosition, visit func(hatSql.Row, hatSql.SQLKeysetPosition) error) (bool, error) {
	if name != "CACHE" || key != "events" || field != "score" || desc || nullsFirst || nullsLast {
		return false, fmt.Errorf("unexpected keyset order request")
	}
	resolver.afters = append(resolver.afters, after)
	start := 0
	if after.Valid {
		start = int(after.Tie) + 1
	}
	for index := start; index < len(resolver.rows); index++ {
		if err := ctx.Err(); err != nil {
			return true, err
		}
		resolver.visited++
		position := hatSql.SQLKeysetPosition{Value: resolver.rows[index][field], Tie: uint64(index), Valid: true}
		if err := visit(resolver.rows[index], position); err != nil {
			return true, err
		}
	}
	return true, nil
}

func keysetPaginationRows(count int) []hatSql.Row {
	rows := make([]hatSql.Row, 0, count)
	for index := 0; index < count; index++ {
		rows = append(rows, hatSql.Row{"id": "id-" + strconv.Itoa(index), "score": index})
	}
	return rows
}

func rowsToQueryRows(rows []hatSql.Row) []hatSql.Row {
	result := make([]hatSql.Row, len(rows))
	for index, row := range rows {
		result[index] = hatSql.Row{"id": row["id"], "score": row["score"]}
	}
	return result
}
