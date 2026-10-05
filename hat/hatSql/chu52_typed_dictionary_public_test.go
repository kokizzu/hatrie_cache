package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCHU52TypedDictionaryPublicAPI(t *testing.T) {
	dictionary, err := hatSql.NewSQLExternalDictionary(hatSql.SQLExternalDictionaryOptions{
		Name:    "public",
		KeyKind: hatSql.SQLExternalDictionaryKeyInt64,
		LoadEntries: func(context.Context) ([]hatSql.SQLExternalDictionaryEntry, error) {
			return []hatSql.SQLExternalDictionaryEntry{{Key: int64(1), Value: "one"}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := dictionary.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if value, found, err := dictionary.LookupKey(int64(1)); err != nil || !found || value != "one" {
		t.Fatalf("public LookupKey() = %#v/%v/%v", value, found, err)
	}
}
