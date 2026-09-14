package hatCache

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestCH013ExecuteSQLMutationUsesAdmissionGate(t *testing.T) {
	trie := CreateHatTrie()
	defer trie.Destroy()
	admission, err := hatSql.NewSQLMutationAdmission(hatSql.SQLMutationAdmissionOptions{MinInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewSQLMutationAdmission() error = %v", err)
	}
	options := SQLQueryOptions{MutationAdmission: admission}
	first, err := ExecuteSQLMutation(context.Background(), trie, "INSERT INTO cache (key, value) VALUES ('admission:first', 'ok')", nil, options)
	if err != nil || first.Affected != 1 {
		t.Fatalf("first ExecuteSQLMutation() = %#v, %v; want one affected row", first, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ExecuteSQLMutation(ctx, trie, "INSERT INTO cache (key, value) VALUES ('admission:blocked', 'no')", nil, options); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ExecuteSQLMutation() error = %v, want context.Canceled", err)
	}
	if got := trie.GetString("admission:blocked"); got != "" {
		t.Fatalf("blocked mutation became visible: %q", got)
	}
}
