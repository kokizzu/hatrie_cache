package hatCache

import (
	"context"
	"reflect"
	"testing"
)

func TestTR033SQLReturningIncludesBeforeRowsWithoutChangingRows(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("profile:1", "Ada")

	result, err := ExecuteSQLMutation(context.Background(), trie, `
UPDATE cache SET value = 'Grace' WHERE key = 'profile:1'
RETURNING key, value, exists`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLMutation(UPDATE RETURNING) error = %v", err)
	}
	wantColumns := []string{"key", "value", "exists"}
	if !reflect.DeepEqual(result.Columns, wantColumns) {
		t.Fatalf("columns = %#v, want %#v", result.Columns, wantColumns)
	}
	wantBefore := []SQLRow{{"key": "profile:1", "value": "Ada", "exists": true}}
	if !reflect.DeepEqual(result.BeforeRows, wantBefore) {
		t.Fatalf("before rows = %#v, want %#v", result.BeforeRows, wantBefore)
	}
	wantAfter := []SQLRow{{"key": "profile:1", "value": "Grace", "exists": true}}
	if !reflect.DeepEqual(result.Rows, wantAfter) {
		t.Fatalf("after rows = %#v, want %#v", result.Rows, wantAfter)
	}
}

func TestTR033SQLReturningDeleteExposesBeforeRows(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("profile:1", "Ada")

	result, err := ExecuteSQLMutation(context.Background(), trie, `
DELETE FROM cache WHERE key = 'profile:1'
RETURNING key, value, exists`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLMutation(DELETE RETURNING) error = %v", err)
	}
	want := []SQLRow{{"key": "profile:1", "value": "Ada", "exists": true}}
	if !reflect.DeepEqual(result.BeforeRows, want) {
		t.Fatalf("delete before rows = %#v, want %#v", result.BeforeRows, want)
	}
	if !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("delete rows = %#v, want existing compatibility result %#v", result.Rows, want)
	}
}

func TestTR033SQLReturningInsertHasNoBeforeRows(t *testing.T) {
	trie := newTestTrie(t)

	result, err := ExecuteSQLMutation(context.Background(), trie, `
INSERT INTO cache (key, value) VALUES ('profile:1', 'Ada')
RETURNING key, value, exists`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLMutation(INSERT RETURNING) error = %v", err)
	}
	if result.BeforeRows != nil {
		t.Fatalf("insert before rows = %#v, want nil", result.BeforeRows)
	}
}

func TestTR033SQLReturningConflictUpdateIncludesBeforeRows(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("profile:1", "Ada")

	result, err := ExecuteSQLMutation(context.Background(), trie, `
INSERT INTO cache (key, value) VALUES ('profile:1', 'Grace')
ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
RETURNING key, value`, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLMutation(ON CONFLICT DO UPDATE RETURNING) error = %v", err)
	}
	if want := []SQLRow{{"key": "profile:1", "value": "Ada"}}; !reflect.DeepEqual(result.BeforeRows, want) {
		t.Fatalf("conflict before rows = %#v, want %#v", result.BeforeRows, want)
	}
	if want := []SQLRow{{"key": "profile:1", "value": "Grace"}}; !reflect.DeepEqual(result.Rows, want) {
		t.Fatalf("conflict after rows = %#v, want %#v", result.Rows, want)
	}
}

func TestTR033SQLMutationWithoutReturningDoesNotCaptureBeforeRows(t *testing.T) {
	trie := newTestTrie(t)
	trie.UpsertString("profile:1", "Ada")

	result, err := ExecuteSQLMutation(context.Background(), trie, "UPDATE cache SET value = 'Grace' WHERE key = 'profile:1'", nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("ExecuteSQLMutation(UPDATE) error = %v", err)
	}
	if result.BeforeRows != nil {
		t.Fatalf("non-returning before rows = %#v, want nil", result.BeforeRows)
	}
}
