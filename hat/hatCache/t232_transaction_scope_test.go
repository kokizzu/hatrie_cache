package hatCache

import (
	"errors"
	"testing"
)

func TestT232SQLTransactionScopeRollsBackNestedWorkAndKeepsOuterWork(t *testing.T) {
	trie := newTestTrie(t)
	tx, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	defer tx.Rollback()

	if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('outer:before', 'keep')"); err != nil {
		t.Fatalf("outer before Execute() error = %v", err)
	}

	wantInnerErr := errors.New("abort inner scope")
	if err := tx.Scope(func(tx *SQLTransaction) error {
		if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('outer:scope', 'keep')"); err != nil {
			return err
		}
		if err := tx.Scope(func(tx *SQLTransaction) error {
			if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('inner:discard', 'discard')"); err != nil {
				return err
			}
			return wantInnerErr
		}); !errors.Is(err, wantInnerErr) {
			t.Fatalf("inner Scope() error = %v, want %v", err, wantInnerErr)
		}
		_, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('outer:after', 'keep')")
		return err
	}); err != nil {
		t.Fatalf("outer Scope() error = %v", err)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	for key, want := range map[string]string{
		"outer:before": "keep",
		"outer:scope":  "keep",
		"outer:after":  "keep",
	} {
		if got := trie.GetString(key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}
	if trie.Exists("inner:discard") {
		t.Fatal("inner scope mutation survived rollback")
	}
}

func TestT232SQLTransactionScopeCommitsExplicitNestedScope(t *testing.T) {
	trie := newTestTrie(t)
	tx, err := BeginSQLTransaction(trie)
	if err != nil {
		t.Fatalf("BeginSQLTransaction() error = %v", err)
	}
	defer tx.Rollback()

	if err := tx.Savepoint("__hatrie_scope_1"); err != nil {
		t.Fatalf("reserved-prefix Savepoint() error = %v", err)
	}
	scope, err := tx.BeginScope()
	if err != nil {
		t.Fatalf("BeginScope() error = %v", err)
	}
	if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('explicit', 'committed')"); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if err := scope.Commit(); err != nil {
		t.Fatalf("scope.Commit() error = %v", err)
	}
	if err := scope.Commit(); !errors.Is(err, ErrSQLTransactionScopeClosed) {
		t.Fatalf("second scope.Commit() error = %v, want %v", err, ErrSQLTransactionScopeClosed)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("transaction Commit() error = %v", err)
	}
	if got := trie.GetString("explicit"); got != "committed" {
		t.Fatalf("explicit = %q, want committed", got)
	}
}

func TestT232SQLTransactionScopeRollsBackOnCallbackErrorAndPanic(t *testing.T) {
	t.Run("callback_error", func(t *testing.T) {
		trie := newTestTrie(t)
		tx, err := BeginSQLTransaction(trie)
		if err != nil {
			t.Fatalf("BeginSQLTransaction() error = %v", err)
		}
		defer tx.Rollback()
		wantErr := errors.New("scope callback failed")
		if err := tx.Scope(func(tx *SQLTransaction) error {
			_, executeErr := tx.Execute("INSERT INTO cache (key, value) VALUES ('error:discard', 'discard')")
			if executeErr != nil {
				return executeErr
			}
			return wantErr
		}); !errors.Is(err, wantErr) {
			t.Fatalf("Scope() error = %v, want %v", err, wantErr)
		}
		if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('error:after', 'keep')"); err != nil {
			t.Fatalf("transaction after callback error = %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}
		if trie.Exists("error:discard") || trie.GetString("error:after") != "keep" {
			t.Fatalf("callback-error scope state = discard=%v after=%q", trie.Exists("error:discard"), trie.GetString("error:after"))
		}
	})

	t.Run("panic", func(t *testing.T) {
		trie := newTestTrie(t)
		tx, err := BeginSQLTransaction(trie)
		if err != nil {
			t.Fatalf("BeginSQLTransaction() error = %v", err)
		}
		defer tx.Rollback()
		func() {
			defer func() {
				if recover() == nil {
					t.Fatal("Scope() did not re-panic callback panic")
				}
			}()
			_ = tx.Scope(func(tx *SQLTransaction) error {
				_, executeErr := tx.Execute("INSERT INTO cache (key, value) VALUES ('panic:discard', 'discard')")
				if executeErr != nil {
					return executeErr
				}
				panic("scope panic")
			})
		}()
		if _, err := tx.Execute("INSERT INTO cache (key, value) VALUES ('panic:after', 'keep')"); err != nil {
			t.Fatalf("transaction after panic = %v", err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("Commit() error = %v", err)
		}
		if trie.Exists("panic:discard") || trie.GetString("panic:after") != "keep" {
			t.Fatalf("panic scope state = discard=%v after=%q", trie.Exists("panic:discard"), trie.GetString("panic:after"))
		}
	})
}
