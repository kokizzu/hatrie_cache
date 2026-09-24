package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLSessionTransactionSettings(t *testing.T) {
	want := SQLSessionTransactionSettings{
		Isolation:  SQLSessionTransactionIsolationRepeatableRead,
		Timeout:    5 * time.Millisecond,
		ReadOnly:   true,
		Durability: SQLSessionTransactionDurabilityImmediate,
	}
	session, err := NewSQLSessionWithTransactionSettings(nil, want)
	if err != nil {
		t.Fatalf("NewSQLSessionWithTransactionSettings() error = %v", err)
	}
	if got := session.TransactionSettings(); got != want {
		t.Fatalf("TransactionSettings() = %#v, want %#v", got, want)
	}
	if err := session.CreateTemporaryTable("blocked", []Row{{"id": int64(1)}}); !errors.Is(err, ErrSQLSessionReadOnly) {
		t.Fatalf("read-only CreateTemporaryTable() error = %v, want %v", err, ErrSQLSessionReadOnly)
	}
	if err := session.StoreNamedResult("blocked", SQLQueryResult{}); !errors.Is(err, ErrSQLSessionReadOnly) {
		t.Fatalf("read-only StoreNamedResult() error = %v, want %v", err, ErrSQLSessionReadOnly)
	}
	if err := session.CreateView("blocked", `FROM CACHE('rows') SELECT id`); !errors.Is(err, ErrSQLSessionReadOnly) {
		t.Fatalf("read-only CreateView() error = %v, want %v", err, ErrSQLSessionReadOnly)
	}
	if err := session.DropTemporaryTableChecked("blocked"); !errors.Is(err, ErrSQLSessionReadOnly) {
		t.Fatalf("read-only DropTemporaryTableChecked() error = %v, want %v", err, ErrSQLSessionReadOnly)
	}

	if err := session.SetTransactionSettings(SQLSessionTransactionSettings{Timeout: -time.Nanosecond}); !errors.Is(err, ErrSQLSessionTransactionSettingsInvalid) {
		t.Fatalf("negative timeout error = %v, want %v", err, ErrSQLSessionTransactionSettingsInvalid)
	}
	if err := session.SetTransactionSettings(SQLSessionTransactionSettings{Isolation: "unknown"}); !errors.Is(err, ErrSQLSessionTransactionSettingsInvalid) {
		t.Fatalf("unknown isolation error = %v, want %v", err, ErrSQLSessionTransactionSettingsInvalid)
	}

	if err := session.ResetTransactionSettings(); err != nil {
		t.Fatalf("ResetTransactionSettings() error = %v", err)
	}
	if got := session.TransactionSettings(); got != (SQLSessionTransactionSettings{}) {
		t.Fatalf("reset settings = %#v, want zero", got)
	}
	if err := session.CreateTemporaryTable("allowed", []Row{{"id": int64(1)}}); err != nil {
		t.Fatalf("CreateTemporaryTable() after reset error = %v", err)
	}
}

func TestSQLSessionTransactionSettingsApplyTimeout(t *testing.T) {
	source := tu05BlockingSQLSource{}
	session, err := NewSQLSessionWithTransactionSettings(source, SQLSessionTransactionSettings{Timeout: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	_, err = session.Execute(context.Background(), `FROM CACHE('blocked') SELECT id`, nil, SQLQueryOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute() error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("session timeout took %s", elapsed)
	}
}

type tu05BlockingSQLSource struct{}

func (tu05BlockingSQLSource) ResolveSQLSource(name, key string) ([]Row, error) {
	return nil, errors.New("context-aware resolver was not used")
}

func (tu05BlockingSQLSource) ResolveSQLSourceContext(ctx context.Context, name, key string) ([]Row, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
