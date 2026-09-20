package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSQLSessionTransactionSettingsDefaultsAndReset(t *testing.T) {
	session := NewSQLSession(nil)
	want := SQLSessionTransactionSettings{
		Isolation:  SQLSessionIsolationReadCommitted,
		Durability: SQLSessionDurabilityMemory,
	}
	if got := session.TransactionSettings(); got != want {
		t.Fatalf("default transaction settings = %#v, want %#v", got, want)
	}

	custom := SQLSessionTransactionSettings{
		Isolation:  SQLSessionIsolationSerializable,
		Timeout:    2 * time.Second,
		ReadOnly:   true,
		Durability: SQLSessionDurabilityDurable,
	}
	if err := session.SetTransactionSettings(custom); err != nil {
		t.Fatalf("SetTransactionSettings() error = %v", err)
	}
	if got := session.TransactionSettings(); got != custom {
		t.Fatalf("custom transaction settings = %#v, want %#v", got, custom)
	}

	session.ResetTransactionSettings()
	if got := session.TransactionSettings(); got != want {
		t.Fatalf("reset transaction settings = %#v, want %#v", got, want)
	}
}

func TestSQLSessionTransactionSettingsRejectInvalidValues(t *testing.T) {
	session := NewSQLSession(nil)
	base := session.TransactionSettings()
	cases := []SQLSessionTransactionSettings{
		{Isolation: SQLSessionIsolation("unknown"), Durability: SQLSessionDurabilityMemory},
		{Isolation: SQLSessionIsolationReadCommitted, Durability: SQLSessionDurability("unknown")},
		{Isolation: SQLSessionIsolationReadCommitted, Durability: SQLSessionDurabilityMemory, Timeout: -time.Second},
	}
	for index, settings := range cases {
		if err := session.SetTransactionSettings(settings); err == nil {
			t.Fatalf("case %d accepted invalid settings %#v", index, settings)
		}
		if got := session.TransactionSettings(); got != base {
			t.Fatalf("case %d changed settings to %#v after rejection", index, got)
		}
	}
}

type tu05DeadlineSource struct{}

func (tu05DeadlineSource) ResolveSQLSource(string, string) ([]Row, error) {
	return nil, errors.New("legacy resolver used")
}

func (tu05DeadlineSource) ResolveSQLSourceContext(ctx context.Context, _ string, _ string) ([]Row, error) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 50*time.Millisecond {
		return nil, errors.New("session timeout was not propagated")
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestSQLSessionTransactionSettingsPropagateTimeout(t *testing.T) {
	session := NewSQLSession(tu05DeadlineSource{})
	if err := session.SetTransactionSettings(SQLSessionTransactionSettings{
		Isolation:  SQLSessionIsolationReadCommitted,
		Timeout:    5 * time.Millisecond,
		Durability: SQLSessionDurabilityMemory,
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := session.Execute(ctx, `FROM CACHE('blocked') SELECT value`, nil, SQLQueryOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Execute() error = %v, want context deadline", err)
	}
}

func TestSQLSessionReadOnlyTransactionBlocksSessionDDL(t *testing.T) {
	session := NewSQLSession(nil)
	if err := session.SetTransactionSettings(SQLSessionTransactionSettings{
		Isolation:  SQLSessionIsolationReadCommitted,
		ReadOnly:   true,
		Durability: SQLSessionDurabilityMemory,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Execute(context.Background(), `CREATE TEMP TABLE people AS FROM VALUES ('Ada') AS rows(name) SELECT name`, nil, SQLQueryOptions{}); !errors.Is(err, ErrSQLSessionReadOnly) {
		t.Fatalf("read-only CREATE TEMP TABLE error = %v, want %v", err, ErrSQLSessionReadOnly)
	}
}

func TestSQLSessionReadOnlyTransactionBlocksDirectMutations(t *testing.T) {
	session := NewSQLSession(nil)
	if err := session.CreateTemporaryTable("people", []Row{{"name": "Ada"}}); err != nil {
		t.Fatal(err)
	}
	if err := session.SetTransactionSettings(SQLSessionTransactionSettings{ReadOnly: true}); err != nil {
		t.Fatal(err)
	}
	session.DropTemporaryTable("people")
	rows, err := session.ResolveSQLSource("CACHE", "people")
	if err != nil || len(rows) != 1 {
		t.Fatalf("read-only DropTemporaryTable() changed state: rows=%#v err=%v", rows, err)
	}
	if _, err := session.ApplyViewChanges([]SQLSessionViewChange{{
		Name: "people_view", Query: `FROM CACHE('people') SELECT name`,
	}}); !errors.Is(err, ErrSQLSessionReadOnly) {
		t.Fatalf("read-only ApplyViewChanges() error = %v, want %v", err, ErrSQLSessionReadOnly)
	}
}

func BenchmarkSQLSessionExecuteDefaultSettings(b *testing.B) {
	session := NewSQLSession(nil)
	query := `FROM VALUES (1) AS rows(id) SELECT id`
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := session.Execute(context.Background(), query, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSQLSessionExecuteTimeoutSettings(b *testing.B) {
	session := NewSQLSession(nil)
	if err := session.SetTransactionSettings(SQLSessionTransactionSettings{
		Isolation:  SQLSessionIsolationReadCommitted,
		Timeout:    time.Minute,
		Durability: SQLSessionDurabilityMemory,
	}); err != nil {
		b.Fatal(err)
	}
	query := `FROM VALUES (1) AS rows(id) SELECT id`
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		if _, err := session.Execute(context.Background(), query, nil, SQLQueryOptions{}); err != nil {
			b.Fatal(err)
		}
	}
}
