package hatSql

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestTU05SessionTransactionSettingsInheritAndReset(t *testing.T) {
	session, err := NewSQLTransactionSettingsSession(SQLTransactionSettings{
		Isolation:  SQLTransactionIsolationRepeatableRead,
		ReadOnly:   true,
		Timeout:    2 * time.Second,
		Durability: SQLTransactionDurabilityRelaxed,
	})
	if err != nil {
		t.Fatalf("NewSQLTransactionSettingsSession() error = %v", err)
	}
	if got := session.Defaults(); got.Isolation != SQLTransactionIsolationRepeatableRead || !got.ReadOnly || got.Timeout != 2*time.Second || got.Durability != SQLTransactionDurabilityRelaxed {
		t.Fatalf("Defaults() = %#v", got)
	}

	if err := session.SetDefaults(SQLTransactionSettings{Isolation: SQLTransactionIsolationSerializable}); err != nil {
		t.Fatalf("SetDefaults() error = %v", err)
	}
	if got := session.Defaults(); got.Isolation != SQLTransactionIsolationSerializable || got.ReadOnly || got.Timeout != 0 || got.Durability != SQLTransactionDurabilityDurable {
		t.Fatalf("normalized defaults = %#v", got)
	}
	if err := session.ResetDefaults(); err != nil {
		t.Fatalf("ResetDefaults() error = %v", err)
	}
	if got, want := session.Defaults(), DefaultSQLTransactionSettings(); got != want {
		t.Fatalf("reset defaults = %#v, want %#v", got, want)
	}
}

func TestTU05SessionTransactionSettingsPatchAndScope(t *testing.T) {
	session, err := NewSQLTransactionSettingsSession(DefaultSQLTransactionSettings())
	if err != nil {
		t.Fatalf("NewSQLTransactionSettingsSession() error = %v", err)
	}
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	scope, err := session.Begin(parent, SQLTransactionSettingsPatch{
		Isolation:   SQLTransactionIsolationSerializable,
		ReadOnly:    true,
		ReadOnlySet: true,
		Timeout:     time.Second,
		TimeoutSet:  true,
		Durability:  SQLTransactionDurabilityRelaxed,
	})
	if err != nil {
		t.Fatalf("Begin() error = %v", err)
	}
	if got := scope.Settings(); got.Isolation != SQLTransactionIsolationSerializable || !got.ReadOnly || got.Timeout != time.Second || got.Durability != SQLTransactionDurabilityRelaxed {
		t.Fatalf("scope settings = %#v", got)
	}
	if _, ok := scope.Context().Deadline(); !ok {
		t.Fatal("scope context has no timeout deadline")
	}
	if scope.Done() {
		t.Fatal("scope is done before commit")
	}
	if err := scope.Commit(); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if !scope.Done() {
		t.Fatal("scope is not done after commit")
	}
	if err := scope.Rollback(); !errors.Is(err, ErrSQLTransactionSettingsScopeClosed) {
		t.Fatalf("second completion error = %v, want closed", err)
	}
}

func TestTU05SessionTransactionSettingsRejectInvalidValues(t *testing.T) {
	for name, settings := range map[string]SQLTransactionSettings{
		"negative timeout":   {Timeout: -time.Nanosecond},
		"unknown isolation":  {Isolation: SQLTransactionIsolation(99)},
		"unknown durability": {Durability: SQLTransactionDurability(99)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewSQLTransactionSettingsSession(settings); !errors.Is(err, ErrSQLTransactionSettingsInvalid) {
				t.Fatalf("error = %v, want invalid settings", err)
			}
		})
	}
	session, err := NewSQLTransactionSettingsSession(DefaultSQLTransactionSettings())
	if err != nil {
		t.Fatalf("NewSQLTransactionSettingsSession() error = %v", err)
	}
	if _, err := session.Resolve(SQLTransactionSettingsPatch{Timeout: -time.Nanosecond, TimeoutSet: true}); !errors.Is(err, ErrSQLTransactionSettingsInvalid) {
		t.Fatalf("invalid patch error = %v, want invalid settings", err)
	}
}
