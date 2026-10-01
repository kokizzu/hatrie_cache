package hatSql

import (
	"context"
	"testing"
)

var (
	tu05AfterSettingsSink SQLTransactionSettings
	tu05AfterScopeSink    *SQLTransactionSettingsScope
)

func BenchmarkTU05AfterSessionSettingsResolve(b *testing.B) {
	session, err := NewSQLTransactionSettingsSession(DefaultSQLTransactionSettings())
	if err != nil {
		b.Fatal(err)
	}
	patch := SQLTransactionSettingsPatch{ReadOnly: true, ReadOnlySet: true}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		settings, err := session.Resolve(patch)
		if err != nil {
			b.Fatal(err)
		}
		tu05AfterSettingsSink = settings
	}
}

func BenchmarkTU05AfterSessionSettingsBeginRollback(b *testing.B) {
	session, err := NewSQLTransactionSettingsSession(DefaultSQLTransactionSettings())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		scope, err := session.Begin(context.Background(), SQLTransactionSettingsPatch{})
		if err != nil {
			b.Fatal(err)
		}
		tu05AfterScopeSink = scope
		if err := scope.Rollback(); err != nil {
			b.Fatal(err)
		}
	}
}
