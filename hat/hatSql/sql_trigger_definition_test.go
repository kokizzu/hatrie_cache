package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestParseSQLTriggerDefinition(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   SQLTriggerDefinition
	}{
		{
			name:   "canonical",
			source: "CREATE TRIGGER audit_insert AFTER INSERT ON people FOR EACH ROW",
			want: SQLTriggerDefinition{
				Name: "audit_insert", Timing: "AFTER", Operation: "INSERT", Source: "people",
			},
		},
		{
			name:   "case insensitive with semicolon",
			source: "create trigger audit_delete after delete on people for each row;",
			want: SQLTriggerDefinition{
				Name: "audit_delete", Timing: "AFTER", Operation: "DELETE", Source: "people",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParseSQLTriggerDefinition(test.source)
			if err != nil {
				t.Fatalf("ParseSQLTriggerDefinition() error = %v", err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ParseSQLTriggerDefinition() = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestParseSQLTriggerDefinitionRejectsUnsupportedOrMalformedStatements(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   error
	}{
		{name: "before timing", source: "CREATE TRIGGER audit BEFORE INSERT ON people FOR EACH ROW", want: ErrSQLTriggerUnsupportedTiming},
		{name: "unsupported operation", source: "CREATE TRIGGER audit AFTER SELECT ON people FOR EACH ROW", want: ErrSQLTriggerUnsupportedOperation},
		{name: "missing row clause", source: "CREATE TRIGGER audit AFTER INSERT ON people", want: ErrSQLTriggerDefinitionInvalid},
		{name: "trailing statement", source: "CREATE TRIGGER audit AFTER INSERT ON people FOR EACH ROW; SELECT 1", want: ErrSQLTriggerDefinitionInvalid},
		{name: "empty source", source: "", want: ErrSQLTriggerDefinitionInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseSQLTriggerDefinition(test.source)
			if !errors.Is(err, test.want) {
				t.Fatalf("ParseSQLTriggerDefinition() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestRegisterSQLTriggerUsesParsedDefinitionAndTransactionOrdering(t *testing.T) {
	registry := NewSQLTriggerRegistry()
	log := make([]string, 0, 3)
	if err := registry.RegisterSQLTrigger("CREATE TRIGGER audit AFTER INSERT ON people FOR EACH ROW", func(context.Context, SQLTriggerEvent) (SQLTriggerAction, error) {
		log = append(log, "prepare")
		return SQLTriggerAction{Commit: func(context.Context) error {
			log = append(log, "trigger")
			return nil
		}}, nil
	}); err != nil {
		t.Fatalf("RegisterSQLTrigger() error = %v", err)
	}
	transaction, err := registry.BeginSQLTriggerTransaction(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := transaction.Add(SQLTriggerEvent{Source: "people", Operation: "INSERT", Key: "ada"}); err != nil {
		t.Fatal(err)
	}
	if err := transaction.Commit(func(context.Context, []SQLTriggerEvent) (SQLTriggerAction, error) {
		log = append(log, "primary")
		return SQLTriggerAction{}, nil
	}); err != nil {
		t.Fatalf("Commit() error = %v", err)
	}
	if want := []string{"prepare", "primary", "trigger"}; !reflect.DeepEqual(log, want) {
		t.Fatalf("transaction log = %v, want %v", log, want)
	}
}

func BenchmarkParseSQLTriggerDefinition(b *testing.B) {
	source := "CREATE TRIGGER audit_insert AFTER INSERT ON people FOR EACH ROW"
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := ParseSQLTriggerDefinition(source); err != nil {
			b.Fatal(err)
		}
	}
}
