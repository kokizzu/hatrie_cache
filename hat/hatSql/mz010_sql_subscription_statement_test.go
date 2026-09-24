package hatSql

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

func TestMZ010ParseSQLSubscriptionStatement(t *testing.T) {
	tests := []struct {
		name   string
		source string
		mode   SQLSubscriptionMode
		query  string
	}{
		{name: "subscribe snapshot default", source: "SUBSCRIBE FROM CACHE('people') SELECT id", mode: SQLSubscriptionModeSnapshot, query: "FROM CACHE('people') SELECT id"},
		{name: "subscribe snapshot explicit", source: "subscribe snapshot FROM CACHE('people') SELECT id", mode: SQLSubscriptionModeSnapshot, query: "FROM CACHE('people') SELECT id"},
		{name: "subscribe differential", source: "SUBSCRIBE DIFFERENTIAL FROM CACHE('people') SELECT id", mode: SQLSubscriptionModeDifferential, query: "FROM CACHE('people') SELECT id"},
		{name: "tail alias", source: "TAIL FROM CACHE('people') SELECT id", mode: SQLSubscriptionModeDifferential, query: "FROM CACHE('people') SELECT id"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			statement, err := ParseSQLSubscriptionStatement(test.source)
			if err != nil {
				t.Fatalf("ParseSQLSubscriptionStatement() error = %v", err)
			}
			if statement.Mode != test.mode || statement.Definition.Query != test.query {
				t.Fatalf("statement = %#v, want mode %q query %q", statement, test.mode, test.query)
			}
		})
	}
}

func TestMZ010ParseSQLSubscriptionStatementRejectsInvalidInput(t *testing.T) {
	for _, source := range []string{"", "SUBSCRIBE", "TAIL", "WATCH FROM CACHE('people') SELECT id"} {
		_, err := ParseSQLSubscriptionStatement(source)
		if !errors.Is(err, ErrSQLSubscriptionStatementSyntax) {
			t.Errorf("ParseSQLSubscriptionStatement(%q) error = %v, want %v", source, err, ErrSQLSubscriptionStatementSyntax)
		}
	}
}

func TestMZ010TailStatementFeedsDifferentialSubscription(t *testing.T) {
	rows := map[string][]Row{"people": {{"id": 1, "name": "Ada"}}}
	resolver := SourceResolverFunc(func(_ string, key string) ([]Row, error) {
		return CloneRows(rows[key]), nil
	})
	statement, err := ParseSQLSubscriptionStatement("TAIL FROM CACHE('people') SELECT id, name")
	if err != nil {
		t.Fatal(err)
	}
	registry := NewQuerySubscriptions(2)
	subscription, err := registry.SubscribeDifferentialSQL(context.Background(), statement.Definition, resolver, QueryOptions{})
	if err != nil {
		t.Fatalf("SubscribeDifferentialSQL() error = %v", err)
	}
	defer subscription.Close()
	initial := <-subscription.Updates()
	if len(initial.Deltas) != 1 || initial.Deltas[0].Diff != 1 {
		t.Fatalf("initial differential batch = %#v", initial)
	}
	if !reflect.DeepEqual(initial.Deltas[0].Row, Row{"id": 1, "name": "Ada"}) {
		t.Fatalf("initial row = %#v", initial.Deltas[0].Row)
	}
}
