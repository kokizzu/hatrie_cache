package hatSql_test

import (
	"context"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func BenchmarkSQLTriggerTransaction(b *testing.B) {
	for _, withTrigger := range []bool{false, true} {
		name := "without_trigger"
		if withTrigger {
			name = "with_trigger"
		}
		b.Run(name, func(b *testing.B) {
			registry := hatSql.NewSQLTriggerRegistry()
			if withTrigger {
				if err := registry.Register(hatSql.SQLTrigger{
					Name: "audit", Source: "people", Operation: "INSERT",
					Prepare: func(context.Context, hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
						return hatSql.SQLTriggerAction{}, nil
					},
				}); err != nil {
					b.Fatal(err)
				}
			}
			events := make([]hatSql.SQLTriggerEvent, 32)
			for index := range events {
				events[index] = hatSql.SQLTriggerEvent{Source: "people", Operation: "INSERT", Key: string(rune('a' + index))}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				transaction, err := registry.BeginSQLTriggerTransaction(context.Background())
				if err != nil {
					b.Fatal(err)
				}
				for _, event := range events {
					if err := transaction.Add(event); err != nil {
						b.Fatal(err)
					}
				}
				if err := transaction.Commit(func(context.Context, []hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
					return hatSql.SQLTriggerAction{}, nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
