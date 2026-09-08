package hatCache

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestExecuteSQLMutationTriggersAreOptInAndAtomic(t *testing.T) {
	trie := newTestTrie(t)
	registry := hatSql.NewSQLTriggerRegistry()
	prepared := make([]hatSql.SQLTriggerEvent, 0, 4)
	committed := make([]string, 0, 4)
	if err := registry.Register(hatSql.SQLTrigger{
		Name:   "audit",
		Source: "cache",
		Prepare: func(_ context.Context, event hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
			prepared = append(prepared, event)
			return hatSql.SQLTriggerAction{Commit: func(context.Context) error {
				committed = append(committed, event.Operation)
				return nil
			}}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := ExecuteSQLMutation(context.Background(), trie, "INSERT INTO cache (key, value) VALUES ('legacy', 'off')", nil, SQLQueryOptions{}); err != nil {
		t.Fatalf("legacy mutation error = %v", err)
	}
	if len(prepared) != 0 || len(committed) != 0 {
		t.Fatalf("default trigger activity = prepared %v committed %v, want none", prepared, committed)
	}

	options := SQLQueryOptions{TriggerRegistry: registry}
	if result, err := ExecuteSQLMutation(context.Background(), trie, "INSERT INTO cache (key, value) VALUES ('user:1', 'Ada')", nil, options); err != nil || result.Affected != 1 {
		t.Fatalf("INSERT result = %#v, error %v", result, err)
	}
	if result, err := ExecuteSQLMutation(context.Background(), trie, "UPDATE cache SET value = 'Grace' WHERE key = 'user:1'", nil, options); err != nil || result.Affected != 1 {
		t.Fatalf("UPDATE result = %#v, error %v", result, err)
	}
	if result, err := ExecuteSQLMutation(context.Background(), trie, "DELETE FROM cache WHERE key = 'user:1'", nil, options); err != nil || result.Affected != 1 {
		t.Fatalf("DELETE result = %#v, error %v", result, err)
	}

	if got, want := committed, []string{"INSERT", "UPDATE", "DELETE"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("committed operations = %v, want %v", got, want)
	}
	if len(prepared) != 3 {
		t.Fatalf("prepared events = %d, want 3", len(prepared))
	}
	if prepared[0].Source != "cache" || prepared[0].Key != "user:1" || prepared[0].Before != nil || !reflect.DeepEqual(prepared[0].After, hatSql.Row{"key": "user:1", "value": "Ada"}) {
		t.Fatalf("INSERT event = %#v", prepared[0])
	}
	if !reflect.DeepEqual(prepared[1].Before, hatSql.Row{"key": "user:1", "value": "Ada"}) || !reflect.DeepEqual(prepared[1].After, hatSql.Row{"key": "user:1", "value": "Grace"}) {
		t.Fatalf("UPDATE event = %#v", prepared[1])
	}
	if !reflect.DeepEqual(prepared[2].Before, hatSql.Row{"key": "user:1", "value": "Grace"}) || prepared[2].After != nil {
		t.Fatalf("DELETE event = %#v", prepared[2])
	}
	if trie.Exists("user:1") {
		t.Fatal("DELETE did not remove user:1")
	}

	rejectRegistry := hatSql.NewSQLTriggerRegistry()
	wantErr := errors.New("audit policy rejected mutation")
	if err := rejectRegistry.Register(hatSql.SQLTrigger{
		Name:   "reject",
		Source: "cache",
		Prepare: func(context.Context, hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
			return hatSql.SQLTriggerAction{}, wantErr
		},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := ExecuteSQLMutation(context.Background(), trie, "INSERT INTO cache (key, value) VALUES ('rejected', 'value')", nil, SQLQueryOptions{TriggerRegistry: rejectRegistry}); !errors.Is(err, wantErr) {
		t.Fatalf("rejected mutation error = %v, want %v", err, wantErr)
	}
	if trie.Exists("rejected") {
		t.Fatal("rejected mutation was applied")
	}
}

func TestExecuteSQLMutationTriggerCommitFailureRollsBackInsert(t *testing.T) {
	trie := newTestTrie(t)
	registry := hatSql.NewSQLTriggerRegistry()
	wantErr := errors.New("audit sink unavailable")
	if err := registry.Register(hatSql.SQLTrigger{
		Name:   "audit",
		Source: "cache",
		Prepare: func(context.Context, hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
			return hatSql.SQLTriggerAction{Commit: func(context.Context) error { return wantErr }}, nil
		},
	}); err != nil {
		t.Fatal(err)
	}

	if _, err := ExecuteSQLMutation(context.Background(), trie, "INSERT INTO cache (key, value) VALUES ('rollback', 'value')", nil, SQLQueryOptions{TriggerRegistry: registry}); !errors.Is(err, wantErr) {
		t.Fatalf("commit failure error = %v, want %v", err, wantErr)
	}
	if trie.Exists("rollback") {
		t.Fatal("primary insert remained after trigger commit failure")
	}
	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "SETINT", Key: "counter", Value: "7"}); !response.OK {
		t.Fatalf("seed counter response = %#v", response)
	}
	if _, err := ExecuteSQLMutation(context.Background(), trie, "INSERT INTO cache (key, counter) VALUES ('counter', 9)", nil, SQLQueryOptions{TriggerRegistry: registry}); !errors.Is(err, wantErr) {
		t.Fatalf("counter commit failure error = %v, want %v", err, wantErr)
	}
	if response := trie.ExecuteCommand(CacheCommandRequest{Command: "GET", Key: "counter"}); !response.OK || response.Value != "7" {
		t.Fatalf("counter after rollback = %#v, want 7", response)
	}
}

func TestExecuteSQLMutationTriggersRejectUnsupportedShapes(t *testing.T) {
	trie := newTestTrie(t)
	options := SQLQueryOptions{TriggerRegistry: hatSql.NewSQLTriggerRegistry()}
	for _, source := range []string{
		"INSERT INTO cache (key, value) FROM VALUES ('bulk', 'value') AS rows(key, value) SELECT key, value",
		"MERGE INTO cache (key, value) VALUES ('merge', 'value') WHEN NOT MATCHED THEN INSERT",
		"BEGIN ATOMIC; INSERT INTO cache (key, value) VALUES ('batch', 'value'); COMMIT",
	} {
		if _, err := ExecuteSQLMutation(context.Background(), trie, source, nil, options); err == nil {
			t.Fatalf("unsupported trigger mutation %q was accepted", source)
		}
	}
	for _, key := range []string{"bulk", "merge", "batch"} {
		if trie.Exists(key) {
			t.Fatalf("unsupported trigger mutation created %q", key)
		}
	}
}

func BenchmarkExecuteSQLMutationTrigger(b *testing.B) {
	for _, withTrigger := range []bool{false, true} {
		name := "disabled"
		if withTrigger {
			name = "enabled"
		}
		b.Run(name, func(b *testing.B) {
			trie := CreateHatTrie()
			b.Cleanup(trie.Destroy)
			options := SQLQueryOptions{}
			if withTrigger {
				registry := hatSql.NewSQLTriggerRegistry()
				if err := registry.Register(hatSql.SQLTrigger{
					Name:   "benchmark",
					Source: "cache",
					Prepare: func(context.Context, hatSql.SQLTriggerEvent) (hatSql.SQLTriggerAction, error) {
						return hatSql.SQLTriggerAction{}, nil
					},
				}); err != nil {
					b.Fatal(err)
				}
				options.TriggerRegistry = registry
			}
			b.ReportAllocs()
			b.ResetTimer()
			for index := 0; index < b.N; index++ {
				if _, err := ExecuteSQLMutation(context.Background(), trie, "INSERT INTO cache (key, value) VALUES ('hot', 'value')", nil, options); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
