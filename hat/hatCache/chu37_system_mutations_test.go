package hatCache

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestSQLSystemMutationsProviderExposesCompleteMetadata(t *testing.T) {
	started := time.Date(2026, 9, 19, 2, 3, 4, 0, time.FixedZone("UTC+8", 8*60*60))
	finished := started.Add(2 * time.Second)
	affected := []string{"part-b", "part-a"}
	provider := SQLSystemMutationProviderFunc(func() ([]SQLSystemMutation, error) {
		return []SQLSystemMutation{
			{
				MutationID:    "mut-42",
				Sequence:      42,
				Command:       "MERGE",
				Key:           "orders",
				State:         "failed",
				Progress:      100,
				AffectedParts: affected,
				ErrorCode:     "E_IO",
				ErrorMessage:  "redacted I/O failure",
				StartedAt:     started,
				FinishedAt:    finished,
			},
			{
				MutationID: "mut-41",
				Sequence:   41,
				Command:    "SET",
				Key:        "orders/41",
				State:      "committed",
				Progress:   100,
			},
		}, nil
	})
	resolver := NewSQLSystemTablesResolver(nil, SQLSystemTablesResolverOptions{
		MutationProvider: provider,
	})

	rows, err := resolver.ResolveSQLSource("CACHE", SQLSystemMutationsTable)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0]["mutation_id"] != "mut-41" || rows[1]["mutation_id"] != "mut-42" {
		t.Fatalf("rows are not ordered by sequence: %#v", rows)
	}
	want := SQLRow{
		"sequence":       int64(42),
		"mutation_id":    "mut-42",
		"command":        "MERGE",
		"key":            "orders",
		"state":          "failed",
		"progress":       int64(100),
		"affected_parts": []string{"part-b", "part-a"},
		"error_code":     "E_IO",
		"error_message":  "redacted I/O failure",
		"started_at":     started.UTC(),
		"finished_at":    finished.UTC(),
	}
	if !reflect.DeepEqual(rows[1], want) {
		t.Fatalf("row = %#v, want %#v", rows[1], want)
	}
	affected[0] = "changed-after-snapshot"
	if rows[1]["affected_parts"].([]string)[0] != "part-b" {
		t.Fatalf("affected parts were not copied: %#v", rows[1])
	}
	if _, exposed := rows[1]["value"]; exposed {
		t.Fatalf("mutation values must not be exposed: %#v", rows[1])
	}
}

func TestSQLSystemMutationsProviderRejectsInvalidAndUnboundedRows(t *testing.T) {
	tests := []struct {
		name    string
		rows    []SQLSystemMutation
		limit   int
		wantErr error
	}{
		{
			name:    "limit",
			rows:    []SQLSystemMutation{{Sequence: 1}, {Sequence: 2}},
			limit:   1,
			wantErr: ErrSQLSystemMutationsLimitExceeded,
		},
		{
			name:    "negative progress",
			rows:    []SQLSystemMutation{{Sequence: 1, Progress: -1}},
			wantErr: ErrSQLSystemMutationInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := NewSQLSystemTablesResolver(nil, SQLSystemTablesResolverOptions{
				MutationProvider: SQLSystemMutationProviderFunc(func() ([]SQLSystemMutation, error) {
					return test.rows, nil
				}),
				MutationLimit: test.limit,
			})
			_, err := resolver.ResolveSQLSource("CACHE", SQLSystemMutationsTable)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestSQLSystemMutationsProviderPropagatesSnapshotError(t *testing.T) {
	want := errors.New("mutation catalog unavailable")
	resolver := NewSQLSystemTablesResolver(nil, SQLSystemTablesResolverOptions{
		MutationProvider: SQLSystemMutationProviderFunc(func() ([]SQLSystemMutation, error) {
			return nil, want
		}),
	})
	_, err := resolver.ResolveSQLSource("CACHE", SQLSystemMutationsTable)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}
