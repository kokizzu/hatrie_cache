package hatSql

import (
	"context"
	"strings"
	"testing"
)

const ch028MaxThreadsQuery = "FROM VALUES (1), (2) AS values(id) SELECT id SETTINGS max_threads = 2"

func TestCH028MaxThreadsSetting(t *testing.T) {
	query, err := parseSQLQuery(ch028MaxThreadsQuery)
	if err != nil {
		t.Fatalf("parse max_threads query: %v", err)
	}
	if query.maxThreads != 2 {
		t.Fatalf("parsed max_threads = %d, want 2", query.maxThreads)
	}

	options := SQLQueryOptions{}
	if err := applySQLMaxThreads(query, &options); err != nil {
		t.Fatalf("apply max_threads: %v", err)
	}
	if options.Workers != 2 {
		t.Fatalf("applied Workers = %d, want 2", options.Workers)
	}

	result, err := ExecuteSQLQueryContext(context.Background(), ch028MaxThreadsQuery, nil, SQLQueryOptions{})
	if err != nil {
		t.Fatalf("execute max_threads query: %v", err)
	}
	if len(result.Rows) != 2 || result.Rows[0]["id"] != int64(1) || result.Rows[1]["id"] != int64(2) {
		t.Fatalf("max_threads result = %#v, want two ordered rows", result.Rows)
	}
}

func TestCH028MaxThreadsPreservesExplicitWorkers(t *testing.T) {
	query, err := parseSQLQuery(ch028MaxThreadsQuery)
	if err != nil {
		t.Fatalf("parse max_threads query: %v", err)
	}
	options := SQLQueryOptions{Workers: 1}
	if err := applySQLMaxThreads(query, &options); err != nil {
		t.Fatalf("apply max_threads with explicit Workers: %v", err)
	}
	if options.Workers != 1 {
		t.Fatalf("explicit Workers = %d, want 1", options.Workers)
	}
	options = SQLQueryOptions{Workers: 8}
	if err := applySQLMaxThreads(query, &options); err != nil {
		t.Fatalf("apply max_threads as upper bound: %v", err)
	}
	if options.Workers != 2 {
		t.Fatalf("Workers above max_threads = %d, want 2", options.Workers)
	}
}

func TestCH028MaxThreadsDefaultRemainsSequential(t *testing.T) {
	query, err := parseSQLQuery("FROM VALUES (1) AS values(id) SELECT id")
	if err != nil {
		t.Fatalf("parse default query: %v", err)
	}
	options := SQLQueryOptions{}
	if err := applySQLMaxThreads(query, &options); err != nil {
		t.Fatalf("apply default query: %v", err)
	}
	if options.Workers != 0 {
		t.Fatalf("default Workers = %d, want 0", options.Workers)
	}
}

func TestCH028MaxThreadsSettingStreams(t *testing.T) {
	var rows []SQLRow
	err := ExecuteSQLQueryRows(context.Background(), ch028MaxThreadsQuery, nil, nil, SQLQueryOptions{}, func(_ []string, row SQLRow) error {
		rows = append(rows, row)
		return nil
	})
	if err != nil {
		t.Fatalf("stream max_threads query: %v", err)
	}
	if len(rows) != 2 || rows[0]["id"] != int64(1) || rows[1]["id"] != int64(2) {
		t.Fatalf("streamed max_threads rows = %#v, want two ordered rows", rows)
	}
}

func TestCH028MaxThreadsAdmission(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{
			name:   "zero",
			source: "FROM VALUES (1) AS values(id) SELECT id SETTINGS max_threads = 0",
			want:   "max_threads",
		},
		{
			name:   "over limit",
			source: "FROM VALUES (1) AS values(id) SELECT id SETTINGS max_threads = 257",
			want:   "max_threads",
		},
		{
			name:   "unknown setting",
			source: "FROM VALUES (1) AS values(id) SELECT id SETTINGS max_block_size = 2",
			want:   "unsupported SQL setting",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseSQLQuery(test.source)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(test.want)) {
				t.Fatalf("parse error = %v, want text %q", err, test.want)
			}
		})
	}
}
