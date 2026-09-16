package hatSql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"hatrie_cache/hat/hatSql"
)

func TestCH018ProjectionRefreshStatusTracksLagFailureAndRecovery(t *testing.T) {
	rows := map[string][]hatSql.Row{"people": {{"name": "Ada"}}}
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		available, ok := rows[key]
		if !ok {
			return nil, fmt.Errorf("source %q unavailable", key)
		}
		return hatSql.CloneRows(available), nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	runner, err := hatSql.NewIncrementalProjectionRunner(
		views,
		resolver,
		hatSql.QueryOptions{},
		hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	initial := runner.Status()
	if initial.Name != "people" || !initial.Enabled || initial.State != hatSql.ProjectionRefreshStateIdle || initial.AppliedSequence != 0 || initial.ObservedSequence != 0 || initial.Lag != 0 {
		t.Fatalf("initial status = %#v", initial)
	}

	delete(rows, "people")
	if _, err := runner.Apply(context.Background(), []hatSql.ProjectionChange{{Sequence: 1, Dependency: "people"}}); err == nil {
		t.Fatal("failed Apply() error = nil")
	}
	failed := runner.Status()
	if failed.State != hatSql.ProjectionRefreshStateFailed || failed.AppliedSequence != 0 || failed.ObservedSequence != 1 || failed.Lag != 1 || failed.ConsecutiveFailures != 1 || failed.LastError == "" || failed.LastAttemptAt.IsZero() || !failed.LastSuccessAt.IsZero() {
		t.Fatalf("failed status = %#v", failed)
	}

	rows["people"] = []hatSql.Row{{"name": "Lin"}}
	if _, err := runner.Apply(context.Background(), []hatSql.ProjectionChange{{Sequence: 1, Dependency: "people"}}); err != nil {
		t.Fatal(err)
	}
	recovered := runner.Status()
	if recovered.State != hatSql.ProjectionRefreshStateHealthy || recovered.AppliedSequence != 1 || recovered.ObservedSequence != 1 || recovered.Lag != 0 || recovered.ConsecutiveFailures != 0 || recovered.LastError != "" || recovered.LastSuccessAt.IsZero() {
		t.Fatalf("recovered status = %#v", recovered)
	}
}

func TestCH018ProjectionRefreshStatusBoundsErrorsAndTracksRebuild(t *testing.T) {
	available := true
	resolver := hatSql.SourceResolverFunc(func(_ string, key string) ([]hatSql.Row, error) {
		if !available {
			return nil, fmt.Errorf("source %q unavailable: %s", key, strings.Repeat("e", 2048))
		}
		return []hatSql.Row{{"name": "Ada"}}, nil
	})
	views := hatSql.NewMaterializedViews()
	if _, err := views.Create(context.Background(), hatSql.MaterializedViewDefinition{
		Name:         "people_view",
		Query:        "FROM CACHE('people') SELECT name",
		Dependencies: []string{"people"},
	}, resolver, hatSql.QueryOptions{}); err != nil {
		t.Fatal(err)
	}
	runner, err := hatSql.NewIncrementalProjectionRunner(
		views,
		resolver,
		hatSql.QueryOptions{},
		hatSql.IncrementalProjectionRunnerOptions{Name: "people", Enabled: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	available = false
	if _, err := runner.Apply(context.Background(), []hatSql.ProjectionChange{{Sequence: 1, Dependency: "people"}}); err == nil {
		t.Fatal("Apply() error = nil")
	}
	failed := runner.Status()
	if len(failed.LastError) != 1024 {
		t.Fatalf("bounded error length = %d, want 1024", len(failed.LastError))
	}

	available = true
	if _, err := runner.Rebuild(context.Background(), []string{"people"}, 4); err != nil {
		t.Fatal(err)
	}
	rebuilt := runner.Status()
	if rebuilt.State != hatSql.ProjectionRefreshStateHealthy || rebuilt.AppliedSequence != 4 || rebuilt.ObservedSequence != 4 || rebuilt.Lag != 0 || rebuilt.ConsecutiveFailures != 0 || rebuilt.LastError != "" || rebuilt.LastSuccessAt.IsZero() {
		t.Fatalf("rebuilt status = %#v", rebuilt)
	}
}

func TestCH018ProjectionRefreshStatusDefaultsOffAndReportsUnappliedFutureInput(t *testing.T) {
	resolver := hatSql.SourceResolverFunc(func(string, string) ([]hatSql.Row, error) {
		return nil, nil
	})
	views := hatSql.NewMaterializedViews()
	disabled, err := hatSql.NewIncrementalProjectionRunner(
		views,
		resolver,
		hatSql.QueryOptions{},
		hatSql.IncrementalProjectionRunnerOptions{Name: "disabled"},
	)
	if err != nil {
		t.Fatal(err)
	}
	status := disabled.Status()
	if status.Enabled || status.State != hatSql.ProjectionRefreshStateDisabled || status.Lag != 0 {
		t.Fatalf("disabled status = %#v", status)
	}

	enabled, err := hatSql.NewIncrementalProjectionRunner(
		views,
		resolver,
		hatSql.QueryOptions{},
		hatSql.IncrementalProjectionRunnerOptions{Name: "future", Enabled: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := enabled.Apply(context.Background(), []hatSql.ProjectionChange{{Sequence: 3, Dependency: "people"}}); err == nil {
		t.Fatal("future Apply() error = nil")
	}
	status = enabled.Status()
	if status.State != hatSql.ProjectionRefreshStateFailed || status.AppliedSequence != 0 || status.ObservedSequence != 3 || status.Lag != 3 || status.ConsecutiveFailures != 1 || status.LastError == "" {
		t.Fatalf("future status = %#v", status)
	}
}
