package hatSql

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

func TestSQLBackgroundIndexBuilderPublishesAfterMonotoneFrontierProgress(t *testing.T) {
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var mu sync.Mutex
	var applied []DifferentialRow
	published := false

	definition := SQLBackgroundIndexBuildDefinition{
		Name: "accounts-by-region",
		Batches: []SQLBackgroundIndexBuildBatch{
			{Frontier: 10, Updates: []DifferentialRow{{Key: "a", Diff: 1, Row: Row{"region": "sg"}}}},
			{Frontier: 20, Updates: []DifferentialRow{{Key: "b", Diff: 1, Row: Row{"region": "jp"}}}},
		},
		Apply: func(updates []DifferentialRow) error {
			mu.Lock()
			for _, update := range updates {
				applied = append(applied, cloneSQLBackgroundIndexUpdate(update))
			}
			mu.Unlock()
			if len(applied) == 1 {
				close(firstStarted)
				<-releaseFirst
			}
			return nil
		},
		Publish: func() error {
			published = true
			return nil
		},
	}
	builder, err := NewSQLBackgroundIndexBuilder(definition, SQLBackgroundIndexBuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	initial := builder.Status()
	if initial.State != SQLBackgroundIndexBuildQueued || initial.BuildFrontier != 0 || initial.TargetFrontier != 20 || initial.TotalRows != 2 {
		t.Fatalf("initial status = %#v", initial)
	}
	if err := builder.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	<-firstStarted
	running := builder.Status()
	if running.State != SQLBackgroundIndexBuildRunning || running.BuildFrontier != 0 || running.ProcessedRows != 0 || running.AppliedBatches != 0 {
		t.Fatalf("running status = %#v", running)
	}
	close(releaseFirst)
	if err := builder.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	finished := builder.Status()
	if finished.State != SQLBackgroundIndexBuildReady || finished.BuildFrontier != 20 || finished.ProcessedRows != 2 || finished.AppliedBatches != 2 || !published {
		t.Fatalf("finished status = %#v, published=%t", finished, published)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(applied, []DifferentialRow{
		{Key: "a", Diff: 1, Row: Row{"region": "sg"}},
		{Key: "b", Diff: 1, Row: Row{"region": "jp"}},
	}) {
		t.Fatalf("applied = %#v", applied)
	}
}

func TestSQLBackgroundIndexBuilderCancellationDoesNotPublish(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	published := false
	definition := SQLBackgroundIndexBuildDefinition{
		Name:    "cancelled",
		Batches: []SQLBackgroundIndexBuildBatch{{Frontier: 10, Updates: []DifferentialRow{{Key: "a", Diff: 1}}}},
		Apply: func([]DifferentialRow) error {
			close(started)
			<-release
			return nil
		},
		Publish: func() error {
			published = true
			return nil
		},
	}
	builder, err := NewSQLBackgroundIndexBuilder(definition, SQLBackgroundIndexBuildOptions{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := builder.Start(ctx); err != nil {
		t.Fatal(err)
	}
	<-started
	cancel()
	close(release)
	if err := builder.Wait(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("Wait() error = %v, want context canceled", err)
	}
	status := builder.Status()
	if status.State != SQLBackgroundIndexBuildCanceled || status.BuildFrontier != 10 || status.ProcessedRows != 1 || published {
		t.Fatalf("cancelled status = %#v, published=%t", status, published)
	}
}

func TestSQLBackgroundIndexBuilderRejectsInvalidDefinitions(t *testing.T) {
	validBatch := SQLBackgroundIndexBuildBatch{Frontier: 10, Updates: []DifferentialRow{{Key: "a", Diff: 1}}}
	tests := []struct {
		name       string
		definition SQLBackgroundIndexBuildDefinition
		options    SQLBackgroundIndexBuildOptions
	}{
		{name: "name required", definition: SQLBackgroundIndexBuildDefinition{Batches: []SQLBackgroundIndexBuildBatch{validBatch}, Apply: func([]DifferentialRow) error { return nil }}},
		{name: "apply required", definition: SQLBackgroundIndexBuildDefinition{Name: "x", Batches: []SQLBackgroundIndexBuildBatch{validBatch}}},
		{name: "frontier regresses", definition: SQLBackgroundIndexBuildDefinition{Name: "x", Batches: []SQLBackgroundIndexBuildBatch{{Frontier: 20}, {Frontier: 10}}, Apply: func([]DifferentialRow) error { return nil }}},
		{name: "initial exceeds target", definition: SQLBackgroundIndexBuildDefinition{Name: "x", Batches: []SQLBackgroundIndexBuildBatch{validBatch}, Apply: func([]DifferentialRow) error { return nil }}, options: SQLBackgroundIndexBuildOptions{InitialFrontier: 20}},
		{name: "target is not reached", definition: SQLBackgroundIndexBuildDefinition{Name: "x", Batches: []SQLBackgroundIndexBuildBatch{validBatch}, Apply: func([]DifferentialRow) error { return nil }}, options: SQLBackgroundIndexBuildOptions{TargetFrontier: 20}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewSQLBackgroundIndexBuilder(test.definition, test.options); err == nil {
				t.Fatal("NewSQLBackgroundIndexBuilder() error = nil")
			}
		})
	}
}

func TestSQLBackgroundIndexBuilderEmptyBuildCanAdvanceToTarget(t *testing.T) {
	builder, err := NewSQLBackgroundIndexBuilder(SQLBackgroundIndexBuildDefinition{
		Name:  "empty",
		Apply: func([]DifferentialRow) error { return nil },
	}, SQLBackgroundIndexBuildOptions{InitialFrontier: 10, TargetFrontier: 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := builder.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	status := builder.Status()
	if status.State != SQLBackgroundIndexBuildReady || status.BuildFrontier != 20 || status.TotalRows != 0 {
		t.Fatalf("empty build status = %#v", status)
	}
}

func TestSQLBackgroundIndexBuilderClonesInputBatches(t *testing.T) {
	row := Row{"name": "Ada"}
	definition := SQLBackgroundIndexBuildDefinition{
		Name:    "clone-input",
		Batches: []SQLBackgroundIndexBuildBatch{{Frontier: 1, Updates: []DifferentialRow{{Key: "a", Diff: 1, Row: row}}}},
		Apply: func(updates []DifferentialRow) error {
			if updates[0].Row["name"] != "Ada" {
				t.Fatalf("builder input was not cloned: %#v", updates)
			}
			return nil
		},
	}
	builder, err := NewSQLBackgroundIndexBuilder(definition, SQLBackgroundIndexBuildOptions{CloneInputs: true})
	if err != nil {
		t.Fatal(err)
	}
	row["name"] = "mutated"
	if err := builder.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := builder.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}
