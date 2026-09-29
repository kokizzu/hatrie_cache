package hatSql

import (
	"context"
	"errors"
	"testing"
)

func TestSQLBackgroundIndexBuilderApplyAndPublishFailuresStayUnready(t *testing.T) {
	applyErr := errors.New("staging failed")
	publishErr := errors.New("publication failed")
	t.Run("apply failure", func(t *testing.T) {
		published := false
		builder, err := NewSQLBackgroundIndexBuilder(SQLBackgroundIndexBuildDefinition{
			Name: "apply-failure",
			Batches: []SQLBackgroundIndexBuildBatch{
				{Frontier: 10, Updates: []DifferentialRow{{Key: "a", Diff: 1}}},
				{Frontier: 20, Updates: []DifferentialRow{{Key: "b", Diff: 1}}},
			},
			Apply: func(updates []DifferentialRow) error {
				if updates[0].Key == "b" {
					return applyErr
				}
				return nil
			},
			Publish: func() error {
				published = true
				return nil
			},
		}, SQLBackgroundIndexBuildOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := builder.Wait(context.Background()); !errors.Is(err, applyErr) {
			t.Fatalf("Wait() error = %v, want apply error", err)
		}
		status := builder.Status()
		if status.State != SQLBackgroundIndexBuildFailed || status.BuildFrontier != 10 || status.ProcessedRows != 1 || published {
			t.Fatalf("apply failure status = %#v, published=%t", status, published)
		}
	})

	t.Run("publish failure", func(t *testing.T) {
		builder, err := NewSQLBackgroundIndexBuilder(SQLBackgroundIndexBuildDefinition{
			Name:    "publish-failure",
			Batches: []SQLBackgroundIndexBuildBatch{{Frontier: 10, Updates: []DifferentialRow{{Key: "a", Diff: 1}}}},
			Apply:   func([]DifferentialRow) error { return nil },
			Publish: func() error { return publishErr },
		}, SQLBackgroundIndexBuildOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err := builder.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := builder.Wait(context.Background()); !errors.Is(err, publishErr) {
			t.Fatalf("Wait() error = %v, want publish error", err)
		}
		status := builder.Status()
		if status.State != SQLBackgroundIndexBuildFailed || status.BuildFrontier != 10 || status.ProcessedRows != 1 {
			t.Fatalf("publish failure status = %#v", status)
		}
	})
}
