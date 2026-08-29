package hatSql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"hatrie_cache/hat/hatSql"
)

func TestRetentionArchivalJobDryRunArchivesBeforeDelete(t *testing.T) {
	now := time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC)
	items := []hatSql.RetentionItem{{Key: "old", UpdatedAt: now.Add(-48 * time.Hour), Bytes: 12}, {Key: "new", UpdatedAt: now.Add(-time.Hour), Bytes: 8}}
	var archived, deleted []string
	job := hatSql.RetentionArchivalJob{
		List: func(context.Context) ([]hatSql.RetentionItem, error) { return items, nil },
		Archive: func(_ context.Context, item hatSql.RetentionItem) error {
			archived = append(archived, item.Key)
			return nil
		},
		Delete: func(_ context.Context, item hatSql.RetentionItem) error {
			deleted = append(deleted, item.Key)
			return nil
		},
	}
	dry, err := job.Run(context.Background(), hatSql.RetentionPolicy{Before: now.Add(-24 * time.Hour), DryRun: true})
	if err != nil || dry.Eligible != 1 || dry.EligibleBytes != 12 || dry.Archived != 0 || dry.Deleted != 0 || len(archived) != 0 || len(deleted) != 0 {
		t.Fatalf("dry Run() = %#v, %v, archived=%#v deleted=%#v", dry, err, archived, deleted)
	}
	real, err := job.Run(context.Background(), hatSql.RetentionPolicy{Before: now.Add(-24 * time.Hour)})
	if err != nil || real.Archived != 1 || real.Deleted != 1 || len(archived) != 1 || len(deleted) != 1 || archived[0] != "old" || deleted[0] != "old" {
		t.Fatalf("real Run() = %#v, %v, archived=%#v deleted=%#v", real, err, archived, deleted)
	}
}

func TestRetentionArchivalJobNeverDeletesWhenArchiveFails(t *testing.T) {
	item := hatSql.RetentionItem{Key: "old", UpdatedAt: time.Unix(0, 0)}
	deleted := false
	job := hatSql.RetentionArchivalJob{
		List:    func(context.Context) ([]hatSql.RetentionItem, error) { return []hatSql.RetentionItem{item}, nil },
		Archive: func(context.Context, hatSql.RetentionItem) error { return errors.New("archive unavailable") },
		Delete:  func(context.Context, hatSql.RetentionItem) error { deleted = true; return nil },
	}
	if _, err := job.Run(context.Background(), hatSql.RetentionPolicy{Before: time.Now()}); err == nil || deleted {
		t.Fatalf("Run() error/deleted = %v/%v, want archive error and no delete", err, deleted)
	}
}
