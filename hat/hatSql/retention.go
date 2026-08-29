package hatSql

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// RetentionItem is one source item eligible for retention or archival.
type RetentionItem struct {
	Key       string
	UpdatedAt time.Time
	Bytes     int64
}

// RetentionPolicy selects items older than Before. DryRun reports candidates
// without writing archives or deleting source data.
type RetentionPolicy struct {
	Before time.Time
	DryRun bool
}

// RetentionReport is the auditable result of one archival operation.
type RetentionReport struct {
	Eligible      int
	EligibleBytes int64
	Archived      int
	Deleted       int
	DryRun        bool
}

// RetentionArchivalJob delegates inventory, archival, and deletion to the
// caller's storage adapter. Real runs archive every item before any deletion.
type RetentionArchivalJob struct {
	List    func(context.Context) ([]RetentionItem, error)
	Archive func(context.Context, RetentionItem) error
	Delete  func(context.Context, RetentionItem) error
}

func (job RetentionArchivalJob) Run(ctx context.Context, policy RetentionPolicy) (RetentionReport, error) {
	if job.List == nil || job.Archive == nil || job.Delete == nil || policy.Before.IsZero() {
		return RetentionReport{}, fmt.Errorf("retention inventory, archive, delete, and cutoff are required")
	}
	items, err := job.List(ctx)
	if err != nil {
		return RetentionReport{}, err
	}
	eligible := make([]RetentionItem, 0, len(items))
	for _, item := range items {
		if item.Key != "" && item.UpdatedAt.Before(policy.Before) {
			eligible = append(eligible, item)
		}
	}
	sort.Slice(eligible, func(left, right int) bool { return eligible[left].Key < eligible[right].Key })
	report := RetentionReport{Eligible: len(eligible), DryRun: policy.DryRun}
	for _, item := range eligible {
		if item.Bytes > 0 {
			report.EligibleBytes += item.Bytes
		}
	}
	if policy.DryRun {
		return report, nil
	}
	for _, item := range eligible {
		if err := job.Archive(ctx, item); err != nil {
			return report, fmt.Errorf("archive %q: %w", item.Key, err)
		}
		report.Archived++
	}
	for _, item := range eligible {
		if err := job.Delete(ctx, item); err != nil {
			return report, fmt.Errorf("delete %q after archival: %w", item.Key, err)
		}
		report.Deleted++
	}
	return report, nil
}
