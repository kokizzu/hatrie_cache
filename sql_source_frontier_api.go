package hatriecache

import (
	"context"

	"hatrie_cache/hat/hatSql"
)

var (
	ErrSQLSourceFrontierUnavailable           = hatSql.ErrSQLSourceFrontierUnavailable
	ErrSQLSourceFrontierNotReady              = hatSql.ErrSQLSourceFrontierNotReady
	ErrSQLSourceFrontierBehind                = hatSql.ErrSQLSourceFrontierBehind
	ErrSQLSourceFrontierBarrierNil            = hatSql.ErrSQLSourceFrontierBarrierNil
	ErrSQLSourceFrontierBarrierTrackerNil     = hatSql.ErrSQLSourceFrontierBarrierTrackerNil
	ErrSQLSourceFrontierBarrierContextNil     = hatSql.ErrSQLSourceFrontierBarrierContextNil
	ErrSQLFrontierSnapshotProviderUnsupported = hatSql.ErrSQLFrontierSnapshotProviderUnsupported
)

type SQLSourceFrontierResolver = hatSql.SQLSourceFrontierResolver
type SQLSourceFrontierPartition = hatSql.SQLSourceFrontierPartition
type SQLSourceFrontier = hatSql.SQLSourceFrontier
type SQLSourceFrontierSnapshot = hatSql.SQLSourceFrontierSnapshot
type SQLSourceFrontierTracker = hatSql.SQLSourceFrontierTracker
type SQLSourceFrontierBarrier = hatSql.SQLSourceFrontierBarrier
type SQLFrontierSnapshotProvider = hatSql.SQLFrontierSnapshotProvider

// NewSQLSourceFrontierTracker creates a fixed source-partition frontier tracker.
func NewSQLSourceFrontierTracker(partitions []SQLSourceFrontierPartition) (*SQLSourceFrontierTracker, error) {
	return hatSql.NewSQLSourceFrontierTracker(partitions)
}

// NewSQLSourceFrontierBarrier wraps a source-partition frontier tracker.
func NewSQLSourceFrontierBarrier(tracker *SQLSourceFrontierTracker) (*SQLSourceFrontierBarrier, error) {
	return hatSql.NewSQLSourceFrontierBarrier(tracker)
}

// NewSQLSourceFrontierBarrierFromPartitions creates a tracker and barrier.
func NewSQLSourceFrontierBarrierFromPartitions(partitions []SQLSourceFrontierPartition) (*SQLSourceFrontierBarrier, error) {
	return hatSql.NewSQLSourceFrontierBarrierFromPartitions(partitions)
}

// BeginSQLFrontierSnapshot waits for a common frontier and opens an immutable view.
func BeginSQLFrontierSnapshot(ctx context.Context, resolver SQLSourceResolver, barrier *SQLSourceFrontierBarrier, frontier uint64) (SQLSourceResolver, func(), error) {
	return hatSql.BeginSQLFrontierSnapshot(ctx, resolver, barrier, frontier)
}
