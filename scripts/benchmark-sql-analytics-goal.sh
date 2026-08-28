#!/usr/bin/env sh
set -eu

go test ./hat/hatSql -run '^$' -bench 'Benchmark(GeoIndexWithinRadius|KeyGraphTraverse|MatchOrderedEventSequence|JoinOverlappingIntervals|TimeBucketRollupAdd|SQLApproximateAggregates)$' -benchmem -count=1
