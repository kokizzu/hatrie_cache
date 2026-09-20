#!/usr/bin/env bash
set -euo pipefail

rg -n -C 5 \
  'type SpaceChangefeed struct|type SpaceChangefeedEvent struct|type SpaceChangefeedStats struct|func \(feed \*SpaceChangefeed\) (Publish|Subscribe|Stats|Close)|func \(feed \*SpaceChangefeedSubscription\) (Advance|Checkpoint|Close)|ErrSpaceChangefeed|spaceChangefeed' \
  hat/hatReplication/tu39_space_changefeed.go

sed -n '1,110p' hat/hatReplication/c249_offset_inspection_test.go

sed -n '1,180p' hat/hatReplication/c249_offset_inspection_benchmark_test.go

rg -n -C 4 'C247|C248|C249' README.md INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n -C 12 'C247|C249' BENCHMARK.md
