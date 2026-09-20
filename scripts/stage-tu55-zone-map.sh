#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  Makefile \
  PRODUCT_IDEA_GAPS.md \
  TU55_ZONE_MAP_INDEX.md \
  hat/hatDataStructure/tu55_zone_map_benchmark_test.go \
  hat/hatDataStructure/tu55_zone_map_test.go \
  hat/hatDataStructure/zone_map_index.go \
  scripts/benchmark-tu55-zone-map.sh \
  scripts/commit-tu55-zone-map.sh \
  scripts/format-tu55-zone-map.sh \
  scripts/push-tu55-zone-map.sh \
  scripts/race-tu55-zone-map.sh \
  scripts/review-tu55-zone-map.sh \
  scripts/stage-tu55-zone-map.sh \
  scripts/test-tu55-zone-map-package.sh \
  scripts/test-tu55-zone-map.sh \
  scripts/vet-tu55-zone-map.sh
