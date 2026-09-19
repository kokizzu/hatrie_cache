#!/usr/bin/env bash
set -euo pipefail

git diff --check -- \
  BENCHMARK.md \
  CHU33_REMOTE_PART_PUBLICATION.md \
  PRODUCT_IDEA_GAPS.md \
  README.md \
  hat/hatStorage/remote_part_publication.go \
  hat/hatStorage/chu33_remote_part_publication_test.go \
  hat/hatStorage/chu33_remote_part_publication_benchmark_test.go \
  scripts/format-chu33.sh \
  scripts/test-chu33-package.sh \
  scripts/race-chu33.sh \
  scripts/vet-chu33.sh

for path in \
  CHU33_REMOTE_PART_PUBLICATION.md \
  hat/hatStorage/remote_part_publication.go \
  hat/hatStorage/chu33_remote_part_publication_test.go \
  hat/hatStorage/chu33_remote_part_publication_benchmark_test.go \
  scripts/format-chu33.sh \
  scripts/test-chu33-package.sh \
  scripts/race-chu33.sh \
  scripts/vet-chu33.sh; do
  test -f "$path"
done

printf '%s\n' 'CH-U33 diff verification passed.'
