#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT
git show HEAD:Makefile > "$base_makefile"
{
printf '\n'
printf '%s\n' \
  'test-delay-queue-c215:' \
  $'\t@bash ./scripts/test-delay-queue-c215.sh' \
  '' \
  'benchmark-delay-queue-c215:' \
  $'\t@bash ./scripts/benchmark-delay-queue-c215.sh' \
  '' \
  'format-delay-queue-c215:' \
  $'\t@bash ./scripts/format-delay-queue-c215.sh' \
  '' \
  'verify-delay-queue-c215:' \
  $'\t@bash ./scripts/verify-delay-queue-c215.sh' \
  '' \
  'inspect-delay-queue-diff-c215:' \
  $'\t@bash ./scripts/inspect-delay-queue-diff-c215.sh' \
  '' \
  'stage-delay-queue-c215:' \
  $'\t@bash ./scripts/stage-delay-queue-c215.sh' \
  '' \
  'commit-delay-queue-c215:' \
  $'\t@bash ./scripts/commit-delay-queue-c215.sh' \
  '' \
  'push-delay-queue-c215:' \
  $'\t@bash ./scripts/push-delay-queue-c215.sh' >> "$base_makefile"
}

git add BENCHMARK.md DELAY_QUEUE_POP_READY_FASTPATH.md INSPIRATION.md hat/hatDataStructure/delay_queue.go hat/hatDataStructure/delay_queue_pop_ready_fastpath_test.go scripts/test-delay-queue-c215.sh scripts/benchmark-delay-queue-c215.sh scripts/format-delay-queue-c215.sh scripts/verify-delay-queue-c215.sh scripts/inspect-delay-queue-diff-c215.sh scripts/stage-delay-queue-c215.sh scripts/commit-delay-queue-c215.sh scripts/push-delay-queue-c215.sh
makefile_blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
