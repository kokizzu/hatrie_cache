#!/usr/bin/env bash
set -euo pipefail

base_makefile=$(mktemp)
trap 'rm -f "$base_makefile"' EXIT
git show HEAD:Makefile > "$base_makefile"
{
printf '\n'
printf '%s\n' \
  'test-read-replica-policy-c213:' \
  $'\t@bash ./scripts/test-read-replica-policy-c213.sh' \
  '' \
  'benchmark-read-replica-policy-c213:' \
  $'\t@bash ./scripts/benchmark-read-replica-policy-c213.sh' \
  '' \
  'format-read-replica-policy-c213:' \
  $'\t@bash ./scripts/format-read-replica-policy-c213.sh' \
  '' \
  'verify-read-replica-policy-c213:' \
  $'\t@bash ./scripts/verify-read-replica-policy-c213.sh' \
  '' \
  'inspect-read-replica-diff-c213:' \
  $'\t@bash ./scripts/inspect-read-replica-diff-c213.sh' \
  '' \
  'stage-read-replica-policy-c213:' \
  $'\t@bash ./scripts/stage-read-replica-policy-c213.sh' \
  '' \
  'commit-read-replica-policy-c213:' \
  $'\t@bash ./scripts/commit-read-replica-policy-c213.sh' \
  '' \
  'push-read-replica-policy-c213:' \
  $'\t@bash ./scripts/push-read-replica-policy-c213.sh' >> "$base_makefile"
}

git add BENCHMARK.md INSPIRATION.md READ_REPLICA_SELECTION_FASTPATH.md hat/hatReplication/read_consistency.go hat/hatReplication/read_replica_policy.go hat/hatReplication/read_replica_selection_fastpath_test.go scripts/test-read-replica-policy-c213.sh scripts/benchmark-read-replica-policy-c213.sh scripts/format-read-replica-policy-c213.sh scripts/verify-read-replica-policy-c213.sh scripts/inspect-read-replica-diff-c213.sh scripts/stage-read-replica-policy-c213.sh scripts/commit-read-replica-policy-c213.sh scripts/push-read-replica-policy-c213.sh
makefile_blob=$(git hash-object -w "$base_makefile")
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --name-only
