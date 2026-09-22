#!/usr/bin/env bash
set -euo pipefail

allowed() {
  case "$1" in
    Makefile|INSPIRATION_ROUND2.md|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|M228_EXACTLY_ONCE_SOURCE_RESTART.md|hat/hatReplication/m228_exactly_once_restart.go|hat/hatReplication/m228_exactly_once_restart_test.go|hat/hatReplication/m228_exactly_once_restart_benchmark_test.go|scripts/m228-exactly-once-restart.sh|scripts/status-m228-exactly-once-restart.sh|scripts/stage-m228-exactly-once-restart.sh|scripts/commit-m228-exactly-once-restart.sh|scripts/push-m228-exactly-once-restart.sh|scripts/cleanup-hatrie-plan.sh)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

git add -- Makefile INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md M228_EXACTLY_ONCE_SOURCE_RESTART.md hat/hatReplication/m228_exactly_once_restart.go hat/hatReplication/m228_exactly_once_restart_test.go hat/hatReplication/m228_exactly_once_restart_benchmark_test.go scripts/m228-exactly-once-restart.sh scripts/status-m228-exactly-once-restart.sh scripts/stage-m228-exactly-once-restart.sh scripts/commit-m228-exactly-once-restart.sh scripts/push-m228-exactly-once-restart.sh scripts/cleanup-hatrie-plan.sh

staged_files="$(git diff --cached --name-only)"
while IFS= read -r path; do
  if [ -n "$path" ] && ! allowed "$path"; then
    printf 'unexpected staged path: %s\n' "$path" >&2
    exit 1
  fi
done <<EOF
$staged_files
EOF

git diff --cached --check
git status --short
