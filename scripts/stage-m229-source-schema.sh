#!/usr/bin/env bash
set -euo pipefail

allowed() {
  case "$1" in
    Makefile|INSPIRATION_ROUND2.md|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md|M229_SOURCE_SCHEMA_EVOLUTION.md|hat/hatReplication/tu39_space_changefeed.go|hat/hatReplication/m229_source_schema.go|hat/hatReplication/m229_source_schema_test.go|hat/hatReplication/m229_source_schema_benchmark_test.go|scripts/cleanup-hatrie-plan.sh|scripts/m229-source-schema.sh|scripts/show-inspiration-round2.sh|scripts/status-m229-source-schema.sh|scripts/stage-m229-source-schema.sh|scripts/commit-m229-source-schema.sh|scripts/push-m229-source-schema.sh)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

git add -- Makefile INSPIRATION_ROUND2.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md M229_SOURCE_SCHEMA_EVOLUTION.md hat/hatReplication/tu39_space_changefeed.go hat/hatReplication/m229_source_schema.go hat/hatReplication/m229_source_schema_test.go hat/hatReplication/m229_source_schema_benchmark_test.go scripts/cleanup-hatrie-plan.sh scripts/m229-source-schema.sh scripts/show-inspiration-round2.sh scripts/status-m229-source-schema.sh scripts/stage-m229-source-schema.sh scripts/commit-m229-source-schema.sh scripts/push-m229-source-schema.sh

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
