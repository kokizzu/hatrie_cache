#!/usr/bin/env bash
set -euo pipefail

allowed_paths=(
  MZ003_FRONTIER_COMPACTION_SCHEDULER.md
  INSPIRATION_BACKLOG.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  hat/hatPipeline/frontier_compaction_scheduler.go
  hat/hatPipeline/frontier_retention.go
  hat/hatPipeline/mz003_frontier_compaction_scheduler_test.go
  scripts/inspect-mz003-c227.sh
  scripts/run-mz003-c227.sh
  scripts/race-mz003-c227.sh
  scripts/vet-mz003-c227.sh
  scripts/benchmark-mz003-c227.sh
  scripts/test-mz003-package-c227.sh
  scripts/format-mz003-c227.sh
  scripts/race-mz003-package-c227.sh
  scripts/inspect-mz003-docs-c227.sh
  scripts/inspect-benchmark-tail-mz003-c227.sh
  scripts/inspect-mz003-status-c227.sh
  scripts/stage-mz003-c227.sh
  scripts/commit-mz003-c227.sh
  scripts/push-mz003-c227.sh
)

staged_paths="$(git diff --cached --name-only)"
if [[ -n "$staged_paths" ]]; then
  while IFS= read -r path; do
    case " ${allowed_paths[*]} " in
      *" $path "*) ;;
      *)
        printf 'refusing to mix pre-staged path: %s\n' "$path" >&2
        exit 1
        ;;
    esac
  done <<< "$staged_paths"
fi

git add -- \
  MZ003_FRONTIER_COMPACTION_SCHEDULER.md \
  INSPIRATION_BACKLOG.md \
  ENGINE_IDEAS.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  hat/hatPipeline/frontier_compaction_scheduler.go \
  hat/hatPipeline/frontier_retention.go \
  hat/hatPipeline/mz003_frontier_compaction_scheduler_test.go \
  scripts/inspect-mz003-c227.sh \
  scripts/run-mz003-c227.sh \
  scripts/race-mz003-c227.sh \
  scripts/vet-mz003-c227.sh \
  scripts/benchmark-mz003-c227.sh \
  scripts/test-mz003-package-c227.sh \
  scripts/format-mz003-c227.sh \
  scripts/race-mz003-package-c227.sh \
  scripts/inspect-mz003-docs-c227.sh \
  scripts/inspect-benchmark-tail-mz003-c227.sh \
  scripts/inspect-mz003-status-c227.sh \
  scripts/stage-mz003-c227.sh \
  scripts/commit-mz003-c227.sh \
  scripts/push-mz003-c227.sh

makefile_tmp="$(mktemp)"
trap 'rm -f "$makefile_tmp"' EXIT
git show HEAD:Makefile > "$makefile_tmp"
printf '%s\n' \
  '' \
  '.PHONY: inspect-mz003-c227' \
  'inspect-mz003-c227:' \
  $'\tbash ./scripts/inspect-mz003-c227.sh' \
  '' \
  '.PHONY: test-mz003-c227' \
  'test-mz003-c227:' \
  $'\tbash ./scripts/run-mz003-c227.sh' \
  '' \
  '.PHONY: inspect-mz003-docs-c227' \
  'inspect-mz003-docs-c227:' \
  $'\tbash ./scripts/inspect-mz003-docs-c227.sh' \
  '' \
  '.PHONY: inspect-benchmark-tail-mz003-c227' \
  'inspect-benchmark-tail-mz003-c227:' \
  $'\tbash ./scripts/inspect-benchmark-tail-mz003-c227.sh' \
  '' \
  '.PHONY: race-mz003-c227' \
  'race-mz003-c227:' \
  $'\tbash ./scripts/race-mz003-c227.sh' \
  '' \
  '.PHONY: race-mz003-package-c227' \
  'race-mz003-package-c227:' \
  $'\tbash ./scripts/race-mz003-package-c227.sh' \
  '' \
  '.PHONY: vet-mz003-c227' \
  'vet-mz003-c227:' \
  $'\tbash ./scripts/vet-mz003-c227.sh' \
  '' \
  '.PHONY: benchmark-mz003-c227' \
  'benchmark-mz003-c227:' \
  $'\tbash ./scripts/benchmark-mz003-c227.sh' \
  '' \
  '.PHONY: test-mz003-package-c227' \
  'test-mz003-package-c227:' \
  $'\tbash ./scripts/test-mz003-package-c227.sh' \
  '' \
  '.PHONY: format-mz003-c227' \
  'format-mz003-c227:' \
  $'\tbash ./scripts/format-mz003-c227.sh' >> "$makefile_tmp"
makefile_blob="$(git hash-object -w "$makefile_tmp")"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
printf '%s\n' 'Staged MZ-003 paths:'
git diff --cached --name-only
