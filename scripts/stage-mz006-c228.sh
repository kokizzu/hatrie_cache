#!/usr/bin/env bash
set -euo pipefail

allowed_paths=(
  MZ006_OBJECT_STORE_GARBAGE_COLLECTION.md
  INSPIRATION_BACKLOG.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
  hat/hatBackup/object_store_gc.go
  hat/hatBackup/mz006_object_store_gc_test.go
  hat/hatBackup/mz006_object_store_gc_benchmark_test.go
  scripts/inspect-mz006-c228.sh
  scripts/run-mz006-c228.sh
  scripts/test-mz006-package-c228.sh
  scripts/race-mz006-c228.sh
  scripts/race-mz006-package-c228.sh
  scripts/vet-mz006-c228.sh
  scripts/format-mz006-c228.sh
  scripts/benchmark-mz006-c228.sh
  scripts/inspect-mz006-status-c228.sh
  scripts/stage-mz006-c228.sh
  scripts/commit-mz006-c228.sh
  scripts/push-mz006-c228.sh
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
  MZ006_OBJECT_STORE_GARBAGE_COLLECTION.md \
  INSPIRATION_BACKLOG.md \
  ADOPTED_QUERY_ENGINE_IDEAS.md \
  BENCHMARK.md \
  hat/hatBackup/object_store_gc.go \
  hat/hatBackup/mz006_object_store_gc_test.go \
  hat/hatBackup/mz006_object_store_gc_benchmark_test.go \
  scripts/inspect-mz006-c228.sh \
  scripts/run-mz006-c228.sh \
  scripts/test-mz006-package-c228.sh \
  scripts/race-mz006-c228.sh \
  scripts/race-mz006-package-c228.sh \
  scripts/vet-mz006-c228.sh \
  scripts/format-mz006-c228.sh \
  scripts/benchmark-mz006-c228.sh \
  scripts/inspect-mz006-status-c228.sh \
  scripts/stage-mz006-c228.sh \
  scripts/commit-mz006-c228.sh \
  scripts/push-mz006-c228.sh

makefile_tmp="$(mktemp)"
trap 'rm -f "$makefile_tmp"' EXIT
git show HEAD:Makefile > "$makefile_tmp"
printf '%s\n' \
  '' \
  '.PHONY: inspect-mz006-c228' \
  'inspect-mz006-c228:' \
  $'\tbash ./scripts/inspect-mz006-c228.sh' \
  '' \
  '.PHONY: test-mz006-c228' \
  'test-mz006-c228:' \
  $'\tbash ./scripts/run-mz006-c228.sh' \
  '' \
  '.PHONY: test-mz006-package-c228' \
  'test-mz006-package-c228:' \
  $'\tbash ./scripts/test-mz006-package-c228.sh' \
  '' \
  '.PHONY: race-mz006-c228' \
  'race-mz006-c228:' \
  $'\tbash ./scripts/race-mz006-c228.sh' \
  '' \
  '.PHONY: race-mz006-package-c228' \
  'race-mz006-package-c228:' \
  $'\tbash ./scripts/race-mz006-package-c228.sh' \
  '' \
  '.PHONY: vet-mz006-c228' \
  'vet-mz006-c228:' \
  $'\tbash ./scripts/vet-mz006-c228.sh' \
  '' \
  '.PHONY: format-mz006-c228' \
  'format-mz006-c228:' \
  $'\tbash ./scripts/format-mz006-c228.sh' \
  '' \
  '.PHONY: benchmark-mz006-c228' \
  'benchmark-mz006-c228:' \
  $'\tbash ./scripts/benchmark-mz006-c228.sh' \
  '' \
  '.PHONY: inspect-mz006-status-c228' \
  'inspect-mz006-status-c228:' \
  $'\tbash ./scripts/inspect-mz006-status-c228.sh' \
  '' \
  '.PHONY: stage-mz006-c228' \
  'stage-mz006-c228:' \
  $'\tbash ./scripts/stage-mz006-c228.sh' \
  '' \
  '.PHONY: commit-mz006-c228' \
  'commit-mz006-c228:' \
  $'\tbash ./scripts/commit-mz006-c228.sh' \
  '' \
  '.PHONY: push-mz006-c228' \
  'push-mz006-c228:' \
  $'\tbash ./scripts/push-mz006-c228.sh' >> "$makefile_tmp"
makefile_blob="$(git hash-object -w "$makefile_tmp")"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
printf '%s\n' 'Staged MZ-006 paths:'
git diff --cached --name-only
