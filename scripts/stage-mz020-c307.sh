#!/usr/bin/env bash
set -euo pipefail

paths=(
  hat/hatPipeline/mz020_resizable_scheduler.go
  hat/hatPipeline/mz020_resizable_scheduler_test.go
  hat/hatPipeline/mz020_resizable_scheduler_benchmark_test.go
  scripts/format-mz020-c305.sh
  scripts/test-mz020-c305.sh
  scripts/test-race-mz020-c305.sh
  scripts/vet-mz020-c305.sh
  scripts/benchmark-mz020-c305.sh
  scripts/review-mz020-c307.sh
  scripts/stage-mz020-c307.sh
  scripts/inspect-staged-mz020-c307.sh
  scripts/commit-mz020-c307.sh
  scripts/push-mz020-c307.sh
  MZ020_RESIZABLE_SCHEDULER.md
  README.md
  ENGINE_IDEAS.md
  ADOPTED_QUERY_ENGINE_IDEAS.md
  BENCHMARK.md
)

for path in "${paths[@]}"; do
  test -f "$path"
done

while IFS= read -r path; do
  case "$path" in
    Makefile|hat/hatPipeline/mz020_resizable_scheduler.go|hat/hatPipeline/mz020_resizable_scheduler_test.go|hat/hatPipeline/mz020_resizable_scheduler_benchmark_test.go|scripts/format-mz020-c305.sh|scripts/test-mz020-c305.sh|scripts/test-race-mz020-c305.sh|scripts/vet-mz020-c305.sh|scripts/benchmark-mz020-c305.sh|scripts/review-mz020-c307.sh|scripts/stage-mz020-c307.sh|scripts/inspect-staged-mz020-c307.sh|scripts/commit-mz020-c307.sh|scripts/push-mz020-c307.sh|MZ020_RESIZABLE_SCHEDULER.md|README.md|ENGINE_IDEAS.md|ADOPTED_QUERY_ENGINE_IDEAS.md|BENCHMARK.md) ;;
    *) printf 'unexpected pre-staged path: %s\n' "$path" >&2; exit 1 ;;
  esac
done < <(git diff --cached --name-only)

makefile=$(mktemp)
trap 'rm -f "$makefile"' EXIT
git show HEAD:Makefile > "$makefile"
cat >> "$makefile" <<'EOF'

.PHONY: format-mz020-c305
format-mz020-c305:
	@bash scripts/format-mz020-c305.sh

.PHONY: test-mz020-c305
test-mz020-c305:
	@bash scripts/test-mz020-c305.sh

.PHONY: test-race-mz020-c305
test-race-mz020-c305:
	@bash scripts/test-race-mz020-c305.sh

.PHONY: vet-mz020-c305
vet-mz020-c305:
	@bash scripts/vet-mz020-c305.sh

.PHONY: benchmark-mz020-c305
benchmark-mz020-c305:
	@bash scripts/benchmark-mz020-c305.sh

.PHONY: review-mz020-c307
review-mz020-c307:
	@bash scripts/review-mz020-c307.sh

.PHONY: stage-mz020-c307
stage-mz020-c307:
	@bash scripts/stage-mz020-c307.sh

.PHONY: inspect-staged-mz020-c307
inspect-staged-mz020-c307:
	@bash scripts/inspect-staged-mz020-c307.sh

.PHONY: commit-mz020-c307
commit-mz020-c307:
	@bash scripts/commit-mz020-c307.sh

.PHONY: push-mz020-c307
push-mz020-c307:
	@bash scripts/push-mz020-c307.sh
EOF
blob=$(git hash-object -w "$makefile")
git update-index --add --cacheinfo "100644,$blob,Makefile"
git add -- "${paths[@]}"
