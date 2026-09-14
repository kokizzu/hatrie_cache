#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
    ADOPTED_QUERY_ENGINE_IDEAS.md
    BENCHMARK.md
    ENGINE_IDEAS.md
    MZ032_LATE_DATA_RECLOCK.md
    README.md
    hat/hatPipeline/mz032_late_data_reclock.go
    hat/hatPipeline/mz032_late_data_reclock_test.go
    hat/hatPipeline/mz032_late_data_reclock_benchmark_test.go
    scripts/benchmark-mz032-late-data-reclock.sh
    scripts/commit-mz032-late-data-reclock.sh
    scripts/format-mz032-late-data-reclock.sh
    scripts/inspect-authoritative-next-idea.sh
    scripts/inspect-mz032-c203.sh
    scripts/push-mz032-late-data-reclock.sh
    scripts/race-mz032-late-data-reclock.sh
    scripts/review-mz032-late-data-reclock.sh
    scripts/test-mz032-late-data-reclock.sh
    scripts/verify-mz032-late-data-reclock.sh
    scripts/vet-mz032-late-data-reclock.sh
)

staged_paths_text=$(git diff --cached --name-only | LC_ALL=C sort)
feature_paths_text=$(printf '%s\n' "${feature_paths[@]}" | LC_ALL=C sort)
if [[ -n "$staged_paths_text" && "$staged_paths_text" != "$feature_paths_text" ]]; then
    printf '%s\n' 'Refusing to commit: the index contains paths outside the MZ-032 feature.' >&2
    git diff --cached --name-only >&2
    exit 1
fi

git add "${feature_paths[@]}"

temporary_directory=$(mktemp -d)
trap 'rm -rf "$temporary_directory"' EXIT
git show HEAD:Makefile > "$temporary_directory/head.Makefile"
cp "$temporary_directory/head.Makefile" "$temporary_directory/expected.Makefile"
cat >> "$temporary_directory/expected.Makefile" <<'EOF'

.PHONY: inspect-authoritative-next-idea
inspect-authoritative-next-idea:
	@bash scripts/inspect-authoritative-next-idea.sh

.PHONY: inspect-mz032-c203
inspect-mz032-c203:
	@bash scripts/inspect-mz032-c203.sh

.PHONY: test-mz032-late-data-reclock
test-mz032-late-data-reclock:
	@bash scripts/test-mz032-late-data-reclock.sh

.PHONY: benchmark-mz032-late-data-reclock
benchmark-mz032-late-data-reclock:
	@bash scripts/benchmark-mz032-late-data-reclock.sh

.PHONY: format-mz032-late-data-reclock race-mz032-late-data-reclock vet-mz032-late-data-reclock review-mz032-late-data-reclock verify-mz032-late-data-reclock commit-mz032-late-data-reclock push-mz032-late-data-reclock
format-mz032-late-data-reclock:
	@bash scripts/format-mz032-late-data-reclock.sh

race-mz032-late-data-reclock:
	@bash scripts/race-mz032-late-data-reclock.sh

vet-mz032-late-data-reclock:
	@bash scripts/vet-mz032-late-data-reclock.sh

review-mz032-late-data-reclock:
	@bash scripts/review-mz032-late-data-reclock.sh

verify-mz032-late-data-reclock:
	@bash scripts/verify-mz032-late-data-reclock.sh

commit-mz032-late-data-reclock:
	@bash scripts/commit-mz032-late-data-reclock.sh

push-mz032-late-data-reclock:
	@bash scripts/push-mz032-late-data-reclock.sh
EOF

diff_status=0
git diff --no-index \
    "$temporary_directory/head.Makefile" \
    "$temporary_directory/expected.Makefile" \
    > "$temporary_directory/Makefile.patch" || diff_status=$?
if [[ "$diff_status" -ne 1 ]]; then
    printf '%s\n' "Unexpected Makefile diff status: $diff_status" >&2
    exit 1
fi
sed -i \
    -e 's|^diff --git .*|diff --git a/Makefile b/Makefile|' \
    -e 's|^--- a/.*head\.Makefile|--- a/Makefile|' \
    -e 's|^+++ b/.*expected\.Makefile|+++ b/Makefile|' \
    "$temporary_directory/Makefile.patch"
git apply --cached "$temporary_directory/Makefile.patch"

expected_paths=("${feature_paths[@]}" Makefile)
expected_paths_text=$(printf '%s\n' "${expected_paths[@]}" | LC_ALL=C sort)
actual_paths_text=$(git diff --cached --name-only | LC_ALL=C sort)
if [[ "$actual_paths_text" != "$expected_paths_text" ]]; then
    printf '%s\n' 'Refusing to commit: staged paths are not isolated to MZ-032.' >&2
    git diff --cached --name-only >&2
    exit 1
fi

git diff --cached --check
git commit -m 'feat(hatPipeline): add late-data reclock'
