#!/usr/bin/env bash
set -euo pipefail

files=(
  BENCHMARK.md
  INSPIRATION.md
  README.md
  hat/hatCache/async_command_http.go
  hat/hatCache/monitoring.go
  hat/hatCache/slow_command_capture.go
  hat/hatCache/slow_command_capture_test.go
  hat/hatCache/slow_command_capture_benchmark_test.go
  scripts/benchmark-t154.sh
  scripts/commit-t154.sh
  scripts/format-t154.sh
  scripts/push-t154.sh
  scripts/review-t154.sh
  scripts/stage-t154.sh
  scripts/test-race-t154.sh
  scripts/test-t154.sh
  scripts/vet-t154.sh
)

staged=$(git diff --cached --name-only)
if [[ -n "$staged" ]]; then
  expected=$(printf '%s\n' Makefile "${files[@]}" | sort)
  if [[ "$staged" != "$expected" ]]; then
    echo "refusing to stage T154 with pre-existing staged changes" >&2
    git diff --cached --name-status >&2
    exit 1
  fi
  git reset -- Makefile "${files[@]}"
fi

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

git show :Makefile > "$tmp_dir/Makefile.base"
cp "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.t154"
cat >> "$tmp_dir/Makefile.t154" <<'MAKEFILE'

.PHONY: review-t154
review-t154:
	bash ./scripts/review-t154.sh

.PHONY: stage-t154
stage-t154:
	bash ./scripts/stage-t154.sh

.PHONY: commit-t154
commit-t154:
	bash ./scripts/commit-t154.sh

.PHONY: push-t154
push-t154:
	bash ./scripts/push-t154.sh
MAKEFILE

set +e
git diff --no-index -- "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.t154" > "$tmp_dir/Makefile.patch"
diff_status=$?
set -e
if [[ "$diff_status" -ne 1 ]]; then
  echo "unexpected Makefile diff status: $diff_status" >&2
  exit 1
fi

sed -i '1c diff --git a/Makefile b/Makefile' "$tmp_dir/Makefile.patch"
sed -i '3c --- a/Makefile' "$tmp_dir/Makefile.patch"
sed -i '4c +++ b/Makefile' "$tmp_dir/Makefile.patch"
git apply --cached -- "$tmp_dir/Makefile.patch"
git add -- "${files[@]}"

git diff --cached --check
echo "staged T154 paths:"
git diff --cached --name-only
