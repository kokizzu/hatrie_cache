#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  INSPIRATION.md
  Makefile
  README.md
  hat/hatCache/monitoring.go
  hat/hatCache/replication.go
  hat/hatCache/replication_lag_test.go
  hat/hatReplication/model.go
  scripts/commit-t051.sh
  scripts/format-t051.sh
  scripts/push-t051.sh
  scripts/review-t051.sh
  scripts/stage-t051.sh
  scripts/test-race-t051.sh
  scripts/test-t051.sh
  scripts/vet-t051.sh
)

if ! git diff --cached --quiet --; then
  echo "refusing to stage T051 with pre-existing staged changes" >&2
  exit 1
fi

for path in "${feature_paths[@]}"; do
  if [[ ! -e "$path" ]]; then
    echo "missing T051 path: $path" >&2
    exit 1
  fi
done

git add -- \
  INSPIRATION.md \
  README.md \
  hat/hatCache/monitoring.go \
  hat/hatCache/replication.go \
  hat/hatCache/replication_lag_test.go \
  hat/hatReplication/model.go \
  scripts/commit-t051.sh \
  scripts/format-t051.sh \
  scripts/push-t051.sh \
  scripts/review-t051.sh \
  scripts/stage-t051.sh \
  scripts/test-race-t051.sh \
  scripts/test-t051.sh \
  scripts/vet-t051.sh

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT
git show :Makefile > "$tmpdir/base-makefile"
cp "$tmpdir/base-makefile" "$tmpdir/t051-makefile"
printf '\n%s\n' \
  '.PHONY: test-t051' \
  'test-t051:' \
  $'\tbash ./scripts/test-t051.sh' \
  '' \
  '.PHONY: format-t051' \
  'format-t051:' \
  $'\tbash ./scripts/format-t051.sh' \
  '' \
  '.PHONY: test-race-t051' \
  'test-race-t051:' \
  $'\tbash ./scripts/test-race-t051.sh' \
  '' \
  '.PHONY: vet-t051' \
  'vet-t051:' \
  $'\tbash ./scripts/vet-t051.sh' \
  '' \
  '.PHONY: review-t051' \
  'review-t051:' \
  $'\tbash ./scripts/review-t051.sh' \
  '' \
  '.PHONY: stage-t051' \
  'stage-t051:' \
  $'\tbash ./scripts/stage-t051.sh' \
  '' \
  '.PHONY: commit-t051' \
  'commit-t051:' \
  $'\tbash ./scripts/commit-t051.sh' \
  '' \
  '.PHONY: push-t051' \
  'push-t051:' \
  $'\tbash ./scripts/push-t051.sh' \
  >> "$tmpdir/t051-makefile"

set +e
git diff --no-index --no-ext-diff "$tmpdir/base-makefile" "$tmpdir/t051-makefile" > "$tmpdir/raw-makefile.patch"
diff_status=$?
set -e
if [[ "$diff_status" -ne 1 ]]; then
  echo "failed to generate isolated Makefile patch" >&2
  exit 1
fi
sed \
  -e '1c diff --git a/Makefile b/Makefile' \
  -e 's#^--- .*#--- a/Makefile#' \
  -e 's#^+++ .*#+++ b/Makefile#' \
  "$tmpdir/raw-makefile.patch" > "$tmpdir/makefile.patch"
git apply --cached "$tmpdir/makefile.patch"

git diff --cached --check
git diff --cached --name-only
