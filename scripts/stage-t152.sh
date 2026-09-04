#!/usr/bin/env bash
set -euo pipefail

files=(
  INSPIRATION.md
  hat/hatCache/replication_chaos_test.go
  scripts/commit-t152.sh
  scripts/format-t152.sh
  scripts/push-t152.sh
  scripts/review-t152.sh
  scripts/stage-t152.sh
  scripts/test-race-t152.sh
  scripts/test-t152.sh
)

staged=$(git diff --cached --name-only)
if [[ -n "$staged" ]]; then
  expected=$(printf '%s\n' Makefile "${files[@]}" | sort)
  if [[ "$staged" != "$expected" ]]; then
    echo "refusing to stage T152 with pre-existing staged changes" >&2
    git diff --cached --name-status >&2
    exit 1
  fi
  git reset -- Makefile "${files[@]}"
fi

tmp_dir=$(mktemp -d)
trap 'rm -rf "$tmp_dir"' EXIT

git show :Makefile > "$tmp_dir/Makefile.base"
cp "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.t152"
cat >> "$tmp_dir/Makefile.t152" <<'MAKEFILE'

.PHONY: test-t152
test-t152:
	bash ./scripts/test-t152.sh

.PHONY: format-t152
format-t152:
	bash ./scripts/format-t152.sh

.PHONY: test-race-t152
test-race-t152:
	bash ./scripts/test-race-t152.sh

.PHONY: review-t152
review-t152:
	bash ./scripts/review-t152.sh

.PHONY: stage-t152
stage-t152:
	bash ./scripts/stage-t152.sh

.PHONY: commit-t152
commit-t152:
	bash ./scripts/commit-t152.sh

.PHONY: push-t152
push-t152:
	bash ./scripts/push-t152.sh
MAKEFILE

set +e
git diff --no-index -- "$tmp_dir/Makefile.base" "$tmp_dir/Makefile.t152" > "$tmp_dir/Makefile.patch"
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
echo "staged T152 paths:"
git diff --cached --name-only
