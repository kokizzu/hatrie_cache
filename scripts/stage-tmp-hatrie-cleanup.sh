#!/usr/bin/env bash
set -euo pipefail

git add \
  scripts/audit-tmp-hatrie-builds.sh \
  scripts/commit-tmp-hatrie-cleanup.sh \
  scripts/push-tmp-hatrie-cleanup.sh \
  scripts/stage-tmp-hatrie-cleanup.sh

tmp_dir=$(mktemp -d)
trap 'rm -rf -- "$tmp_dir"' EXIT

git show HEAD:Makefile > "$tmp_dir/head"
cp "$tmp_dir/head" "$tmp_dir/next"
cat >> "$tmp_dir/next" <<'EOF'

.PHONY: audit-tmp-hatrie-builds
audit-tmp-hatrie-builds:
	bash ./scripts/audit-tmp-hatrie-builds.sh preview

.PHONY: cleanup-tmp-hatrie-builds
cleanup-tmp-hatrie-builds:
	bash ./scripts/audit-tmp-hatrie-builds.sh clean

.PHONY: stage-tmp-hatrie-cleanup
stage-tmp-hatrie-cleanup:
	bash ./scripts/stage-tmp-hatrie-cleanup.sh

.PHONY: commit-tmp-hatrie-cleanup
commit-tmp-hatrie-cleanup:
	bash ./scripts/commit-tmp-hatrie-cleanup.sh

.PHONY: push-tmp-hatrie-cleanup
push-tmp-hatrie-cleanup:
	bash ./scripts/push-tmp-hatrie-cleanup.sh
EOF

set +e
git diff --no-index -- "$tmp_dir/head" "$tmp_dir/next" > "$tmp_dir/makefile.patch"
diff_status=$?
set -e
if ((diff_status > 1)); then
  printf 'failed to create Makefile patch (status %s)\n' "$diff_status" >&2
  exit "$diff_status"
fi

sed \
  -e 's#^diff --git .*#diff --git a/Makefile b/Makefile#' \
  -e 's#^--- .*#--- a/Makefile#' \
  -e 's#^+++ .*#+++ b/Makefile#' \
  "$tmp_dir/makefile.patch" > "$tmp_dir/makefile.normalized.patch"
git apply --cached --unidiff-zero "$tmp_dir/makefile.normalized.patch"

git diff --cached --check
git diff --cached --stat
