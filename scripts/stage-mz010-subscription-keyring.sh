#!/usr/bin/env bash
set -euo pipefail

git add \
  BENCHMARK.md \
  CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
  MZ010_SUBSCRIPTION_KEYRING.md \
  hat/hatSql/mz010_subscription_keyring.go \
  hat/hatSql/mz010_subscription_keyring_benchmark_test.go \
  hat/hatSql/mz010_subscription_keyring_test.go \
  scripts/benchmark-mz010-subscription-keyring.sh \
  scripts/commit-mz010-subscription-keyring.sh \
  scripts/format-mz010-subscription-keyring.sh \
  scripts/push-mz010-subscription-keyring.sh \
  scripts/race-mz010-subscription-keyring.sh \
  scripts/stage-mz010-subscription-keyring.sh \
  scripts/test-mz010-subscription-keyring.sh \
  scripts/vet-mz010-subscription-keyring.sh

tmp_dir=$(mktemp -d)
trap 'rm -rf -- "$tmp_dir"' EXIT

git show HEAD:Makefile > "$tmp_dir/head"
cp "$tmp_dir/head" "$tmp_dir/next"
cat >> "$tmp_dir/next" <<'EOF'

.PHONY: test-mz010-subscription-keyring
test-mz010-subscription-keyring:
	bash ./scripts/test-mz010-subscription-keyring.sh

.PHONY: benchmark-mz010-subscription-keyring
benchmark-mz010-subscription-keyring:
	bash ./scripts/benchmark-mz010-subscription-keyring.sh

.PHONY: format-mz010-subscription-keyring
format-mz010-subscription-keyring:
	bash ./scripts/format-mz010-subscription-keyring.sh

.PHONY: race-mz010-subscription-keyring
race-mz010-subscription-keyring:
	bash ./scripts/race-mz010-subscription-keyring.sh

.PHONY: vet-mz010-subscription-keyring
vet-mz010-subscription-keyring:
	bash ./scripts/vet-mz010-subscription-keyring.sh

.PHONY: stage-mz010-subscription-keyring
stage-mz010-subscription-keyring:
	bash ./scripts/stage-mz010-subscription-keyring.sh

.PHONY: commit-mz010-subscription-keyring
commit-mz010-subscription-keyring:
	bash ./scripts/commit-mz010-subscription-keyring.sh

.PHONY: push-mz010-subscription-keyring
push-mz010-subscription-keyring:
	bash ./scripts/push-mz010-subscription-keyring.sh
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
