#!/usr/bin/env bash
set -euo pipefail

git fetch origin master >/dev/null
if ! git diff --quiet HEAD origin/master; then
	printf '%s\n' 'origin/master moved while C214 was being prepared; refusing to publish automatically' >&2
	exit 1
fi

git add \
  Makefile \
  README.md \
  INSPIRATION_ROUND2.md \
  C214_BOUNDED_WASM_UDF.md \
  hat/hatCache/sql_function.go \
  hat/hatCache/sql_wazero.go \
  hat/hatCache/sql_wasm_limits_test.go \
  scripts/benchmark-c214.sh \
  scripts/commit-c214.sh \
  scripts/format-c214.sh \
  scripts/race-c214.sh \
  scripts/test-c214-full.sh \
  scripts/test-c214-package.sh \
  scripts/test-c214-wasm-limits.sh \
  scripts/vet-c214.sh
git diff --cached --check
git commit -m 'feat(hatCache): bound WASM UDF resources'
git push origin HEAD:master
