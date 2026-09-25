#!/usr/bin/env bash
set -euo pipefail

git add -- \
  BENCHMARK.md \
  ENGINE_IDEAS.md \
  TT024_POSITIONAL_TEXT_INDEX.md \
  hat/hatCache/sql_text_phrase.go \
  hat/hatCache/sql_text_phrase_benchmark_test.go \
  hat/hatCache/sql_text_phrase_test.go \
  hat/hatSchema/text_index.go \
  hat/hatSchema/text_index_resolver.go \
  hat/hatSchema/tt024_text_proximity_index_test.go \
  hat/hatSql/contracts.go \
  hat/hatSql/query.go \
  hat/hatSql/text_phrase_test.go \
  hat/hatSql/text_proximity.go \
  scripts/benchmark-tt024-text-union.sh \
  scripts/commit-tt024-text-union.sh \
  scripts/format-tt024-text-union.sh \
  scripts/push-tt024-text-union.sh \
  scripts/race-tt024-text-union.sh \
  scripts/stage-tt024-text-union.sh \
  scripts/test-tt024-text-union-package.sh \
  scripts/test-tt024-text-union.sh \
  scripts/vet-tt024-text-union.sh

patch_file="$(mktemp /tmp/hatrie-cache-tt024-text-union-stage.XXXXXX)"
feature_patch="$(mktemp /tmp/hatrie-cache-tt024-text-union-feature.XXXXXX)"
cleanup() {
	rm -f "$patch_file" "$feature_patch"
}
trap cleanup EXIT

git diff -- Makefile > "$patch_file"
awk '
  /^diff --git / { print; next }
  /^index / || /^--- / || /^\+\+\+ / { print; next }
  /^@@ / {
    if (hunk_seen) {
      exit
    }
    hunk_seen = 1
    print
    next
  }
  hunk_seen { print }
' "$patch_file" > "$feature_patch"

git apply --cached "$feature_patch"
git diff --cached --check
git status --short
