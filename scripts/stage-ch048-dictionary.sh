#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
  BENCHMARK.md
  ENGINE_IDEAS.md
  hat/hatSql/ch048_dictionary_predicate_test.go
  hat/hatSql/columnar_dictionary_predicate.go
  hat/hatSql/query.go
  scripts/benchmark-ch048-dictionary.sh
  scripts/format-ch048-dictionary.sh
  scripts/race-ch048-dictionary.sh
  scripts/stage-ch048-dictionary.sh
  scripts/commit-ch048-dictionary.sh
  scripts/push-ch048-dictionary.sh
  scripts/test-ch048-dictionary-package.sh
  scripts/test-ch048-dictionary.sh
  scripts/vet-ch048-dictionary.sh
)
git add -- "${feature_paths[@]}"

diff_file="$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-ch048-dictionary-stage.XXXXXX")"
selected_patch="$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-ch048-dictionary-patch.XXXXXX")"
trap 'rm -f "$diff_file" "$selected_patch"' EXIT
git diff -- Makefile > "$diff_file"
awk '
function flush_hunk() {
  if (hunk ~ /ch048-dictionary/) {
    if (!emitted) {
      printf "%s", header
      emitted = 1
    }
    printf "%s", hunk
  }
}
/^@@ / {
  flush_hunk()
  hunk = $0 "\n"
  next
}
hunk == "" {
  header = header $0 "\n"
  next
}
{
  hunk = hunk $0 "\n"
}
END {
  flush_hunk()
}
' "$diff_file" > "$selected_patch"
if [[ ! -s "$selected_patch" ]]; then
  echo "feature Makefile hunk not found" >&2
  exit 1
fi
git apply --cached "$selected_patch"
git diff --cached --check
git status --short
