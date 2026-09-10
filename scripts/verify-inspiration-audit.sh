#!/usr/bin/env bash
set -euo pipefail

file=${1:-CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md}
if [ ! -f "$file" ]; then
  printf 'missing generated audit: %s\n' "$file" >&2
  exit 1
fi

awk '
/^## ClickHouse \(50 ideas\)$/ {section = "C"; next}
/^## Materialize \(50 ideas\)$/ {section = "M"; next}
/^## Tarantool \(50 ideas\)$/ {section = "T"; next}
/^## / {section = ""; next}
/^- \[[x -]\] C[0-9]+/ && section == "C" {c++}
/^- \[[x -]\] M[0-9]+/ && section == "M" {m++}
/^- \[[x -]\] T[0-9]+/ && section == "T" {t++}
END {
  if (c != 50 || m != 50 || t != 50) {
    printf "audit counts C=%d M=%d T=%d, want 50 each\n", c, m, t > "/dev/stderr"
    exit 1
  }
  print "audit counts: C=50 M=50 T=50"
}
' "$file"

if ! rg -q '^## Currently Open Or Deferred$' "$file"; then
  printf 'missing open-items section in %s\n' "$file" >&2
  exit 1
fi

if ! rg -q '\[CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT\.md\]\(CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT\.md\)' README.md; then
  printf 'README does not link the inspiration audit\n' >&2
  exit 1
fi

if [ "$(awk 'NR == 1 {print; exit}' README.md)" != "# hatrie_cache" ]; then
  printf 'README title changed unexpectedly\n' >&2
  exit 1
fi

git diff --check
