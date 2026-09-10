#!/usr/bin/env bash
set -euo pipefail

output=${1:-CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md}
tmp=$(mktemp "${output}.tmp.XXXXXX")
trap 'rm -f "$tmp"' EXIT

awk '
BEGIN {
  print "# ClickHouse, Materialize, And Tarantool Audit"
  print ""
  print "This catalog keeps 50 canonical ideas from each source product and"
  print "copies their current repository status from `INSPIRATION.md`. It is an"
  print "implementation ledger, not a claim that every idea is desirable here."
  print ""
  print "Status uses the source catalog: `[x]` adopted or verified, `[ ]` open"
  print "or intentionally deferred, and `[-]` rejected or rolled back."
  print ""
}
/^## ClickHouse Ideas[[:space:]]*$/ {
  section = "ClickHouse"
  print "## ClickHouse (50 ideas)"
  next
}
/^## Materialize Ideas[[:space:]]*$/ {
  section = "Materialize"
  print "## Materialize (50 ideas)"
  next
}
/^## Tarantool Ideas[[:space:]]*$/ {
  section = "Tarantool"
  print "## Tarantool (50 ideas)"
  next
}
/^## / {
  section = ""
  next
}
/^- \[[x -]\] [CMT][0-9]+/ {
  if ($0 ~ /^- \[ \] [CMT][0-9]+/) {
    open[++openCount] = $0
  }
  if (section == "") {
    next
  }
  code = $3
  sub(/[a-z].*$/, "", code)
  key = section SUBSEP code
  if (!(key in seen) && count[section] < 50) {
    print
    seen[key] = 1
    count[section]++
  }
  next
}
END {
  if (count["ClickHouse"] != 50 || count["Materialize"] != 50 || count["Tarantool"] != 50) {
    printf "expected 50 ideas per product, got C=%d M=%d T=%d\n", count["ClickHouse"], count["Materialize"], count["Tarantool"] > "/dev/stderr"
    exit 1
  }
  print ""
  print "## Currently Open Or Deferred"
  print ""
  print "These are the unchecked items from the complete source catalog;"
  print "sub-items with the same numeric ID are retained where the catalog"
  print "records a separate implementation boundary."
  print ""
  for (entryIndex = 1; entryIndex <= openCount; entryIndex++) {
    print open[entryIndex]
  }
}
' INSPIRATION.md > "$tmp"

mv "$tmp" "$output"
trap - EXIT
