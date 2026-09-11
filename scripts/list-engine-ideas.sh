#!/usr/bin/env bash
set -euo pipefail

awk '
  /^## (ClickHouse|Materialize|Tarantool) candidates/ { section = $2; print "===== " section " ====="; next }
  /^\| (CH|MZ|TT)-[0-9][0-9][0-9] / { print }
' ENGINE_IDEAS.md
