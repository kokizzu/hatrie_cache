#!/usr/bin/env bash
set -euo pipefail

echo "Open checklist items:"
rg -n "^- \[ \] (C|M|T)" INSPIRATION.md
echo "Open audit items:"
rg -n "^- \[ \] (C|M|T)" CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md
