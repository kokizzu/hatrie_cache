#!/bin/sh
set -eu

find . -maxdepth 3 -type f \( -name '*.ipynb' -o -name '*notebook*' \) -print
grep -n -E 'QueryRows|application/x-ndjson|ExportArrow|ExportParquet' hat/hatSql/*.go SQL.md
