#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
rg -n -A 36 -B 8 'COUNT\(|aggregate|SUM\(|GROUP BY' "$root/SQL.md"
