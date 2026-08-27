#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

sed -n '20,120p' "$root/SQL.md"
printf '\n== mutation grammar ==\n'
sed -n '470,580p' "$root/SQL.md"
