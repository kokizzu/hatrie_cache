#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

rg -n -A 110 '^type HatTrie struct' "$root/hat/hatCache/main.go"
printf '\n== existence lookup ==\n'
sed -n '4700,4785p' "$root/hat/hatCache/main.go"
