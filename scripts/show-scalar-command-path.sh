#!/usr/bin/env sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)

printf '== command dispatch ==\n'
rg -n -A 120 'func \(.*HatTrie.*\) ExecuteCommand' "$root/hat/hatCache" --glob '*.go'

printf '\n== string mutation methods ==\n'
rg -n -A 80 'func \(.*HatTrie.*\) (UpsertString|SetString|UpsertCounter|SetCounter)' "$root/hat/hatCache/main.go"

printf '\n== locked key locations ==\n'
rg -n -A 70 'func \(.*HatTrie.*\) (freshLocationCheckedLocked|upsertReplacementLocation|peekLocked)' "$root/hat/hatCache/main.go"
