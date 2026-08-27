#!/bin/sh
set -eu

printf '%s\n' 'Packages:'
go list -f '{{.ImportPath}}' ./... | sort

printf '%s\n' '' 'Top-level Go files:'
find . -maxdepth 1 -type f -name '*.go' -printf '%f\n' | sort

printf '%s\n' '' 'Recently added package directories:'
git log --diff-filter=A --name-only --format='' -n 40 -- '*/**.go' |
  awk -F/ 'NF > 1 { print $1 "/" $2 }' |
  sort -u
