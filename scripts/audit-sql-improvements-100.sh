#!/usr/bin/env sh
set -eu

printf '%s\n' '=== SQL package inventory ==='
find ./hat/hatSql -maxdepth 1 -type f -name '*.go' -printf '%f\n' | sort
printf '%s\n' '=== SQL feature terms ==='
grep -R -n -E 'TODO|FIXME|unsupported|not supported|not implemented|cannot' ./hat/hatSql --include='*.go' | head -n 400
