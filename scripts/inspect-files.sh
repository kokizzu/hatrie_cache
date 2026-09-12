#!/bin/sh
set -eu

if [ "$#" -eq 0 ]; then
  printf '%s\n' 'usage: make inspect-files FILES="path ..."' >&2
  exit 2
fi

for path in "$@"; do
  if [ ! -f "$path" ]; then
    printf 'missing file: %s\n' "$path" >&2
    exit 1
  fi
  printf '\n===== %s =====\n' "$path"
  nl -ba "$path"
done
