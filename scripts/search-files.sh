#!/bin/sh
set -eu

if [ "$#" -eq 0 ] || [ -z "$1" ]; then
  printf '%s\n' 'usage: make search-files PATTERN="pattern" [FILES="path ..."]' >&2
  exit 2
fi

pattern=$1
shift
if [ "$#" -eq 0 ]; then
  rg -n "$pattern" .
else
  rg -n "$pattern" "$@"
fi
