#!/bin/sh
set -eu

if git diff --cached --quiet
then
  printf '%s\n' 'No staged MZ-23 changes found.' >&2
  exit 1
fi
git commit -m 'Add bounded sink delivery audit [skip ci]'
