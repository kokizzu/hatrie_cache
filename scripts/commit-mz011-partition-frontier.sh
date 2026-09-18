#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet; then
  printf '%s\n' 'No staged MZ-11 partition frontier changes.' >&2
  exit 1
fi
git commit -m 'Add Kafka-style partition offset frontiers [skip ci]'
