#!/usr/bin/env bash
set -euo pipefail

fixture=$(mktemp -d /tmp/hatrie-cleanup-test.XXXXXX)
trap 'rm -rf -- "$fixture"' EXIT

mkdir -p "$fixture/hatrie-stale" "$fixture/hatrie-recent"
touch -d '2 days ago' "$fixture/hatrie-stale"

plan_file="$fixture/plan"
HATRIE_TMP_ROOT="$fixture" \
HATRIE_TMP_PLAN_FILE="$plan_file" \
HATRIE_TMP_MIN_AGE_SECONDS=1 \
bash scripts/cleanup-hatrie-tmp-safe.sh plan

grep -Fqx "$fixture/hatrie-stale" "$plan_file"
if grep -Fqx "$fixture/hatrie-recent" "$plan_file"; then
  printf 'recent fixture unexpectedly scheduled for cleanup\n' >&2
  exit 1
fi

HATRIE_TMP_ROOT="$fixture" \
HATRIE_TMP_PLAN_FILE="$plan_file" \
HATRIE_TMP_MIN_AGE_SECONDS=1 \
bash scripts/cleanup-hatrie-tmp-safe.sh apply

[[ ! -e "$fixture/hatrie-stale" ]]
[[ -d "$fixture/hatrie-recent" ]]
printf 'hatrie temp cleanup test passed\n'
