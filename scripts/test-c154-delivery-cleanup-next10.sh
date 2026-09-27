#!/usr/bin/env bash
set -euo pipefail

script="scripts/deliver-c154-replication-rollout-next10.sh"
bash -n "$script"
grep -Fqx "  trap cleanup_delivery_tmp_dir EXIT" <(awk '/trap .*tmp_dir|trap cleanup/ { print }' "$script")
grep -Fq "cleanup_delivery_tmp_dir()" "$script"
if grep -Fq "RETURN" "$script"; then
  printf 'delivery script still uses the non-exit cleanup trap\n' >&2
  exit 1
fi
printf 'delivery cleanup trap verified\n'
