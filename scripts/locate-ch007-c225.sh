#!/usr/bin/env bash
set -euo pipefail

rg -n -C 2 'CH-007|Row TTL' --glob '*.md'
printf '%s\n' '' 'CH007_ROW_TTL.md:'
sed -n '1,180p' CH007_ROW_TTL.md
printf '%s\n' '' 'BENCHMARK.md CH-007 section:'
sed -n '42,90p' BENCHMARK.md
