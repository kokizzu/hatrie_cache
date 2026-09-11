#!/usr/bin/env bash
set -euo pipefail

rg -n -F -- 'PERSISTENT_STORAGE_DISK_RESERVE.md' README.md
rg -n -F -- 'TT-016' ENGINE_IDEAS.md
rg -n -F -- 'Disk-space reserve admission' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -n -F -- '## Persistent Storage Disk Reserve Admission' BENCHMARK.md
rg -n -F -- '7,011,204 ns' PERSISTENT_STORAGE_DISK_RESERVE.md
rg -n -F -- '1.03x slower' BENCHMARK.md
