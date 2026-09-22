#!/usr/bin/env bash
set -euo pipefail

test -s T215_PER_SPACE_STORAGE_POLICY.md
rg -q 'T215_PER_SPACE_STORAGE_POLICY.md' README.md
rg -q 'T215_PER_SPACE_STORAGE_POLICY.md' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '^## T215 Per-Space Memtx Versus On-Disk Storage Policy$' BENCHMARK.md
rg -q 'make benchmark-t215' T215_PER_SPACE_STORAGE_POLICY.md BENCHMARK.md
rg -q 'StorageSpaceMemtx' T215_PER_SPACE_STORAGE_POLICY.md
rg -q 'StorageSpaceOnDisk' T215_PER_SPACE_STORAGE_POLICY.md
