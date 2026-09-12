#!/bin/sh
set -eu

test -f TT046_MEMORY_ACCOUNTING.md
rg -q 'TT046_MEMORY_ACCOUNTING.md' README.md
rg -q 'TT-046' ENGINE_IDEAS.md
rg -q 'TT-046 Per-Structure Memory Accounting' BENCHMARK.md
rg -q 'api/memory/structures' TT046_MEMORY_ACCOUNTING.md
rg -q 'hatrie_cache_structure_backing_bytes' TT046_MEMORY_ACCOUNTING.md
