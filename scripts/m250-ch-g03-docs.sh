#!/usr/bin/env bash
set -euo pipefail

test -s CH003_PARALLEL_HASH_JOIN.md
rg -n 'CH-G03|JoinWorkers|CH003_PARALLEL_HASH_JOIN' CH003_PARALLEL_HASH_JOIN.md BENCHMARK.md README.md IDEA_GAP_CATALOG.md
