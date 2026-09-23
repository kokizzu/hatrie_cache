#!/usr/bin/env bash
set -euo pipefail

for path in C240_READ_ONLY_BACKUP_ATTACHMENT.md BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md README.md; do
    test -s "$path"
done
rg -q 'C240_READ_ONLY_BACKUP_ATTACHMENT.md' README.md BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md
rg -q '\[x\] C240 Read-only backup database attachment' INSPIRATION_ROUND2.md
rg -q '^## C240 read-only backup attachment$' BENCHMARK.md
