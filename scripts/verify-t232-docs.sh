#!/usr/bin/env bash
set -euo pipefail

for file in TT032_ATOMIC_TRANSACTION_SCOPES.md README.md BENCHMARK.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION_ROUND2.md; do
	test -s "$file"
done
rg -q 'SQLTransaction\.Scope' TT032_ATOMIC_TRANSACTION_SCOPES.md README.md
rg -q 'T032: Atomic Transaction Scopes' BENCHMARK.md
rg -q '\| T232 \| Tarantool \| atomic transaction scopes' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q '\[x\] T232 Atomic transaction scopes' INSPIRATION_ROUND2.md
printf '%s\n' 'T232 documentation references verified.'
