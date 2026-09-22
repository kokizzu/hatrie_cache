#!/usr/bin/env bash
set -euo pipefail

printf '\n%s\n' 'Core transaction implementation:'
for file in hat/hatCache/sql_transaction.go hat/hatCache/sql_transaction_options.go hat/hatCache/atomic_command.go; do
	printf '\n--- %s ---\n' "$file"
	sed -n '1,430p' "$file"
done

printf '\n%s\n' 'Existing transaction tests around savepoints and atomic commands:'
sed -n '100,390p' hat/hatCache/sql_test.go
sed -n '1,180p' hat/hatCache/tr034_savepoint_benchmark_test.go
sed -n '1,180p' hat/hatCache/atomic_command_test.go

printf '\n%s\n' 'T232 backlog references:'
rg -n 'T232|t232|atomic transaction scopes|nested rollback' --glob '*.md' --glob '*.go' . 2>/dev/null || true

printf '\n%s\n' 'Documentation insertion points:'
sed -n '4950,4990p' README.md
sed -n '37260,37320p' BENCHMARK.md
sed -n '1360,1395p' ADOPTED_QUERY_ENGINE_IDEAS.md
sed -n '165,180p' INSPIRATION_ROUND2.md
printf '\n%s\n' 'T032 documentation blocks:'
rg -n -A 14 '^## T032|The scoped API was effectively|The scoped API' TT032_ATOMIC_TRANSACTION_SCOPES.md BENCHMARK.md
