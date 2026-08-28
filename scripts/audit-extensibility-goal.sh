#!/usr/bin/env sh
set -eu

grep -n -A 3 -B 3 'sql-extensions\|extensibility-goal' ./Makefile

printf '%s\n' '=== UDF and capabilities ==='
sed -n '1,280p' ./hat/hatSql/go_function.go
sed -n '1,220p' ./hat/hatSql/contracts.go
printf '%s\n' '=== plugin and virtual sources ==='
sed -n '1,260p' ./hat/hatSql/external.go
printf '%s\n' '=== CDC, idempotency, dedup, diff, harness ==='
grep -R -l -E 'CDC|ChangeLog|Idempot|Dedup|ContentHash|Diff|ContractTest' ./hat ./cmd --include='*.go' | sort
