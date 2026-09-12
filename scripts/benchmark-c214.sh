#!/usr/bin/env bash
set -euo pipefail

baseline=/tmp/hatrie-cache-c214-baseline
cleanup() {
	git worktree remove --force "$baseline" >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
git worktree add --detach "$baseline" origin/master >/dev/null

benchmark='^BenchmarkSQLWASMFunctionBatch/(1000|10000|100000)$'
printf '%s\n' '== C214 baseline: origin/master =='
(cd "$baseline" && go test ./hat/hatCache -run '^$' -bench "$benchmark" -benchtime=1s -benchmem -count=5)

printf '%s\n' '== C214 current: bounded WASM runtime =='
go test ./hat/hatCache -run '^$' -bench "$benchmark" -benchtime=1s -benchmem -count=5

printf '%s\n' '== C214 opt-in timeout: interruptible WASM runtime =='
go test ./hat/hatCache -run '^$' -bench '^BenchmarkSQLWASMFunctionBatchWithTimeout$' -benchtime=1s -benchmem -count=5
