#!/usr/bin/env sh
set -eu

printf '%s\n' '== atomic API =='
go test -run '^$' -bench '^BenchmarkCompareAndSwapString$' -benchmem -count=3 ./hat/hatCache
printf '%s\n' '== non-atomic GET then SET baseline =='
go test -run '^$' -bench '^BenchmarkGetThenSetString$' -benchmem -count=3 ./hat/hatCache
printf '%s\n' '== public command API =='
go test -run '^$' -bench '^BenchmarkExecuteCommandCASString$' -benchmem -count=3 ./hat/hatCache
printf '%s\n' '== public GET then SET baseline =='
go test -run '^$' -bench '^BenchmarkExecuteCommandGetThenSetString$' -benchmem -count=3 ./hat/hatCache
