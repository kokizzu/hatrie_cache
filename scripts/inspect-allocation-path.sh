#!/usr/bin/env sh
set -eu

printf '%s\n' '== native compaction admission =='
rg -n 'hattrie_memory_bytes|ahtable_memory_bytes|MemoryCompactionOptions|ErrMemoryCompactionBudgetExceeded|DefaultMemoryCompactionMaxTemporaryBytes' hat/hatCache api.go
printf '%s\n' '== server compaction settings =='
rg -n 'memoryCompactionInterval|memoryCompactionMaxTemporaryBytes|memory-compaction-' cmd/hatrie-cache/main.go
printf '%s\n' '== sanitizer strict-overcommit handling =='
rg -n 'strict overcommit|AddressSanitizer|15392894357504' scripts README.md
