#!/usr/bin/env bash
set -euo pipefail

BENCHTIME="${BENCHTIME:-1s}"
COUNT="${COUNT:-5}"

go test ./hat/hatDataStructure \
  -run '^$' \
  -bench 'BenchmarkTupleFieldOperationJournal(BaselineApply|AppendMemory|MarshalBinary|ReplayAndApply)$' \
  -benchtime="$BENCHTIME" \
  -benchmem \
  -count="$COUNT"

printf '%s\n' '--- durable append ---'
go test ./hat/hatDataStructure \
  -run '^$' \
  -bench 'BenchmarkTupleFieldOperationJournalAppendDurable$' \
  -benchtime=100ms \
  -benchmem \
  -count="$COUNT"
