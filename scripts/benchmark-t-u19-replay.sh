#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTU19TupleUpdateJournalReplay$' -benchmem -count=5
