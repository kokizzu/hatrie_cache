#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkJournalWriteQuorumState$' -benchmem -count=7
