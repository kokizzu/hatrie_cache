#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkM209(MonotoneLogicalTimestampAdvanceIfNewer|MonotoneLogicalTimestampAdvance|RawAtomicAdvance)$' -benchmem -benchtime=2s -count=5
go test ./hat/hatReplication -run '^$' -bench '^BenchmarkM209ChangefeedFrontierAdvance(Baseline)?$' -benchmem -benchtime=2s -count=5
go test ./hat/hatSql -run '^$' -bench '^BenchmarkM209SQLSourceFrontierObserve$' -benchmem -benchtime=2s -count=5
