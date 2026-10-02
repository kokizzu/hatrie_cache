#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^(BenchmarkTU22BaselineSequentialTwoIndex(Upsert|ChangingKeys)|BenchmarkTU22CrossIndexUniqueSet(Upsert|ChangingKeys))$' -benchmem -count=5
