#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTU22BaselineSequentialUniqueIndexUpsert$' -benchmem -count=5 -timeout=120s
