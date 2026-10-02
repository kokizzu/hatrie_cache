#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkTU23BaselineMultiKeyUpsert$' -benchmem -count=5
