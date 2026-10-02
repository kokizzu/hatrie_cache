#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure -run '^$' -bench '^(BenchmarkTU23BaselineMultiKeyUpsert|BenchmarkTU23MultiKeyIndexUpsert)$' -benchmem -count=5
