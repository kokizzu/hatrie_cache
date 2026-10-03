#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatDataStructure \
  -run '^$' \
  -bench '^BenchmarkT022UniqueIndexGroupUpsert$' \
  -benchmem \
  -count=5
