#!/usr/bin/env bash
set -euo pipefail
trap 'make cleanup-hatrie-tmp-after-test' EXIT
go test ./hat/hatDataStructure -run '^$' -bench '^BenchmarkT215StorageSpace' -benchmem -count=3
