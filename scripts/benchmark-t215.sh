#!/usr/bin/env bash
set -euo pipefail

go test -tags=t215 -run '^$' -bench='BenchmarkT215' -benchtime=3s -count=3 -benchmem ./hat/hatDataStructure
