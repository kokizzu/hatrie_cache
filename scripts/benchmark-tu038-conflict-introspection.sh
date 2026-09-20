#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkTU038Conflict' -benchmem -benchtime=2s -count=5
