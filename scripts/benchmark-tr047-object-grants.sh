#!/usr/bin/env bash
set -euo pipefail

go test -count=5 ./hat/hatAuth -run '^$' -bench '^BenchmarkPolicyAuthorize(Legacy|Object|ObjectSource)$' -benchmem
