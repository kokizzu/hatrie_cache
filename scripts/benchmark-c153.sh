#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology ./hat/hatCache -run '^$' -bench 'BenchmarkTopology(Store(Set|ApplyCommit)|Consensus)' -benchmem -count=5
