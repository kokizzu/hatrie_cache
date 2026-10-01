#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'Benchmark(DirectReadOnlyCheck|ReadOnlyGate)' -benchmem -count=5
