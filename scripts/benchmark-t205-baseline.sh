#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^BenchmarkT205BaselineProgressArithmetic$' -benchmem -count=5 -tags t205baseline
