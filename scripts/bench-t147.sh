#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'BenchmarkCommand(Error|Success)ResponseJSON$' -benchmem -count=3
