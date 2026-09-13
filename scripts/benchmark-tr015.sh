#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkPebblePropertiesBaseline$' -benchmem -count=5
