#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^BenchmarkT237ConnectionPoolBaseline$' -benchmem -count=5
