#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPeer -run '^$' -bench '^BenchmarkT241AuthorizationBaseline$' -benchmem -count=5
