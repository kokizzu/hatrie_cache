#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkTU208ExistingExplicitWriteQuorum' -benchmem -count=5
