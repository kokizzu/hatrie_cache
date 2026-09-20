#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench '^Benchmark(ResolveConflictVersion|TU38ConflictResolutionWithIntrospection)$' -benchmem -count=5
