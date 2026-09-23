#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkConflictPolicyResolution/(direct-lww|registry-default)$' -benchmem -count=5
