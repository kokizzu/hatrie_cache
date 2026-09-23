#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^$' -bench 'BenchmarkConflictPolicyResolution/(direct-lww|registry-default)$|BenchmarkT210ConflictHook' -benchmem -benchtime=500ms -count=3
