#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql/operator_memory.go ./hat/hatSql/operator_memory_test.go -run 'TestCHG42OperatorMemoryTracker' -count=1
